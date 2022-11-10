% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

%% @author  Biokoda d.o.o.
%% @doc MySQL serveride protocol implementation using ranch_protocol for ActorDB<br/>
%%      Accepts connections from mysql drivers or console and responds to queries<br/>
%%      Supports complete SQLite syntax set with extensions<br/>
%%
%%  ```
%% MySQL Server Protocol implementation for ActorDB
%%
%%   Auth phase:
%%   1.  send_handshake         server --> client        (phase = handshake)
%%   2.  recv_handshake         client --> server        (phase = handshake)
%%   3.  send_ok                server --> client        (phase = handshake -> phase = command)
%%
%%   Command phase:
%%   1.  recv_command           client --> server
%%   2.  send(Packet)           server --> client
%%
%%  '''
%%  @end
-module(myactor_proto).

-behaviour(ranch_protocol).

-include("myactor.hrl").

-export([start_link/4]).
-export([init/4]).

%% To print complete trace with binary data uncomment this while developing
% -define(PRINT_TRACE,true).

-ifdef(PRINT_TRACE).
%% Defines for PRINT_TRACE, we send data to lager
-define(PROTO_DBG(A,B),lager:info(A,B)).
-define(PROTO_DBG(A),lager:info(A)).
-define(PROTO_WARN(A,B),lager:warning(A,B)).
-define(PROTO_WARN(A),lager:warning(A)).
-define(PROTO_NTC(A,B),lager:notice(A,B)).
-define(PROTO_NTC(A),lager:notice(A)).
-else.
%% We skip detailed logging when PRINT_TRACE is not defined
-define(PROTO_DBG(A,B),lager_dbg).
-define(PROTO_DBG(A),lager_dbg).
-define(PROTO_WARN(A,B),lager_warn).
-define(PROTO_WARN(A),lager_warn).
-define(PROTO_NTC(A,B),lager_notice).
-define(PROTO_NTC(A),lager_notice).
-endif.
-define(PROTO_ERR(A,B),lager:error(A,B)).
-define(PROTO_ERR(A),lager:error(A)).

start_link(Ref, Socket, Transport, Opts) ->
	?PROTO_DBG("got new connection",[]),
	Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
	{ok, Pid}.

%% @spec get_comm(#cst{}) -> {module(), socket()}
%% @doc  Retrieve transport and socket in a tuple from current connection state.
get_comm(Cst) when is_record(Cst,cst) ->
	{Cst#cst.transport, Cst#cst.socket}.

init(Ref, Socket, Transport, _Opts = []) ->
	?PROTO_DBG("init new connection ~p ~p ~p ~p ~p",[self(),Ref,Socket,Transport,_Opts]),
	ok = ranch:accept_ack(Ref),
	Transport:setopts(Socket,[{keepalive,true}]),
	Hash = myactor_util:genhash(),
	case application:get_env(actordb_core,client_inactivity_timeout) of
		{ok,RTimeout} when RTimeout > 0 ->
			ok;
		_ ->
			RTimeout = infinity
	end,
	Cst = #cst{recv_timeout = RTimeout, hash = Hash, phase = handshake, transport = Transport, socket = Socket},
	send_handshake(Cst,Hash), % after client connects to the server we send handshake response #
	loop(Socket, Transport, Cst).

%% @spec loop(port(),module(),#cst{}) -> loop()
%% @doc  Accepts data from client until socket closes and calls functions depending on hte #cst.phase
loop(Socket, Transport, State0) ->
	case Transport:recv(Socket, 0, State0#cst.recv_timeout) of
		{ok, Data} ->
			Buff0 = State0#cst.buf,
			State1 = State0#cst{buf = <<Buff0/binary, Data/binary>>},  % append to buffer until complete packet is combined
			case State1#cst.buf of
				<<PacketLength:24/little,SequenceId:8/big, ClientPayload/binary>> ->  % The sequence-id is incremented with each packet and may wrap around.
																					  % It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
					?PROTO_DBG("received ~p bytes, packet length = ~p bytes; sequenceid = ~p ; client payload size = ~p bytes => ~p (process: ~p)",
						[size(Data),PacketLength,SequenceId,size(ClientPayload),ClientPayload,self()]),
					case size(ClientPayload) of
						PacketLength ->
							?PROTO_DBG("packet received, processing on phase ~p",[State1#cst.phase]),
							State2 = State1#cst{sequenceid = SequenceId},
							case State2#cst.phase of
								handshake ->
									HsData = recv_handshake(ClientPayload),
									?PROTO_DBG("handshake data: ~p",[HsData]),
									StateHs = send_ok(State2),
									State3 = StateHs#cst{phase=command,
										username = butil:ds_val(username,HsData), password = butil:ds_val(password,HsData)};
								command ->
									State3 = recv_command(State2,ClientPayload);
								_Phase ->
									State3 = State2,
									?PROTO_ERR("unknown phase: ~p",[_Phase])
							end,
							loop(Socket,Transport,State3#cst{buf = <<>>});
						_ ->
							loop(Socket,Transport,State1)
					end;
				_ ->
					loop(Socket, Transport, State1)
			end,
			loop(Socket, Transport, State0);
		{error,closed} ->
			exit(normal);
		{error,_Err} ->
			exit(normal)
	end.

%% @spec create_packet(#cst{},binary()) -> iolist()
%% @doc Creates a MySQL packet from binary.
%%  ```
%%      3              payload length
%%      1              sequence id
%%      string[len]    payload
%%  '''
create_packet(Cst,Payload) when is_binary(Payload) ->
	create_packet(Cst,Payload,byte_size(Payload));
create_packet(Cst,Payload) ->
	create_packet(Cst,Payload,iolist_size(Payload)).

%% @spec create_packet(#cst{},binary()|iolist(),integer()) -> iolist()
%% @doc Creates a MySQL packet from list or binary where a precalculated PacketLength is used.<br/>
%%      Use this function directly only when payload size is/can be pre-calculated.
%%  ```
%%      3              payload length
%%      1              sequence id
%%      string[len]    payload
%%  '''
create_packet(#cst{sequenceid = SequenceId} = _Cst,Payload,PacketLength) ->
	?PROTO_DBG("create_packet | sending packet with packet length = ~p , sequenceid = ~p , payload = ~p",[PacketLength,SequenceId,Payload]),
	[<<PacketLength:24/little, SequenceId:8/big>>, Payload];
create_packet(SequenceId,Payload,PacketLength) ->
	[<<PacketLength:24/little, SequenceId:8/big>>, Payload].

%% @spec send_packet(#cst{},binary()|iolist()) -> send()
%% @doc  Creates MySQL packet from payload and sends it
send_packet(Cst,Payload) ->
	Packet = create_packet(Cst,Payload),
	send(Cst,Packet).

%% @spec send(#cst{},binary()|iolist()) -> #cst{}
%% @doc  Sends binary data via transport. <br/>
%%       If a backpressure sleep is set while executing last query<br/>
%%       this process goes to sleep and waits until it is resumed.<br/>
%%       Backpressure action is resetted everytime we send data out via transport method.
send(Cst,Bin) ->
	{Transport,Socket} = get_comm(Cst),
	?PROTO_DBG("send | packet ~p",[Bin]),
	Transport:send(Socket,Bin),
	BpAction = Cst#cst.bp_action,   % backpressure action
	case BpAction of    % handle backpressure action
		_ when BpAction#bp_action.action == sleep ->
			?PROTO_DBG("backpressure sleep after data sent : ~p",[BpAction#bp_action.state]),
			actordb:sleep_bp(BpAction#bp_action.state);
		_ ->
			ok
	end,
	case BpAction of
		undefined ->
			Cst;
		_ ->
			Cst#cst{bp_action=BpAction#bp_action{action=undefined}} % reset current state
	end.


%% @spec send_handshake(#cst{},binary()) -> send_packet()
%% @doc  Sends initial handshake via transport<br/>
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake">Link</a>
send_handshake(Cst,Hash) when is_record(Cst,cst) ->
	?PROTO_DBG("send_handshake | creating handshake with hash ~p",[Hash]),
	ServerSign = ?MYACTOR_VER,
	Id = 0,
	LenAuth = 21,
	Caps = 16#80000 bor %% PLAIN AUTH
		16#200 bor %% PROTOCOL 4.1
		16#8000 bor %% for mysql_native_password
		16#00002000 bor %% transactions
		16#00000008 bor %% schema name
		16#00008000 bor %% secure connection
		16#00010000 bor %% client multi statements
		16#00020000 bor %% client multi results
		16#00040000 bor %% multiple results in execute
		0,
	<<CapsLow:16/little, CapsUp:16/little>> = <<Caps:32/little>>,
	<<Auth1:8/binary, Auth2/binary>> = Hash,
	Charset = 33,
	StatusFlags = 16#0002 bor 0, % server status autocommit enabled
	HandshakePayload = <<16#0a, ServerSign/binary, 0:8, Id:32/little,
		Auth1/binary, 0:8, CapsLow:16/little,
		Charset:8, StatusFlags:16/little,
		CapsUp:16/little, LenAuth:8, 0:80,
		Auth2/binary, 0:8, "mysql_native_password", 0:8 >>,
	send_packet(Cst,HandshakePayload).

%% @spec recv_handshake(binary()) -> iolist()
%% @doc  Decodes initial handshake received from client and returns a property list<br/>
%%  ```
%%       [{username,binary()},{password,binary()},{charset,binary()},{dbname,binary()},{plugin_auth,binary()},{capabilities,caps_list()}]
%%  '''
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse">Link</a>
recv_handshake(Data) ->
	<<CapsFlags:32/little, _MaxPacketSize:32/little, Charset:8, _Reserved0:23/binary, Rest0/binary>> = Data,
	Caps = read_capabilities(CapsFlags),
	{Username,Rest1} = split_zero(Rest0),
	{Password,Rest2} = unpack_password(Rest1,Caps),
	{DbName,Rest3} = {undefined,Rest2} ,% read_capability(Rest2,Caps,?CAPABILITY_CONNECT_WITH_DB),
	{PluginAuth,_Rest4} = read_capability(Rest3,Caps,plugin_auth),
	case _Rest4 of
		<<>> ->
			ok;
		_ ->
			?PROTO_WARN("recv_handshake | client sent connect attrs, not handled yet")
	end,
	?PROTO_DBG("recv_handshake | client handshake, capabilities = ~p, max packet size = ~p, username = ~p, password = ~p, plugin_auth = ~p",
				[Caps,_MaxPacketSize,Username,Password,PluginAuth]),
	[{username,Username},{password,Password},{charset,Charset},{dbname,DbName},{plugin_auth,PluginAuth},{capabilities,Caps}].

%% @spec send_ok(#cst{}) -> send_packet()
%% @doc  Sends OK response to the client<br/>
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet">Link</a>
send_ok(Cst) ->
	AffectedRows = myactor_util:mysql_var_integer(0),
	LastInsertId = myactor_util:mysql_var_integer(0),
	send_ok(Cst,AffectedRows,LastInsertId).

%% @spec send_ok(#cst{},tuple()) -> send_packet()
%% @doc  Sends OK response to the client<br/>
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet">Link</a>
% send_ok(Cst,{rowid,LastInsertId}) ->
%     send_ok(Cst,myactor_util:mysql_var_integer(0),myactor_util:mysql_var_integer(LastInsertId));

%% @spec send_ok(#cst{},tuple()) -> send_packet()
%% @doc  Sends OK response to the client<br/>
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet">Link</a>
% send_ok(Cst,{affected_count,AffectedRows}) ->
%     send_ok(Cst,myactor_util:mysql_var_integer(AffectedRows),myactor_util:mysql_var_integer(0)).

%% @spec send_ok(#cst{},tuple(),tuple()) -> send_packet()
%% @doc  Sends OK response to the client<br/>
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet">Link</a>
send_ok(Cst,{affected_count,AffectedRows},{rowid,LastInsertId}) ->
	send_ok(Cst,myactor_util:mysql_var_integer(AffectedRows),myactor_util:mysql_var_integer(LastInsertId));

%% @spec send_ok(#cst{}, integer(), integer()) -> send_packet()
%% @doc  Sends OK response to the client<br/>
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet">Link</a>
send_ok(Cst,AffectedRows,LastInsertId) ->
	Cst0 = Cst#cst{sequenceid=Cst#cst.sequenceid+1},
	send_packet(Cst0,<<?OK_HEADER,AffectedRows/binary,LastInsertId/binary,16#02,16#00,16#00,16#00>>).

%% @spec send_err(#cst{},binary()) -> send_packet()
%% @doc  Sends error response to the client<br/>
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-ERR_Packet">Link</a>
send_err(Cst,ErrorDescription) ->
	Cst0 = Cst#cst{sequenceid=Cst#cst.sequenceid+1},
	% 16#23 = # sql state marker
	send_packet(Cst0,<<?ERR_HEADER,16#01,16#00,16#23,$H,$Y,$0,$0,$0,ErrorDescription/binary>>).

%% @spec recv_command(#cst{},binary()) -> #cst{}
%% @doc  Decodes command from client, prepares and sends a response and returns a new state #cst{}<br/>
%%       Implemented after: <a target="_blank" href="http://dev.mysql.com/doc/internals/en/text-protocol.html">Link</a><br/>
%%       Important: Not all commands are implemented.
recv_command(#cst{bp_action = undefined} = Cst1,Bin) ->
	BpState = actordb:start_bp(Cst1#cst.username,Cst1#cst.password,Cst1#cst.hash),
	recv_command(Cst1#cst{bp_action = #bp_action{state = BpState}},Bin);
recv_command(Cst,<<?COM_INIT_DB,_DbName/binary>>) ->
	send_ok(Cst);
recv_command(Cst,<<?COM_QUIT>>) ->
	{Transport,Socket} = get_comm(Cst),
	Transport:close(Socket),
	Cst;
recv_command(Cst,<<?COM_PING>>) ->
	send_ok(Cst);
recv_command(#cst{bp_action = #bp_action{state = BpState}} = Cst,<<?COM_QUERY,Query/binary>>) ->
	?PROTO_DBG("got query (~s)",[Query]),
	Q0 = myactor_util:rem_spaces(Query),
	HasActorCmd = myactor_util:is_actor(Q0),
	case catch actordb_sqlparse:parse_statements(BpState,Query) of
		[] ->
			send_ok(Cst);
		["show connection state"++_] ->
			?PROTO_DBG("showing connection state: ~p",[Cst]),
			Flags = [ [butil:tobin(Flag)," "] ||  Flag <- Cst#cst.current_actor_flags],
			CstBpState = io_lib:format("~p",[Cst#cst.bp_action]),
			multirow_response(Cst,  {<<"param">>,<<"value">>},
									[
									{<<"socket">>,erlang:port_to_list(Cst#cst.socket)},
									{<<"transport">>,butil:tobin(Cst#cst.transport)},
									{<<"phase">>,butil:tobin(Cst#cst.phase)},
									{<<"buf">>,butil:tobin(Cst#cst.buf)},
									{<<"hash">>,<<"">>},
									{<<"sequenceid">>,butil:tobin(Cst#cst.sequenceid)},
									{<<"capabilities">>,butil:tobin(Cst#cst.capabilities)},
									{<<"current_actor">>,butil:tobin(Cst#cst.current_actor)},
									{<<"current_actor_flags">>,iolist_to_binary(Flags)},
									{<<"bp_action">>,butil:tobin(CstBpState)},
									{<<"queueing">>,butil:tobin(Cst#cst.queueing)},
									{<<"query_queue">>,butil:tobin(Cst#cst.query_queue)}],text);
		["select @@version_comment"++_] ->
			?PROTO_DBG("got version comment query"),
			multirow_response(Cst,  {<<"@@version_comment">>},
									[{?MYACTOR_VER}],text);
		["select timediff( curtime(), utc_time() )"++_] ->
			?PROTO_DBG("got timediff query"),
			multirow_response(Cst,  {<<"timediff( curtime(), utc_time() )">>},
									[{<<"01:00:00">>}],text);
		["select database()"++_] ->
			?PROTO_DBG("got select database() query"),
			send_ok(Cst);
		["set "++_] ->  % actordb does not support set queries for now
			?PROTO_DBG("set names() query"),
			send_ok(Cst);
		["commit"++_] ->
			?PROTO_DBG("commit query"),
			send_ok(Cst);
		["begin"++_] ->
			?PROTO_DBG("begin query"),
			send_ok(Cst);
		["savepoint"++_] ->
			?PROTO_DBG("savepoint query"),
			send_ok(Cst);
		["rollback"++_] ->
			?PROTO_DBG("rollback query"),
			send_ok(Cst);
		["release"++_] ->
			?PROTO_DBG("release query"),
			send_ok(Cst);
		["show databases"++_] ->
			?PROTO_DBG("got show dbs query"),
			send_ok(Cst);
		["show full tables"++_] ->
			?PROTO_DBG("got show full tables query"),
			multirow_response(Cst,{<<"tables_in_db">>},[],text);
		["select @@session."++Rest] ->
			?PROTO_DBG("got session select variable"),
			{Cols,Rows} = myactor_static:session_variable(Rest),
			multirow_response(Cst,Cols,Rows,text);
		["show collation"++_] ->
			?PROTO_DBG("got show collation query"),
			multirow_response(Cst,{<<"collation">>,<<"charset">>,{<<"id">>,t_longlong},<<"default">>,<<"compiled">>,<<"sortlen">>},
									myactor_static:show_collation(),text);
		["show variables"++_] ->    % for java driver
			?PROTO_DBG("got show variables query"),
			%send_ok(Cst);
			multirow_response(Cst,  {<<"variable_name">>,<<"value">>},
									myactor_static:show_variables(),text);
		["queue"++_]  when Cst#cst.queueing == true ->
			?ERR_DESC(Cst,queuing_already_active), % = ErrDesc
			send_err(Cst,<<ErrDesc/binary>>);
		["exec queue"++_] when Cst#cst.queueing == false ->
			?ERR_DESC(Cst,no_queue), % = ErrDesc
			send_err(Cst,<<ErrDesc/binary>>);
		["clear queue"++_] ->
			?PROTO_DBG("clearing queue statements @ ~p",[self()]),
			Cst0 = Cst#cst{queueing = false, query_queue = <<>>},
			send_ok(Cst0);
		["queue"++_]  when Cst#cst.queueing == false ->
			?PROTO_DBG("queueing statements @ ~p",[self()]),
			Cst0 = Cst#cst{queueing = true},
			send_ok(Cst0);
		["exec queue"++_] when Cst#cst.queueing == true ->
			?PROTO_DBG("executing queued statements @ ~p ~p",[self(),Cst]),
			QQ = Cst#cst.query_queue,
			Stmts0 = actordb_sqlparse:parse_statements(QQ),
			?PROTO_DBG("query queue statement = ~p",[QQ]),
			?PROTO_DBG("queue command = ~p",[Stmts0]),
			Cst0 = Cst#cst{query_queue = <<>>, queueing = false},
			execute_query(Cst0,Stmts0,QQ,[],text);
		{[{{Actor,ActorIds,Flags},false,[]}],false} -> % parsed "actor <actor>(ids)" statement
			?PROTO_DBG("actor statement for ~p (~p) with flags ~p",[Actor,ActorIds,Flags]),
			ActorIdsBin = myactor_util:build_idsbin(ActorIds),
			DbName = <<Actor/binary,$(,ActorIdsBin/binary,$)>>,
			case Cst#cst.queueing == true of
				true ->
					% we add use statement to queue and leave current actor intact while query queue is in progress
					ActorQCmd = <<"actor ">>,
					Cst0 = queue_append(Cst,<<ActorQCmd/binary,DbName/binary>>),
					send_ok(Cst0);
				false ->
					send_ok(Cst#cst{current_actor=butil:tolist(DbName),current_actor_flags=Flags})
			end;
		_Stmts when Cst#cst.queueing == true ->
			?PROTO_DBG("queueing statement ~p",[Q0]),
			?PROTO_DBG("queue = ~p",[Cst#cst.query_queue]),
			case Cst#cst.query_queue of
				<<>> ->
					case HasActorCmd of
						true ->
							Cst0 = queue_append(Cst,Q0),
							send_ok(Cst0);
						_ ->
							?ERR_DESC(Cst,"First statement in a QUEUE has to be ACTOR to define an Actor."), % = ErrDesc
							send_err(Cst,<<ErrDesc/binary>>)
					end;
				_ ->
					Cst0 = queue_append(Cst,Q0),
					send_ok(Cst0)
			end;
		{error,Err} ->
			?ERR_DESC(Cst,{error,Err}),
			send_err(Cst,<<ErrDesc/binary>>);
		[[{columns,_C},{rows,_}] = Stmts|_] ->
			BpState = (Cst#cst.bp_action)#bp_action.state,
			execute_query_result(Cst,BpState,Query,text,{ok,{ok,Stmts}});
		Stmts ->
			?PROTO_DBG("stmts term = ~p",[Stmts]),
			case HasActorCmd of
				true ->
					Stmts0 = Stmts;
				false when Cst#cst.current_actor =/= undefined ->
					case lists:member(create,Cst#cst.current_actor_flags) of
						true ->
							Flags = " create";
						_ ->
							Flags = ""
					end,
					ActorQ = iolist_to_binary(["actor ",Cst#cst.current_actor,Flags,";"]),
					Stmts0 = actordb_sqlparse:parse_statements(<<ActorQ/binary,Query/binary>>);
				false when Stmts == ["select @@max_allowed_packet"] ->
					Stmts0 = [[{columns,{<<"max_allowed_packet">>}},{rows,[{1024*1024*16}]}]];
				false ->
					Stmts0 = {error,no_actor_defined}
			end,
			case Stmts0 of
				{error,no_actor_defined} ->
					?ERR_DESC(Cst,{error,no_actor_defined}), % = ErrDesc
					send_err(Cst,<<ErrDesc/binary>>);
				[[{columns,_}|_] = Stmts|_] ->
					BpState = (Cst#cst.bp_action)#bp_action.state,
					execute_query_result(Cst,BpState,Query,text,{ok,{ok,Stmts}});
				_ ->
					?PROTO_DBG("stmts0 query = ~p",[Stmts0]),
					execute_query(Cst,Stmts0,Query,[],text)
			end

	end;
recv_command(#cst{bp_action = #bp_action{state = Bp}} = Cst,<<?COM_STMT_PREPARE,Query/binary>>) ->
	case actordb_sqlparse:parse_statements(Query) of
		{[{{Type,_,_},_IsWrite,[Sql|_]}],_} = Parsed ->
			case (catch actordb_dummy:prepare(Type,Sql)) of
				{ok,NParam,NCols} ->
					case actordb_backpressure:getval(Bp,prepnum) of
						undefined ->
							PrepIndex = 0;
						PrepIndex1 ->
							PrepIndex = PrepIndex1+1
					end,
					actordb_backpressure:save(Bp,prepnum,PrepIndex),
					actordb_backpressure:save(Bp,{prep,PrepIndex},{Parsed,NParam,NCols,Query}),
					Resp = [<<0,PrepIndex:32/little,NCols:16/little, NParam:16/little,0,0:16/little>>],
					Seq = Cst#cst.sequenceid+1,
					send(Cst,create_packet(Seq,Resp)),
					case NParam > 0 of
						true ->
							TupleParamCols = erlang:make_tuple(NParam,<<"nmparam">>),
							ParamTypes = #coltypes{defined = false, cols = erlang:make_tuple(NParam,get_type(<<>>))},
							{_,ColDefPack1} = multirow_columndefs(Cst#cst{sequenceid = Seq},TupleParamCols,ParamTypes),
							send(Cst,ColDefPack1),
							send(Cst,create_packet(Seq+NParam+1,<<?EOF_HEADER,0:16/little,16#0022:16/little>>)),
							Seq1 = Seq+NParam+1;
						false ->
							Seq1 = Seq
					end,

					case NCols > 0 of
						true ->
							TupleColCols = erlang:make_tuple(NCols,<<"nmcol">>),
							ColTypes = #coltypes{defined = false, cols = erlang:make_tuple(NCols,get_type(<<>>))},
							{_,ColDefPack2} = multirow_columndefs(Cst#cst{sequenceid = Seq1},TupleColCols,ColTypes),
							send(Cst,ColDefPack2),
							send(Cst,create_packet(Seq1+NCols+1,<<?EOF_HEADER,0:16/little,16#0022:16/little>>));
						_ ->
							Cst
					end;
				_ ->
					?ERR_DESC(Cst,{error,invalid_query}),
					send_err(Cst,<<ErrDesc/binary>>)
			end;
		_ ->
			case myactor_util:is_actor(myactor_util:rem_spaces(Query)) of
				false ->
					?ERR_DESC(Cst,{error,actor_statement_missing}),
					send_err(Cst,<<ErrDesc/binary>>);
				_ ->
					?ERR_DESC(Cst,{error,invalid_query}),
					send_err(Cst,<<ErrDesc/binary>>)
			end
	end;
recv_command(#cst{bp_action = #bp_action{state = Bp}} = Cst,
		<<?COM_STMT_EXECUTE,Id:32/little,_Flags,1:32/little,BinRem/binary>>) ->
	case actordb_backpressure:getval(Bp,{prep,Id}) of
		undefined ->
			Query = <<>>,
			?ERR_DESC(Cst,{error,unknown_id}),
			send_err(Cst,<<ErrDesc/binary>>);
		{Parsed,NParam,_NCols,Sql} ->
			BitmapSize = (NParam + 7) div 8,
			TypesSize = NParam*2,
			case BinRem of
				<<_Bitmap:BitmapSize/binary,_NewParamBound,Types:TypesSize/binary,Values/binary>> ->
					Vals = myactor_util:exec_vals(Types,Values),
					execute_query(Cst,Parsed,Sql,[[Vals]],binary);
				<<>> ->
					execute_query(Cst,Parsed,Sql,[],binary);
				_ ->
					Query = <<>>,
					?ERR_DESC(Cst,{error,parse_error}),
					send_err(Cst,<<ErrDesc/binary>>)
			end
	end;
recv_command(Cst,_Comm) ->
	?PROTO_ERR("unknown command received, responding with ok (~p)",[_Comm]),
	send_ok(Cst).

%% @spec queue_append(#cst{},binary()) -> #cst{}
%% @doc  Appends query data to query queue and returns a new state.
queue_append(Cst,Query) ->
	?PROTO_DBG("cst= ~p query=~p",[Cst,Query]),
	Queue = Cst#cst.query_queue,
	Delimiter = <<";">>,
	case Queue of
		<<>> ->
			Cst#cst{query_queue = Query};
		_ ->
			Cst#cst{query_queue = <<Queue/binary,Delimiter/binary,Query/binary>>}
	end.

%% @spec execute_query(#cst{},parsed_statements(),binary()) -> #cst{}
%% @doc  Executes a query and sends response to client. <br/>
%%       1. Sets backpressure state if needed <br/>
%%       2. Executes the query<br/>
%%       3. Sends the response to the socket where backpressure is handled<br/>
%%
execute_query(Cst,Stmts0,Query,BindVals,Protocol) ->
	BpState = (Cst#cst.bp_action)#bp_action.state,
	Res = (catch actordb:exec_bp1(BpState,byte_size(Query),Stmts0,BindVals)),
	execute_query_result(Cst,BpState,Query,Protocol,Res).

execute_query_result(Cst,_BpState,Query,_Protocol,{'EXIT',Err}) ->
	?ERR_DESC(Cst,Err), % = ErrDesc
	send_err(Cst,<<ErrDesc/binary>>);
execute_query_result(Cst,BpState,Query,Protocol,{Rs,Result}) ->
	?PROTO_DBG("actor responded with ~p",[Result]),
	% sleep
	case Rs of
		sleep ->
			?PROTO_DBG("exec_bp1 status = ~p, sleeping actor with state ~p",[Rs,BpState]),
			Cst0 = Cst#cst{bp_action=#bp_action{action=sleep,state=BpState}};
		_ ->
			?PROTO_DBG("exec_bp1 status = ~p",[Rs]),
			Cst0 = Cst#cst{bp_action=#bp_action{action=undefined,state=BpState}},
			%Cst0 = Cst,
			ok
	end,
	?PROTO_DBG("result = ~p",[Result]),
	case Result of
		ok ->   % update queries
			send_ok(Cst0);
		{ok,{changes,LastInsertId,NumChanges}} -> % insert queries
			send_ok(Cst0,{affected_count,NumChanges},{rowid,LastInsertId});
		{ok,[{columns,Cols},{rows,Rows}]} ->    % data queries
			multirow_response(Cst0,Cols,Rows,Protocol);
		{ok,[{changes,_,_}|_] = MultiResponse} ->
			case lists:last(MultiResponse) of
				{changes,LastInsertId,NumChanges} ->
					send_ok(Cst0,{affected_count,NumChanges},{rowid,LastInsertId});
				[{columns,Cols},{rows,Rows}] ->
					multirow_response(Cst0,Cols,Rows,Protocol);
				_ ->
					send_ok(Cst0)
			end;
		{ok,[[{columns,_},{rows,_}]|_] = MultiResponse } ->    % data queries
			case lists:last(MultiResponse) of
				{changes,LastInsertId,NumChanges} ->
					send_ok(Cst0,{affected_count,NumChanges},{rowid,LastInsertId});
				[{columns,Cols},{rows,Rows}] ->
					multirow_response(Cst0,Cols,Rows,Protocol);
				_ ->
					send_ok(Cst0)
			end;
		{error,Str} ->
			send_err(Cst0,<<(butil:tobin(Str))/binary>>);
		{sql_error,SqlErr} ->
			?ERR_DESC(Cst0,SqlErr), % = ErrDesc
			send_err(Cst0,<<ErrDesc/binary>>);
		{sql_error,SqlErr,ErrQuery} ->
			?ERR_DESC(Cst0,{SqlErr,{err_query,ErrQuery}}),  % = ErrDesc
			send_err(Cst0,<<ErrDesc/binary>>);
		_Oth ->
			?ERR_DESC(Cst0,{unknown_query,_Oth}),
			send_err(Cst0,<<ErrDesc/binary>>)
	end;
execute_query_result(Cst,_BpState,Query,_Protocol,Error) ->
	?ERR_DESC(Cst,Error),
	send_err(Cst,<<ErrDesc/binary>>).

%% @spec multirow_response(#cst{},term(),term()) -> #cst{}
%% @doc  Builds a multirow response from actordb query response and sends it to socket
multirow_response(Cst,Cols,Rows,Prot) ->
	?PROTO_NTC("multirow response:~nstate:~p~ncolumns:~p~nrows:~p~n",[Cst,Cols,Rows]),
	NumCols = size(Cols),
	NumColsLenEnc = myactor_util:mysql_var_integer(NumCols),
	% packet with number of columndefinitions
	Cst0 = Cst#cst{sequenceid=1}, % field num packet sequence id
	FieldNumPack = create_packet(Cst0,NumColsLenEnc),       % length = 1, packet num = 1, number of columns = ColumnCount (length encoded integer)
	% column definitions packets
	CstDf0 = multirow_columndefs_prep(Cst0,Cols),   % we only set new "current state" but keep the old one
													% so we can build the header once the types are known in a later phase
	% eof marker before row responses
	Warnings = 0,
	ServerStatus = 16#0022,
	Cst1 = CstDf0#cst{sequenceid=CstDf0#cst.sequenceid+1},  % eof marker sequence id
	EofMarker = create_packet(Cst1,<<?EOF_HEADER,Warnings:16/little,ServerStatus:16/little>>),
	ResultSetSize = length(Rows),
	?PROTO_DBG("creating row data; size = ~p",[ResultSetSize]),
	{ColTypes,ResultSetPack} = multirow_encoderows(Cst1,Rows,Prot),
	?PROTO_DBG("coltypes = ~p",[ColTypes]),
	?PROTO_DBG("resulset pack = ~p",[ResultSetPack]),
	Cst2 = Cst1#cst{sequenceid=Cst1#cst.sequenceid+ResultSetSize+1},    % eof marker 2 sequence id
	EofMarker2 = create_packet(Cst2,<<?EOF_HEADER,Warnings:16/little,ServerStatus:16/little>>),
	% create row responses:
	{_,ColDefPack} = multirow_columndefs(Cst0,Cols,ColTypes),
	?PROTO_DBG("multirow_response | field num packet = ~p",[FieldNumPack]),
	?PROTO_DBG("multirow_response | column definition packets = ~p",[ColDefPack]),
	?PROTO_DBG("multirow_response | eof marker = ~p",[EofMarker]),
	?PROTO_DBG("multirow_response | row data = ~p",[ResultSetPack]),
	?PROTO_DBG("multirow_response | eof marker #2 = ~p",[EofMarker2]),
	%BinOut = iolist_to_binary([FieldNumPack,ColDefPack,EofMarker,ResultSetPack,EofMarker2]),
	BinOut = [FieldNumPack,ColDefPack,EofMarker,ResultSetPack,EofMarker2],
	?PROTO_DBG("multirow_response | binary out = ~p",[BinOut]),
	%send_ok(Cst).
	send(Cst,BinOut).


%% @spec multirow_columndefs_prep(#cst{},term()) -> #cst{}
%% @doc  Calculate a new after "column-definitions" state. We need this since we detect types while we build the request.<br/>
%%       This way we ensure that sequenceid's of the packets following the column definition packet are correct since
%%       we create the column definition packet before we are sending data to the socket.
multirow_columndefs_prep(Cst,Cols) ->
	Cst#cst{sequenceid=Cst#cst.sequenceid+size(Cols)}.

%% @spec multirow_columndefs(#cst{},term(),#coltypes{}) -> {#cst{},iolist()}
%% @doc  Calculate a new after "column-definitions" state. We need this since we detect types while we build the request.<br/>
multirow_columndefs(Cst,Cols,ColTypes) ->
	multirow_columndefs0(Cst,Cols,ColTypes,1,size(Cols),<<>>).

%% @spec multirow_columndefs0(#cst{},term(),#coltypes{},integer(),integer(),iolist()) -> {#cst{},iolist()}
%% @doc  Calculate a new after "column-definitions" state. We need this since we detect types while we build the request.<br/>
multirow_columndefs0(CstN,_,_,ColId,NumCols,Bin) when ColId == (NumCols+1) ->
	{CstN,Bin};
multirow_columndefs0(Cst,Cols,undefined,ColId,NumCols,Bin) ->
	ColTypes = #coltypes{cols=erlang:make_tuple(size(Cols),get_type(undefined))},
	multirow_columndefs0(Cst,Cols,ColTypes,ColId,NumCols,Bin);
multirow_columndefs0(Cst,Cols,ColTypes,ColId,NumCols,Bin) ->
	Cst0 = Cst#cst{sequenceid=Cst#cst.sequenceid+1},
	ColDef = element(ColId,Cols),
	case ColDef of
		{ColName,ColType} ->
			ok;
		ColName ->
			ColType = element(ColId,ColTypes#coltypes.cols)
	end,
	ColType0 = map_coltype(ColType),
	?PROTO_DBG("building column ~p with type (~p)~p",[ColName,ColType,ColType0]),
	% ?PROTO_NTC("preparing column ~p ~p",[ColId,ColName]),
	Def = myactor_util:binary_to_varchar(<<"def">>),
	Schema = myactor_util:binary_to_varchar(<<"actordb">>),
	Table = myactor_util:binary_to_varchar(<<"table_name">>),
	OriginalTable = myactor_util:binary_to_varchar(<<"original_table_name">>),
	Name = myactor_util:binary_to_varchar(ColName),
	OriginalName = myactor_util:binary_to_varchar(<<"original_name">>),
	Charset = ?CHARSET_UTF8,
	Length = 192,
	%Type = ?T_STRING,
	Type = ColType0,
	Flags = 0,
	Decimals = 0,
	% column definition data
	ColBin = create_packet(Cst0,
		[Def, Schema,Table,	OriginalTable, Name, OriginalName,
		<<16#0c, Charset:16/little,Length:32/little,Type:8, Flags:16/little,
		Decimals:8/little,0:16/little>>]),
	multirow_columndefs0(Cst0,Cols,ColTypes,ColId+1,NumCols,[Bin,ColBin]).

%% @spec multirow_encoderows(#cst{},list()) -> {#cst{},iolist()}
%% @doc  Utility funciton. Encode multirow response into binary data. While encoding we detect types that are used to build correct column definitions.
multirow_encoderows(Cst,Rows,Prot) ->
	Cst0 = Cst#cst{sequenceid=Cst#cst.sequenceid+length(Rows)}, % we need to go in reverse order since ActorDB gives us data in that day
	multirow_encoderows(Cst0,Rows,Prot,[],undefined).
%% @spec multirow_encoderows(#cst{},list(),io_list(),#coltypes{}) -> {#cst{},iolist()}
%% @doc  Encode multirow response into binary data. While encoding we detect types that are used to build correct column definitions.
multirow_encoderows(_,[],_,Bin,ColTypes) ->   % ColTypes = we need to detect column types
	{ColTypes,Bin};
multirow_encoderows(Cst,[Row|Rest],Prot,Bin,ColTypes) ->
	case ColTypes of
		undefined ->
			ColTypes0 = #coltypes{defined = false, cols = erlang:make_tuple(size(Row),get_type(undefined))};
		_ when ColTypes#coltypes.defined == false ->
			?PROTO_DBG("defining column types... "),
			ColTypes0 = ColTypes;
		_ ->
			?PROTO_DBG("all column types are now defined. "),
			ColTypes0 = ColTypes
	end,
	{ColTypes1,RowPacket} = multirow_encoderow(Cst,Row,Prot,ColTypes0),
	Cst0 = Cst#cst{sequenceid=Cst#cst.sequenceid-1},    % again, reverse order
	multirow_encoderows(Cst0,Rest,Prot,[RowPacket|Bin],ColTypes1).

%% @spec multirow_encoderow(#cst{},term(),#coltypes{}) -> {#coltypes{},iolist()}
%% @doc  Encode a single row and check for types while encoding.
multirow_encoderow(Cst,Row,Prot,ColTypes) ->
	{ColTypes0,RowBin,BinSize} = multirow_encoderow0(Row,Prot,ColTypes),
	{ColTypes0,create_packet(Cst,RowBin,BinSize)}.

%% @spec multirow_encoderow0(term(),#coltypes{}) -> {#coltypes{},iolist(),integer()}
%% @doc  Utility funciton. Encode a single row and check for types while encoding. We precalculate the size of the row for faster package creation.
multirow_encoderow0(Row,text,ColTypes) ->
	RowLength = size(Row),
	multirow_encoderow0(Row,RowLength,RowLength,[],0,ColTypes);
multirow_encoderow0(Row,binary,ColTypes) ->
	BM = myactor_util:null_bitmap(Row),
	row_vals_t(ColTypes,Row,1,byte_size(BM)+1,[<<0>>,BM]).

row_vals_t(CT,T,Pos,Size,Out) when tuple_size(T) >= Pos ->
	Bin = myactor_util:enc_val(element(Pos,T)),
	row_vals_t(set_type(CT,Pos,element(Pos,T)),T,Pos+1,Size+iolist_size(Bin),[Out,Bin]);
row_vals_t(CT,_,_,Size,Out) ->
	{CT,Out,Size}.

%% @spec multirow_encoderow0(term(),integer(),integer(),io_list(),integer(),#coltypes{}) -> {#coltypes{},iolist(),integer()}
%% @doc  Encode a single row and check for types while encoding. We precalculate the size of the row for faster package creation.
multirow_encoderow0(_,_,DataIdx,Bin,BinSize,ColTypes) when DataIdx < 1 ->   % +1 to capture the last rowdata
	{ColTypes,Bin,BinSize};
multirow_encoderow0(Row,RowLength,DataIdx,Bin,BinSize,ColTypes) ->
	Val = element(DataIdx,Row),
	?PROTO_DBG("detecting type | value: ~p | type: ~p",[Val,get_type(Val)]),
	case Val of
		null ->
			ColTypes0 = ColTypes,
			BinPacket = ?NULL;
		undefined ->
			ColTypes0 = ColTypes,
			BinPacket = ?NULL;
		{blob,Binary} ->
			ColTypes0 = set_type(ColTypes,DataIdx,Val),
			BinPacket = myactor_util:binary_to_varchar(Binary);
		_ ->
			ColTypes0 = set_type(ColTypes,DataIdx,Val),
			BinData = butil:tobin(Val),
			BinPacket = myactor_util:binary_to_varchar(BinData)
	end,
	multirow_encoderow0(Row,RowLength,DataIdx-1,[BinPacket|Bin],BinSize+iolist_size(BinPacket),ColTypes0).

%% @spec set_type(#coltypes{},integer(),binary()|integer()|float()) -> #coltypes{}
%% @doc  Sets a type for a column where a value is defined. If a value is unknown we skip setting this column's type.
%% When all types are known we skip further type setting.
% set_type(#coltypes{defined=true} = ColTypes ,_,_) ->
% 	?PROTO_DBG("notice skippping, all is known"),
% 	ColTypes;
set_type(ColTypes,_,undefined)  ->
	ColTypes;
set_type(ColTypes,_,null) ->
	ColTypes;
set_type(ColTypes,Index,Value) ->
	VType = get_type(Value),
	case element(Index,ColTypes#coltypes.cols) of
		t_unknown ->
			ColTypes0 = ColTypes#coltypes{ cols = setelement(Index,ColTypes#coltypes.cols,get_type(Value)) },
			ColTypes0#coltypes{defined = cols_defined(ColTypes0)};
		t_tiny when VType == t_short; VType == t_int; VType == t_longlong ->
			ColTypes0 = ColTypes#coltypes{ cols = setelement(Index,ColTypes#coltypes.cols,get_type(Value)) },
			ColTypes0#coltypes{defined = cols_defined(ColTypes0)};
		t_short when VType == t_int; VType == t_longlong ->
			ColTypes0 = ColTypes#coltypes{ cols = setelement(Index,ColTypes#coltypes.cols,get_type(Value)) },
			ColTypes0#coltypes{defined = cols_defined(ColTypes0)};
		t_int when VType == t_longlong ->
			ColTypes0 = ColTypes#coltypes{ cols = setelement(Index,ColTypes#coltypes.cols,get_type(Value)) },
			ColTypes0#coltypes{defined = cols_defined(ColTypes0)};
		_ ->
			ColTypes
	end.

%% @spec get_type(any()) -> atom()
%% @doc  Returns an atom representing the detected value. If type is not detected <i>t_unknown</i> is returned.
get_type(undefined) ->
	t_unknown;
% get_type(Val) when is_integer(Val), Val >= -16#80, Val =< 16#ff ->
% 	t_tiny;
% get_type(Val) when is_integer(Val), Val >= -16#8000, Val =< 16#ffff ->
% 	t_short;
% get_type(Val) when is_integer(Val), Val >= -16#80000000, Val =< 16#ffffffff ->
% 	t_int;
% get_type(Val) when is_integer(Val), Val >= -16#8000000000000000, Val =< 16#ffffffffffffffff ->
% 	t_longlong;
get_type(V) when is_integer(V) ->
	t_longlong;
get_type(Val) when is_float(Val) ->
	t_double;
get_type({blob,_}) ->
	t_blob;
get_type(_) ->
	t_text.

%% @spec map_coltype(atom()) -> binary()
%% @doc  Maps a type atom to binary representation to be used in packet when creating a column definition. See: {@link myactor_proto:multirow_columndefs0/6}
% map_coltype(t_tiny) ->
% 	?T_TINY;
% map_coltype(t_int) ->
% 	?T_LONG;
map_coltype(t_longlong) ->
	?T_LONGLONG;
map_coltype(t_double) ->
	?T_DOUBLE;
map_coltype(t_blob) ->
	?T_BLOB;
map_coltype(_) ->
	?T_STRING.

%% @spec cols_defined(#coltypes{}) -> true | false
%% @doc  Utility function. Returns <i>true</i> when all column types of #coltypes.cols are known
cols_defined(ColTypes) when is_record(ColTypes,coltypes) ->
	cols_defined0(ColTypes,1,size(ColTypes#coltypes.cols)).

%% @spec cols_defined0(#coltypes{},integer(),integer()) -> true | false
%% @doc  Returns <i>true</i> when all column types of #coltypes.cols are known
cols_defined0(ColTypes,Index,Size) when is_record(ColTypes,coltypes) ->
	case (Index > Size) of
		true ->
			true;
		_ ->
			case element(Index,ColTypes#coltypes.cols) of
				t_unknown ->
					false;
				_ ->
					cols_defined0(ColTypes,Index+1,Size)
			end
	end.

%% @spec unpack_password(binary(),list()) -> binary()
%% @doc  Returns password depending on the type of encoded data. Used in client handshake.
unpack_password(Data1,Caps) ->
	case lists:member(auth_lenenc_client_data,Caps) of
		true ->
			?PROTO_DBG("unpack_password | read lengthencoded password"),
			myactor_util:read_lenenc_string(Data1);
		false ->
			case lists:member(secure_connection,Caps) of
				true ->
					<<PassLen, Pass:PassLen/binary, Data2/binary>> = Data1,
					?PROTO_DBG("unpack_password | read secure password, len=~p",[PassLen]),
					{Pass, Data2};
				false ->
					?PROTO_DBG("unpack_password | read zero terminated password"),
					split_zero(Data1)
			end
	end.

%% @spec read_capability(binary(),list(),atom()) -> binary()
%% @doc  Reads a capability from binary representation if such capability is present in the capabilities list.
read_capability(DataN,Caps,Capability) ->
	case lists:member(Capability,Caps) of
		true ->
			split_zero(DataN);
		false ->
			{<<>>,DataN}
	end.

%% @spec split_zero(binary()) -> {binary(),binary()}
%% @doc  Splits a binary after first occurance of <i>0</i>.
split_zero(String) ->
	[B1, B2] = binary:split(String, <<0>>),
	{B1, B2}.

%% @spec read_capabilities(binary()) -> list()
%% @doc  Utility function. Creates capability list from the capabilites flag.
read_capabilities(Flag) ->
	read_capabilities0(Flag,?CAPABILITY_MAP,[]).

%% @spec read_capabilities0(binary(),list(),list()) -> list()
%% @doc  Creates capability list from the capabilites flag.
read_capabilities0(_,[],CapList) ->
	CapList;
read_capabilities0(Flag,[{CapVal,Cap}|Caps],CapList) ->
	case Flag band CapVal of
		0 ->
			read_capabilities0(Flag,Caps,CapList);
		_ ->
			read_capabilities0(Flag,Caps,[Cap|CapList])
	end.
