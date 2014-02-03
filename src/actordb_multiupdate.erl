% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_multiupdate).
-behaviour(gen_server).
-define(LAGERDBG,true).
-export([start/1, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
% -compile(export_all).
% -export([cb_schema/3,cb_path/3,cb_call/3,cb_cast/2,cb_info/2,cb_init/2]).
-export([multiread/1,exec/1,exec/2,get_schema/1,transaction_state/2]).
-include_lib("actordb.hrl").
-include_lib("kernel/include/file.hrl").

% -record(dp,{id,recovering = false}).

exec(S) ->
	exec(actordb_local:pick_mupdate(),S).
exec(Name,S) when is_integer(Name) ->
	case distreg:whereis({multiupdate,Name}) of
		undefined ->
			start(Name),
			erlang:yield(),
			exec(Name,S);
		Pid ->
			% Bin = butil:dec2hex(term_to_binary(S,[compressed])),
			gen_server:call(Pid,{exec,S})
	end.

start(Name) ->
	case distreg:whereis({multiupdate,Name}) of
		undefined ->
			case gen_server:start(?MODULE, Name, []) of
				{error,{registered,Pid}} ->
					{ok,Pid};
				{ok,Pid} ->
					{ok,Pid}
			end;
		Pid ->
			{ok,Pid}
	end.

transaction_state(Name,Id) ->
	case distreg:whereis({multiupdate,Name}) of
		undefined ->
			start(Name),
			erlang:yield(),
			transaction_state(Name,Id);
		Pid ->
			gen_server:call(Pid,{transaction_state,Id})
	end.

stop(Name) ->
	case distreg:whereis({multiupdate,Name}) of
		undefined ->
			ok;
		Pid ->
			gen_server:call(Pid, stop)
	end.

print_info() ->
	gen_server:cast(?MODULE,print_info).


% If node goes down, this updater may be started on another node in cluster.
% The only operation it will alow is checking transaction state and abandoning any transactions that are incomplete.
-record(dp,{name, currow, confirming, callqueue, curfrom, execproc, curnum = 0,local = true}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	

sqlname(P) ->
	{P#dp.name,?MULTIUPDATE_TYPE}.

handle_call({transaction_state,Id},_From,P) ->
	case actordb_actor:read(sqlname(P),[create],<<"SELECT * FROM transactions WHERE id=",(butil:tobin(Id))/binary,";">>) of
		{ok,[{columns,_},{rows,[{_,Commited}]}]} ->
			{reply,{ok,Commited},P};
		{ok,[_,{rows,[]}]} ->
			?AERR("Trying to read transaction_state that does not exist updater ~p id ~p",[P#dp.name,Id]),
			{reply,undefined,P};
		Err ->
			{reply,Err,P}
	end;
handle_call({exec,S},From,#dp{execproc = undefined, local = true} = P) ->
	actordb_local:mupdate_busy(P#dp.name,true),

	case actordb_actor:write(sqlname(P),[create],<<"INSERT INTO transactions (commited) VALUES (0);">>) of
		{ok,{changes,Num,_}} ->
			ok;
		{ok,{rowid,Num}} ->
			ok
	end,

	{Pid,_} = spawn_monitor(fun() -> 
		% Send sql
		% put(nchanges,0),
		case catch do_multiupdate(P#dp{currow = Num},S) of
			ok ->
				% NChanges = get(nchanges),
				erase(),
				?ADBG("multiupdate commiting"),
				% With this write transaction is commited. 
				% If any node goes down, actors will check back to this updater process
				%  if transaction is set to commited or not. If commited is set and they have not commited, they will execute their sql
				%  which they have saved locally.
				ok = actordb_sqlproc:okornot(
						actordb_actor:write(sqlname(P),[create],<<"UPDATE transactions SET commited=1 WHERE id=",(butil:tobin(Num))/binary,
														" AND (commited=0 OR commited=1);">>)),
				% Inform all actors that they should commit.
				case catch do_multiupdate(P#dp{currow = Num, confirming = true},S) of
					ok ->
						ok;
					Err ->
						?AERR("Multiupdate confirm error ~p",[Err])
				end,
				% exit({ok,{changes,-1,NChanges}});
				exit(ok);
			Err ->
				?AERR("Multiupdate failed ~p",[Err]),
				case catch do_multiupdate(P#dp{currow = Num, confirming = false},S) of
					ok ->
						ok;
					Err ->
						?AERR("Multiupdate confirm error ~p",[Err])
				end,
				% commited = -1 means transaction has been abandoned.
				% Only update if commited=0. This is a safety measure in case node went offline in the meantime and
				%  other nodes in cluster changed db to failed transaction.
				% Once commited is set to 1 or -1 it is final.
				ok = actordb_sqlproc:okornot(actordb_actor:write(sqlname(P),[create],abandon_sql(Num))),
				exit(abandoned)
		end
	end),
	{noreply,P#dp{execproc = Pid, curnum = Num, curfrom = From}};
handle_call({exec,_,_} = Msg,From,P) ->
	case P#dp.local of
		false ->
			{reply, notlocal,P};
		_ ->
			{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue)}}
	end;
handle_call(print_info,_,P) ->
	io:format("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast(_, P) ->
	{noreply, P}.

handle_info({'DOWN',_Monitor,_Ref,PID,Result}, #dp{execproc = PID} = P) ->
	?ADBG("Multiupdate proc dead ~p, waiting list ~p",[Result,not queue:is_empty(P#dp.callqueue)]),
	actordb_local:mupdate_busy(P#dp.name,false),
	gen_server:reply(P#dp.curfrom,Result),
	case Result of
		ok ->
			ok;
		{ok,_} ->
			ok;
		abandoned ->
			ok;
		_ ->
			ok = actordb_sqlproc:okornot(actordb_actor:write(sqlname(P),[create],abandon_sql(P#dp.curnum)))
	end,
	case queue:is_empty(P#dp.callqueue) of
		true ->
			{noreply,P#dp{curfrom = undefined, execproc = undefined}};
		false ->
			{{value,Call},CQ} = queue:out_r(P#dp.callqueue),
			{From,Msg} = Call,
			case handle_call(Msg,From,P#dp{curfrom = undefined, callqueue = CQ, execproc = undefined}) of
				{reply,ok,NP1} ->
					{noreply,NP1};
				{noreply,NP1} ->
					{noreply,NP1}
			end
	end;
handle_info(timeout,P) ->
	erlang:send_after(1000,self(),timeout),
	case P#dp.local of
		false ->
			{stop,normal,P};
		_ ->
			{noreply,P}
	end;
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info(_, P) -> 
	{noreply, P}.
	
terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init(Name1) ->
	P = #dp{name = Name1, callqueue = queue:new(), local = lists:member(Name1,actordb_local:local_mupdaters())},
	actordb_local:mupdate_busy(P#dp.name,false),
	case distreg:reg({multiupdate,P#dp.name}) of
		ok ->
			{ok,_} = actordb_actor:start(P#dp.name,?MULTIUPDATE_TYPE,[create]),
			ok = actordb_local:reg_mupdater(P#dp.name,self()),
			erlang:send_after(1000,self(),timeout),
			case actordb_actor:read(sqlname(P),[create],<<"SELECT max(id),commited FROM transactions;">>) of
				{ok,[{columns,_},{rows,[{Id,0}]}]} ->
					ok = actordb_sqlproc:okornot(actordb_actor:write(sqlname(P),[create],abandon_sql(Id)));
				{ok,_} ->
					ok
			end,
			{ok,P};
		name_exists ->
			{stop,{registered,distreg:whereis({multiupdate,P#dp.name})}}
	end.

abandon_sql(Id) ->
	<<"UPDATE transactions SET commited=-1 WHERE id=",(butil:tobin(Id))/binary," AND (commited=0 OR commited=-1);">>.

get_schema(0) ->
	{schema_version(), [schema(N) || N <- lists:seq(1,schema_version())]};
get_schema(Version) ->
	case schema_version() > Version of
		true ->
			{schema_version(),[schema(N) || N <- lists:seq(Version+1,schema_version())]};
		false ->
			{Version,[]}
	end.

schema_version() ->
	1.

schema(1) ->
	<<"CREATE TABLE transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, commited INTEGER DEFAULT 0);">>.


 
% For variables as a result of an sql statement, multiupdate uses the process dictionary.
% Process dictionary because random access is required and nothing else is remotely as fast.
% Structure:
% [{VariableName,cols},{columnname1,columnname2,columnname3,...}]
% [{VariableName,Index},{rowval1,rowval2,rowval3,...}]
% If a variable has local scope (it's from a for statement), it has a different {VariableName,cols}
% [{VariableName,cols},{gvar,GlobalVarName,GlobalVarIndex}]


multiread(L) ->
	erase(),
	put(nchanges,0),
	case catch do_multiupdate(undefined,L) of
		ok ->
			case get({<<"RESULT">>,cols}) of
				undefined ->
					?ADBG("Nothing in result? ~p",[get()]),
					ok;
				Cols ->
					Rows = read_mr_rows(get({<<"RESULT">>,nrows})-1,[]),
					erase(),
					{ok,[{columns,Cols},{rows,Rows}]}
			end;
		Err ->
			?AERR("Error multiread ~p",[Err])
	end.

read_mr_rows(N,L) when N < 0 ->
	L;
read_mr_rows(N,L) ->
	case get({<<"RESULT">>,N}) of
		undefined ->
			read_mr_rows(N-1,L);
		Row ->
			read_mr_rows(N-1,[Row|L])
	end.

% actordb:exec("use type1(all); insert into tab1 values (1,'a1');insert into tab1 values (2,'a2');").
% actordb:exec("use type1(all);{{ACTORS}}SELECT * FROM tab1;use type1(foreach X.txt in ACTORS);insert into tab1 values ({{uniqid}},'{{X.txt}}');").

% 
% 		LOOP OVER BLOCKS (1 block is: "use actortype(....);statement1;statement2;statementN")
% 
do_multiupdate(P,[{AInfo,IsWrite,Statements}|T]) ->
	case AInfo of
		{Type1,Gvar,Column,BlockVar,Flags} ->
			Type = actordb_util:typeatom(Type1),
			case get({Gvar,cols}) of
				undefined ->
					?AERR("global var columns not found ~p",[Gvar]),
					do_multiupdate(P,[]);
				Columns ->
					case findpos(1,Column,Columns) of
						N when is_integer(N) ->
							NRows = get({Gvar,nrows}),
							do_foreach(P,Type,Flags,N,Gvar,BlockVar,IsWrite,Statements,NRows-1),
							do_multiupdate(P,T);
						_ ->
							?AERR("global var column not found ~p ~p",[Column,Columns]),
							% TODO: error
							do_multiupdate(P,[])
					end
			end;
		{Type1,Actors,Flags} ->
			Type = actordb_util:typeatom(Type1),
			case statements_to_binary(undefined,Statements,<<>>,[]) of
				undefined ->
					do_multiupdate(P,[]);
				curactor ->
					case Actors of
						$* ->
							% ?AINF("Move over shards ~p~n",[actordb_shardtree:all()]),
							move_over_shards(Type,Flags,P,IsWrite,Statements,curactor,actordb_shardtree:all());
						_ ->
							case Actors of
								[_] ->
									IsMulti = false;
								_ ->
									IsMulti = true
							end,
							do_block(P,IsMulti,Type,Flags,Actors,IsWrite,Statements,curactor)
					end,
					do_multiupdate(P,T);
				{StBin,Varlist} ->
					case Actors of
						$* ->
							% ?AINF("Move over shards ~p~n",[actordb_shardtree:all()]),
							move_over_shards(Type,Flags,P,IsWrite,StBin,Varlist,actordb_shardtree:all());
						_ ->
							case Actors of
								[_] ->
									IsMulti = false;
								_ ->
									IsMulti = true
							end,
							do_block(P,IsMulti,Type,Flags,Actors,IsWrite,StBin,Varlist)
					end,
					do_multiupdate(P,T)
			end
	end;
do_multiupdate(_,[]) ->
	ok.

% 
% 			TYPE 1 - moving over all actors for type by traversing shard tree
% 
move_over_shards(Type,Flags,P,IsWrite,StBin,Varlist,{Shard,_UpperLimit,Nd,Left,Right}) ->
	% A bit of an ugly hack with proc. dictionary. The problem is a shard might get visited
	%  more then once without this failsafe. If a node has too few shards, it will split them in half
	%  thus doubling it's shard count. If this happens during a query a shard is likely to get visited twice.
	case get({shard_visited,Shard}) of
		undefined ->
			put({shard_visited,Shard},true),
			move_over_shard_actors(Nd,Type,Flags,Shard,[],0,0,P,IsWrite,StBin,Varlist,undefined);
		_ ->
			ok
	end,
	case Left of
		undefined when Right /= undefined ->
			move_over_shards(Type,Flags,P,IsWrite,StBin,Varlist,Right);
		undefined ->
			ok;
		_ when Right == undefined ->
			move_over_shards(Type,Flags,P,IsWrite,StBin,Varlist,Left);
		_ ->
			move_over_shards(Type,Flags,P,IsWrite,StBin,Varlist,Left),
			move_over_shards(Type,Flags,P,IsWrite,StBin,Varlist,Right)
	end.


% Next = shard that is splitting (integer type), or name of node where shard is moving
move_over_shard_actors(Nd,Type,Flags,Shard,[],1000,CountAll,P,IsWrite,StBin,Varlist,NextShard) when NextShard /= undefined ->
	move_over_shard_actors(Nd,Type,Flags,Shard,[],1000,CountAll,P,IsWrite,StBin,Varlist,undefined);
move_over_shard_actors(Nd,Type,_Flags,Shard,[],_CountNow,_CountAll,_P,_IsWrite,[count],_Varlist,_NextShard) ->
	case get({<<"RESULT">>,cols}) of
		undefined ->
			put({<<"RESULT">>,cols},{<<"count">>}),
			put({<<"RESULT">>,0},{0}),
			put({<<"RESULT">>,nrows},1);
		_ ->
			ok
	end,
	{CurCount} = get({<<"RESULT">>,0}),
	case bkdcore:node_name() == Nd of
		true ->
			Count = actordb_shard:count_actors(Shard,Type);
		false ->
			Count = actordb:rpc(Nd,Shard,{actordb_shard,count_actors,[Shard,Type]})
	end,
	put({<<"RESULT">>,0},{CurCount+Count}),
	ok;
move_over_shard_actors(Nd,Type,Flags,Shard,[],CountNow,CountAll,P,IsWrite,StBin,Varlist,NextShard) ->
	Iskv = actordb_schema:iskv(Type),
	case ok of
		_ when Iskv ->
			do_actor(P,true,Type,Flags,{Shard,1},IsWrite,StBin,Varlist);
		_ when CountNow == 0; CountNow == 1000 ->
			case bkdcore:node_name() == Nd of
				true ->
					List = actordb_shard:list_actors(Shard,Type,CountAll,1000);
				false ->
					List = actordb:rpc(Nd,Shard,{actordb_shard,list_actors,[Shard,Type,CountAll,1000]})
			end,
			?ADBG("Moving over shard ~p ~p ~p",[Shard,Type,List]),
			case List of
				{ok,[]} ->
					ok;
				{ok,L} ->
					move_over_shard_actors(Nd,Type,Flags,Shard,L,0,CountAll,P,IsWrite,StBin,Varlist,NextShard);
				{ok,[],Next} when is_binary(Next) ->
					move_over_shard_actors(Next,Type,Flags,Shard,[],0,0,P,IsWrite,StBin,Varlist,undefined);
				{ok,[],Next} when is_integer(Next) ->
					move_over_shard_actors(Nd,Type,Flags,Next,[],0,0,P,IsWrite,StBin,Varlist,undefined);
				{ok,L,Next} ->
					move_over_shard_actors(Nd,Type,Flags,Shard,L,0,CountAll,P,IsWrite,StBin,Varlist,Next)
			end;
		_ ->
			case NextShard of
				undefined ->
					Shard;
				_ when is_binary(NextShard) ->
					move_over_shard_actors(NextShard,Type,Flags,Shard,[],0,0,P,IsWrite,StBin,Varlist,undefined);
				_ when is_integer(NextShard) ->
					case get({shard_visited,NextShard}) of
						undefined ->
							put({shard_visited,NextShard},true),
							move_over_shard_actors(Nd,Type,Flags,NextShard,[],0,0,P,IsWrite,StBin,Varlist,undefined);
						_ ->
							ok
					end
			end
	end;
move_over_shard_actors(Nd,Type,Flags,Shard,Actors,CountNow,CountAll,P,IsWrite,[list],Varlist,Next) ->
	case get({<<"RESULT">>,cols}) of
		undefined ->
			put({<<"RESULT">>,cols},{<<"actor">>}),
			put({<<"RESULT">>,nrows},0);
		_ ->
			ok
	end,
	CurNRows = get({<<"RESULT">>,nrows}),
	Count = lists:foldl(fun({Actor},Cnt) -> 
				put({<<"RESULT">>,CurNRows+Cnt},{Actor}),
				Cnt+1
			end,0,Actors),
	put({<<"RESULT">>,nrows},CurNRows+Count),
	move_over_shard_actors(Nd,Type,Flags,Shard,[],CountNow+Count,CountAll+Count,P,IsWrite,[list],Varlist,Next);
move_over_shard_actors(Nd,Type,Flags,Shard,Actors,CountNow,CountAll,P,IsWrite,StBin,Varlist,Next) ->
	Count = lists:foldl(fun({Actor},Cnt) -> 
				do_actor(P,true,Type,Flags,Actor,IsWrite,StBin,Varlist),
				Cnt+1
			end,0,Actors),
	move_over_shard_actors(Nd,Type,Flags,Shard,[],CountNow+Count,CountAll+Count,P,IsWrite,StBin,Varlist,Next).

% 
% 			TYPE 2 - looping over a list with for
% 
do_foreach(P,_,_,_,_,_,_,_,N) when N < 0 ->
	P;
do_foreach(P,Type,Flags,ActorColumn,Gvar,Blockvar,IsWrite,Statements,N) ->
	case get({Gvar,N}) of
		undefined ->
			?ADBG("do_foreach no gvar for ~p",[N]),
			P;
		Row ->
			put({Blockvar,cols},{foreach,Gvar,N}),
			Ac = butil:tobin(element(ActorColumn,Row)),
			?ADBG("do_foreach ~p ~p",[N,Ac]),
			{StBin,Varlist} = statements_to_binary(Ac,Statements,<<>>,[]),
			do_actor(P,true,Type,Flags,Ac,IsWrite,StBin,Varlist),
			do_foreach(P,Type,Flags,ActorColumn,Gvar,Blockvar,IsWrite,Statements,N-1)
	end.
	

% 
% 			TYPE 3 - regular query on single or list of actors
% 
do_block(P,IsMulti,Type,Flags,[Actor|T],IsWrite,Statements,Varlist) ->
	do_actor(P,IsMulti,Type,Flags,Actor,IsWrite,Statements,Varlist),
	do_block(P,IsMulti,Type,Flags,T,IsWrite,Statements,Varlist);
do_block(_,_,_,_,[],_,_,_) ->
	ok.


do_actor(_,_,_,_,_,_,<<>>,_) ->
	ok;
do_actor(P,IsMulti,Type,Flags,Actor,IsWrite,Statements1,curactor) ->
	{StBin,Varlist} = statements_to_binary(Actor,Statements1,<<>>,[]),
	do_actor(P,IsMulti,Type,Flags,Actor,IsWrite,StBin,Varlist);
do_actor(P,IsMulti,Type,Flags,Actor,IsWrite,Statements1,Varlist) ->
	case is_tuple(P) of
		true when IsWrite  ->
			case Statements1 of
				[Pragma] when is_atom(Pragma) ->
					Statements = {{P#dp.currow,P#dp.name,bkdcore:node_name()},[Pragma]};
				_ ->
					Statements = {{P#dp.currow,P#dp.name,bkdcore:node_name()},Statements1}
			end,
			?ADBG("do_actor write ~p ~p",[Actor,Statements]),
			case P#dp.confirming of
				undefined ->
					Res = actordb:direct_call(Actor,Type,Flags,IsWrite,Statements,true);
				true ->
					Res = actordb:direct_call(Actor,Type,Flags,IsWrite,{{P#dp.currow,P#dp.name,bkdcore:node_name()},commit},true);
				false ->
					Res = actordb:direct_call(Actor,Type,Flags,IsWrite,{{P#dp.currow,P#dp.name,bkdcore:node_name()},abort},true)
			end;
		_ ->
			?ADBG("do_actor read ~p ~p",[Actor,Statements1]),
			Res = actordb:direct_call(Actor,Type,Flags,IsWrite,Statements1,true)
	end,
	?ADBG("do_actor varlist ~p",[Varlist]),
	case Res of
		{sql_error,Str} ->
			exit({sql_error,Str});
		{ok,[{columns,_},{rows,_}] = L} ->
			store_vars(IsMulti,Actor,Varlist,[L]);
		{ok,[_|_] = L} ->
			store_vars(IsMulti,Actor,Varlist,L);
		{ok,{changes,_,_NChanges}} ->
			% put(nchanges,get(nchanges)+NChanges);
			ok;
		{ok,_} ->
			ok;
		ok ->
			ok;
		{error,nocreate} ->
			ok;
		_ ->
			exit(Res)
	end,
	?ADBG("do_actor res ~p",[Res]),
	ok.

store_vars(IsMulti,{Shard,_},L1,L2) ->
	store_vars(IsMulti,butil:tobin(Shard),L1,L2);
store_vars(IsMulti,Actor,[Varname|T],[Resh|ResRem]) ->
	[{columns,Cols},{rows,Rows}] = Resh,
	case IsMulti of
		true ->
			case get({Varname,cols}) of
				undefined ->
					N = 0,
					put({Varname,cols},erlang:append_element(Cols,<<"actor">>));
				_ ->
					N = get({Varname,nrows})
			end;
		false ->
			case get({Varname,cols}) of
				undefined ->
					N = 0,
					put({Varname,cols},Cols);
				_ ->
					N = get({Varname,nrows})
			end
	end,
	store_rows(IsMulti,Actor,Varname,N,Rows),
	store_vars(IsMulti,Actor,T,ResRem);
% Add column to another variable.
store_vars(IsMulti,Actor,[{A1,C1,A2,C2}|T],[]) ->
	?ADBG("Adding columns ~p",[{A1,C1,A2,C2}]),
	Val = get_pd_column(A2,C2),
	case get({A1,cols}) of
		{foreach,Gvar,GVarIndex} ->
			ColumnKey = {Gvar,cols},
			RowKey = {Gvar,GVarIndex},
			Columns = get(ColumnKey);
		Columns ->
			RowKey = {A1,0},
			ColumnKey = {A1,cols}
	end,
	?ADBG("getpos cols ~p",[findpos(1,C1,Columns)]),
	case findpos(1,C1,Columns) of
		false ->
			Index = tuple_size(Columns) + 1,
			put(ColumnKey,erlang:append_element(Columns,C1));
		Index ->
			ok
	end,
	Row = get(RowKey),
	case Index > tuple_size(Row) of
		true when Index-1 == tuple_size(Row) ->
			put(RowKey,erlang:append_element(Row,Val));
		true ->
			Row1 = lists:foldl(fun(_,Tuple) -> erlang:append_element(Tuple,undefined) end,
								Row,
								lists:seq(tuple_size(Row)+1,Index-1)),
			put(RowKey,erlang:append_element(Row1,Val));
		false ->
			put(RowKey,setelement(Index,Row,Val))
	end,
	store_vars(IsMulti,Actor,T,[]);
store_vars(_,_,[],_) ->
	ok.

store_rows(true,Actor,Varname,N,[Row|T]) ->
	put({Varname,N},erlang:append_element(Row,Actor)),
	store_rows(true,Actor,Varname,N+1,T);
store_rows(false,Actor,Varname,N,[Row|T]) ->
	put({Varname,N},Row),
	store_rows(false,Actor,Varname,N+1,T);
store_rows(_IsMulti,_Actor,Varname,N,[]) ->
	put({Varname,nrows},N),
	ok.

% Takes a block of sql statements and creates a single binary, filling in any {{..}} variables. 
% Also returns a list of result variables ({{Var}}SELECT * ....)
% Statement with no result variables or statement variables. 
statements_to_binary(CurActor,[<<_/binary>> = B|T],Out,VarList) ->
	statements_to_binary(CurActor,T,<<Out/binary,"$",B/binary>>,VarList);
% Not an sql statement but a variable assignment (changing result)
statements_to_binary(CurActor,[{{A1,C1,A2,C2},<<>>}|T],Out,VarList) ->
	statements_to_binary(CurActor,T,Out,[{A1,C1,A2,C2}|VarList]);
% Result var but no statement variables
statements_to_binary(CurActor,[{ResultVar,<<_/binary>> = B}|T],Out,VarList) ->
	statements_to_binary(CurActor,T,<<Out/binary,B/binary>>,[ResultVar|VarList]);
statements_to_binary(_,[X],_,_) when is_atom(X) ->
	{[X],[]};
statements_to_binary(CurActor,[H|T],Out,Varlist) ->
	case H of
		[_|_] = Statements ->
			ResultVar = undefined;
		{ResultVar,Statements} ->
			ok
	end,
	Bin = lists:foldl(fun(_,undefined) -> undefined;
											 (_,curactor) -> curactor;
										   (El,B) ->
							case El of
								curactor when CurActor == undefined ->
									curactor;
								curactor ->
									<<B/binary,CurActor/binary>>;
								uniqid ->
									{ok,Idi} = bkdcore_idgen:getid(),
									Id = butil:tobin(Idi),
									<<B/binary,Id/binary>>;
								{uniqid,Column} ->
									case get({uniqid,Column}) of
										undefined ->
											{ok,Idi} = bkdcore_idgen:getid(),
											Id = butil:tobin(Idi),
											put({uniqid,Column},Id);
										Id ->
											ok
									end,
									<<B/binary,Id/binary>>;
								{Var,Column} ->
									case get_pd_column(Var,Column) of
										undefined ->
											<<B/binary,"{{",Var/binary,".",Column/binary,"}}">>;
										Valx ->
											<<B/binary,Valx/binary>>
									end;
								<<_/binary>> = El ->
									<<B/binary,El/binary>>;
								% single char
								_  ->
									<<B/binary,El>>
							end
					end,<<>>,Statements),
	case Bin of
		undefined ->
			undefined;
		curactor ->
			curactor;
		_ ->
			case ResultVar of
				undefined ->
					% If statement has no result var, add $ to beginning so result of statement is not returned on success.
					statements_to_binary(CurActor,T,<<Out/binary,"$",Bin/binary>>,Varlist);
				_ ->
					statements_to_binary(CurActor,T,<<Out/binary,Bin/binary>>,[ResultVar|Varlist])
			end
	end;
statements_to_binary(_CurActor,[],O,Varlist) ->
	{O,lists:reverse(Varlist)}.

get_pd_column(Var,Column) ->
	case get({Var,cols}) of
		undefined ->
			undefined;
		{foreach,Gvar,GVarIndex} ->
			case findpos(1,Column,get({Gvar,cols})) of
				false ->
					undefined;
				Index ->
					element(Index,get({Gvar,GVarIndex}))
			end;
		Columns ->
			case findpos(1,Column,Columns) of
				false ->
					undefined;
				Index ->
					element(Index,get({Var,0}))
			end
	end.

findpos(N,_,Tuple) when N > tuple_size(Tuple) ->
	false;
findpos(N,Val,Tuple) when element(N,Tuple) == Val ->
	N;
findpos(N,Val,Tuple) ->
	findpos(N+1,Val,Tuple).





