% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_multiupdate).
-behaviour(gen_server).
-define(LAGERDBG,true).
-export([start/1, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, 
code_change/3,print_info/0]).
% -compile(export_all).
% -export([cb_schema/3,cb_path/3,cb_call/3,cb_cast/2,cb_info/2,cb_init/2]).
-export([multiread/1,exec/1,exec/2,get_schema/1,transaction_state/2,overruled/2]).
-include("actordb.hrl").
-include_lib("kernel/include/file.hrl").

overruled(Name,Id) ->
	case distreg:whereis({multiupdate,Name}) of
		undefined ->
			start(Name),
			erlang:yield(),
			transaction_state(Name,Id);
		Pid ->
			gen_server:call(Pid,{overruled,Id})
	end.

exec(S) ->
	exec(actordb_local:pick_mupdate(),S).
exec(Name,S) when is_integer(Name) ->
	case distreg:whereis({multiupdate,Name}) of
		undefined ->
			start(Name),
			erlang:yield(),
			exec(Name,S);
		Pid ->
			case (catch gen_server:call(Pid,{exec,S}, 7000)) of
				{'EXIT',{timeout,_}} ->
					Pid ! {stop,timeout},
					{error,timeout};
				R ->
					R
			end
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
-record(dp,{name, currow, confirming, callqueue, curfrom, curmsg, execproc, curnum = 0,local = true}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).

sqlname(P) ->
	{P#dp.name,?MULTIUPDATE_TYPE}.

handle_call({transaction_state,Id},_From,P) ->
	case actordb_actor:read(#{actor => sqlname(P), flags => [create], statements => {<<"#d06;">>,[[[butil:toint(Id)]]]}}) of
		{ok,[{columns,_},{rows,[{_,0}]}]} when P#dp.execproc == undefined ->
			{stop,normal,P};
		{ok,[{columns,_},{rows,[{_,Commited}]}]} ->
			{reply,{ok,Commited},P};
		{ok,[_,{rows,[]}]} ->
			?AERR("Trying to read transaction_state that does not exist updater ~p id ~p",[P#dp.name,Id]),
			{reply,undefined,P};
		Err ->
			{reply,Err,P}
	end;
handle_call({overruled,Id},_From,P) ->
	?ADBG("Mutliupdate call overruled"),
	case handle_call({transaction_state,Id},undefined,P) of
		{reply,{ok,0},_} ->
			true = P#dp.curnum == butil:toint(Id),
			P#dp.execproc ! overruled;
		_ ->
			ok
	end,
	{reply, ok, P};
handle_call({exec,S} = Msg,From,#dp{execproc = undefined, local = true} = P) ->
	actordb_local:mupdate_busy(P#dp.name,true),
	case actordb_actor:write(#{actor => sqlname(P), flags => [create], statements => <<"#d07;">>}) of
	%case actordb_actor:write(sqlname(P),#{flags => [create], statements => <<"#d07;">>}) of
		{ok,{changes,Num,_}} ->
			ok;
		{ok,{rowid,Num}} ->
			ok
	end,

	{Pid,_} = spawn_monitor(fun() ->
		Executor = self(),
		spawn(fun() -> erlang:monitor(process,Executor), 
			receive 
			{'DOWN',_Monitor,_Ref,_,_} ->
				ok
			after 7000 ->
				exit(Executor,timeout)
			end
		end),
		% Send sql
		put(nchanges,0),
		case catch do_multiupdate(P#dp{currow = Num},S) of
			ok ->
				% NChanges = get(nchanges),
				% erase(),
				?ADBG("multiupdate commiting"),
				% With this write transaction is commited.
				% If any node goes down, actors will check back to this updater process
				%  if transaction is set to commited or not. If commited is set and they have not commited, they will execute their sql
				%  which they have saved locally.
				ok = actordb_sqlite:okornot(
						actordb_actor:write(#{actor => sqlname(P),flags => [create], statements => {<<"#s08;">>,[[[Num]]]}})),
				% Inform all actors that they should commit.
				TC = [actordb:rpcast(NodeName,{actordb_sqlprocutil,transaction_done,[Num,P#dp.name,done]}) || {{node,NodeName},_} <- get()],
				?ADBG("T Cast: ~p",[TC]),
				exit({ok,{changes,0,get(nchanges)}});
			Err ->
				?AERR("Multiupdate ~p.'__mupdate__' failed=~p",[P#dp.name, Err]),
				[actordb:rpcast(NodeName,{actordb_sqlprocutil,transaction_done,[Num,P#dp.name,abandoned]}) || {{node,NodeName},_} <- get()],
				% commited = -1 means transaction has been abandoned.
				% Only update if commited=0. This is a safety measure in case node went offline in the meantime and
				%  other nodes in cluster changed db to failed transaction.
				% Once commited is set to 1 or -1 it is final.
				ok = actordb_sqlite:okornot(actordb_actor:write(#{ actor => sqlname(P), flags => [create], statements => abandon_sql(Num)})),
				case Err of
					overruled ->
						exit({error,overruled});
					_ ->
						exit({error,abandoned})
				end
		end
	end),
	{noreply,P#dp{execproc = Pid, curnum = Num, curfrom = From, curmsg = Msg}};
handle_call({exec,_} = Msg,From,P) ->
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
	handle_info({stop},P).

handle_cast(_, P) ->
	{noreply, P}.

handle_info({'DOWN',_Monitor,_Ref,PID,{error,overruled}}, #dp{execproc = PID} = P) ->
	?ADBG("Multiupdate overruled"),
	case handle_call(P#dp.curmsg,P#dp.curfrom,P#dp{curfrom = undefined, execproc = undefined, curmsg = undefined}) of
		{reply,ok,NP1} ->
			{noreply,NP1};
		{noreply,NP1} ->
			{noreply,NP1}
	end;
handle_info({'DOWN',_Monitor,_Ref,PID,Result}, #dp{execproc = PID} = P) ->
	?ADBG("Multiupdate proc dead ~p, waiting list ~p",[Result,not queue:is_empty(P#dp.callqueue)]),
	actordb_local:mupdate_busy(P#dp.name,false),
	gen_server:reply(P#dp.curfrom,Result),
	case Result of
		ok ->
			ok;
		{ok,_} ->
			ok;
		{error,abandoned} ->
			ok;
		overruled ->
			ok;
		_ ->
			ok = actordb_sqlite:okornot(actordb_actor:write(
				#{actor => sqlname(P), flags => [create], statements => abandon_sql(P#dp.curnum)}))
	end,
	case queue:is_empty(P#dp.callqueue) of
		true ->
			{noreply,P#dp{curfrom = undefined, execproc = undefined, curmsg = undefined}};
		false ->
			{{value,Call},CQ} = queue:out_r(P#dp.callqueue),
			{From,Msg} = Call,
			case handle_call(Msg,From,P#dp{curfrom = undefined, callqueue = CQ, execproc = undefined, curmsg = undefined}) of
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
	case P#dp.execproc of
		undefined ->
			ok;
		Pid ->
			exit(Pid,gen_server_stop)
	end,
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
			case actordb_actor:read(#{actor => sqlname(P), flags => [create], statements => <<"SELECT max(id),commited FROM transactions;">>}) of
				{ok,[{columns,_},{rows,[{Id,0}]}]} ->
					ok = actordb_sqlite:okornot(actordb_actor:write(#{actor => sqlname(P), flags => [create], statements => abandon_sql(Id)}));
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
	<<"$CREATE TABLE transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, commited INTEGER DEFAULT 0);">>.



% For variables as a result of an sql statement, multiupdate uses the process dictionary.
% Process dictionary because random access is required and nothing else is remotely as fast.
% Structure:
% [{VariableName,cols},{columnname1,columnname2,columnname3,...}]
% [{VariableName,Index},{rowval1,rowval2,rowval3,...}]
% If a variable has local scope (it's from a for statement), it has a different {VariableName,cols}
% [{VariableName,cols},{gvar,GlobalVarName,GlobalVarIndex}]
% It also stores every node where an actor lives: [{{node,Nodename},true},...]

multiread(L) ->
	ExistingPD = get(),
	erase(),
	put(nchanges,0),
	case catch do_multiupdate(undefined,L) of
		ok ->
			case get({<<"RESULT">>,cols}) of
				undefined ->
					put({<<"RESULT">>,nrows},0),
					Cols = {<<"actor">>};
				Cols ->
					ok
			end,
			Rows = read_mr_rows(get({<<"RESULT">>,nrows})-1,[]),
			erase(),
			[put(K,V) || {K,V} <- ExistingPD],
			{ok,[{columns,Cols},{rows,Rows}]};
		{'EXIT',Err} ->
			?AERR("Error multiread ~p",[Err]),
			Err
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
do_multiupdate(P,[#{type := Type1, actor := Actors, flags := _Flags, var := undefined, column := undefined, blockvar := undefined } = H |T]) ->
	Statements = maps:get(statements, H),
	case statements_to_binary(undefined,Statements,<<>>,[]) of
		undefined ->
			do_multiupdate(P,[]);
		curactor ->
			case Actors of
				{$*,_Where} ->
					move_over_shards(H#{type := actordb_util:typeatom(Type1)}, P, curactor, actordb_shardtree:all());
				_ ->
					case Actors of
						[_] ->
							IsMulti = false;
						_ ->
							IsMulti = true
					end,
					do_block(P,IsMulti,H,curactor)
			end,
			do_multiupdate(P,T);
		{StBin,Varlist} ->
			case Actors of
				{$*,_Where} ->
					move_over_shards(H#{statements := StBin, type := actordb_util:typeatom(Type1)}, P, Varlist, actordb_shardtree:all());
				_ ->
					case Actors of
						[_] ->
							IsMulti = false;
						_ ->
							IsMulti = true
					end,
					do_block(P,IsMulti,H#{statements := StBin},Varlist)
			end,
			do_multiupdate(P,T)
	end;
do_multiupdate(P,[#{type := Type1, var := Gvar, column := Column, blockvar := _BlockVar, flags := _Flags} = H|T]) ->
	Type = actordb_util:typeatom(Type1),
	case get({Gvar,cols}) of
		undefined ->
			?AERR("global var columns not found ~p",[Gvar]),
			do_multiupdate(P,[]);
		Columns ->
			?AERR("Columns ~p",[Columns]),
			case findpos(1,Column,Columns) of
				N when is_integer(N) ->
					NRows = get({Gvar,nrows}),
					do_foreach(P,N,H#{type := Type},{0,NRows}),
					do_multiupdate(P,T);
				_ ->
					?AERR("global var column not found ~p ~p",[Column,Columns]),
					% TODO: error
					do_multiupdate(P,[])
			end
	end;

do_multiupdate(_,[]) ->
	ok.

%
% 			TYPE 1 - moving over all actors for type by traversing shard tree
%
move_over_shards(#{statements := [{list,{0,0}}]} = H, _, _, _) ->
	H;
move_over_shards(H, P, Varlist, {Shard,_UpperLimit,Nd,Left,Right}) ->
	% A bit of an ugly hack with proc. dictionary. The problem is a shard might get visited
	%  more then once without this failsafe. If a node has too few shards, it will split them in half
	%  thus doubling it's shard count. If this happens during a query a shard is likely to get visited twice.
	case get({shard_visited,Shard}) of
		undefined ->
			put({shard_visited,Shard},true),
			H1 = move_over_shard_actors(Nd, H, Shard, [], 0, 0, P, Varlist, undefined);
		_ ->
			H1 = H
	end,
	case Left of
		undefined when Right /= undefined ->
			move_over_shards(H1, P, Varlist, Right);
		undefined ->
			H1;
		_ when Right == undefined ->
			move_over_shards(H1, P, Varlist, Left);
		_ ->
			move_over_shards(move_over_shards(H1, P, Varlist, Left), P, Varlist, Right)
	end.


% Next = shard that is splitting (integer type), or name of node where shard is moving
move_over_shard_actors(Nd, H, Shard, [], 1000, CountAll, P, Varlist, NextShard) when NextShard /= undefined ->
	move_over_shard_actors(Nd, H, Shard, [],1000, CountAll, P, Varlist, undefined);

move_over_shard_actors(Nd,#{actor := {$*,Where},type := Type, statements := [count]} = H, 
		Shard, [], _CountNow, _CountAll, _P, _Varlist, _NextShard) ->
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
			Count = actordb_shard:count_actors(Shard,Type,Where);
		false ->
			Count = actordb:rpc(Nd,Shard,{actordb_shard,count_actors,[Shard,Type,Where]})
	end,
	put({<<"RESULT">>,0},{CurCount+Count}),
	H;
move_over_shard_actors(Nd,#{actor := {$*,Where},type := Type, flags := _Flags, statements := _StBin, iswrite := _IsWrite} = H,
				Shard, [], CountNow, CountAll, P, Varlist, Next) ->
	% If  _StBin=[{list,_}] then do not treat it like a kv type
	Iskv = actordb_schema:iskv(Type) andalso is_tuple(hd(_StBin)) == false,
	case ok of
		_ when Iskv ->
			do_actor(P,true,H#{actor := {Shard,1}},Varlist),
			H;
		_ when CountNow == 0; CountNow == 1000 ->
			case bkdcore:node_name() == Nd of
				true ->
					List = actordb_shard:list_actors(Shard,Type,Where,CountAll,1000);
				false ->
					List = actordb:rpc(Nd,Shard,{actordb_shard,list_actors,[Shard,Type,Where,CountAll,1000]})
			end,
			?ADBG("Moving over shard ~p ~p ~p",[Shard,Type,List]),
			case List of
				{ok,[]} ->
					put({empty_shard,Shard},[]),
					H;
				{ok,L} ->
					move_over_shard_actors(Nd, H, Shard, L, 0, CountAll, P, Varlist, Next);
				{ok,[],NextShard,NextShardNode} ->
					case get({shard_visited,NextShard}) of
						undefined ->
							put({shard_visited,NextShard},true),
							move_over_shard_actors(NextShardNode, H, NextShard,[], 0, 0, P, Varlist, undefined);
						_ ->
							H
					end;
				{ok,L,NextShard1,NextShardNode1} ->
					move_over_shard_actors(Nd, H, Shard, L, 0, CountAll, P, Varlist,{NextShard1, NextShardNode1})
			end;
		_ ->
			case Next of
				undefined ->
					H;
				{NextShard,NextShardNode} ->
					case get({shard_visited,NextShard}) of
						undefined ->
							put({shard_visited,NextShard},true),
							move_over_shard_actors(NextShardNode, H, NextShard, [], 0, 0, P, Varlist, undefined);
						_ ->
							H
					end
			end
	end;
move_over_shard_actors(Nd,#{statements := [{list,{Limit,Offset}}]} = H,
			Shard, Actors, CountNow, CountAll, P, Varlist, Next) ->
	case get({<<"RESULT">>,cols}) of
		undefined ->
			put({<<"RESULT">>,cols},{<<"actor">>}),
			put({<<"RESULT">>,nrows},0);
		_ ->
			ok
	end,
	CurNRows = get({<<"RESULT">>,nrows}),
	% Count = lists:foldl(fun({Actor},Cnt) ->
	% 			put({<<"RESULT">>,CurNRows+Cnt},{Actor}),
	% 			Cnt+1
	% 		end,0,Actors),
	case list_actors_append(CurNRows,0,Actors,Limit,Offset) of
		{Count,Limit1,Offset1} ->
			put({<<"RESULT">>,nrows},CurNRows+Count),
			move_over_shard_actors(Nd,H#{statements => [{list,{Limit1,Offset1}}]},
				Shard,[],CountNow+Count,CountAll+Count,P,Varlist,Next);
		{done,Count} ->
			put({<<"RESULT">>,nrows},CurNRows+Count),
			H#{statements => [{list,{0,0}}]}
	end;	
move_over_shard_actors(Nd,#{type := _Type, flags := _Flags, statements := _StBin, iswrite := _IsWrite} = H,
		Shard, Actors, CountNow, CountAll, P, Varlist, Next) ->
	Count = lists:foldl(fun({Actor},Cnt) ->
				do_actor(P,true,H#{ actor => Actor},Varlist),
				Cnt+1
			end,0,Actors),
	move_over_shard_actors(Nd,H,Shard,[],CountNow+Count,CountAll+Count,P,Varlist,Next).

list_actors_append(CurNRows,Count,[{Actor}|L],Limit,Offset) when Limit > 0, Offset == 0 ->
	put({<<"RESULT">>,CurNRows+Count},{Actor}),
	list_actors_append(CurNRows,Count+1,L,Limit-1,0);
list_actors_append(CurNRows,C,[_|T],L,Offset) when Offset > 0 ->
	list_actors_append(CurNRows,C+1,T,L,Offset-1);
list_actors_append(_CurNRows,C,[],L,O) ->
	{C,L,O};
list_actors_append(_,C,_,_,_) ->
	{done,C}.

%
% 			TYPE 2 - looping over a list with for
%
do_foreach(P,_,_,{N,N}) ->
	P;
do_foreach(P,ActorColumn, #{type := _Type, flags := _Flags, var := Gvar, blockvar := Blockvar,
	iswrite := _IsWrite, statements := Statements} = H,{N,Max}) ->
	case get({Gvar,N}) of
		undefined ->
			?ADBG("do_foreach no gvar for ~p",[N]),
			P;
		Row ->
			put({Blockvar,cols},{foreach,Gvar,N}),
			Ac = butil:tobin(element(ActorColumn,Row)),
			?ADBG("do_foreach ~p ~p",[N,Ac]),
			{StBin,Varlist} = statements_to_binary(Ac,Statements,<<>>,[]),
			do_actor(P,true,H#{actor := Ac, statements := StBin},Varlist),
			do_foreach(P,ActorColumn,H,{N+1,Max})
	end.


%
% 			TYPE 3 - regular query on single or list of actors
%
do_block(P,IsMulti,#{type := _Type, flags := _Flags, actor := [Actor|T], iswrite := _IsWrite, statements := _Statements} = H,Varlist) ->
	case Actor of
		{Var,Column} ->
			Actor1 = get_pd_column(Var,Column);
		_ ->
			Actor1 = Actor
	end,
	do_actor(P,IsMulti,H#{ actor := Actor1},Varlist),
	do_block(P,IsMulti,H#{ actor := T},Varlist);

do_block(_,_,#{actor := []},_) ->
	ok.



do_actor(_,_,#{statement := <<>>},_) ->
	ok;
do_actor(P,IsMulti,#{type := _Type, flags := _Flags, actor := Actor, iswrite := _IsWrite, statements := Statements1} = H,curactor) ->
	{StBin,Varlist} = statements_to_binary(Actor,Statements1,<<>>,[]),
	do_actor(P, IsMulti, H#{statements := StBin}, Varlist);

do_actor(P,IsMulti,#{type := Type, flags := Flags, actor := Actor, iswrite := IsWrite, statements := Statements1},Varlist) ->
	%todo check statemnts1 for bindingvals
	Call = #{type => Type, actor => Actor, flags => Flags, iswrite => IsWrite, dorpc => true, bindingvals => []},
	case is_tuple(P) of
		true when IsWrite  ->
			case Statements1 of
				[Pragma] when is_atom(Pragma) ->
					Statements = {{P#dp.currow,P#dp.name,bkdcore:node_name()}, [Pragma]};
				_ ->
					Statements = {{P#dp.currow,P#dp.name,bkdcore:node_name()}, Statements1}
			end,
			?ADBG("do_actor ~p.'__mupdate__' write to=~p statements=~p",[P#dp.name,Actor,Statements]),
			case P#dp.confirming of
				undefined ->
					{ExecPid,_} = spawn_monitor(fun() -> exit(actordb:direct_call(Call#{statements => Statements})) end);
				true ->
					{ExecPid,_} = spawn_monitor(fun() -> 
						Call1 = Call#{statements => {{P#dp.currow,P#dp.name,bkdcore:node_name()},commit}},
						exit(actordb:direct_call(Call1)) end);
				false ->
					{ExecPid,_} = spawn_monitor(fun() ->
						Call1 = Call#{statements => {{P#dp.currow,P#dp.name,bkdcore:node_name()},abort}},
						exit(actordb:direct_call(Call1)) end)
			end;
		_ ->
			?ADBG("do_actor read ~p ~p",[Actor,Statements1]),
			{ExecPid,_} = spawn_monitor(fun() -> 
				Call1 = Call#{ statements => Statements1 },
				exit(actordb:direct_call(Call1)) end)
	end,
	receive
		overruled ->
			Res = undefined,
			throw(overruled);
		{'DOWN',_Monitor,_Ref,Pid,Res} when Pid == ExecPid ->
			ok
	end,
	?ADBG("do_actor varlist ~p",[Varlist]),
	case Res of
		{<<_/binary>> = FinalNode,Res1} ->
			put({node,FinalNode},true);
		Res1 ->
			ok
	end,
	% ?ADBG("Res=~p",[Res1]),
	case Res1 of
		{sql_error,Str} ->
			throw({sql_error,Str});
		{sql_error,Str,SqlRes} ->
			throw({sql_error,Str,SqlRes});
		{ok,[{columns,_},{rows,_}] = L} ->
			% ?AINF("Res store vars =~p",[{IsMulti,Actor,Varlist,[L]}]),
			store_vars(IsMulti,Actor,Varlist,[L]);
		{ok,[_|_] = L} ->
			sum_changes(L),
			store_vars(IsMulti,Actor,Varlist,L);
		{ok,{changes,_,NChanges}} ->
			put(nchanges,get(nchanges)+NChanges);
		{ok,_} ->
			ok;
		ok ->
			put(nchanges,get(nchanges)+1);
		{error,nocreate} ->
			ok;
		_ ->
			throw(Res)
	end,
	?ADBG("do_actor res ~p",[Res]),
	ok.

sum_changes([{changes,_,N}|T]) ->
	put(nchanges,get(nchanges)+N),
	sum_changes(T);
sum_changes([_|T]) ->
	sum_changes(T);
sum_changes([]) ->
	ok.

store_vars(IsMulti,{Shard,_},L1,L2) ->
	store_vars(IsMulti,butil:tobin(Shard),L1,L2);
store_vars(IsMulti,Actor,[Varname|T],[[{columns,Cols},{rows,Rows}]|ResRem]) ->
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
	case get({A1,cols}) of
		{foreach,Gvar,GVarIndex} ->
			ColumnKey = {Gvar,cols},
			RowKey = {Gvar,GVarIndex},
			Columns = get(ColumnKey);
		Columns ->
			GVarIndex = 0,
			RowKey = {A1,0},
			ColumnKey = {A1,cols}
	end,
	Val = get_pd_column(A2,C2,GVarIndex),
	?ADBG("Adding columns ~p, val ~p",[{A1,C1,A2,C2},Val]),
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
store_vars(IsMulti,Actor,[_|T],[_|T1]) ->
	store_vars(IsMulti,Actor,T,T1);
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

% Takes a block of sql statements and creates an iolist, filling in any {{..}} variables.
% Also returns a list of result variables ({{Var}}SELECT * ....)
% Statement with no result variables or statement variables.
statements_to_binary(CurActor,{Statements,PrepParams},<<>>,VarList) ->
	{StatementOut,VarlistOut} = statements_to_binary(CurActor,Statements,[],VarList),
	{{StatementOut,PrepParams},VarlistOut};
statements_to_binary(CurActor,L,<<>>,VarList) ->
	statements_to_binary(CurActor,L,[],VarList);
statements_to_binary(CurActor,[<<_/binary>> = B|T],Out,VarList) ->
	statements_to_binary(CurActor,T,[[$$,B]|Out],VarList);
% Not an sql statement but a variable assignment (changing result)
statements_to_binary(CurActor,[{{A1,C1,A2,C2},<<>>}|T],Out,VarList) ->
	statements_to_binary(CurActor,T,Out,[{A1,C1,A2,C2}|VarList]);
% Result var but no statement variables
statements_to_binary(CurActor,[{ResultVar,<<_/binary>> = B}|T],Out,VarList) when is_binary(ResultVar) ->
	statements_to_binary(CurActor,T,[B|Out],[ResultVar|VarList]);
statements_to_binary(_,[X],_,_) when is_atom(X); is_atom(element(1,X)) ->
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
					{ok,Idi} = actordb_idgen:getid(),
					Id = butil:tobin(Idi),
					<<B/binary,Id/binary>>;
				{uniqid,Column} ->
					case get({uniqid,Column}) of
						undefined ->
							{ok,Idi} = actordb_idgen:getid(),
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
				_ ->
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
					statements_to_binary(CurActor,T,[[$$,Bin]|Out],Varlist);
				_ ->
					statements_to_binary(CurActor,T,[Bin|Out],[ResultVar|Varlist])
			end
	end;
statements_to_binary(_CurActor,[],O,Varlist) ->
	{lists:reverse(O),lists:reverse(Varlist)}.

get_pd_column(Var,Column) ->
	get_pd_column(Var,Column,0).
get_pd_column(Var,Column,GIndex) ->
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
					element(Index,get({Var,GIndex}))
			end
	end.

findpos(N,_,Tuple) when N > tuple_size(Tuple) ->
	false;
findpos(N,Val,Tuple) when element(N,Tuple) == Val ->
	N;
findpos(N,Val,Tuple) ->
	findpos(N+1,Val,Tuple).
