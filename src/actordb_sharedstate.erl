% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_sharedstate).
-compile(export_all).
% -include_lib("actordb_core/include/actordb.hrl").
-include("actordb_sqlproc.hrl").
-define(GLOBALETS,globalets).
-define(MASTER_GROUP_SIZE,7).
-define(IDMAX_START,1000).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 							API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(st,{name,type,time_since_ping = 0,
master_group = [], waiting = false,
current_write = [], evnum = 0, am_i_master = false, timer,
nodelist, nodepos = 0}).
% Prepared statement. We need version so that an individual actor can detect when
%  an old statement has been deleted and another one has taken its place.
-record(ps,{iswrite, actor_type, version = 0, sql}).

start({Name,Type}) ->
	start(Name,Type).
start({Name,Type},Flags) ->
	case lists:keyfind(slave,1,Flags) of
		false ->
			start(Name,Type,[{slave,false}|Flags]);
		true ->
			start(Name,Type,Flags)
	end;
start(Name,Type) ->
	start(Name,Type,[{slave,false}]).
start(Name,Type,Flags) ->
	start(Name,Type,#st{name = Name,type = Type},Flags).
start(Name,Type1,State,Opt) ->
	Type = actordb_util:typeatom(Type1),
	case distreg:whereis({Name,Type}) of
		undefined ->
			actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},create,
				{state,State},no_election_timeout|Opt]);
		Pid ->
			{ok,Pid}
	end.

start_wait(Name,Type) ->
	start(Name,Type,#st{name = Name,type = Type, waiting = true},[{slave,false},create,
		no_election_timeout,lock,{lockinfo,wait}]).

read_global_users() ->
	case ets:info(?GLOBALETS,size) of
		undefined ->
			nostate;
		_ ->
			case ets:match_object(?GLOBALETS,{users,'$1'}) of
				[{users,OtherUsers}] -> OtherUsers;
				_ -> []
			end
	end.

read_global_users(Username,Host) ->
	case ets:info(?GLOBALETS,size) of
		undefined ->
			nostate;
		_ ->
			OtherUsers = read_global_users(),
			lists:filtermap(fun(User) ->
				case User of
					{_,Username,Host,_} -> {true, User};
					_ -> false
				end
			end, OtherUsers)
	end.

read_global() ->
	ets:tab2list(?GLOBALETS).
read_global(Key) ->
	read_global(Key,undefined).
read_global(Key,Def) ->
	case ets:info(?GLOBALETS,size) of
		undefined ->
			nostate;
		_ ->
			butil:ds_val(Key,?GLOBALETS,Def)
	end.
read_cluster(Key) ->
	read(?STATE_NM_LOCAL,Key).

force_write(L) ->
	case actordb_sqlproc:write({?STATE_NM_GLOBAL,?STATE_TYPE},[create],
			{{?MODULE,cb_force_write,[L]},undefined,undefined},?MODULE) of
		{ok,_} ->
			ok;
		ok ->
			ok;
		Err ->
			Err
	end.

get_id_chunk(Size) ->
	case actordb_sqlproc:write({?STATE_NM_GLOBAL,?STATE_TYPE},[create],
			{{?MODULE,cb_idchunk,[Size]},undefined,undefined},?MODULE) of
		{ok,_} ->
			ok;
		ok ->
			ok;
		Res ->
			Res
	end.

write_global_on(Node,K,V) ->
	case actordb_sqlproc:write({?STATE_NM_GLOBAL,?STATE_TYPE},[create],
			{{?MODULE,cb_write,[Node,[{K,V}]]},undefined,undefined},?MODULE) of
		{ok,_} ->
			ok;
		ok ->
			ok;
		Err ->
			Err
	end.
write_global([_|_] = L) ->
	write(?STATE_NM_GLOBAL,L).
write_global(Key,Val) ->
	write(?STATE_NM_GLOBAL,[{Key,Val}]).
write_cluster([_|_] = L) ->
	write(?STATE_NM_LOCAL,L).
write_cluster(Key,Val) ->
	write(?STATE_NM_LOCAL,[{Key,Val}]).

delete_prepared(<<"#",_,TI:2/binary,SI:2/binary,";">>) ->
	TypeIndex = butil:toint(TI)+1,
	SqlIndex = butil:toint(SI)+1,
	All = butil:ds_val(prepstatements,?GLOBALETS),
	case get_prepared(TypeIndex,SqlIndex,All) of
		P when P#ps.sql /= undefined ->
			Read = {read_sql(prepstatements),{?MODULE,cb_delete_prepared,[TypeIndex,SqlIndex]}},
			case actordb_sqlproc:read({?STATE_NM_GLOBAL,?STATE_TYPE},[create],Read,?MODULE) of
				{ok,_} ->
					ok;
				ok ->
					ok;
				Err ->
					Err
			end;
		_ ->
			ok
	end.
save_prepared(ActorType1,IsWrite,Sql1) ->
	ActorType = actordb_util:typeatom(ActorType1),
	Sql = butil:tobin(Sql1),
	All = butil:ds_val(prepstatements,?GLOBALETS),
	case All of
		undefined ->
			Doit = true;
		_ ->
			case find_prep_actor(ActorType,1,All) of
				undefined ->
					Doit = true;
				{ActorPos,ExistingPrepTuple} ->
					case find_matching_prepsql(1,Sql,ExistingPrepTuple) of
						undefined ->
							Doit = true;
						Pos ->
							Doit = prepared_name(ActorPos,element(Pos,ExistingPrepTuple),Pos)
					end
			end
	end,
	case Doit of
		true ->
			Read = {read_sql(prepstatements),{?MODULE,cb_update_prepared,[ActorType,IsWrite,Sql]}},
			case actordb_sqlproc:read({?STATE_NM_GLOBAL,?STATE_TYPE},[create],Read,?MODULE) of
				{ok,_} ->
					ok;
				ok ->
					ok;
				Err ->
					Err
			end;
		_ ->
			Doit
	end.

init_state(Nodes,Groups,Misc,{_,_,_} = Configs) ->
	init_state(Nodes,Groups,Misc,[Configs]);
init_state(Nodes,Groups,Misc,Configs) ->
	?ADBG("Init state ~p",[Nodes]),
	case actordb_sqlproc:call({?STATE_NM_GLOBAL,?STATE_TYPE},[],{init_state,Nodes,Groups,Misc,Configs},?MODULE) of
		ok ->
			ok;
		_Err ->
			?AERR("Init state error: ~p",[_Err]),
			error
	end.

is_ok() ->
	ets:info(?GLOBALETS,size) /= undefined.

subscribe_changes(Mod) ->
	case application:get_env(actordb,sharedstate_notify) of
		undefined ->
			L = [];
		{ok,L} ->
			ok
	end,
	case ets:info(?GLOBALETS,size) of
		undefined ->
			ok;
		_ ->
			butil:safesend(Mod,{actordb,sharedstate_change})
	end,
	application:set_env(actordb,sharedstate_notify,[Mod|L]).

whois_global_master() ->
	read_global(master).
am_i_global_master() ->
	read_global(master) == actordb_conf:node_name().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 							Helpers
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
set_init_state_if_none(N,G,C) ->
	case bkdcore:nodelist() of
		[] ->
			set_init_state(N,G,C);
		_ ->
			ok
	end.
set_init_state(Nodes,Groups,Configs) ->
	[ok = bkdcore_changecheck:setcfg(butil:tolist(Key),Val) || {Key,Val} <- Configs],
	bkdcore_changecheck:set_nodes_groups(Nodes,Groups),
	actordb_local:connect_all(),
	MG = add_master_group([]),
	case lists:member(actordb_conf:node_name(),MG) of
		true ->
			butil:safesend(actordb_tunnel, {raft_connections,lists:delete(actordb_conf:node_name(), MG)});
		false ->
			butil:safesend(actordb_tunnel, {raft_connections,bkdcore:cluster_nodes()})
	end.


cfgnames() ->
	{ok,CL} = application:get_env(bkdcore,cfgfiles),
	[CfgName || {CfgName,_} <- CL].

write(Name,L) ->
	case actordb_sqlproc:write({Name,?STATE_TYPE},[create],{{?MODULE,cb_write,[L]},undefined,undefined},?MODULE) of
		{ok,_} ->
			ok;
		ok ->
			ok;
		Err ->
			Err
	end.

read(Name,Key) ->
	case actordb_sqlproc:read({Name,?STATE_TYPE},[create],read_sql(Key),?MODULE) of
		{ok,[{columns,_},{rows,[{_,ValEncoded}]}]} ->
			binary_to_term(base64:decode(ValEncoded));
		_ ->
			undefined
	end.

read_sql({A,B}) ->
	read_sql([butil:tobin(A),",",butil:tobin(B)]);
read_sql(Key) ->
	[<<"SELECT * FROM state WHERE id='">>,butil:tobin(Key),"';"].
write_sql({A,B},Val) ->
	write_sql([butil:tobin(A),",",butil:tobin(B)],Val);
write_sql(Key,Val) ->
	[<<"INSERT OR REPLACE INTO state VALUES ('">>,butil:tobin(Key),
		"','",base64:encode(term_to_binary(Val)),"');"].

state_to_sql(Name) ->
	case Name of
		?STATE_NM_GLOBAL ->
			File = "stateglobal";
		?STATE_NM_LOCAL ->
			File = "statecluster"
	end,
	case butil:readtermfile([bkdcore:statepath(),"/",File]) of
		{_,[_|_] = State} ->
			[[$$,write_sql(Key,Val)] || {{_App,Key},Val} <- State, Key /= master_group];
		_ ->
			[]
	end.

set_global_state(MasterNode,State) ->
	?ADBG("Setting global state ~p",[State]),
	case ets:info(?GLOBALETS,size) of
		undefined ->
			ets:new(?GLOBALETS, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{read_concurrency,true}]);
		_ ->
			ok
	end,
	case butil:ds_val(prepstatements,State) of
		undefined ->
			ok;
		NewPrepTuples ->
			case butil:ds_val(prepstatements,?GLOBALETS) of
				NewPrepTuples ->
					ok;
				_ ->
					ListProp = tuple_to_list(NewPrepTuples),
					Vers = list_to_tuple([list_to_tuple([PS#ps.version || PS <- tuple_to_list(PT)]) || {_Type,PT} <- ListProp]),
					Sqls = list_to_tuple([list_to_tuple([PS#ps.sql || PS <- tuple_to_list(PT)]) || {_Type,PT} <- ListProp]),
					% ?AINF("Setting new prepstatements table ~p~n~p",[Vers,Sqls]),
					actordb_sqlite:store_prepared_table(Vers,Sqls)
			end
	end,
	% If any cfg changed, call setcfg for it.
	[begin
		Cfg = butil:toatom(Cfg1),
		NewVal = butil:ds_val(Cfg,State),
		case butil:ds_val(Cfg,?GLOBALETS) of
			OldVal when NewVal /= undefined, OldVal /= NewVal ->
				?ADBG("Setting config ~p",[Cfg]),
				bkdcore_changecheck:setcfg(butil:tolist(Cfg),NewVal);
			_ ->
				ok
		end
	end || Cfg1 <- cfgnames()],
	% If nodes/groups changed inform changecheck.
	[NewNodes,NewGroups] = butil:ds_vals([nodes,groups],State),
	case ok of
		_ when NewNodes /= undefined andalso NewGroups /= undefined ->
			[OldNodes,OldGroups] = butil:ds_vals([nodes,groups],?GLOBALETS),
			case (NewNodes /= OldNodes orelse NewGroups /= OldGroups) of
				true ->
					bkdcore_changecheck:set_nodes_groups(NewNodes,NewGroups),
					% GlobGroupCfg = [{G,bkdcore:nodelist(G)} || G <- bkdcore:groups_of_type(cluster)],
					% application:set_env(kernel,global_groups,GlobGroupCfg),
					% global_group:sync(),
					% global:sync(),
					actordb_local:connect_all(),
					actordb_local:mod_netchanges(),
					spawn(fun() ->timer:sleep(500),?ADBG("Nodes: ~p",[nodes()]),
									start(?STATE_NM_LOCAL,?STATE_TYPE,[{slave,length(nodes()) > 0},{startreason,startup}]) end);
				false ->
					ok
			end;
		_ ->
			ok
	end,
	ets:insert(?GLOBALETS,[{master,MasterNode}|State]),
	case application:get_env(actordb,sharedstate_notify) of
		{ok,[_|_] = L} ->
			[butil:safesend(Somewhere,{actordb,sharedstate_change}) || Somewhere <- L];
		_ ->
			ok
	end.

check_timer(S) ->
	case S#st.timer of
		undefined ->
			S#st{timer = erlang:send_after(1000,self(),ping_timer)};
		T ->
			case erlang:read_timer(T) of
				false ->
					S#st{timer = erlang:send_after(1000,self(),ping_timer)};
				_ ->
					S
			end
	end.

add_master_group(ExistingGroup) ->
	AllNodes = bkdcore:nodelist_allclusters(),
	ClusterCandidates = bkdcore:all_cluster_nodes() -- ExistingGroup,
	case length(ExistingGroup)+length(ClusterCandidates) >= ?MASTER_GROUP_SIZE of
		true ->
			{Nodes,_} = lists:split(?MASTER_GROUP_SIZE,ClusterCandidates);
		false ->
			Nodes = ClusterCandidates ++ takemax(?MASTER_GROUP_SIZE - length(ClusterCandidates) - length(ExistingGroup),
				(AllNodes -- ClusterCandidates) -- ExistingGroup)
	end,
	Nodes.

takemax(N,L) when N > 0 ->
	case length(L) >= N of
		true ->
			{A,_} = lists:split(N,L),
			A;
		false ->
			L
	end;
takemax(_,_) ->
	[].

create_nodelist() ->
	L1 = lists:delete(actordb_conf:node_name(),bkdcore:nodelist()),
	Masters = butil:ds_val(master_group,?GLOBALETS),
	case lists:member(actordb_conf:node_name(), Masters) of
		true ->
			L = L1 -- Masters;
		false ->
			L = []
	end,
	list_to_tuple(lists:sort(fun(A,B) -> actordb_util:hash([actordb_conf:node_name(), A]) <
						   actordb_util:hash([actordb_conf:node_name(), B])
				end,L)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 							Callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Prepared statements are stored for all types in a single value.
% We do not use property lists but property tuples. Actor type position is important and needs to be fixed,
% because it gets passed on to driver.
% {
%  {ActorType,{#ps{},#ps{},#ps{},...}},
%  {ActorType1,{#ps{},#ps{},..}}
% }
% Tuple is increased in size with new prepared statements. Max size is 100.
cb_update_prepared(S,Result,ActorType,IsWrite,PrepSql) ->
	NewPS = #ps{iswrite = IsWrite, sql = PrepSql, actor_type = ActorType},
	case Result of
		{ok,[{columns,_},{rows,[]}]} ->
			PrepOut = {{ActorType,{NewPS}}},
			prepared_reply(S,NewPS,1,1,PrepOut);
		{ok,[{columns,_},{rows,[{_,Val}]}]} ->
			AllPreps = binary_to_term(base64:decode(Val)),
			case find_prep_actor(ActorType,1,AllPreps) of
				undefined when tuple_size(AllPreps) < 100 ->
					PrepOut = append_tuple({ActorType,{NewPS}},AllPreps),
					prepared_reply(S,NewPS,1,tuple_size(PrepOut),PrepOut);
				undefined ->
					{reply,max_types};
				{ActorPos,ExistingPrepTuple} ->
					case find_matching_prepsql(1,PrepSql,ExistingPrepTuple) of
						undefined ->
							case find_free_prep_el(1,ExistingPrepTuple) of
								undefined when tuple_size(ExistingPrepTuple) < 100 ->
									% Add NewPS to end of tuple
									NewPrepTuple = append_tuple(NewPS,ExistingPrepTuple),
									PrepOut = setelement(ActorPos,AllPreps,{ActorType,NewPrepTuple}),
									prepared_reply(S,NewPS,tuple_size(NewPrepTuple),ActorPos,PrepOut);
								undefined ->
									{reply,max_prepared};
								N ->
									OldPS = element(N,ExistingPrepTuple),
									NewPrepTuple = setelement(N,ExistingPrepTuple,NewPS#ps{version = OldPS#ps.version+1}),
									PrepOut = setelement(ActorPos,AllPreps,{ActorType,NewPrepTuple}),
									prepared_reply(S,NewPS,N,ActorPos,PrepOut)
							end;
						N ->
							OldPS = element(N,ExistingPrepTuple),
							{reply,prepared_name(ActorPos,OldPS,N)}
					end
			end
	end.
cb_delete_prepared(S,Result,TypeIndex,SqlIndex) ->
	case Result of
		{ok,[{columns,_},{rows,[]}]} ->
			{reply,ok};
		{ok,[{columns,_},{rows,[{_,Val}]}]} ->
			AllPreps = binary_to_term(base64:decode(Val)),
			case get_prepared(TypeIndex,SqlIndex,AllPreps) of
				P when P#ps.sql /= undefined ->
					{Name,Sqls1} = element(TypeIndex,AllPreps),
					Sqls = setelement(SqlIndex,Sqls1,P#ps{sql = undefined, version = P#ps.version+1}),
					NewPreps = setelement(TypeIndex,AllPreps,{Name,Sqls}),
					{reply_write,ok,write_sql(prepstatements,NewPreps),S#st{current_write = append_cw([{prepstatements,NewPreps}],S#st.current_write)}};
				_ ->
					{reply,ok}
			end
	end.

append_tuple(Val,Tuple) ->
	{_,ExistingList} = lists:foldl(fun(Prep,{N,List}) -> {N+1,[{N+1,Prep}|List]} end,{0,[]},tuple_to_list(Tuple)),
	N = tuple_size(Tuple)+1,
	NewList = [{N,Val}|lists:reverse(ExistingList)],
	erlang:make_tuple(N,#ps{},NewList).
get_prepared(TypeIndex,SqlIndex,All) ->
	case TypeIndex =< tuple_size(All) of
		true ->
			{_Name,Sqls} = element(TypeIndex,All),
			case SqlIndex =< tuple_size(Sqls) of
				true ->
					element(SqlIndex,Sqls);
				false ->
					undefined
			end;
		_ ->
			undefined
	end.
prepared_name(ActorTypeIndex,PS,N) ->
	Index = butil:tobin([string:right(butil:tolist(ActorTypeIndex-1),2,$0),string:right(butil:tolist(N-1),2,$0)]),
	case PS#ps.iswrite of
		true ->
			<<"#w",Index/binary,";">>;
		false ->
			<<"#r",Index/binary,";">>
	end.
prepared_reply(S,PS,N,ActorTypeIndex,PrepOut) ->
	{reply_write,
		prepared_name(ActorTypeIndex,PS,N),
		write_sql(prepstatements,PrepOut),
		S#st{current_write = append_cw([{prepstatements,PrepOut}],S#st.current_write)}}.

find_prep_actor(Actor,N,Prep) when N =< tuple_size(Prep) ->
	case element(N,Prep) of
		{Actor,Tuple} ->
			{N,Tuple};
		_ ->
			find_prep_actor(Actor,N+1,Prep)
	end;
find_prep_actor(_,_,_) ->
	undefined.


find_matching_prepsql(N,Sql,T) when N =< tuple_size(T) ->
	case element(N,T) of
		#ps{sql = Sql} ->
			N;
		_ ->
			find_matching_prepsql(N+1,Sql,T)
	end;
find_matching_prepsql(_,_,_) ->
	undefined.

find_free_prep_el(N,PT) when N =< tuple_size(PT) ->
	case element(N,PT) of
		#ps{sql = undefined} ->
			N;
		_ ->
			find_free_prep_el(N+1,PT)
	end;
find_free_prep_el(_,_) ->
	undefined.

cb_force_write(#st{name = ?STATE_NM_GLOBAL} = S,L) ->
	{Sql,NS} = cb_write(S,L),
	{isolate,Sql,NS}.

append_cw({K,V},CW) ->
	append_cw([{K,V}],CW);
append_cw([{K,V}|T],CW) ->
	append_cw(T,lists:keystore(K,1,CW,{K,V}));
append_cw([],CW) ->
	CW.


cb_idchunk(S,Chunk) ->
	case lists:keyfind(idmax,1,S#st.current_write) of
		{idmax,From} ->
			ok;
		false ->
			From = butil:ds_val(idmax,?GLOBALETS)
	end,
	{reply,{From+1,From+Chunk},write_sql(idmax,From+Chunk),S#st{current_write = append_cw({idmax,From+Chunk},S#st.current_write)}}.

cb_write(#st{name = ?STATE_NM_GLOBAL} = S,Master,L) ->
	Me = actordb_conf:node_name(),
	case Me == Master of
		true ->
			cb_write(S,L);
		false ->
			{reply,{master_is,Me}}
	end.

cb_write(#st{name = ?STATE_NM_LOCAL} = _S,L) ->
	?ADBG("Write local ~p",[L]),
	[write_sql(Key,Val) || {Key,Val} <- L];
cb_write(#st{name = ?STATE_NM_GLOBAL} = S, L) ->
	{[write_sql(Key,Val) || {Key,Val} <- L],S#st{current_write = append_cw(L,S#st.current_write)}}.

% Type = actor type (atom)
% Version = what is current version (0 for no version)
% Return:
% {LatestVersion,IolistSqlStatements}
cb_schema(S,_Type,Version) ->
	case schema_version() > Version of
		true ->
			{schema_version(),[schema(S,N) || N <- lists:seq(Version+1,schema_version())]};
		false ->
			{Version,[]}
	end.
schema(S,1) ->
	Table = <<"$CREATE TABLE state (id TEXT PRIMARY KEY, val TEXT) WITHOUT ROWID;">>,
	case S#st.master_group of
		[_|_] when S#st.name == ?STATE_NM_GLOBAL ->
			MG = [$$,write_sql(master_group,S#st.master_group)];
		_ ->
			MG = []
	end,
	[Table,MG,state_to_sql(S#st.name)].
schema_version() ->
	1.

cb_path(_,_Name,_Type) ->
	"state/".

% Start or get pid of slave process for actor (executed on slave nodes in cluster)
cb_slave_pid(Name,Type) ->
	cb_slave_pid(Name,Type,[]).
cb_slave_pid(?STATE_NM_GLOBAL,?STATE_TYPE,Opts) ->
	actordb_sharedstate:start_wait(?STATE_NM_GLOBAL,?STATE_TYPE);
cb_slave_pid(Name,?STATE_TYPE,Opts) ->
	start_slave(Name,Opts).

start_slave(Name,Opts) ->
	Type = ?STATE_TYPE,
	Actor = {Name,Type},
	case distreg:whereis(Actor) of
		undefined ->
			{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},{slave,true},
				{state,#st{name = Name,type = Type}},create|Opts]),
			{ok,Pid};
		Pid ->
			{ok,Pid}
	end.

% have_state() ->
% 	StPth = actordb_sharedstate:cb_path(1,2,3)++butil:tolist(?STATE_NM_GLOBAL)++"."++butil:tolist(?STATE_TYPE),
% 	{ok,_Db,SchemaTables,_PageSize} = actordb_sqlite:init(StPth,wal),
% 	exit(SchemaTables /= []).

cb_candie(_,_,_,_) ->
	never.

cb_checkmoved(_Name,_Type) ->
	undefined.

cb_startstate(Name,Type) ->
	#st{name = Name, type = Type}.

cb_idle(_S) ->
	ok.

cb_write_done(#st{name = ?STATE_NM_LOCAL} = S,_Term,Evnum) ->
	?ADBG("cb_write_done ~p",[S#st.name]),
	{ok,check_timer(S#st{evnum = Evnum})};
cb_write_done(#st{name = ?STATE_NM_GLOBAL} = S,_Term,Evnum) ->
	?ADBG("cb_write_done ~p",[S#st.name]),
	set_global_state(actordb_conf:node_name(), S#st.current_write),
	NS = check_timer(S#st{current_write = [], evnum = Evnum, am_i_master = true}),

	Masters = butil:ds_val(master_group,?GLOBALETS),
	?ADBG("Global write done masters ~p",[Masters]),
	case [Nd || Nd <- Masters, bkdcore:node_address(Nd) == undefined] of
		[] when length(Masters) < ?MASTER_GROUP_SIZE ->
			case add_master_group(Masters) of
				[] ->
					?ADBG("No nodes to add to masters ~p",[bkdcore:nodelist()]),
					ok;
				New ->
					?AINF("Adding new node to master group ~p",[New]),
					spawn(fun() -> write_global(master_group,New++Masters) end)
			end;
		[] ->
			ok;
		SomeRemoved ->
			WithoutRemoved = Masters -- SomeRemoved,
			case add_master_group(WithoutRemoved) of
				[] ->
					spawn(fun() -> write_global(master_group,WithoutRemoved) end);
				New ->
					spawn(fun() -> write_global(master_group,New++WithoutRemoved) end)
			end
	end,
	{ok,NS#st{master_group = Masters}}.

% We are redirecting calls (so we know who master is and state is established).
% But master_ping needs to be handled. It tells us if state has changed.
cb_redirected_call(S,MovedTo,{master_ping,MasterNode,Evnum,State},_MovedOrSlave) ->
	% ?ADBG("received ping ~p",[S#st.name]),
	Now = actordb_local:elapsed_time(),
	case S#st.evnum < Evnum of
		true ->
			?ADBG("Setting new state from ping"),
			case S#st.name of
				?STATE_NM_GLOBAL ->
					set_global_state(MasterNode,State);
				?STATE_NM_LOCAL ->
					ok
			end,
			{reply,ok,check_timer(S#st{evnum = Evnum, nodelist = create_nodelist(),
										time_since_ping = Now, am_i_master = false}),MasterNode};
		false ->
			{reply,ok,check_timer(S#st{time_since_ping = Now, am_i_master = false}),MovedTo}
	end;
cb_redirected_call(_,_,_,_) ->
	ok.

check_bckp() ->
	{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init("globalbckp",wal),
	case SchemaTables of
		[] ->
			?AINF("Did not find globalbckp"),
			Users = Schema = [],
			NumShards = ?NUM_SHARDS,
			Idmax = ?IDMAX_START;
		_ ->
			% case actordb_sqlite:exec(Db,"select * from state where id
				% in ('idmax','schema.yaml','users','shards','num_shards');") of
			case actordb_sqlite:exec(Db,"select * from state;") of
				{ok,[{columns,_},{rows,[_|_] = PL}]} ->
					case butil:ds_val(<<"users">>,PL) of
						undefined ->
							Users = [];
						UsersBin ->
							Users = binary_to_term(base64:decode(UsersBin))
					end,
					case butil:ds_val(<<"num_shards">>,PL) of
						undefined ->
							NumShards = application:get_env(actordb_core,num_shards,?NUM_SHARDS);
						_ ->
							NumShards = binary_to_term(base64:decode(butil:ds_val(<<"num_shards">>,PL)))
					end,
					Schema = binary_to_term(base64:decode(butil:ds_val(<<"schema.yaml">>,PL))),
					Idmax = binary_to_term(base64:decode(butil:ds_val(<<"idmax">>,PL)));
				_INBCKP ->
					Users = Schema = [],
					NumShards = ?NUM_SHARDS,
					Idmax = ?IDMAX_START
			end,
			actordb_sqlite:wal_rewind(Db,0),
			?AINF("Used idmax from backup, schema=~p, idmax=~p, users=~p",[Schema,Idmax,Users])
	end,
	{Schema,Users,Idmax,NumShards}.

% Initialize state on slaves (either inactive or part of master group).
cb_unverified_call(#st{waiting = true, name = ?STATE_NM_GLOBAL} = S,{master_ping,MasterNode,Evnum,State})  ->
	?ADBG("unverified call ping for global sharedstate",[]),
	[MasterGroup] = butil:ds_vals([master_group],State),
	set_global_state(MasterNode,State),
	case lists:member(actordb_conf:node_name(),MasterGroup) of
		false ->
			{{moved,MasterNode},S#st{waiting = false, evnum = Evnum}};
		true ->
			{reinit_master,slave}
	end;
% Initialize state on first master.
cb_unverified_call(S,{init_state,Nodes,Groups,Misc1,Configs1}) ->
	case S#st.waiting of
		false ->
			{reply,{error,already_started}};
		true ->
			% If there is backup, read some data that we should move over
			{Schema,Users,IdMax,NumShards} = check_bckp(),
			% If config is being saved again, ignore old data.
			case butil:ds_val('schema.yaml',Configs1) of
				NoneSch when NoneSch == []; NoneSch == undefined ->
					Configs = append_cw({'schema.yaml',Schema},Configs1);
				_ ->
					Configs = Configs1
			end,
			% If users are being saved, ignore old ones
			case butil:ds_val(users,Misc1) of
				None when None == []; None == undefined ->
					Misc = append_cw({users,Users},Misc1);
				_ ->
					Misc = Misc1
			end,
			set_init_state(Nodes,Groups,Configs),
			[bkdcore_rpc:cast(Nd,{?MODULE,set_init_state_if_none,[Nodes,Groups,Configs]}) ||
				Nd <- bkdcore:nodelist_allclusters(), Nd /= actordb_conf:node_name()],
			timer:sleep(100),
			Sql = [$$,write_sql(nodes,Nodes),
				   $$,write_sql(groups,Groups),
				   $$,write_sql(num_shards,NumShards),
				   $$,write_sql(idmax,IdMax),  % Default idmax or from backup
				   [[$$,write_sql(Key,Val)] || {Key,Val} <- Misc],
				   [[$$,write_sql(Key,Val)] || {Key,Val} <- Configs]],
			?ADBG("Writing init state ~p",[Sql]),
			{reinit,Sql,S#st{current_write = [{idmax,IdMax},{nodes,Nodes},{groups,Groups}|Misc++Configs]}}
	end;
cb_unverified_call(S,#write{mfa = {?MODULE,cb_force_write,[L]}}) ->
	cb_force_write(S,L);
cb_unverified_call(_S,_Msg)  ->
	queue.


cb_nodelist(#st{name = ?STATE_NM_LOCAL} = S,_HasSchema) ->
	case bkdcore:nodelist() of
		[] ->
			?AERR("Local state without nodelist."),
			exit(normal);
		_L ->
			?ADBG("local nodelist=~p",[_L]),
			{ok,S,bkdcore:cluster_nodes()}
	end;
cb_nodelist(#st{name = ?STATE_NM_GLOBAL} = S,HasSchema) ->
	?ADBG("global nodelist haveschema=~p",[HasSchema]),
	case HasSchema of
		true ->
			file:delete([bkdcore:statepath(),"/stateglobal"]),
			% {read,read_sql(master_group)};
			{read,<<"select * from state;">>};
		false ->
			case butil:readtermfile([bkdcore:statepath(),"/stateglobal"]) of
				{_,[_|_] = State} ->
					Nodes = butil:ds_val({bkdcore,master_group},State);
				_ ->
					case lists:sort(bkdcore:nodelist()) of
						[] = Nodes ->
							?AERR("Global state without nodelist."),
							exit(normal);
						_ ->
							Nodes = add_master_group([])
					end
			end,
			return_mg(S,Nodes)
	end.
cb_nodelist(S,true,{ok,[{columns,_},{rows,Rows}]} = ReadResult) ->
	case bkdcore:nodelist() of
		[] ->
			{ok,NS} = cb_init(S,0,ReadResult);
		_ ->
			NS = S
	end,
	Nodes = binary_to_term(base64:decode(butil:ds_val(<<"master_group">>,Rows))),
	return_mg(NS,Nodes).

return_mg(S,Nodes) ->
	case lists:member(actordb_conf:node_name(),Nodes) of
		true ->
			{ok,S#st{current_write = append_cw({master_group,Nodes},S#st.current_write),
					master_group = Nodes},
				Nodes -- [actordb_conf:node_name()]};
		false ->
			?AERR("Not member of mg ~p ~p",[actordb_conf:node_name(), Nodes]),
			exit(normal)
	end.

cb_replicate_type(#st{name = ?STATE_NM_GLOBAL} = _S) ->
	2;
cb_replicate_type(_) ->
	1.

% These only get called on master
cb_call(_Msg,_From,_S) ->
	{reply,{error,uncrecognized_call}}.
cb_cast(_Msg,_S) ->
	noreply.

% Either global or cluster master executes timer. Master always pings slaves. Slaves ping
%  passive nodes (nodes outside master_group)
cb_info(ping_timer,#st{am_i_master = false,nodelist = undefined} = S)  ->
	cb_info(ping_timer,S#st{nodelist = create_nodelist()});
cb_info(ping_timer,#st{} = S)  ->
	Now = actordb_local:elapsed_time(),
	% self() ! raft_refresh,
	case S#st.name of
		?STATE_NM_GLOBAL ->
			Msg = {master_ping,actordb_conf:node_name(),S#st.evnum,ets:tab2list(?GLOBALETS)},
			case S#st.am_i_master of
				true ->
					% ?ADBG("Pinging nodes amimaster=~p, nodes=~p",[S#st.am_i_master,S#st.master_group]),
					Pos = S#st.nodepos,
					[bkdcore_rpc:cast(Nd,{actordb_sqlproc,call_slave,
								[?MODULE,S#st.name,S#st.type,Msg]}) || Nd <- S#st.master_group, Nd /= actordb_conf:node_name()];
				false ->
					Pos = S#st.nodepos+3,
					case tuple_size(S#st.nodelist) >= 3 of
						true ->
							[begin
								Nd = element(((NdPos+S#st.nodepos) rem tuple_size(S#st.nodelist))+1,S#st.nodelist),
								% ?ADBG("Pinging node=~p",[Nd]),
								bkdcore_rpc:cast(Nd,{actordb_sqlproc,call_slave,
										[?MODULE,S#st.name,S#st.type,Msg]})
							 end || NdPos <- lists:seq(0,2)];
						false ->
							[begin
								bkdcore_rpc:cast(Nd,{actordb_sqlproc,call_slave,
										[?MODULE,S#st.name,S#st.type,Msg]})
							 end || Nd <- tuple_to_list(S#st.nodelist)]
					end
			end;
		_ ->
			Pos = S#st.nodepos
	end,
	{noreply,check_timer(S#st{time_since_ping = Now, nodepos = Pos})};
cb_info(_Msg,_S) ->
	?AERR("Invalid info msg ~p ~p",[_Msg,_S]),
	noreply.
cb_init(#st{name = ?STATE_NM_LOCAL} = S,_EvNum) ->
	?ADBG("local cb_init",[]),
	{ok,check_timer(S)};
cb_init(#st{name = ?STATE_NM_GLOBAL} = _S,_EvNum) ->
	?ADBG("global cb_init",[]),
	{doread,<<"select * from state;">>}.
cb_init(S,Evnum,{ok,[{columns,_},{rows,State1}]}) ->
	State = [{butil:toatom(Key),binary_to_term(base64:decode(Val))} || {Key,Val} <- State1],

	Nodes = butil:ds_val(nodes,State),
	case Nodes of
		[_|_] ->
			Me = element(1,bkdcore_changecheck:read_node(butil:tolist(node()))),
			Nodes1 = [element(1,bkdcore_changecheck:read_node(Nd)) || Nd <- Nodes],
			case lists:member(Me,Nodes1) of
				false ->
					?AERR("Local node not part of node list. Me=~p, Nodes=~p",[Me,Nodes1]),
					spawn(fun() -> actordb:stop() end),
					timer:sleep(2000),
					spawn(fun() -> init:stop() end);
				true ->
					ok
			end;
		_ ->
			ok
	end,
	?ADBG("Init Setting global state ~p",[State]),
	set_global_state(actordb_conf:node_name(),State),
	{ok,S#st{evnum = Evnum, waiting = false}}.

cb_init_engine(S) ->
	S.
cb_spawnopts(_) ->
	[].
