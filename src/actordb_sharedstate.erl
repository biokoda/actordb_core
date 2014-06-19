% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sharedstate).
-compile(export_all).
-include("actordb.hrl").
-define(GLOBALETS,globalets).

-define(MASTER_GROUP_SIZE,7).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 							API
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
-record(st,{name,type,time_since_ping = {0,0,0}, 
			master_group = [], waiting = false, 
			current_write = [], evnum = 0, am_i_master = false, timer,
			nodelist, nodepos = 0}).

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
							  {state,State}|Opt]);
		Pid ->
			{ok,Pid}
	end.

start_wait(Name,Type) ->
	start(Name,Type,#st{name = Name,type = Type, waiting = true},[{slave,false},create,lock,{lockinfo,wait}]).

read_global(Key) ->
	case ets:info(?GLOBALETS,size) of
		undefined ->
			nostate;
		_ ->
			butil:ds_val(Key,?GLOBALETS)
	end.
read_cluster(Key) ->
	read(?STATE_NM_LOCAL,Key).

write_global_on(Node,K,V) ->
	case actordb_sqlproc:write({?STATE_NM_GLOBAL,?STATE_TYPE},[create],{{?MODULE,cb_write,[Node,[{K,V}]]},undefined,undefined},?MODULE) of
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


init_state(Nodes,Groups,{_,_,_} = Configs) ->
	init_state(Nodes,Groups,[Configs]);
init_state(Nodes,Groups,Configs) ->
	case actordb_sqlproc:call({?STATE_NM_GLOBAL,?STATE_TYPE},[],{init_state,Nodes,Groups,Configs},?MODULE) of
		ok ->
			ok;
		_ ->
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
	bkdcore_changecheck:set_nodes_groups(Nodes,Groups).
	

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
		"','",base64:encode(term_to_binary(Val,[compressed])),"');"].

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
	% If any cfg changed, call setcfg for it.
	[begin
		NewVal = butil:ds_val(Cfg,State),
		case butil:ds_val(Cfg,?GLOBALETS) of
			OldVal when NewVal /= undefined, OldVal /= NewVal ->
				?ADBG("Setting config ~p",[Cfg]),
				bkdcore_changecheck:setcfg(butil:tolist(Cfg),NewVal);
			_ ->
				ok
		end
	end || Cfg <- cfgnames()],
	% If nodes/groups changed inform changecheck.
	[NewNodes,NewGroups] = butil:ds_vals([nodes,groups],State),
	case ok of
		_ when NewNodes /= undefined andalso NewGroups /= undefined ->
			[OldNodes,OldGroups] = butil:ds_vals([nodes,groups],?GLOBALETS),
			case (NewNodes /= OldNodes orelse NewGroups /= OldGroups) of
			   	true ->
			   		bkdcore_changecheck:set_nodes_groups(NewNodes,NewGroups),
			   		spawn(fun() -> start(?STATE_NM_LOCAL,?STATE_TYPE) end);
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
	AllNodes = bkdcore:nodelist(),
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
	L = lists:delete(actordb_conf:node_name(),bkdcore:nodelist()),
	list_to_tuple(lists:sort(fun(A,B) -> actordb_util:hash([actordb_conf:node_name(), A]) <
						   actordb_util:hash([actordb_conf:node_name(), B]) 
				end,L)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 							Callbacks
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

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
	{[write_sql(Key,Val) || {Key,Val} <- L],S#st{current_write = L}}.
 
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
	P = [bkdcore:statepath(),"/state/"],
	filelib:ensure_dir(P),
	P.

% Start or get pid of slave process for actor (executed on slave nodes in cluster)
cb_slave_pid(Name,Type) ->
	cb_slave_pid(Name,Type,[]).
cb_slave_pid(Name,Type,Opts) ->
	Actor = {Name,Type},
	case distreg:whereis(Actor) of
		undefined ->
			{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},{slave,true},
											  {state,#st{name = Name,type = Type}},create|Opts]),
			{ok,Pid};
		Pid ->
			{ok,Pid}
	end.

cb_candie(_,_,_,_) ->
	never.

cb_checkmoved(_Name,_Type) ->
	undefined.

cb_startstate(Name,Type) ->
	#st{name = Name, type = Type}.

cb_idle(_S) ->
	ok.

cb_write_done(#st{name = ?STATE_NM_LOCAL} = S,Evnum) ->
	?ADBG("cb_write_done ~p",[S#st.name]),
	{ok,check_timer(S#st{evnum = Evnum})};
cb_write_done(#st{name = ?STATE_NM_GLOBAL} = S,Evnum) ->
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
	Now = os:timestamp(),
	case S#st.evnum < Evnum of
		true ->
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

% Initialize state on slaves (either inactive or part of master group).
cb_unverified_call(#st{waiting = true, name = ?STATE_NM_GLOBAL} = S,{master_ping,MasterNode,Evnum,State})  ->
	?ADBG("unverified call ping for global sharedstate",[]),
	[MasterGroup] = butil:ds_vals([master_group],State),
	set_global_state(MasterNode,State),
	% set_init_state(Nodes,Groups,cfgnames()),
	case lists:member(actordb_conf:node_name(),MasterGroup) of
		false ->
			{{moved,MasterNode},S#st{waiting = false, evnum = Evnum}};
		true ->
			reinit
	end;
% Initialize state on first master.
cb_unverified_call(S,{init_state,Nodes,Groups,Configs}) ->
	case S#st.waiting of
		false ->
			{reply,{error,already_started}};
		true ->
			% [bkdcore_changecheck:setcfg(butil:tolist(CfgName),CfgVal) || {CfgName,CfgVal} <- Configs],
			% bkdcore_changecheck:set_nodes_groups(Nodes,Groups),
			set_init_state(Nodes,Groups,Configs),
			[bkdcore_rpc:cast(Nd,{?MODULE,set_init_state_if_none,[Nodes,Groups,Configs]}) || 
				Nd <- bkdcore:nodelist(), Nd /= actordb_conf:node_name()],
			Sql = [$$,write_sql(nodes,Nodes),
				   $$,write_sql(groups,Groups),
				   [[$$,write_sql(Key,Val)] || {Key,Val} <- Configs]],
			{reinit,Sql,S#st{current_write = [{nodes,Nodes},{groups,Groups}|Configs]}}
	end;
cb_unverified_call(_S,_Msg)  ->
	queue.


cb_nodelist(#st{name = ?STATE_NM_LOCAL} = S,_HasSchema) ->
	?ADBG("local nodelist",[]),
	case bkdcore:nodelist() of
		[] ->
			?AERR("Local state without nodelist."),
			exit(normal);
		_ ->
			{ok,S,bkdcore:cluster_nodes()}
	end;
cb_nodelist(#st{name = ?STATE_NM_GLOBAL} = S,HasSchema) ->
	?ADBG("global nodelist",[]),
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
			{ok,S#st{current_write = [{master_group,Nodes}|S#st.current_write], 
					 master_group = Nodes},
			    Nodes -- [actordb_conf:node_name()]};
		false ->
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
	Now = os:timestamp(),
	self() ! raft_refresh,
	case S#st.name of
		?STATE_NM_GLOBAL ->
			Msg = {master_ping,actordb_conf:node_name(),S#st.evnum,ets:tab2list(?GLOBALETS)},
			case S#st.am_i_master of
				true ->
					Pos = S#st.nodepos,
					[bkdcore_rpc:cast(Nd,{actordb_sqlproc,call_slave,
								[?MODULE,S#st.name,S#st.type,Msg]}) || Nd <- S#st.master_group];
				false ->
					Pos = S#st.nodepos+3,
					case tuple_size(S#st.nodelist) >= 3 of
						true ->
							[begin
								Nd = element(((NdPos+S#st.nodepos) rem tuple_size(S#st.nodelist))+1,S#st.nodelist),
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
cb_info(_,_S) ->
	noreply.
cb_init(#st{name = ?STATE_NM_LOCAL} = S,_EvNum) ->
	?ADBG("local cb_init",[]),
	{ok,check_timer(S)};
cb_init(#st{name = ?STATE_NM_GLOBAL} = _S,_EvNum) ->
	?ADBG("global cb_init",[]),
	{doread,<<"select * from state;">>}.
cb_init(S,Evnum,{ok,[{columns,_},{rows,State1}]}) ->
	State = [{butil:toatom(Key),binary_to_term(base64:decode(Val))} || {Key,Val} <- State1],
	set_global_state(actordb_conf:node_name(),State),
	{ok,S#st{evnum = Evnum, waiting = false}}.






