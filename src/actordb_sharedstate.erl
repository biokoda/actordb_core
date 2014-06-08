% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sharedstate).
-compile(export_all).
-include("actordb.hrl").
% 
% Implements actor on top of actordb_sqlproc
% 

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 							API
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
-record(st,{name,type,time_since_ping = {0,0,0}, master_group = [], waiting = false}).

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
			actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},
							  {state,State}|Opt]);
		Pid ->
			{ok,Pid}
	end.

start_wait(Name,Type) ->
	start(Name,Type,#st{name = Name,type = Type, waiting = true},[{slave,false},create,lock,{lockinfo,wait}]).

read_global(Key) ->
	read(?STATE_NM_GLOBAL,Key).
read_cluster(Key) ->
	read(?STATE_NM_LOCAL,Key).

write_global(Key,Val) ->
	write(?STATE_NM_GLOBAL,Key,Val).
write_cluster(Key,Val) ->
	write(?STATE_NM_LOCAL,Key,Val).

init_state(Nodes,Groups,{_,_,_} = Configs) ->
	init_state(Nodes,Groups,[Configs]);
init_state(Nodes,Groups,Configs) ->
	case actordb_sqlproc:call({?STATE_NM_GLOBAL,?STATE_TYPE},[],{init_state,Nodes,Groups,Configs},?MODULE) of
		ok ->
			set_init_state(Nodes,Groups,Configs),
			ok;
		_ ->
			error
	end.

get_state() ->
	{ok,Nodes} = read_global(nodes),
	{ok,Groups} = read_global(groups),
	{ok,CL} = application:get_env(bkdcore,cfgfiles),
	Cfgs = [begin 
		{ok,CfgInfo} = read_global(CfgName),
		 {CfgName,CfgInfo}
	 end || {CfgName,_} <- CL],
	 {ok,{Nodes,Groups,Cfgs}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 							Helpers
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
set_init_state(Nodes,Groups,Configs) ->
	[ok = bkdcore_changecheck:setcfg(butil:tolist(Key),Val) || {Key,Val} <- Configs],
	bkdcore_changecheck:set_nodes_groups(Nodes,Groups),
	spawn(fun() -> start(?STATE_NM_LOCAL,?STATE_TYPE) end).

write(Name,Key,Val) ->
	actordb_sqlproc:write({Name,?STATE_TYPE},[create],write_sql(Key,Val),?MODULE).

read(Name,Key) ->
	case actordb_sqlproc:read({Name,?STATE_TYPE},[create],read_sql(Key),?MODULE) of
		{ok,[{columns,_},{rows,[{_,ValEncoded}]}]} ->
			{ok,binary_to_term(base64:decode(ValEncoded))};
		_ ->
			undefined
	end.

read_sql(Key) ->
	[<<"SELECT * FROM state WHERE id=">>,butil:tobin(Key),";"].
write_sql(Key,Val) ->
	[<<"INSERT OR REPLACE INTO state VALUES ('">>,butil:tobin(Key),
		"','",base64:encode(term_to_binary(Val)),"');"].

takemax(N,L) ->
	case length(L) >= N of
		true ->
			{A,_} = lists:split(N,L),
			A;
		false ->
			L
	end.

state_to_sql(Name) -> 
	case Name of
		?STATE_NM_GLOBAL ->
			File = "stateglobal";
		?STATE_NM_LOCAL ->
			File = "statecluster"
	end,
	case butil:readtermfile([bkdcore:statepath(),"/",File]) of
		{_,[_|_]} = State ->
			[[$$,write_sql(Key,Val)] || {{_App,Key},Val} <- State, Key /= master_group];
		_ ->
			[]
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 							Callbacks
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 
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
	[bkdcore:statepath(),"/state/"].

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

cb_idle(S) ->
	Now = os:timestamp(),
	case timer:now_diff(Now,S#st.time_since_ping) >= 1000000 of
		true ->
			self() ! raft_refresh,
			{ok,S#st{time_since_ping = Now}};
		false ->
			ok
	end.

cb_write_done(_S,_Evnum) ->
	ok.

cb_redirected_call(S,_MovedTo,{master_ping,MasterNode,_MasterGroup},moved) ->
	{reply,ok,S,MasterNode};
cb_redirected_call(S,_MovedTo,{master_ping,MasterNode,MasterGroup},slave) ->
	ok;
cb_redirected_call(_,_,_,_) ->
	ok.

% Initialize state on slaves (either inactive or part of master group).
cb_unverified_call(#st{waiting = true} = S,{master_ping,MasterNode,MasterGroup})  ->
	% There is a global master on master node. 
	% Read basic startup state from him.
	% Nodes, Groups, Configs.
	case bkdcore_rpc:call(MasterNode,{?MODULE,get_state,[]}) of
		{ok,{Nodes,Groups,Cfgs}} ->
			set_init_state(Nodes,Groups,Cfgs),
			case lists:member(actordb_conf:node_name(),MasterGroup) of
				false ->
					{moved,MasterNode};
				true ->
					reinit
			end;
		_ ->
			{reply,ok}
	end;
% Initialize state on first master.
cb_unverified_call(S,{init_state,Nodes,Groups,Configs}) ->
	case S#st.waiting of
		false ->
			{reply,{error,already_started}};
		true ->
			Sql = [$$,write_sql(nodes,Nodes),
				   $$,write_sql(groups,Groups),
				   [[$$,write_sql(Key,Val)] || {Key,Val} <- Configs]],
			{reinit,Sql}
	end;
cb_unverified_call(_S,_Msg)  ->
	queue.

cb_nodelist(#st{name = ?STATE_NM_LOCAL} = S,_HasSchema) ->
	% HaveBkdcore = ets:info(bkdcore_nodes,size) > 0,
	case bkdcore:nodelist() of
		[] ->
			?AERR("Local state without nodelist."),
			exit(normal);
		_ ->
			{ok,S,bkdcore:cluster_nodes()}
	end;
cb_nodelist(#st{name = ?STATE_NM_GLOBAL} = S,HasSchema) ->
	case HasSchema of
		true ->
			file:delete([bkdcore:statepath(),"/stateglobal"]),
			{read,read_sql(master_group)};
		false ->
			case butil:readtermfile([bkdcore:statepath(),"/stateglobal"]) of
				{_,[_|_]} = State ->
					Nodes = butil:ds_val({bkdcore,master_group},State);
				_ ->
					case lists:sort(bkdcore:nodelist()) of
						[] = Nodes ->
							?AERR("Global state without nodelist."),
							exit(normal);
						AllNodes ->
							AllClusterNodes = bkdcore:all_cluster_nodes(),
							case length(AllClusterNodes) >= 7 of
								true ->
									{Nodes,_} = lists:split(7,AllClusterNodes);
								false ->
									Nodes = AllClusterNodes ++ takemax(7 - length(AllClusterNodes),AllNodes -- AllClusterNodes)
							end
					end
			end,
			return_mg(S,Nodes)
	end.
cb_nodelist(S,true,{ok,[{columns,_},{rows,[{_,ValEncoded}]}]}) ->
	Nodes = binary_to_term(base64:decode(ValEncoded)),
	return_mg(S,Nodes).

return_mg(S,Nodes) ->
	case lists:member(actordb_conf:node_name(),Nodes) of
		true ->
			{ok,S#st{master_group = Nodes},Nodes -- [actordb_conf:node_name()]};
		false ->
			exit(normal)
	end.

% These only get called on master
cb_call(_Msg,_From,_S) ->
	{reply,{error,uncrecognized_call}}.
cb_cast(_Msg,_S) ->
	noreply.
cb_info(_Msg,_S) ->
	noreply.
cb_init(_S,_EvNum) ->
	ok.



