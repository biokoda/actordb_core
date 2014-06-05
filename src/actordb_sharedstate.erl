% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sharedstate).
-compile(export_all).
-include("actordb.hrl").
-define(NM_LOCAL,<<"local">>).
-define(NM_GLOBAL,<<"global">>).
% 
% Implements actor on top of actordb_sqlproc
% 

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 							API
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
-record(st,{name,type,time_since_ping = {0,0,0}, master_group = []}).

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
start(Name,Type1,Opt) ->
	actordb_util:wait_for_startup(Name,0),
	Type = actordb_util:typeatom(Type1),
	case distreg:whereis({Name,Type}) of
		undefined ->
			actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},
							  {state,#st{name = Name,type = Type}}|Opt]);
		Pid ->
			{ok,Pid}
	end.


read_global(App,Key) ->
	read(?NM_GLOBAL,App,Key).
read_cluster(App,Key) ->
	read(?NM_LOCAL,App,Key).

write_global(App,Key,Val) ->
	write(?NM_GLOBAL,App,Key,Val).
write_cluster(App,Key,Val) ->
	write(?NM_LOCAL,App,Key,Val).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 							Helpers
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

write(Name,App,Key,Val) ->
	actordb_sqlproc:write({Name,?STATE_TYPE},[create],write_sql(App,Key,Val),?MODULE).

read(Name,App,Key) ->
	actordb_sqlproc:read({Name,?STATE_TYPE},[create],read_sql(App,Key),?MODULE).

read_sql(App,Key) ->
	[<<"SELECT * FROM state WHERE id=">>,butil:tobin(App),",",butil:tobin(Key),";"].
write_sql(App,Key,Val) ->
	[<<"INSERT OR REPLACE INTO state VALUES ('">>,butil:tobin(App),",",butil:tobin(Key),
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
		?NM_GLOBAL ->
			File = "stateglobal";
		?NM_LOCAL ->
			File = "statecluster"
	end,
	case butil:readtermfile([bkdcore:statepath(),"/",File]) of
		{_,[_|_]} = State ->
			[[$$,write_sql(App,Key,Val)] || {{App,Key},Val} <- State];
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
		[_|_] when S#st.name == ?NM_GLOBAL ->
			MG = [$$,write_sql(bkdcore,master_group,S#st.master_group)];
		_ ->
			MG = []
	end,
	[Table,MG,state_to_sql(S#st.name)].
schema_version() ->
	1.

cb_path(_,_Name,_Type) ->
	{ok,Pth} = application:get_env(bkdcore,statepath),
	[Pth,"/state/"].

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

cb_redirected_call(S,MovedTo,{master_ping,MasterNode,_MasterGroup},moved) ->
	{reply,ok,S,MasterNode};
cb_redirected_call(S,MovedTo,{master_ping,MasterNode,MasterGroup},slave) ->
	ok;
cb_redirected_call(_,_,_,_) ->
	ok.

cb_nodelist(#st{name = ?NM_LOCAL} = S,_HasSchema) ->
	% HaveBkdcore = ets:info(bkdcore_nodes,size) > 0,
	case bkdcore:nodelist() of
		[] ->
			exit(normal);
		_ ->
			{ok,S,bkdcore:cluster_nodes()}
	end;
cb_nodelist(#st{name = ?NM_GLOBAL} = S,HasSchema) ->
	case HasSchema of
		true ->
			{read,read_sql(bkdcore,master_group)};
		false ->
			case butil:readtermfile([bkdcore:statepath(),"/stateglobal"]) of
				{_,[_|_]} = State ->
					Nodes = butil:ds_val({bkdcore,master_group},State);
				_ ->
					case lists:sort(bkdcore:nodelist()) of
						[] = Nodes ->
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



