% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_actor).
-compile(export_all).
-include("actordb.hrl").
% 
% Implements actor on top of actordb_sqlproc
% 

-record(st,{name,type,doreg}).

start({Name,Type}) ->
	start(Name,Type).
start({Name,Type},Flags) ->
	start(Name,Type,Flags);
start(Name,Type) ->
	start(Name,Type,[]).
start(Name,Type1,Opt) ->
	Type = actordb_util:typeatom(Type1),
	actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},
							  {state,#st{name = Name,type = Type}},{regname,{Name,Type}}|Opt]).

start_steal(Name,Type1,Node,ShardName) ->
	Type = actordb_util:typeatom(Type1),
	?ADBG("Start steal ~p ~p",[Name,Type]),
	{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},{state,#st{name = Name,type = Type,doreg = ShardName}},
									  {regname,{Name,Type}},{copyfrom,Node},{startreason,{steal,Node}}]),
	{ok,Pid}.


read(Shard,{Name,Type} = Actor,Flags,Sql) ->
	case actordb_schema:iskv(Type) of
		true ->
			actordb_shard:kvread(Shard,Name,Type,Sql);
		_ ->
			read(Actor,Flags,Sql)
	end.
read(Actor,Flags,Sql) ->
	actordb_sqlproc:read(Actor,Flags,Sql,?MODULE).

write(Shard,{Name,Type} = Actor,Flags,Sql) ->
	case actordb_schema:iskv(Type) of
		true ->
			actordb_shard:kvwrite(Shard,Name,Type,Sql);
		_ ->
			write(Actor,Flags,Sql)
	end.
write(Actor,Flags,Sql) ->
	actordb_sqlproc:write(Actor,Flags,Sql,?MODULE).






% 
% Callbacks from actordb_sqlproc
% 
 
% Type = actor type (atom)
% Version = what is current version (0 for no version)
% Return:
% {LatestVersion,IolistSqlStatements}
cb_schema(_,Type,Vers) ->
	actordb_util:type_schema(Type,Vers).

cb_path(_,Name,_Type) ->
	actordb_util:actorpath(Name).

% Start or get pid of slave process for actor (executed on slave nodes in cluster)
cb_slave_pid(Name,Type) ->
	cb_slave_pid(Name,Type,[]).
cb_slave_pid(Name,Type,Opts) ->
	Actor = {Name,Type},
	case distreg:whereis(Actor) of
		undefined ->
			{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},{slave,true},{regname,Actor},create|Opts]),
			{ok,Pid};
		Pid ->
			{ok,Pid}
	end.

cb_candie(Mors,Name,_Type,_S) ->
	case Mors of
		master ->
			% Master and is local shard, it is not temporary.
			not actordb_shardmngr:is_local_shard(Name);
		slave ->
			% Slave and is local shard, it is temporary. 
			actordb_shardmngr:is_local_shard(Name)
	end.
cb_checkmoved(Name,Type) ->
	Shard = actordb_shardmngr:find_local_shard(Name,Type),
	case Shard of
		{redirect,_,MovedToNode} ->
			MovedToNode;
		_ ->
			undefined
	end.

cb_startstate(Name,Type) ->
	#st{name = Name, type = Type}.

% These only get called on master
cb_call(_Msg,_From,_S) ->
	{reply,{error,uncrecognized_call}}.
cb_cast(_Msg,_S) ->
	noreply.
cb_info(_Msg,_S) ->
	noreply.
cb_init(S,EvNum) ->
	case ok of
		_ when S#st.type == ?MULTIUPDATE_TYPE ->
			ok;
		_ when S#st.type == ?CLUSTEREVENTS_TYPE ->
			ok;
		_ when S#st.doreg /= undefined ->
			ok = actordb_shard:reg_actor(S#st.doreg,S#st.name,S#st.type);
		_ when EvNum == 0 ->
			LocalShardForReg = actordb_shardmngr:find_local_shard(S#st.name,S#st.type),
			?ADBG("shard for reg ~p",[LocalShardForReg]),
			case LocalShardForReg of
				{redirect,Shard,Node} ->
					actordb:rpc(Node,Shard,{actordb_shard,reg_actor,[Shard,S#st.name,S#st.type]});
				undefined ->
					{Shard,_,Node} = actordb_shardmngr:find_global_shard(S#st.name),
					actordb:rpc(Node,Shard,{actordb_shard,reg_actor,[Shard,S#st.name,S#st.type]});
				Shard ->
					ok = actordb_shard:reg_actor(Shard,S#st.name,S#st.type)
			end;
		_ ->
			ok
	end.



