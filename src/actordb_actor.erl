% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_actor).
-compile(export_all).
-include_lib("actordb_core/include/actordb.hrl").
%
% Implements actor on top of actordb_sqlproc
%

-record(st,{name,type,doreg}).

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
	Type = actordb_util:typeatom(Type1),
	actordb_util:wait_for_startup(Type,Name,0),
	case distreg:whereis({Name,Type}) of
		undefined ->
			actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},
				{state,#st{name = Name,type = Type}}|Opt]);
		Pid ->
			{ok,Pid}
	end.

start_steal(Name,Type1,Node,ShardName) ->
	Type = actordb_util:typeatom(Type1),
	?ADBG("Start steal ~p ~p",[Name,Type]),
	{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},
		{state,#st{name = Name,type = Type,doreg = ShardName}},
		{copyfrom,{move,ShardName,Node}},{startreason,{steal,Node}}]),
	{ok,Pid}.


read(Shard, #{actor := Actor, type := Type, flags := _Flags, statements := Sql} = Call) ->
	BV = maps:get(bindingvals, Call, []),
	case actordb_schema:iskv(Type) of
		true ->
			actordb_shard:kvread(Shard,Actor,Type,{Sql,BV});
		_ ->
			read(Call)
	end.
read(#{actor:= Actor, type:= Type, flags := Flags, statements := Sql} = Call) ->
	BV = maps:get(bindingvals, Call, []),
	actordb_sqlproc:read({Actor, Type},Flags,{Sql,BV},?MODULE);
read(#{actor:= Actor, flags := Flags, statements := Sql} = Call) ->
	BV = maps:get(bindingvals, Call, []),
	actordb_sqlproc:read(Actor,Flags,{Sql,BV},?MODULE).

write(Shard, #{actor := Actor, type := Type, flags := _Flags, statements := Sql} = Call) ->
	BV = maps:get(bindingvals, Call, []),
	case actordb_schema:iskv(Type) of
		true ->
			actordb_shard:kvwrite(Shard,Actor,Type,{Sql,BV});
		_ ->
			write(Call)
	end.
write(#{actor:= Actor, type:= Type, flags := Flags, statements := Sql} = Call) ->
	BV = maps:get(bindingvals, Call, []),
	actordb_sqlproc:write({Actor, Type},Flags,{Sql,BV},?MODULE);
write(#{actor:= Actor, flags := Flags, statements := Sql} = Call) ->
	BV = maps:get(bindingvals, Call, []),
	actordb_sqlproc:write(Actor,Flags,{Sql,BV},?MODULE).


%
% Callbacks from actordb_sqlproc
%

% Type = actor type (atom)
% Version = what is current version (0 for no version)
% Return:
% {LatestVersion,IolistSqlStatements}
cb_schema(_,Type,Vers) ->
	actordb_util:type_schema(Type,Vers).
cb_spawnopts(_) ->
	[].

cb_path(_,Name,_Type) ->
	actordb_util:actorpath(Name).

% Start or get pid of slave process for actor (executed on slave nodes in cluster)
cb_slave_pid(Name,Type) ->
	cb_slave_pid(Name,Type,[]).
cb_slave_pid(Name,Type,Opts) ->
	Actor = {Name,Type},
	case distreg:whereis(Actor) of
		undefined ->
			{ok,Pid} = actordb_sqlproc:start([{actor,Name},{state,#st{name = Name,type = Type}},
				{type,Type},{mod,?MODULE},{slave,true},create|Opts]),
			{ok,Pid};
		Pid ->
			{ok,Pid}
	end.

cb_candie(_,_,?CLUSTEREVENTS_TYPE,_) ->
	false;
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
	case catch actordb_shardmngr:find_local_shard(Name,Type) of
		{redirect,_,MovedToNode} ->
			MovedToNode;
		_ ->
			undefined
	end.

cb_startstate(Name,Type) ->
	#st{name = Name, type = Type}.

cb_idle(_S) ->
	ok.

cb_nodelist(S,_HasSchema) ->
	{ok,S,bkdcore:cluster_nodes()}.

cb_replicate_type(_S) ->
	1.

cb_redirected_call(_S,_MovedTo,_Call,_Type) ->
	ok.

cb_unverified_call(_S,_Msg)  ->
	queue.

cb_write_done(_S,_Evnum) ->
	ok.

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
		_ when EvNum == 1 ->
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

cb_init_engine(S) ->
	S.
