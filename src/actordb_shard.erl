% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_shard).
-define(LAGERDBG,true).
-export([start/1,start/2,start/3,start/4,start_steal/5,
		 whereis/2,try_whereis/2,reg_actor/3,is_reg/3,
		top_actor/2,actor_stolen/5,print_info/2,list_actors/5,count_actors/3,del_actor/3,
		kvread/4,kvwrite/4,get_schema_vers/2]).
-export([cb_list_actors/4, cb_reg_actor/2,cb_del_move_actor/5,cb_schema/3,cb_path/3,cb_idle/1,cb_do_cleanup/2,cb_nodelist/2,
		 cb_slave_pid/2,cb_slave_pid/3,cb_call/3,cb_cast/2,cb_info/2,cb_init/2,cb_init/3,cb_del_actor/2,cb_kvexec/3,
		 cb_redirected_call/4,cb_write_done/3,cb_unverified_call/2,cb_replicate_type/1,
		 newshard_steal_done/3,origin_steal_done/4,cb_candie/4,cb_checkmoved/2,cb_startstate/2, 
		 cb_init_engine/1, cb_spawnopts/1]).
-include("actordb.hrl").
-define(META_NEXT_SHARD,$1).
-define(META_NEXT_SHARD_NODE,$2).
-define(META_CLEANUP_PRE,$3).
-define(META_CLEANUP_AFTER,$4).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%												Explanation
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% A shard is a chunk of hash space for a specific actor type.
% If actordb has multiple types of actors, they all have seperate shards.
% Shard maintains an SQL table of actors that belong to it. Like actordb_actor it runs on top of actordb_sqlproc.
%
% Shard DB table:
% ActorNameHash: actors are hashed across cluster, saving hash to DB enables queries like list of actors that should run
%                on a specific server.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state,{idtype,name,type,to,
	splitcopyfrom,splitcopynode,
	nextshard,nextshardnode,
	% Node name from which shard is copying
	stealingfrom,stealingfromshard,
	% Which actor is in the process of moving atm
	stealingnow, stealingnowpid,stealingnowmon,
	% KV shards and shards that are moving within the same cluster are copied entirely then split in half.
	% Splitting is not done in a single operation but in chunks (~1000 keys at a time).
	cleanup_proc, cleanup_pre, cleanup_after}).

% - Re-balancing (resharding) normal actor types
%   Replicate half of shard by moving upper edge of shard actor-by-actor down.
%   1. Start actor with highest hash value. Tell him to replicate to another node (same method as inter-cluster replication)
%   2. Once moved, lower shard upper limit towards next highest hash value actor. Limit is stored in some public ETS.
%      Every time actordb_shardtree is accessed, check this public ETS table if shard is lowering upper limit.
%      actordb_shardtree is only changed once entire upper half of shard is moved over to another node.
% - Re-balancing kv type
%   1. Pick a shard
%   2. Start copying entire shard to new cluster.
%   2. Once copy completes, delete on both sides. Original keeps lower half, new copy keeps upper half.
%      Original has space between points P1 and P2.
%      Original keeps data between hashes P1 and (P1 + (P2-P1)/2)
%      New copy keeps data between hashes (P1 + 1 + (P2-P1)/2) and P2

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%											API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start({ShardName,Type}) ->
	start(ShardName,Type).
start({Name,Type},Flags) ->
	start(Name,Type,false,Flags);
start(Name,Type1) ->
	start(Name,Type1,false).
start(Name,Type,Opt) when is_list(Opt) ->
	start(Name,Type,false,Opt);
start(Name,Type1,Slave) when is_atom(Slave) ->
	start(Name,Type1,Slave,[]).
start(Name,Type1,Slave,Opt) ->
	?ADBG("shard start ~p ~p ~p",[Name,Type1,Opt]),
	Type = butil:toatom(Type1),
	actordb_util:wait_for_startup(Type,Name,0),
	case [To || {to,To} <- Opt] of
		[To] ->
			ok;
		_ ->
			To = undefined
	end,
	% #state will be provided with every callback from sqlproc.
	Idtype = actordb:actor_id_type(Type),
	{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{slave,Slave},{mod,?MODULE},create,nohibernate,
		{state,#state{idtype = Idtype,name = Name,type = Type,to = To}}|Opt]),
	{ok,Pid}.


% Steal shard from another node
% start_steal({Name,Type},Nd) ->
% 	start_steal(Nd,Name,Type).
start_steal(Nd,FromName,To,NewName,Type1) ->
	?AINF("start_steal node=~p, shardfrom=~p newname=~p type=~p isrunning=~p",
		[Nd,FromName,NewName,Type1,try_whereis(NewName,Type1)]),
	Type = butil:toatom(Type1),
	Idtype = actordb:actor_id_type(Type),
	case lists:member(Nd,bkdcore:cluster_nodes()) of
		false ->
			% kv shards just copy db
			% regular shards that hold the list of actors, require an actor-by-actor copy.
			case actordb_schema:iskv(Type) of
				true ->
					{ok,_Pid} = start(NewName,Type,false,[nohibernate,
						{state,#state{idtype = Idtype, name = NewName,type = Type, to = To}},
						{copyfrom,{split,{?MODULE,origin_steal_done,[bkdcore:node_name(),NewName]},Nd,FromName,NewName}},
						{copyreset,{?MODULE,newshard_steal_done,[Nd,FromName]}}]);
				false ->
					{ok,Pid} = actordb_sqlproc:start([{actor,NewName},{type,Type},{slave,false},{mod,?MODULE},create,nohibernate,
						 {state,#state{idtype = Idtype, name = NewName, stealingfromshard = FromName,
							to = To,stealingfrom = Nd,type = Type}}]),
					spawn(fun() -> actordb_sqlproc:call({NewName,Type},[create],{do_steal,Nd},{?MODULE,start_steal,[Nd]}) end),
					{ok,Pid}
			end;
		% If member of same cluster, we just need to run the shard normally. This will copy over the database.
		% Master might be this new node or if shard already running on another node, it will remain master untill restart.
		true ->
			{ok,_Pid} = start(NewName,Type,false,[nohibernate,{state,#state{to = To,idtype = Idtype, name = NewName,type = Type}},
				{copyfrom,{split,{?MODULE,origin_steal_done,[bkdcore:node_name(),NewName]},Nd,FromName,NewName}},
				{copyreset,{?MODULE,newshard_steal_done,[Nd,FromName]}}])
	end.

% Called on shard that is origin of moving shard.
% This is only called when entire shard is copied over, then split on both sides:
% - kv shards
% - shards that have been moved to another node in the same cluster
origin_steal_done(P,split,NextShardNode,NextShard) ->
	{ok,NP} = cb_idle(P#state{nextshard = NextShard, nextshardnode = NextShardNode, to = NextShard-1,
						cleanup_pre = P#state.name, cleanup_after = NextShard
						}),
	{ok,[%"$DELETE FROM actors WHERE hash >= ",butil:tobin(NextShard),";"
		"$INSERT OR REPLACE INTO __meta VALUES (",?META_NEXT_SHARD_NODE,$,,$',base64:encode(term_to_binary(NextShardNode)),$', ");",
		"$INSERT OR REPLACE INTO __meta VALUES (",?META_NEXT_SHARD,$,,$',butil:tolist(NextShard),$', ");",
		"$INSERT OR REPLACE INTO __meta VALUES (",?META_CLEANUP_PRE,$,,$',butil:tolist(P#state.name),$', ");",
		"$INSERT OR REPLACE INTO __meta VALUES (",?META_CLEANUP_AFTER,$,,$',butil:tolist(NextShard),$', ");"],
	 NP};
origin_steal_done(P,check,NewShardNode,NewShard) ->
	case P#state.nextshard == NewShard andalso  NewShardNode == P#state.nextshardnode of
		true ->
			ok;
		_ ->
			false
	end.

% Called on new shard that is being split in half. Same as above only for kv or shards
%  in the same cluster.
newshard_steal_done(P,Nd,_ShardFrom) ->
	?AINF("steal done ~p",[{P#state.name,P#state.type}]),
	ok = actordb_shardmvr:shard_moved(Nd,P#state.name,P#state.type),
	cb_idle(P#state{cleanup_pre = P#state.name}).
	% ["$DELETE FROM actors WHERE hash < ",butil:tobin(P#state.name),";"].


% Internal function that gets called on seperate process from cb_idle.
% This is so that we don't execute a massive DELETE statement for large shards at the same time.
do_cleanup(ShardName,Type,Pre,After) ->
	case After of
		undefined ->
			ReadSql = [<<"SELECT count(hash) FROM actors WHERE hash < ">>,butil:tobin(Pre),";"];
		_ ->
			ReadSql = [<<"SELECT count(hash) FROM actors WHERE hash > ">>,butil:tobin(After)," OR hash < ",butil:tobin(Pre),";"]
	end,
	Res = actordb_sqlproc:read({ShardName,Type},[create],{ReadSql,{?MODULE,cb_do_cleanup,[]}},?MODULE),
	?ADBG("Docleanup ~p.~p result=~p",[ShardName,Type,Res]).

% callmvr(Shard,M,F,A) ->
% 	Me = bkdcore:node_name(),
% 	case actordb_shardmngr:find_global_shard(Shard,Shard) of
% 		{_Shard,_,Node} when Node == Me ->
% 			?ADBG("apply ~p ~p ~p",[M,F,A]),
% 			apply(M,F,A);
% 		{_Shard,_,Node} ->
% 			?ADBG("rpc callmvr ~p ~p",[Node,{M,F,A}]),
% 			bkdcore:rpc(Node,{M,F,A})
% 	end.


get_schema_vers(ShardName,Type1) ->
	Type = butil:toatom(Type1),
	{ok,[{columns,_},{rows,[{_,Vers}]}]} =
			actordb_sqlproc:read({ShardName,Type},[create],<<"SELECT * FROM __adb WHERE id='schema_vers';">>,?MODULE),
	{ok,butil:toint(Vers)}.


kvread(ShardName,{A,1},Type,Sql) ->
	kvread(ShardName,A,Type,Sql);
kvread(ShardName,Actor,Type,Sql) ->
	?ADBG("kvread ~p",[{ShardName,Actor,Sql}]),
	R = actordb_sqlproc:read({ShardName,Type},[create],{?MODULE,cb_kvexec,[Actor,Sql]},?MODULE),
	?ADBG("kvread res ~p",[R]),
	case R of
		{redirect_shard,Node,NewShard} when is_binary(Node) ->
			actordb:rpc(Node,NewShard,{?MODULE,kvread,[NewShard,Actor,Type,Sql]});
		_ ->
			R
	end.
% Pragma delete should not really be called on kv store. If part of a transaction it will not be
%  a transaction at all. It will delete on transaction start instead of commit.
kvwrite(Shard,{A,1},Type,S) ->
	kvwrite(Shard,A,Type,S);
kvwrite(Shard,Actor,Type,{[delete],_}) ->
	kvwrite(Shard,Actor,Type,[delete]);
kvwrite(Shard,Actor,Type,[delete]) ->
	?ADBG("kvwrite delete ~p.~p",[Actor,Type]),
	ok = actordb_shard:del_actor(Shard,Actor,Type);
kvwrite(Shard,Actor,Type,{_Transaction,[delete]}) ->
	?ADBG("kvwrite delete transactions ~p.~p",[Actor,Type]),
	ok = actordb_shard:del_actor(Shard,Actor,Type);
kvwrite(Shard,Actor,Type,{{_Transaction,[delete]},_}) ->
	?ADBG("kvwrite delete transactions ~p.~p",[Actor,Type]),
	ok = actordb_shard:del_actor(Shard,Actor,Type);
kvwrite(ShardName,Actor,Type,Sql) ->
	?ADBG("kvwrite ~p",[{ShardName,Actor,Sql}]),
	case Sql of
		{{{_,_,_} = Transaction,Sql1},_Recs} ->
			WriteParam = {{?MODULE,cb_kvexec,[Actor,Sql1]},Transaction,Sql1};
		{{_,_,_} = Transaction,Sql1} ->
			WriteParam = {{?MODULE,cb_kvexec,[Actor,Sql1]},Transaction,Sql1};
		_ ->
			WriteParam = {{?MODULE,cb_kvexec,[Actor,Sql]},undefined,Sql}
	end,
	R = actordb_sqlproc:write({ShardName,Type},[create],WriteParam,?MODULE),
	?ADBG("Result ~p",[R]),
	case R of
		{redirect_shard,Node,NewShard} when is_binary(Node) ->
			actordb:rpc(Node,NewShard,{?MODULE,kvwrite,[NewShard,Actor,Type,Sql]});
		_ ->
			R
	end.

print_info(ShardName,Type) ->
	gen_server:cast(whereis(ShardName,Type),print_info).

del_actor(ShardName,ActorName,Type) ->
	case actordb_sqlproc:write({ShardName,Type},[create],{{?MODULE,cb_del_actor,[ActorName]},undefined,undefined},?MODULE) of
		{redirect_shard,Node,NewShard} when is_binary(Node) ->
			actordb:rpc(Node,NewShard,{?MODULE,del_actor,[NewShard,ActorName,Type]});
		{ok,_} ->
			ok;
		ok ->
			ok
	end.

is_reg(_,_,?MULTIUPDATE_TYPE) ->
	true;
is_reg(_,_,?CLUSTEREVENTS_TYPE) ->
	true;
is_reg(ShardName,ActorName,Type1) ->
	Type = butil:toatom(Type1),
	case kvread(ShardName,ActorName,Type,<<"select * from actors where id='",ActorName/binary,"';">>) of
		{ok,[{columns,_},{rows,[]}]} ->
			false;
		{ok,[{columns,_},{rows,[_|_]}]} ->
			true
	end.

reg_actor(_,_,?MULTIUPDATE_TYPE) ->
	ok;
reg_actor(_,_,?CLUSTEREVENTS_TYPE) ->
	ok;
reg_actor(ShardName,ActorName,Type1) ->
	Type = butil:toatom(Type1),
	case actordb_schema:iskv(Type) of
		true ->
			ok;
		_ ->
			?ADBG("reg_actor ~p ~p ~p~n",[ShardName,ActorName,Type1]),
			% Call sqlproc gen_server. It will call cb_reg_actor function in this module,
			%  which will return SQL statement to be executed.
			case actordb_sqlproc:write({ShardName,Type},[create],{{?MODULE,cb_reg_actor,[ActorName]},undefined,undefined},?MODULE) of
				{redirect_shard,Node,NewShard} when is_binary(Node) ->
					actordb:rpc(Node,NewShard,{?MODULE,reg_actor,[NewShard,ActorName,Type]});
				ok ->
					ok;
				{ok,_} ->
					ok
			end
	end.

count_actors(ShardName,Type1,Where1) ->
	Type = butil:toatom(Type1),
	Where = [[<<" WHERE ">>,W] || W <- [Where1], byte_size(W) > 0],
	{ok,[{columns,_},{rows,[{C}]}]} = actordb_sqlproc:read({ShardName,Type},[create],
		[<<"SELECT count(*) FROM actors ">>,Where,$;],?MODULE),
	C.

list_actors(ShardName,Type1,Where,From,Limit) ->
	Type = butil:toatom(Type1),
	R = actordb_sqlproc:read({ShardName,Type},[create],{?MODULE,cb_list_actors,[Where,From,Limit]},?MODULE),
	?ADBG("List actors ~p result ~p",[ShardName,R]),
	case R of
		{ok,[{columns,_},{rows,L}]} ->
			{ok,L};
		{{NextShard,NextShardNode},{ok,[{columns,_},{rows,L}]}} ->
			{ok,L,NextShard,NextShardNode}
	end.


% get_actors(ShardName,Type1) ->
% 	Type = butil:toatom(Type1),
% 	actordb_sqlproc:read({ShardName,Type},<<"SELECT * FROM actors;">>,?MODULE).

top_actor(ShardName,Type1) ->
	Type = butil:toatom(Type1),
	case actordb_sqlproc:read({ShardName,Type},[create],<<"SELECT id,max(hash) FROM actors;">>,?MODULE) of
		{ok,[{columns,_},{rows,[{undefined,undefined}]}]} ->
			undefined;
		{ok,[{columns,_},{rows,[{Id,Hash}]}]} ->
			{ok,Id,Hash};
		{ok,[_,{rows,[]}]} ->
			undefined
	end.

actor_stolen(NewShard,ShardName,Type,Actor,ThiefNode) ->
	case top_actor(ShardName,Type) of
		{ok,_,Hash} ->
			delete_actor_steal(ShardName,NewShard,Type,Actor,ThiefNode,Hash),
			actordb_shardmngr:set_shard_border(ShardName,NewShard,Type,Hash,ThiefNode);
		_ ->
			Hash = actordb_util:hash(butil:tobin(Actor)),
			delete_actor_steal(ShardName,NewShard,Type,Actor,ThiefNode,Hash-1),
			actordb_shardmngr:set_shard_border(ShardName,NewShard,Type,Hash-1,ThiefNode)
	end,
	ok.
delete_actor_steal(ShardName,NewShard,Type1,Actor,ThiefNode,Limit) ->
	Type = butil:toatom(Type1),
	actordb_sqlproc:write({ShardName,Type},[create],{{?MODULE,cb_del_move_actor,[NewShard,Actor,ThiefNode,Limit]},
		undefined,undefined},?MODULE).

delete_next(Name,Type1) ->
	Type = butil:toatom(Type1),
	actordb_sqlproc:write({Name,Type},[create],
		<<"DELETE FROM __meta WHERE id in(",?META_NEXT_SHARD,$,,?META_NEXT_SHARD_NODE,");">>,?MODULE).

try_whereis(N,Type1) ->
	Type = butil:toatom(Type1),
	distreg:whereis({N,Type}).
whereis(ShardName,Type1) ->
	Type = butil:toatom(Type1),
	case distreg:whereis({ShardName,Type}) of
		undefined ->
			start(ShardName,Type),
			whereis(ShardName,Type);
		Pid ->
			Pid
	end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%											Callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% custom write callbacks
%
cb_spawnopts(_) ->
	[].
cb_reg_actor(P,ActorName) ->
	?ADBG("cb_reg_actor ~p ~p ~p ~p ~p",[P#state.name,P#state.type,ActorName,P#state.stealingnow,P#state.stealingfrom]),
	Hash = actordb_util:hash(butil:tobin(ActorName)),
	NM = ActorName,
	Sql = <<"#s05;">>,
	Recs = [[[NM,butil:tobin(Hash)]]],
	case is_integer(P#state.nextshard) of
		true when P#state.nextshard =< Hash, is_binary(P#state.nextshardnode) ->
			{reply,{redirect_shard,P#state.nextshardnode,P#state.nextshard},P};
		_ ->
			% Is this regular actor registration or are we moving actors from another node.
			case P#state.stealingnow == ActorName of
				false ->
					{exec,Sql,Recs};
				% Moving actors. Actor has just been copied over successfully.
				true ->
					Me = self(),
					% First actor needs to be stored in db, then shard can move on to the next actor.
					spawn(fun() -> gen_server:call(Me,{move_to_next,ActorName}) end),
					erlang:demonitor(P#state.stealingnowmon),
					{Sql,Recs,P#state{stealingnow = undefined, stealingnowpid = undefined}}
			end
	end.

cb_kvexec(P,Actor,Sql) ->
	?ADBG("kvexec ~p ~p",[P#state.nextshard,Sql]),
	case is_integer(P#state.nextshard) of
		true ->
			Hash = actordb_util:hash(butil:tobin(Actor)),
			case ok of
				_ when P#state.nextshard =< Hash, is_binary(P#state.nextshardnode) ->
					{reply,{redirect_shard,P#state.nextshardnode,P#state.nextshard}};
				_ ->
					Sql
			end;
		false ->
			Sql
	end.

cb_do_cleanup(P,ReadResult) ->
	case ReadResult of
		{ok,[{columns,_},{rows,[{0}]}]} ->
			?AINF("Finished cleanup on ~p.~p",[P#state.name,P#state.type]),
			NP = P#state{cleanup_pre = undefined, cleanup_after = undefined},
			Sql = ["DELETE from __meta WHERE id=",?META_CLEANUP_PRE,";",
					"DELETE FROM __meta WHERE id=",?META_CLEANUP_AFTER,";"];
		% Limit delete with:
		%  abs(random() % NumToDelete) < 1000
		% If it contains a lot of items, it will delete ~1000 items at a time.
		% {ok,[{columns,_},{rows,[{Count}]}]} when P#state.cleanup_after == undefined ->
		% 	?AINF("Continue cleanup on ~p.~p, itemsleft=~p",[P#state.name,P#state.type,Count]),
		% 	Sql = [<<"DELETE FROM actors WHERE hash < ">>,butil:tobin(P#state.cleanup_pre),
		% 						" AND abs(random() % ",butil:tobin(Count),") < 1000;"],
		% 	NP = P;
		% {ok,[{columns,_},{rows,[{Count}]}]} ->
		% 	?AINF("Continue cleanup on ~p.~p, itemsleft=~p",[P#state.name,P#state.type,Count]),
		% 	Sql = [<<"DELETE FROM actors WHERE (hash < ">>,butil:tobin(P#state.cleanup_after)," OR hash >= ",butil:tobin(P#state.cleanup_pre),") AND ",
		% 			" abs(random() % ",butil:tobin(Count),") < 1000;"],
		% 	NP = P
		{ok,[{columns,_},{rows,[{Count}]}]} ->
			?AINF("Continue cleanup on ~p.~p, itemsleft=~p",[P#state.name,P#state.type,Count]),
			Sql = [<<"DELETE FROM actors WHERE (hash < ">>,butil:tobin(P#state.name)," OR hash > ",butil:tobin(P#state.to),") "
					," AND abs(random() % ",butil:tobin(Count),") < 1000;"
					% ,";"
					],
			NP = P
	end,
	{write,Sql,NP}.

cb_list_actors(P,Where1,From,Limit) ->
	?ADBG("cb_list_actors ~p",[P]),
	Where = [[<<" AND ">>,W] || W <- [Where1], byte_size(W) > 0],
	case is_integer(P#state.nextshard) of
		true ->
			Sql = [<<"SELECT id FROM actors WHERE hash<">>,butil:tobin(P#state.nextshard),Where,<<" LIMIT ">>, (butil:tobin(Limit)),
				<<" OFFSET ">>,(butil:tobin(From)), ";"],
			{reply,{P#state.nextshard,P#state.nextshardnode},Sql,P};
		false when is_integer(P#state.to) ->
			[<<"SELECT id FROM actors WHERE hash<=">>,butil:tobin(P#state.to),Where," AND ",
				"hash>=",butil:tobin(P#state.name),
				<<" LIMIT ">>, (butil:tobin(Limit)),
				<<" OFFSET ">>,(butil:tobin(From)), ";"];
		false ->
			[<<"SELECT id FROM actors WHERE hash >=">>,butil:tobin(P#state.name),Where, <<" LIMIT ">>, (butil:tobin(Limit)),
				<<" OFFSET ">>,(butil:tobin(From)), ";"]
	end.

cb_del_actor(P,ActorName) ->
	Hash = actordb_util:hash(butil:tobin(ActorName)),
	?ADBG("Del actor ~p on shard ~p, {next,nextnode,hash}=~p",[ActorName,P#state.name,{P#state.nextshard,P#state.nextshardnode,Hash}]),
	Sql = ["DELETE FROM actors WHERE id=",at(P#state.idtype,ActorName),";"],
	case is_integer(P#state.nextshard) of
		true when P#state.nextshard =< Hash, is_binary(P#state.nextshardnode) ->
			{reply,{redirect_shard,P#state.nextshardnode,P#state.nextshard},Sql,P};
		_ ->
			{Sql,P}
	end.

cb_del_move_actor(P,_NewShard,Actor,NextShardNode,NextShard) ->
	Sql = [ "DELETE FROM actors WHERE id=",at(P#state.idtype,Actor),";",
		"$INSERT OR REPLACE INTO __meta VALUES (",?META_NEXT_SHARD_NODE,$,,$',
		base64:encode(term_to_binary(NextShardNode)),$', ");",
		"$INSERT OR REPLACE INTO __meta VALUES (",?META_NEXT_SHARD,$,,$',butil:tolist(NextShard),$', ");"
	],
	{Sql,P#state{nextshardnode = NextShardNode, nextshard = NextShard}}.


%
% Mandatory callbacks.
%
cb_startstate(Name,Type) ->
	#state{name = Name, type = Type,idtype = actordb:actor_id_type(Type)}.
cb_candie(Mors,Name,_Type,P) ->
	case Mors of
		master ->
			% Master and is local shard, it is not temporary.
			Local = not actordb_shardmngr:is_local_shard(Name,Name);
		slave ->
			% Slave and is local shard, it is temporary.
			Local = actordb_shardmngr:is_local_shard(Name,Name)
	end,
	case P of
		undefined ->
			Local;
		_ ->
			Local andalso P#state.stealingnow == undefined andalso P#state.stealingfrom == undefined andalso
			P#state.nextshard == undefined
	end.


cb_checkmoved(Name,Type) ->
	Shard = actordb_shardmngr:find_local_shard(Name,Type,Name),
	case Shard of
		{redirect,_,MovedToNode} ->
			MovedToNode;
		_ ->
			undefined
	end.

cb_call({move_to_next,ActorName},Client,P) ->
	?ADBG("Shard move_to_next ~p",[ActorName]),
	% Actor has been copied over. Call other node to forget actor. Then call to send a new one.
	ok = actordb:rpc(P#state.stealingfrom,P#state.stealingfromshard,
		{?MODULE,actor_stolen,[P#state.name,P#state.stealingfromshard,P#state.type,
		ActorName,bkdcore:node_name()]}),
	cb_call(do_steal,Client,P);
% Steal a single actor from node indicated in #state.stealingfrom
% There must be a shard with the same name and type as this one running there.
cb_call(do_steal,_,P) ->
	?ADBG("Shard do_steal ~p ~p ~p ~p",[P#state.name,P#state.type,P#state.stealingnow,P#state.stealingfrom]),
	case P#state.stealingnow of
		undefined when P#state.stealingfrom /= undefined ->
			case actordb_schema:iskv(P#state.type) of
				false ->
					case lists:member(P#state.stealingfrom,bkdcore:cluster_nodes()) of
						false ->
							case actordb:rpc(P#state.stealingfrom,P#state.stealingfromshard,
												{?MODULE,top_actor,[P#state.stealingfromshard,P#state.type]}) of
								{ok,Id,Hash} when Hash >= P#state.name ->
									?ADBG("Found actor ~p",[Id]),
									% Once db is copied over, it will call reg_actor
									{ok,Pid} = actordb_actor:start_steal(Id,P#state.type,P#state.stealingfrom,P#state.name),
									Mon = erlang:monitor(process,Pid);
								Res when Res == undefined; element(1,Res) == ok ->
									?ADBG("Shard steal no top actors ~p",[P#state.stealingnow]),
									Id = undefined,
									Pid = undefined,
									Mon = undefined,
									ok = actordb_shardmvr:shard_moved(P#state.stealingfrom,P#state.name,P#state.type)
							end;
						true ->
							?ADBG("Shard steal same cluster ~p",[P#state.stealingnow]),
							Id = undefined,
							Pid = undefined,
							Mon = undefined,
							ok = actordb_shardmvr:shard_moved(P#state.stealingfrom,P#state.name,P#state.type)
					end,
					{reply,ok,P#state{stealingnow = Id, stealingnowpid = Pid, stealingnowmon = Mon}}
			% 	true ->
			% 		% kv
			% 		{reply,false,P}
			end;
		_ ->
			?ADBG("Not stealing anything"),
			{reply,false,P}
	end;
cb_call({do_steal,Nd},Client,P) ->
	% case P#state.stealingfrom of
	% 	undefined when is_integer(P#state.split_point) == false ->
			cb_call(do_steal,Client,P#state{stealingfrom = Nd});
	% 	_ ->
	% 		{reply,ok}
	% end;
cb_call({check_schema,CurV},_,P) ->
	case cb_schema(P,P#state.type,CurV) of
		{_,[]} ->
			{reply,ok,P};
		{_,_} ->
			{reply,ok,P}
	end;
cb_call(_Msg,_Client,_S) ->
	{reply,{error,uncrecognized_call}}.
cb_cast(_Msg,_S) ->
	noreply.
% Something monitored died. If it is actor we are stealing from, try again in 5s.
cb_info({'DOWN',_Monitor,_,PID,Reason},P) ->
	case ok of
		_ when PID == P#state.stealingnowpid ->
			?AERR("actor stealingnow died, retry after 5s ~p ~p",[P#state.stealingnow,Reason]),
			Me = self(),
			spawn(fun() -> timer:sleep(5000), gen_server:call(Me,do_steal) end),
			{noreply,P#state{stealingnow = undefined, stealingnowpid = undefined, stealingnowmon = undefined}};
		_ when PID == P#state.cleanup_proc ->
			case cb_idle(P#state{cleanup_proc = undefined}) of
				{ok,NS} ->
					{noreply,NS};
				_ ->
					{noreply,P#state{cleanup_proc = undefined}}
			end;
		_ ->
			?ADBG("unknown pid died on shard ~p,stealing ~p ~p",[PID,P#state.stealingnow,Reason]),
			noreply
	end;
cb_info(borders_changed,P) ->
	case ok of
		_ when is_integer(P#state.nextshard) ->
			% If split point is it's own shard, delete borders
			case actordb_shardmngr:find_global_shard(P#state.nextshard,P#state.nextshard) of
				{Shard,_,_} when P#state.nextshard == Shard ->
					spawn(fun() -> 	delete_next(P#state.name,P#state.type) end),
					{noreply,P#state{nextshard = undefined, nextshardnode = undefined}};
				_ ->
					noreply
			end;
		_ ->
			noreply
	end;
cb_info(_,_S) ->
	noreply.
cb_init(S,_EvNum) ->
	ok = actordb_shardmngr:shard_started(self(),S#state.name,S#state.type),
	{doread,<<"SELECT * FROM __meta WHERE id in(",?META_NEXT_SHARD,$,,?META_NEXT_SHARD_NODE,$,,?META_CLEANUP_PRE,$,,?META_CLEANUP_AFTER,");">>}.
cb_init(S,_Ev,{ok,[{columns,_},{rows,Rows}]}) ->
	case Rows of
		[_|_] ->
			CleanupPre = butil:ds_val(butil:toint([?META_CLEANUP_PRE]),Rows),
			CleanupAfter = butil:ds_val(butil:toint([?META_CLEANUP_PRE]),Rows),
			NS = butil:toint(butil:ds_val(butil:toint([?META_NEXT_SHARD]),Rows)),
			NSN = binary_to_term(base64:decode(butil:ds_val(butil:toint([?META_NEXT_SHARD_NODE]),Rows))),
			self() ! borders_changed,
			{ok,S#state{nextshard = NS, nextshardnode = NSN, cleanup_pre = CleanupPre, cleanup_after = CleanupAfter}};
		_ ->
			{ok,S}
	end.

cb_init_engine(S) ->
	S.

cb_idle(#state{cleanup_proc = undefined} = S) when S#state.cleanup_pre /= undefined; S#state.cleanup_after /= undefined ->
	?ADBG("Idle continue cleanup ~p.~p ~p",[S#state.name,S#state.type,{S#state.cleanup_pre,S#state.cleanup_after}]),
	% S#state.cleanup_pre, S#state.cleanup_after
	{Pid,_} = spawn_monitor(fun() ->  do_cleanup(S#state.name,S#state.type,S#state.name,S#state.to) end),
	{ok,S#state{cleanup_proc = Pid}};
cb_idle(_S) ->
	ok.

cb_redirected_call(_S,_MovedTo,_Call,_Type) ->
	ok.

cb_nodelist(S,_HasSchema) ->
	{ok,S,bkdcore:cluster_nodes()}.

cb_replicate_type(_S) ->
	1.

cb_write_done(_S,_Evterm,_Evnum) ->
	ok.

cb_unverified_call(_S,_Msg)  ->
	queue.

cb_slave_pid(Name,Type) ->
	cb_slave_pid(Name,Type,[]).
cb_slave_pid(Name,Type,Opt) ->
	case try_whereis(Name,Type) of
		undefined ->
			start(Name,Type,true,Opt);
		Pid ->
			{ok,Pid}
	end.

cb_path(_P,Name,Type) ->
	Foldername = butil:tolist(Name)++"."++butil:tolist(Type),
	actordb_util:shard_path(Foldername).

% Type = actor type (atom)
% Version = what is current version (0 for no version)
% Return:
% {LatestVersion,IolistSqlStatements}
cb_schema(P,Type,N) when is_atom(P) == false ->
	cb_schema(P#state.idtype,Type,N);
cb_schema(Idtype,Type,0) ->
	case actordb_schema:iskv(butil:toatom(Type)) of
		true ->
			{V,Sql} = actordb_util:type_schema(Type,0),
			{V,[Sql,<<"CREATE INDEX __hind ON actors (hash);">>,schema(2,Type)]};
		false ->
			{schema_version(), [schema(N,Idtype) || N <- lists:seq(1,schema_version())]}
	end;
cb_schema(Idtype,Type,Version) ->
	case actordb_schema:iskv(butil:toatom(Type)) of
		true ->
			actordb_util:type_schema(Type,Version);
		false ->
			case schema_version() > Version of
				true ->
					{schema_version(),[schema(N,Idtype) || N <- lists:seq(Version+1,schema_version())]};
				false ->
					{Version,[]}
			end
	end.

schema_version() ->
	2.

schema(1,Idtype1) ->
	case Idtype1 of
		string ->
			Idtype = <<"TEXT">>;
		integer ->
			Idtype = <<"INTEGER">>
	end,
	<<"$CREATE TABLE actors (id ",Idtype/binary," PRIMARY KEY, hash INTEGER) WITHOUT ROWID;",
	  "$CREATE INDEX __hind ON actors (hash);">>;
schema(2,_Idtype1) ->
	<<"$CREATE TABLE __meta (id INTEGER PRIMARY KEY, val TEXT);">>.

at(IdType,ActorName) ->
	case IdType of
		integer ->
			butil:tobin(ActorName);
		string ->
			[$',butil:tobin(ActorName),$']
	end.
