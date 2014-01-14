% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_shard).
-define(LAGERDBG,true).
-export([start/1,start/2,start_steal/2,start_steal/3,start_split/2,start_split/3,
		 whereis/2,try_whereis/2,reg_actor/3, %get_actors/2,
		top_actor/2,actor_stolen/4,print_info/2,get_upper_limit/2,list_actors/4,del_actor/3,
		kvread/4,kvwrite/4,kv_schema_check/1,get_schema_vers/2]). 
-export([cb_set_upper_limit/2, cb_list_actors/3, cb_reg_actor/2,cb_del_move_actor/4,cb_schema/3,cb_path/3,
		 cb_slave_pid/2,cb_call/3,cb_cast/2,cb_info/2,cb_init/2,cb_init/3,cb_del_actor/2,cb_kvexec/3,
		 split_other_done/3,start_steal_done/2,cb_candie/4,cb_checkmoved/2,cb_startstate/2]).
-include_lib("actordb.hrl").
-define(META_UPPER_LIMIT,$1).
-define(META_MOVINGTO,$2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%																Explanation
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% A shard is a chunk of hash space for a specific actor type. 
% If actordb has multiple types of actors, they all have seperate shards.
% Shard maintains an SQL table of actors that belong to it. Like actordb_actor it runs on top of actordb_sqlproc.
% 
% Shard DB table:
% ActorNameHash: actors are hashed across cluster, saving hash to DB enables queries like list of actors that should run
% 								on a specific server
% BlockedFlag: do not alow DB process to start. 
% MovingAwayFlag: actor is in the process of being moved to another cluster
% [ActorName, ActorNameHash, BlockedFlag, MovingAwayFlag]
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state,{idtype,name,type,
	% upperlimit is changed actor-by-actor if shard being moved or set to half of previous top if actor is being split in half
	upperlimit,
	% if split_point is integer, then shard is splitting.
	% split_point is new upperlimit+1
	split_point, splitproc, 
	% Node to which shard is moving
	thiefnode,
	% Node name from which shard is copying
	stealingfrom,
	% Which actor is in the process of moving atm
	stealingnow, stealingnowpid,stealingnowmon}).

% Replicate half of shard by moving upper edge of shard actor-by-actor down.
% 1. Start actor with highest hash value. Tell him to replicate to another node (same method as inter-cluster replication)
% 2. Once moved, lower shard upper limit towards next highest hash value actor. Limit is stored in some public ETS.
% 	 Every time actordb_shardtree is accessed, check this public ETS table if shard is lowering upper limit.
% 	 actordb_shardtree is only changed once entire upper half of shard is moved over to another node.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%																API
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start({shard,Type,ShardName}) ->
	start(ShardName,Type).
start({shard,Type,Name},Flags) ->
	start(Name,Type,false,Flags);
start(Name,Type1) ->
	start(Name,Type1,false).
start(Name,Type1,Slave) ->
	start(Name,Type1,Slave,[]).
start(Name,Type1,Slave,Opt) ->
	?ADBG("shard start ~p ~p ~p",[Name,Type1,Opt]),
	% #state will be provided with every callback from sqlproc.
	Type = butil:toatom(Type1),
	Idtype = actordb:actor_id_type(Type),
	{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{slave,Slave},{mod,?MODULE},create,
										{state,#state{idtype = Idtype,name = Name,type = Type}},
										{regname,{shard,Type,Name}}|Opt]),
	{ok,Pid}.


% Steal shard from another node
start_steal({shard,Type,Name},Nd) ->
	start_steal(Nd,Name,Type).
start_steal(Nd,Name,Type1) ->
	?AINF("start_steal ~p ~p ~p ~p",[Nd,Name,Type1,try_whereis(Name,Type1)]),
	Type = butil:toatom(Type1),
	Idtype = actordb:actor_id_type(Type),
	case lists:member(Nd,bkdcore:cluster_nodes()) of 
		false ->
			% kv shards just copy db
			% regular shards that hold the list of actors, require an actor-by-actor copy.
			case actordb_schema:iskv(Type) of
				true ->
					{ok,_Pid} = start(Name,Type,false,[{copyfrom,Nd},{copyreset,{?MODULE,start_steal_done,[Nd]}}]);
				false ->
					{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{slave,false},{mod,?MODULE},create,
																	 {state,#state{idtype = Idtype, name = Name, 
																	 				stealingfrom = Nd,type = Type}},
																	 {regname,{shard,Type,Name}}]),
					spawn(fun() -> actordb_sqlproc:call({shard,Type,Name},[create],{do_steal,Nd},{?MODULE,start_steal,[Nd]}) 
							end),
					{ok,Pid}
			end;
		% If member of same cluster, we just need to run the shard normally. This will copy over the database.
		% Master might be this new node or if shard already running on another node, it will remain master untill restart.
		true ->
			{ok,Pid} = start(Name,Type1),
			spawn(fun() ->  actordb_shardmvr:shard_moved(Nd,Name,Type) end),
			{ok,Pid}
	end.

start_steal_done(P,Nd) ->
	actordb_shardmvr:shard_moved(Nd,P#state.name,P#state.type).
	% callmvr(P#state.name,actordb_shardmvr,shard_moved,[Nd,P#state.name,P#state.type]).

% Split shard in half.
start_split({shard,Type1,Name},SplitPoint) ->
	start_split(Name,Type1,SplitPoint).
start_split(Name,Type1,SplitPoint) ->
	?AINF("start_split ~p ~p ~p",[Name,SplitPoint,Type1]),
	Type = butil:toatom(Type1),
	Idtype = actordb:actor_id_type(Type),
	{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,Type},{slave,false},{mod,?MODULE},create,
															 {state,#state{idtype = Idtype,split_point = SplitPoint, 
															 				upperlimit = SplitPoint-1, 
															 				name = Name, type = Type}},
															 {regname,{shard,Type,Name}}]),	
	spawn(fun() -> 
		ok = actordb_sqlproc:write({shard,Type,Name},[create],
								{{?MODULE,cb_set_upper_limit,[SplitPoint]},undefined,undefined},
								{?MODULE,start_split,[SplitPoint]})
	end),
	{ok,Pid}.
	

start_split_other(Name,Type,OriginShard) ->
	?AINF("Start copyfrom ~p ~p from ~p",[Name,Type,OriginShard]),
	{ok,_Pid} = start(Name,Type,false,[{copyfrom,{bkdcore:node_name(),OriginShard}},
									   {copyreset,{?MODULE,split_other_done,
									   				[OriginShard,<<"DELETE FROM actors WHERE hash < ",(butil:tobin(Name))/binary,";",
													"DELETE FROM meta WHERE id in(",?META_UPPER_LIMIT,$,,?META_MOVINGTO,");">>]}}]),
	exit(ok).

split_other_done(P,Origin,Sql) ->
	?AINF("Split other done ~p ~p from ~p",[P#state.name, P#state.type,Origin]),
	callmvr(Origin,actordb_shardmvr,shard_has_split,[Origin,P#state.name,P#state.type]),
	Sql.

callmvr(Shard,M,F,A) ->
	Me = bkdcore:node_name(),
	case actordb_shardmngr:find_global_shard(Shard,Shard) of
		{_Shard,_,Node} when Node == Me ->
			?ADBG("apply ~p ~p ~p",[M,F,A]),
			apply(M,F,A);
		{_Shard,_,Node} ->
			?AINF("rpc callmvr ~p ~p",[Node,{M,F,A}]),
			bkdcore:rpc(Node,{M,F,A})
	end.

% Live update schema. Don't have an idea how to nicely apply it to a running kv process so this does not do anything.
kv_schema_check(_Type) ->
	% [kv_schema_check(Type,ShardName) || {ShardName,_,_} <- actordb_shardmngr:get_local_shards()].
	ok.
% kv_schema_check(Type,ShardName) ->
% 	{ok,Vers} = get_schema_vers(Type,ShardName),
% 	Vers.

get_schema_vers(ShardName,Type1) ->
	Type = butil:toatom(Type1),
	{ok,[{columns,_},{rows,[{_,Vers}]}]} =
			actordb_sqlproc:read({shard,Type,ShardName},[create],<<"SELECT * FROM __adb WHERE id='schema_vers';">>,?MODULE),
	{ok,butil:toint(Vers)}.


kvread(ShardName,{A,1},Type,Sql) ->
	kvread(ShardName,A,Type,Sql);
kvread(ShardName,Actor,Type,Sql) ->
	% actordb_sqlproc:read({shard,Type,Name},Sql,?MODULE).
	?ADBG("kvread ~p",[{ShardName,Actor,Sql}]),
	R = actordb_sqlproc:read({shard,Type,ShardName},[create],{?MODULE,cb_kvexec,[Actor,Sql]},?MODULE),
	?ADBG("kvread res ~p",[R]),
	case R of
		{redirect_shard,Node} when is_binary(Node) ->
			actordb:rpc(Node,ShardName,{?MODULE,kvread,[ShardName,Actor,Type,Sql]});
		{redirect_shard,RShard} when is_integer(RShard) ->
			kvread(RShard,Actor,Type,Sql);
		_ ->
			R
	end.
% Pragma delete should not really be called on kv store. If part of a transaction it will not be
%  a transaction at all. It will delete on transaction start instead of commit.
kvwrite(Shard,{A,1},Type,S) ->
	kvwrite(Shard,A,Type,S);
kvwrite(Shard,Actor,Type,[delete]) ->
	ok = actordb_shard:del_actor(Shard,Actor,Type);
kvwrite(Shard,Actor,Type,{_Transaction,[delete]}) ->
	ok = actordb_shard:del_actor(Shard,Actor,Type);
kvwrite(ShardName,Actor,Type,Sql) ->
	?ADBG("kvwrite ~p",[{ShardName,Actor,Sql}]),
	case Sql of
		{Transaction,Sql1} ->
			WriteParam = {{?MODULE,cb_kvexec,[Actor,Sql1]},Transaction,Sql1};
		_ ->
			WriteParam = {{?MODULE,cb_kvexec,[Actor,Sql]},undefined,Sql}
	end,
	R = actordb_sqlproc:write({shard,Type,ShardName},[create],WriteParam,?MODULE),
	?ADBG("Result ~p",[R]),
	case R of
		{redirect_shard,Node} when is_binary(Node) ->
			actordb:rpc(Node,ShardName,{?MODULE,kvwrite,[ShardName,Actor,Type,Sql]});
		{redirect_shard,RShard} when is_integer(RShard) ->
			kvwrite(RShard,Actor,Type,Sql);
		_ ->
			R
	end.

print_info(ShardName,Type) ->
	gen_server:cast(whereis(ShardName,Type),print_info).

del_actor(ShardName,ActorName,Type) ->
	case actordb_sqlproc:write({shard,Type,ShardName},[create],{{?MODULE,cb_del_actor,[ActorName]},undefined,undefined},?MODULE) of
		{redirect_shard,Node} when is_binary(Node) ->
			actordb:rpc(Node,ShardName,{?MODULE,del_actor,[ShardName,ActorName,Type]});
		{redirect_shard,Shard} when is_integer(Shard) ->
			del_actor(Shard,ActorName,Type);
		{ok,_} ->
			ok;
		ok ->
			ok
	end.

reg_actor(_,_,?MULTIUPDATE_TYPE) ->
	ok;
reg_actor(_,_,?CLUSTEREVENTS_TYPE) ->
	ok;
reg_actor(ShardName,ActorName,Type1) ->
	Type = butil:toatom(Type1),
	?ADBG("reg_actor ~p ~p ~p~n",[ShardName,ActorName,Type1]),
	% Call sqlproc gen_server. It will call cb_reg_actor function in this module, which will return SQL statement to be executed.
	case actordb_sqlproc:write({shard,Type,ShardName},[create],{{?MODULE,cb_reg_actor,[ActorName]},undefined,undefined},?MODULE) of
		{redirect_shard,Node} when is_binary(Node) ->
			actordb:rpc(Node,ShardName,{?MODULE,reg_actor,[ShardName,ActorName,Type]});
		{redirect_shard,Shard} when is_integer(Shard) ->
			reg_actor(Shard,ActorName,Type);
		ok ->
			ok;
		{ok,_} ->
			ok
	end.

list_actors(ShardName,Type1,From,Limit) ->
	Type = butil:toatom(Type1),
	R = actordb_sqlproc:read({shard,Type,ShardName},[create],{?MODULE,cb_list_actors,[From,Limit]},?MODULE),
	?ADBG("List actors ~p result ~p",[ShardName,R]),
	case R of
		{ok,[{columns,_},{rows,L}]} ->
			{ok,L};
		{{_UpperLimit,Thief,undefined},{ok,[{columns,_},{rows,L}]}} ->
			{ok,L,Thief};
		{{_UpperLimit,undefined,NextShard},{ok,[{columns,_},{rows,L}]}} ->
			{ok,L,NextShard}
	end.

get_upper_limit(ShardName,Type1) ->
	Type = butil:toatom(Type1),
	case actordb_sqlproc:read({shard,Type,ShardName},[create],<<"SELECT * FROM meta WHERE id in(",
						?META_UPPER_LIMIT,$,,?META_MOVINGTO,");">>,?MODULE) of
		{ok,[{columns,_},{rows,[_|_] = Rows}]} ->
			{ok,butil:toint(butil:ds_val(butil:toint([?META_UPPER_LIMIT]),Rows)),
					binary_to_term(base64:decode(butil:ds_val(butil:toint([?META_MOVINGTO]),Rows)))};
		_ ->
			undefined
	end.

% get_actors(ShardName,Type1) ->
% 	Type = butil:toatom(Type1),
% 	actordb_sqlproc:read({shard,Type,ShardName},<<"SELECT * FROM actors;">>,?MODULE).

top_actor(ShardName,Type1) ->
	Type = butil:toatom(Type1),
	case actordb_sqlproc:read({shard,Type,ShardName},[create],<<"SELECT id,max(hash) FROM actors;">>,?MODULE) of
		{ok,[{columns,_},{rows,[{undefined,undefined}]}]} ->
			undefined;
		{ok,[{columns,_},{rows,[{Id,Hash}]}]} ->
			{ok,Id,Hash};
		{ok,[_,{rows,[]}]} ->
			undefined
	end.

actor_stolen(ShardName,Type,Actor,ThiefNode) ->
	case top_actor(ShardName,Type) of
		{ok,_,Hash} ->
			delete_actor_steal(ShardName,Type,Actor,ThiefNode,Hash),
			actordb_shardmngr:set_shard_border(ShardName,Type,Hash,ThiefNode);
		_ ->
			Hash = actordb_util:hash(butil:tobin(Actor)),
			delete_actor_steal(ShardName,Type,Actor,ThiefNode,Hash-1),
			actordb_shardmngr:set_shard_border(ShardName,Type,Hash-1,ThiefNode)
	end,
	ok.
delete_actor_steal(ShardName,Type1,Actor,ThiefNode,Limit) ->
	Type = butil:toatom(Type1),
	actordb_sqlproc:write({shard,Type,ShardName},[create],{{?MODULE,cb_del_move_actor,[Actor,ThiefNode,Limit]},
												  undefined,undefined},?MODULE).

delete_limits(Name,Type1,UpperLimit) when is_integer(UpperLimit) ->
	Type = butil:toatom(Type1),
	?AINF("delete_limits ~p ~p ~p",[Name,Type,UpperLimit]),
	actordb_sqlproc:write({shard,Type,Name},[create],
								<<"DELETE FROM actors WHERE hash > ",(butil:tobin(UpperLimit))/binary,";",
								"DELETE FROM meta WHERE id in(",?META_UPPER_LIMIT,$,,?META_MOVINGTO,");">>,?MODULE).

try_whereis(N,Type1) ->
	Type = butil:toatom(Type1),
	distreg:whereis({shard,Type,N}).
whereis(ShardName,Type1) ->
	Type = butil:toatom(Type1),
	case distreg:whereis({shard,Type,ShardName}) of
		undefined ->
			start(ShardName,Type),
			whereis(ShardName,Type);
		Pid ->
			Pid
	end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%																Callbacks
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% custom write callbacks
% 
cb_reg_actor(P,ActorName) ->
	?ADBG("cb_reg_actor ~p ~p ~p ~p ~p",[P#state.name,P#state.type,ActorName,P#state.stealingnow,P#state.stealingfrom]),
	Hash = actordb_util:hash(butil:tobin(ActorName)),
	NM = at(P#state.idtype,ActorName),
	Sql = [<<"INSERT OR REPLACE INTO actors VALUES (">>,NM,$,,(butil:tobin(Hash)), <<");">>],
	case is_integer(P#state.upperlimit) of
		true when P#state.upperlimit < Hash, is_integer(P#state.split_point) ->
			{reply,{redirect_shard,P#state.split_point},Sql,P};
		true when P#state.upperlimit < Hash, is_binary(P#state.thiefnode) ->
			{reply,{redirect_shard,P#state.thiefnode},Sql,P};
		_ ->
			% Is this regular actor registration or are we moving actors from another node.
			case P#state.stealingnow == ActorName of
				false ->
					Sql;
				% Moving actors. Actor has just been copied over successfully.
				true ->
					Me = self(),
					% First actor needs to be stored in db, then shard can move on to the next actor.
					spawn(fun() -> gen_server:call(Me,{move_to_next,ActorName}) end),
					erlang:demonitor(P#state.stealingnowmon),
					{Sql,P#state{stealingnow = undefined, stealingnowpid = undefined}}
			end
	end.

cb_kvexec(P,Actor,Sql) ->
	?ADBG("kvexec ~p ~p",[P#state.upperlimit,Sql]),
	case is_integer(P#state.upperlimit) of
		true ->
			Hash = actordb_util:hash(butil:tobin(Actor)),
			case ok of
				_ when P#state.upperlimit < Hash, is_integer(P#state.split_point) ->
					{reply,{redirect_shard,P#state.split_point}};
				_ when P#state.upperlimit < Hash, is_binary(P#state.thiefnode) ->
					{reply,{redirect_shard,P#state.thiefnode}};
				_ ->
					Sql
			end;
		false ->
			Sql
	end.

cb_list_actors(P,From,Limit) ->
	?ADBG("cb_list_actors ~p",[P]),
	case is_integer(P#state.upperlimit) of
		true ->
			{reply,{P#state.upperlimit,P#state.thiefnode,P#state.split_point},
				<<"SELECT id FROM actors WHERE hash<",(butil:tobin(P#state.upperlimit))/binary," LIMIT ", 
						(butil:tobin(Limit))/binary," OFFSET ",(butil:tobin(From))/binary, ";">>,P};
		false ->
			<<"SELECT id FROM actors LIMIT ", (butil:tobin(Limit))/binary,
				" OFFSET ",(butil:tobin(From))/binary, ";">>
	end.

cb_del_actor(P,ActorName) ->
	?ADBG("cb_del_actor ~p ~p ~p",[P#state.name,ActorName,P#state.type]),
	Hash = actordb_util:hash(butil:tobin(ActorName)),
	Sql = ["DELETE FROM actors WHERE id=",at(P#state.idtype,ActorName),";"],
	case is_integer(P#state.upperlimit) of
		true when P#state.upperlimit < Hash, is_integer(P#state.split_point) ->
			{reply,{redirect_shard,P#state.split_point},Sql,P};
		true when P#state.upperlimit < Hash, is_binary(P#state.thiefnode) ->
			{reply,{redirect_shard,P#state.thiefnode},Sql,P};
		_ ->
			{Sql,P}
	end.

cb_del_move_actor(P,Actor,ThiefNode,UpperLimit) ->
	Sql = [ "DELETE FROM actors WHERE id=",at(P#state.idtype,Actor),";",
			  "$INSERT OR REPLACE INTO meta VALUES (",?META_MOVINGTO,$,,$',base64:encode(term_to_binary(ThiefNode)),$', ");",
			  "$INSERT OR REPLACE INTO meta VALUES (",?META_UPPER_LIMIT,$,,
	  			$',butil:tolist(UpperLimit),$', ");"
	],
	{Sql,P#state{upperlimit = UpperLimit, thiefnode = ThiefNode}}.

cb_set_upper_limit(P,SplitPoint) ->
	?ADBG("cb_set_upper_limit ~p ~p ~p",[P,SplitPoint,P#state.split_point]),
	Sql = [ "$INSERT OR REPLACE INTO meta VALUES (",?META_MOVINGTO,$,,$',base64:encode(term_to_binary(SplitPoint)),$', ");",
			"$INSERT OR REPLACE INTO meta VALUES (",?META_UPPER_LIMIT,$,,$',butil:tolist(SplitPoint-1),$', ");"],
	case is_integer(P#state.split_point) of
		true when is_pid(P#state.splitproc) ->
			{Sql,P};
		_ ->
			{Pid,_} = spawn_monitor(fun() -> ?AINF("Start split from cb_set_upper_limit"),
							start_split_other(SplitPoint,P#state.type,P#state.name) end),
			{Sql,P#state{split_point = SplitPoint,splitproc = Pid, upperlimit = SplitPoint-1}}
	end.

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
			P#state.split_point == undefined andalso P#state.upperlimit == undefined andalso P#state.splitproc == undefined
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
	ok = actordb:rpc(P#state.stealingfrom,P#state.name,{?MODULE,actor_stolen,[P#state.name,P#state.type,
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
							case actordb:rpc(P#state.stealingfrom,P#state.name,{?MODULE,top_actor,[P#state.name,P#state.type]}) of
								{ok,Id,_Hash} ->
									?ADBG("Found actor ~p",[Id]),
									% Once db is copied over, it will call reg_actor
									{ok,Pid} = actordb_actor:start_steal(Id,P#state.type,P#state.stealingfrom,P#state.name),
									Mon = erlang:monitor(process,Pid);
								undefined ->
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
		_ when PID == P#state.splitproc ->
			case Reason of
				ok ->
					?AINF("splitproc done ~p",[Reason]),
					{noreply,P#state{splitproc = undefined}};
				Err ->
					?AERR("splitproc error ~p ~p",[Err]),
					noreply
			end;
		_ ->
			?ADBG("unknown pid died on shard ~p,stealing ~p ~p",[PID,P#state.stealingnow,Reason]),
			noreply
	end;
cb_info(borders_changed,P) ->
	case ok of
		_ when is_integer(P#state.split_point) ->
			% If split point is it's own shard, delete borders
			case actordb_shardmngr:find_global_shard(P#state.split_point,P#state.split_point) of
				{Shard,_,_} when P#state.split_point == Shard ->
					spawn(fun() -> 	delete_limits(P#state.name,P#state.type,P#state.upperlimit) end),
					% ?AINF("borders changed, my split ~p tree ~p",[P#state.upperlimit,actordb_shardtree:all()]),
					{noreply,P#state{split_point = undefined, upperlimit = undefined}};
				_ ->
					noreply
			end;
		_ ->
			noreply
	end;
cb_info(_,_S) ->
	noreply.
cb_init(S,_EvNum) ->
	?ADBG("cb_init shard ~p",[S]),
	ok = actordb_shardmngr:shard_started(self(),S#state.name,S#state.type),
	case is_integer(S#state.split_point) of
		true ->
			{Pid,_} = spawn_monitor(fun() ->?AINF("Start split from init"),
						start_split_other(S#state.split_point,S#state.type,S#state.name) end),
			{ok,S#state{splitproc = Pid}};
		false ->
			% This will cause read to execute and result returned in cb_init/3
			{doread,<<"SELECT * FROM meta WHERE id in(",?META_UPPER_LIMIT,$,,?META_MOVINGTO,");">>}
	end.
cb_init(S,_EvNum,{ok,[{columns,_},{rows,Rows}]}) ->
	?ADBG("cb_init shard ~p ~p ~p",[S,_EvNum,Rows]),
	case Rows of
		[_|_] ->
			Limit = butil:toint(butil:ds_val(butil:toint([?META_UPPER_LIMIT]),Rows)),
			MovingTo = binary_to_term(base64:decode(butil:ds_val(butil:toint([?META_MOVINGTO]),Rows))),
			?AINF("shard started and is moving ~p ~p",[Limit,MovingTo]),
			case ok of
				% Shard is being split in half
				_ when is_integer(MovingTo) ->
					case actordb_shardmngr:find_global_shard(Limit+1,Limit+1) of
						% Has process concluded?
						% If yes delete leftover data from db
						{Shard,_,_} when Limit+1 == Shard ->
							spawn(fun() -> 
									?AINF("init shard that is being split moving to ~p ~p",[MovingTo,S]),
									delete_limits(S#state.name,S#state.type,Limit) 
								 end),
							{ok,S#state{split_point = undefined, upperlimit = undefined}};
						_ ->
							{Pid,_} = spawn_monitor(fun() ->?AINF("Start split from init2"),
										start_split_other(Limit+1,S#state.type,S#state.name) end),
							{ok,S#state{split_point = Limit+1,splitproc = Pid, upperlimit = Limit}}
					end;
				_ when is_binary(MovingTo) ->
					case actordb_shardmngr:find_global_shard(Limit+1,Limit+1) of
						% Shard for limit+1 is globally set somewhere else, this means shard has completely moved over.
						{Shard,_,_} when Shard /= S#state.name ->
							spawn(fun() -> 
									?AINF("init shard that is being moved ~p",[MovingTo]),
									delete_limits(S#state.name,S#state.type,Limit) 
								end),
							{ok,S#state{split_point = undefined, upperlimit = undefined}};
						_ ->
							actordb_shardmngr:set_shard_border(S#state.name,S#state.type,Limit,MovingTo),
							{ok,S#state{upperlimit = Limit,thiefnode = MovingTo}}
					end
			end;
		_ ->
			ok
	end.



cb_slave_pid(Name,Type) ->
	case try_whereis(Name,Type) of
		undefined ->
			start(Name,Type,true);
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
			{V,[Sql,schema(2,Type)]};
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
			Idtype = <<"TEXT">>,
			% If sqlite is up-to-date it supports withoutrowid. This was checked
			%  at actordb startup.
			{ok,Id} = application:get_env(actordb_core,withoutrowid);
		integer ->
			Idtype = <<"INTEGER">>,
			Id = <<>>
	end,
	<<"CREATE TABLE actors (id ",Idtype/binary," PRIMARY KEY, hash INTEGER) ",Id/binary,";",
	  "CREATE INDEX hind ON actors (hash);">>;
schema(2,_Idtype1) ->
	<<"CREATE TABLE meta (id INTEGER PRIMARY KEY, val TEXT);">>.

at(IdType,ActorName) ->
	case IdType of
		integer ->
			butil:tobin(ActorName);
		string ->
			[$',butil:tobin(ActorName),$']
	end.








