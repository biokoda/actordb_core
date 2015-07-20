% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_shardmngr).
-behaviour(gen_server).
-define(LAGERDBG,true).
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([print_info/0,reload/0,deser_prop/1]).
-export([find_local_shard/2,find_local_shard/3,is_local_shard/1,is_local_shard/2,find_global_shard/1,find_global_shard/2,steal_shard/3,
			set_shard_border/5,shard_moved/3,shard_started/3,shard_has_split/2,get_local_shards/0,schema_changed/0]).
% for testing
-export([create_shards/1]).
-include_lib("actordb_core/include/actordb.hrl").
-include_lib("kernel/include/file.hrl").
-define(BORDERETS,shardborders).
% -compile(export_all).

% In charge of shards on a single node.
% Communicates with other nodes in cluster for when a node is down, which shards to take over
% 	 (or not because not enough nodes online)


start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

schema_changed() ->
	gen_server:cast(?MODULE,schema_changed).

% Return shard id for actor. It should be local, but might not be if shard is in the process of moving to another node.
% In this case {redirect,ActualNode} is returned.
find_local_shard({Shard,_Actor},_Type) ->
	% this is a hack for kv
	case is_local_shard(Shard,Shard) of
		true ->
			Shard;
		_ ->
			undefined
	end;
find_local_shard(Actor,Type) ->
	find_local_shard(Actor,Type,actordb_util:hash(butil:tobin(Actor))).
find_local_shard(Actor,Type,Hash) ->
	find_local_shard(Actor,Type,Hash,0).
find_local_shard(Actor,Type,Hash,Retry) ->
	case catch find_shard(actordb_shardtree:local(),Hash) of
		{'EXIT',_} ->
			case Retry > 100 of
				true ->
					exit(initialize_timeout);
				_ ->
					timer:sleep(100),
					find_local_shard(Actor,Type,Hash,Retry+1)
			end;
		false ->
			% If shard is in the process of moving, shardtree will be set to current global shard
			% (the node where shard is moving from).
			{Shard,_,GlobalNode} = find_global_shard(Actor,Hash),
			case actordb_shard:try_whereis(Shard,Type) of
				undefined ->
					undefined;
				_ ->
					case lists:member(GlobalNode,bkdcore:all_cluster_nodes()) of
						true ->
							{redirect,Shard,GlobalNode};
						_ ->
							Shard
					end
			end;
		{Shard,_ShardTo,_Node} ->
			case butil:ds_val({Shard,Type},?BORDERETS) of
				undefined ->
					Shard;
				{UpperLimit,_OtherNode,_OtherShard} when Hash =< UpperLimit ->
					Shard;
				{_,OtherNode,OtherShard} ->
					{redirect,OtherShard,OtherNode}
			end
	end.
is_local_shard(Actor) ->
	is_local_shard(Actor,actordb_util:hash(butil:tobin(Actor))).
is_local_shard(_A,Hash) ->
	case find_shard(actordb_shardtree:local(),Hash) of
		false ->
			false;
		_ ->
			true
	end.
find_global_shard({Shard,_}) ->
	find_global_shard(Shard,Shard);
find_global_shard(Actor) ->
	find_global_shard(Actor,actordb_util:hash(butil:tobin(Actor))).
find_global_shard(_Actor,Hash) ->
	case find_shard(actordb_shardtree:all(),Hash) of
		false ->
			undefined;
		X ->
			X
	end.


% Gets called on node from which shard is being taken.
% Its upper border gets lowered for every actor moved over to new node.
set_shard_border(Shard,NewShard,Type,Limit,NewNode) ->
	butil:ds_add({Shard,Type},{Limit,NewNode,NewShard},?BORDERETS).

% When shard has been completely moved over to a new node, this gets called.
% Once all types for shard have been moved over to another node, it will tell shardmngr on global master node
%  to change which node is in charge of shard.
shard_moved(Shard,Type,Node) ->
	gen_server:call(?MODULE,{shard_moved,Shard,Type,Node},infinity).

get_local_shards() ->
	gen_server:call(?MODULE,get_local_shards,infinity).

steal_shard(Node,Shard,NodeFrom) ->
	gen_server:call(?MODULE,{steal_shard,Node,Shard,NodeFrom},infinity).

shard_has_split(Original,New) ->
	print_info(),
	gen_server:call(?MODULE,{shard_has_split,Original,New},infinity).


find_shard({From, To, Node, _Left, _Right}, H) when H >= From, H =< To ->
	{From,To,Node};
find_shard({_From, To, _Node, _Left, Right}, H) when H >= To ->
	find_shard(Right, H);
find_shard({From, _To, _Node, Left, _Right}, H) when H =< From ->
	find_shard(Left, H);
find_shard(undefined, _) ->
	false;
find_shard(_, _) ->
	false.

shard_started(Pid,Shard,Type) ->
	gen_server:cast(?MODULE,{shard_started,Pid,Shard,Type}).


stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:cast(?MODULE,print_info).
reload() ->
	gen_server:call(?MODULE, reload).

% Normal operation sequence
% 1. Start
% 2. Wait for global state established
% 3. Get global state (list of shards)
% 4. Compile shards into a binary tree for fast access and store into a module for global fast access.
% 5. Inform shardmvr of shard tree. It will determine if something needs to be changed,
%    	like node not having enough shards.
% 6. Start every shard process.


-record(dp,{
% From global state
% [{ShardFrom,ShardTo,Nodename},...]
	allshards,
% Just the shards local to this node
% [{From,To,Nodename},..]
  localshards = [],
% [{Pid,From,Type},..]
  localshardpids = [],
% Processes that do work async. Result returned in exit signal.
  getstatepid,
% Which shards are being taken from this node.
% [{NewShard,OldShard,Node,[ActorType1,ActorType2,...]}]
  shardsbeingtaken = [],
  % Shards previously taken from this node.
  % shardsprevtaken = [],
  dirty = false,
  haveschema = false
  }).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).

% Shard has been completely moved over to another node.
handle_call({shard_moved,NewShard,Type,Node},From,P) ->
	?AINF("Shard moved ~p ~p ~p~n~p",[NewShard,Type,Node,P#dp.shardsbeingtaken]),
	% Have all types for shard been moved?
	% If yes, tell global master shard Node is in charge of shard now.
	case lists:keyfind(NewShard,1,P#dp.shardsbeingtaken) of
		{NewShard,OldShard,Node,Types} ->
			case lists:delete(Type,Types) of
				[] ->
					?AINF("ALl types moved! ~p",[NewShard]),
					CleanedUp = [{NShd,OShd,Nd,TypesToGo} || {NShd,OShd,Nd,TypesToGo} <- P#dp.shardsbeingtaken, NShd /= NewShard],
					ok = actordb_sharedstate:write_cluster(["shardsbeingtaken,",bkdcore:node_name()],CleanedUp),
					handle_call({change_shard_node,NewShard,OldShard,Node},From,P);
				Deleted ->
					SBT = lists:keystore(NewShard,1,P#dp.shardsbeingtaken,{NewShard,OldShard,Node,Deleted}),
					ok = actordb_sharedstate:write_cluster(["shardsbeingtaken,",bkdcore:node_name()],SBT),
					?AINF("Still have not moved, yet to move ~p",[Deleted]),
					{reply,ok,P#dp{shardsbeingtaken = SBT}}
			end;
		false ->
			case find_global_shard(NewShard,NewShard) of
				{NewShard,_,Node} ->
					{reply,ok,P};
				Existing ->
					?AERR("Unknown shard moved?! shard ~p, tonode ~p, myshards ~p beingtaken ~p, current pos ~p",
							[NewShard,Node,P#dp.localshards,P#dp.shardsbeingtaken,Existing]),
					{reply,false,P}
			end
	end;
% Remote node wants to take shard from this node.
handle_call({steal_shard,Nd,Shard,NdToTakeFrom},_From,P) ->
	?AINF("Steal shard to ~p ~p, beingtaken ~p",[Nd,Shard,P#dp.shardsbeingtaken]),
	Me = bkdcore:node_name(),
	case lists:keyfind(Shard,1,P#dp.allshards) of
		{Shard,_,Nd} ->
			{reply,already_have_it,P};
		% {_,_,_} when P#dp.shardsbeingsplit /= [] ->
		% 	{reply,splitting,P};
		{Shard,ShardTo,NdToTakeFrom} ->
			case ok of
				_ when P#dp.shardsbeingtaken /= [] ->
					case lists:keyfind(Shard,2,P#dp.shardsbeingtaken) of
						{_,Shard,Nd,_} ->
							?ADBG("steal again"),
							Doit = already;
						_ ->
							?ADBG("Already have shards being taken ~p",[P#dp.shardsbeingtaken]),
							Doit = false
					end;
				_ when NdToTakeFrom == Me, length(P#dp.localshards) >= ?NUM_SHARDS ->
					?ADBG("Take conditions ok if not in prev ~p",[[]]),
					% Doit = lists:keymember(Shard,1,P#dp.shardsprevtaken) == false;
					Doit = true;
				_ when NdToTakeFrom /= Me ->
					Doit = length([ok || {_,_,Ndx} <- P#dp.allshards,Ndx == Nd]) >= ?NUM_SHARDS andalso
								 lists:member(NdToTakeFrom,bkdcore:all_cluster_nodes()), %andalso
								 %lists:keymember(Shard,1,P#dp.shardsprevtaken) == false,
					?ADBG("Not taking from me, doit ~p",[Doit]);
				_ ->
					?AERR("Steal shard no, state ~p",[?R2P(P)]),
					Doit = false
			end,
			?AINF("steal shard decision = ~p",[Doit]),
			case Doit of
				true ->
					SBT = [{actordb_util:split_point(Shard,ShardTo),Shard,Nd,actordb_util:actor_types()}],
					case actordb_sharedstate:write_cluster(["shardsbeingtaken,",bkdcore:node_name()],SBT) of
						ok ->
							{reply,ok,P#dp{shardsbeingtaken = SBT}};
						Err ->
							?ADBG("Steal err ~p",[Err]),
							{reply,Err,P}
					end;
				already ->
					{reply,ok,P};
				false ->
					{reply,busy,P}
			end;
		{Shard,_,_XNode} ->
			{reply, shard_moved,P}
	end;
% Change list of all shards. Shard has switched node.
handle_call({change_shard_node,NewShard,OldShard,Node},_,P) ->
	?AINF("Change shard node ~p ~p ~p~n",[NewShard,Node,P#dp.shardsbeingtaken]),
	Master = actordb_sharedstate:whois_global_master(),
	{OldShard,OldTo,OldNd} = lists:keyfind(OldShard,1,P#dp.allshards),
	NewAll = [{NewShard,OldTo,Node}|lists:keyreplace(OldShard,1,P#dp.allshards,{OldShard,NewShard-1,OldNd})],
	% NewAll = [case From == OldShard of
	% 				true ->
	% 					{From,NewShard-1,Nd};
	% 				false ->
	% 					{From,To,Nd}
	% 			end || {From,To,Nd} <- P#dp.allshards],
	% PrevTaken = lists:sublist([{Shard,Node}|P#dp.shardsprevtaken],5),
	case Master == bkdcore:node_name() of
		true ->
			case actordb_sharedstate:write_global_on(Master, shards,NewAll) of
				{master_is,Othernode} ->
					case bkdcore:rpc(Othernode,gen_server,call,[?MODULE,{change_shard_node,NewShard,OldShard,Node}]) of
						ok ->
							{reply,ok, P#dp{allshards = NewAll, dirty = true}};
						Err ->
							{reply,Err,P}
					end;
				ok ->
					{reply,ok,P#dp{allshards = NewAll,  dirty = true}}
		end;
		false ->
			case bkdcore:rpc(Master,gen_server,call,[?MODULE,{change_shard_node,NewShard,OldShard,Node}]) of
				ok ->
					{reply,ok, P#dp{allshards = NewAll, dirty = true}};
				Err ->
					{reply,Err,P}
			end
	end;
handle_call({shard_has_split,OriginalShard,NewShard},_,P) ->
	?AINF("shard_has_split original ~p, new ~p",[OriginalShard,NewShard]),
	Master = actordb_sharedstate:whois_global_master(),
	case lists:keyfind(NewShard,1,P#dp.allshards) of
		false ->
			{OriginalShard,To,Node} = lists:keyfind(OriginalShard,1,P#dp.allshards),
			NewAll = [{OriginalShard,NewShard-1,Node},{NewShard,To,Node}|lists:keydelete(OriginalShard,1,P#dp.allshards)],
			case Master == bkdcore:node_name() of
				true ->
					case actordb_sharedstate:write_global(shards,NewAll) of
						ok ->
							{reply,ok,P#dp{allshards = NewAll, dirty = true}};
						Err ->
							{reply,Err,P}
					end;
				false ->
					case bkdcore:rpc(Master,gen_server,call,[?MODULE,{shard_has_split,OriginalShard,NewShard}]) of
						ok ->
							{reply,ok, P#dp{allshards = NewAll, dirty = true}};
						Err ->
							{reply,Err,P}
					end
			end;
		_ ->
			{reply,ok,P}
	end;
handle_call(being_taken,_,P) ->
	{reply,P#dp.shardsbeingtaken,P};
handle_call(get_all_shards,_,P) ->
	{reply,{P#dp.allshards,P#dp.localshards},P};
handle_call(get_local_shards,_,P) ->
	{reply,P#dp.localshards,P};
handle_call(reload, _, P) ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	{reply, ok, ?MODULE:deser_prop(?R2P(P))};
% handle_call(print_info,F,P) ->
% 	?AINF("~p ~p",[F,?R2P(P)]),
% 	{noreply,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

deser_prop(P) ->
	?P2R(P).

handle_cast(schema_changed,P) ->
	?AINF("Schema changed"),
	handle_info({actordb,sharedstate_change},P#dp{haveschema = true});
handle_cast({shard_started,Pid,Shard,Type},P) ->
	?ADBG("shard_started ~p",[Shard]),
	case lists:keymember(Pid,1,P#dp.localshardpids) of
		false ->
			erlang:monitor(process,Pid),
			{noreply,P#dp{localshardpids = [{Pid,Shard,Type}|P#dp.localshardpids]}};
		_ ->
			{noreply,P}
	end;
handle_cast(print_info,P) ->
	?AINF("~p",[?R2P(P)]),
	{noreply,P};
handle_cast(_, P) ->
	{noreply, P}.


handle_info(startshards,P) ->
	Pidl = start_shards(P#dp.localshards,P#dp.localshardpids),
	[Pid ! borders_changed || {Pid,_From,_Type} <- Pidl],
	{noreply,P#dp{localshardpids = Pidl}};
handle_info(compileshards,P) ->
	Local = create_shard_tree(P#dp.localshards),
	All = create_shard_tree(P#dp.allshards),
	Taken = lists:filter(fun({NewShard,_OldShard,_,_}) -> lists:keymember(NewShard,1,P#dp.allshards) == false end,P#dp.shardsbeingtaken),
	?AINF("Compileshards ~p",[Local]),
	bkdcore:mkmodule(actordb_shardtree,[{local,Local},{all,All}]),
	actordb_shardmvr:local_shards_changed(P#dp.allshards,P#dp.localshards),
	% erlang:send_after(2000,self(),startshards),
	% {noreply,P#dp{shardsbeingtaken = Taken}};
	handle_info(startshards,P#dp{shardsbeingtaken = Taken});
handle_info(readshards,P) ->
	?AINF("Readshards ~p",[P#dp.allshards]),
	case P#dp.allshards of
		% Shards exist
		[_|_] ->
			case lists:filter(fun({_From,_To,Nd}) -> Nd == bkdcore:node_name() end,P#dp.allshards) of
				[] ->
					handle_info(compileshards,P);
				% Nothing needs to be changed. Start everything.
				L ->
					handle_info(compileshards,P#dp{localshards = L})
			end;
		% No shards exist, if we are master create them
		_ ->
			case actordb_sharedstate:am_i_global_master() of
				true ->
					L = create_shards(),
					case actordb_sharedstate:write_global(shards,L) of
						ok ->
							handle_info(readshards,P#dp{allshards = L});
						_ when P#dp.getstatepid /= undefined ->
							{noreply,P};
						_ ->
							{noreply,getstate(P)}
					end;
				false when P#dp.getstatepid /= undefined ->
					{noreply,P};
				false ->
					{noreply,getstate(P)}
			end
	end;
	% set_shard_border(Shard,Type,Limit,NewNode) ->
handle_info({'DOWN',_Monitor,_,PID,Result},#dp{getstatepid = PID} = P) ->
	case Result of
		{GlobalShards,Local1} ->
			case Local1 of
				[] ->
					Local = [];
				_ ->
					Local = [{NewShard,OldShard,Nd,TypesToGo} || {NewShard,OldShard,Nd,TypesToGo} <- Local1,
														lists:keymember(NewShard,1,P#dp.shardsbeingtaken) == false]
			end,
			case GlobalShards of
				[_|_] when P#dp.allshards == GlobalShards andalso P#dp.dirty == false ->
					{noreply,P#dp{getstatepid = undefined, shardsbeingtaken = P#dp.shardsbeingtaken++Local}};
				GlobalShards when GlobalShards /= nostate ->
					self() ! readshards,
					{noreply,P#dp{allshards = GlobalShards,getstatepid = undefined, shardsbeingtaken = P#dp.shardsbeingtaken++Local,
									dirty = false}};
				_G->
					{noreply,getstate(P#dp{shardsbeingtaken = P#dp.shardsbeingtaken++Local})}
			end;
		_X ->
			{noreply,getstate(P)}
	end;
handle_info({'DOWN',_Monitor,_,PID,Result},P) ->
	?ADBG("Shard dead ~p ~p~n",[PID,Result]),
	case lists:keyfind(PID,1,P#dp.localshardpids) of
		false ->
			{noreply,P};
		{PID,_,_} ->
			{noreply,P#dp{localshardpids = lists:keydelete(PID,1,P#dp.localshardpids)}}
	end;
% handle_info({bkdcore_sharedstate,cluster_state_change},P) ->
% 	case bkdcore:nodelist() /= [] andalso actordb_sharedstate:is_ok() andalso  of
% 		true ->
% 			handle_info({bkdcore_sharedstate,global_state_change},P);
% 		false ->
% 			{noreply,P}
% 	end;
handle_info({actordb,sharedstate_change},P) ->
	HaveNodes = bkdcore:nodelist() /= [],
	case HaveNodes andalso actordb_sharedstate:is_ok() andalso P#dp.haveschema of
		false ->
			?ADBG("Global statechange conditions failed havenodes=~p, sharedok=~p, haveschema=~p",[HaveNodes,actordb_sharedstate:is_ok(),P#dp.haveschema]),
			{noreply,P};
		_ ->
			?ADBG("GLobal statechange ~p ~p",[bkdcore:node_name(),P#dp.getstatepid]),
			case P#dp.getstatepid of
				undefined ->
					{noreply,getstate(P)};
				_ ->
					{noreply,P}
			end
	end;
handle_info({fprof,Opts},P) ->
	?AINF("Fprof trace: ~p",[fprof:trace(Opts)]),
	{noreply,P};
handle_info(eprof,P) ->
	?AINF("eprof ~p",[eprof:start_profiling([self()])]),
	{noreply,P};
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info(Msg, P) ->
	?AINF("shardmngr unhandled msg ~p",[Msg]),
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	% register and wait for cluster_connected message.
	actordb_sharedstate:subscribe_changes(?MODULE),
	case ets:info(?BORDERETS,size) of
		undefined ->
			ets:new(?BORDERETS, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{read_concurrency,true}]);
		_ ->
			ok
	end,
	self() ! {actordb,sharedstate_change},
	spawn(fun() ->
		case catch actordb_schema:types() of
			{'EXIT',_} ->
				ok;
			_ ->
				schema_changed()
		end
	end),
	{ok,#dp{}}.

getstate(P) ->
	case P#dp.getstatepid of
		undefined ->
			{NPid,_} =  spawn_monitor(fun() -> timer:sleep(2000), async_getstate() end),
			P#dp{getstatepid = NPid};
		_ ->
			P
	end.

async_getstate() ->
	Global = actordb_sharedstate:read_global(shards),
	case Global of
		nostate ->
			exit(nostate);
		_ ->
			ok
	end,
	case actordb_sharedstate:read_cluster([<<"shardsbeingtaken,">>,bkdcore:node_name()]) of
		undefined ->
			Local = [];
		Local when Local /= nostate ->
			ok;
		Local ->
			exit(nostate)
	end,
	exit({Global,Local}).


start_shards([{From,To,_Nd}|T],Existing) ->
	% For every actor type, check if shard has been started for it.
	StartedShards = butil:sparsemap(fun(Type) ->
										case butil:findtrue(fun({_Pid1,From1,Type1}) -> From == From1 andalso Type == Type1
															end,Existing) of
											% Shard does not exist.
											false ->
												Pid = startshard(Type,From,To),
												{Pid,From,Type};
											_X ->
												undefined
										end
									end,actordb_util:actor_types()),
	start_shards(T,StartedShards ++ Existing);
start_shards([],E) ->
	E.

startshard(Type,From,To) ->
	case actordb_shard:try_whereis(From,Type) of
		undefined ->
			{ok,Pid} = actordb_shard:start(From,Type,[{to,To}]);
		Pid ->
			ok
	end,
	erlang:monitor(process,Pid),
	Pid.

create_shard_tree(L) ->
	create_shard_tree1(lists:keysort(1,L)).
create_shard_tree1([]) ->
	undefined;
create_shard_tree1(L) ->
	{Left, Right} = lists:split(round(length(L)/2), L),
	[{Min, Max, Nd}|LeftSide] = lists:reverse(Left),
	{Min, Max, Nd, create_shard_tree1(lists:reverse(LeftSide)), create_shard_tree1(Right)}.


% No shards have been determined. Cluster has just been started for the first time.
% Create ?NUM_SHARDS for each node
create_shards() ->
	Nodes = lists:sort(bkdcore:nodelist_allclusters()),
	create_shards(Nodes).
create_shards(Nodes) ->
	Len = length(Nodes),
	Shardsize = ?NAMESPACE_MAX div (Len*(?NUM_SHARDS)),
	Shards = lists:reverse([?NAMESPACE_MAX|tl(lists:reverse(lists:seq(0,?NAMESPACE_MAX,Shardsize)))]),
	assign_shards(Nodes,Shards,[],Nodes).

assign_shards([Nd|NdT],[From,To|Shards],L,AllNodes) ->
	assign_shards(NdT,[To|Shards],[{From,To-1,Nd}|L],AllNodes);
assign_shards([],[_],L,_) ->
	L;
assign_shards([],Shards,L,AllNodes) ->
	assign_shards(AllNodes,Shards,L,AllNodes).
