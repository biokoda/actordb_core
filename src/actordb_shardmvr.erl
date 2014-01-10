% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_shardmvr).
-behaviour(gen_server).
-define(LAGERDBG,true).
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,whereis/0]).
-export([print_info/0,reload/0,deser_prop/1]).
-export([local_shards_changed/2,shard_moved/3,shard_has_split/3]).
% for testing
-export([split_shards/4,has_neighbour/3]).
-include_lib("actordb.hrl").
-include_lib("kernel/include/file.hrl").
% -compile(export_all).

start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

local_shards_changed(A,L) ->
	gen_server:cast(?MODULE,{local_shards_changed,A,L}).

shard_moved(Nd,Name,Type) ->
	gen_server:call(?MODULE,{shard_moved,Nd,Name,Type},30000).

shard_has_split(Original,Name,Type) ->
	gen_server:call(?MODULE,{shard_has_split,Original,Name,Type},30000).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:cast(?MODULE,print_info).
reload() ->
	gen_server:call(?MODULE, reload).

-record(dp,{can_start = false, allshards, localshards,
% Which shards local node is taking from other nodes
% [{From,To,Node,[Type1,Type2,..]},..]
	shardstoget = [],
% [{Pid,From,To,Nodename,Type}]
	% shardstogetpid = [],
	% [{From,Type,Nd}]
	movingdone = [],
	% [{Name,Type}]
	shards_splitting = [],
	local_shards = [],
	init = false,
	badstop = false}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	


handle_call({shard_has_split,Original,Name,Type},_,P) ->
	?AINF("Shard has split ~p ~p ~p",[Name,Type,P#dp.shards_splitting]),
	case lists:member({Name,Type},P#dp.shards_splitting) of
		true ->
			Without = lists:delete({Name,Type},P#dp.shards_splitting),
			case lists:keyfind(Type,2,Without) of
				false ->
					Res = actordb_shardmngr:shard_has_split(Original,Name);
				_ ->
					Res = ok
			end,
			{reply,Res,P#dp{shards_splitting = Without}};
		false ->
			?AERR("Shard has split but not set to on this node"),
			{reply,ok,P}
	end;
handle_call({shard_moved,Nd,Name,Type},_,P) ->
	?AINF("Shard moved ~p ~p ~p",[Name,Type, P#dp.shardstoget]),
	case lists:keyfind(Name,1,P#dp.shardstoget) of
		{Name,_To,Nd,Types} ->
			case bkdcore:rpc(Nd,{actordb_shardmngr,shard_moved,[Name,Type,bkdcore:node_name()]}) of
				ok ->
					% TG = lists:keydelete(Name,1,P#dp.shardstoget),
					Types1 = lists:delete(Type,Types),
					case Types1 of
						[] ->
							TG = lists:keydelete(Name,1,P#dp.shardstoget);
						_ ->
							TG = [{Name,Type,Nd,Types1}|lists:keydelete(Name,1,P#dp.shardstoget)]
					end,
					?AINF("Shardstoget now ~p~nprev ~p",[TG,P#dp.shardstoget]),
					store_toget(TG),
					{reply,ok,P#dp{movingdone = [{Name,Type,Nd}|P#dp.movingdone], shardstoget = TG}};
				Err ->
					{reply,Err,P}
			end;
		false ->
			?AERR("shard_moved that is not set for it"),
			{reply,ok,P}
	end;
handle_call(get_moves,_,P) ->
	S = [{distreg:whereis({shard,T,F}),F,T} || {F,T} <- P#dp.shards_splitting],
	{reply,{P#dp.shardstoget,P#dp.movingdone,S},P};
handle_call(reload, _, P) ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	{reply, ok, ?MODULE:deser_prop(?R2P(P))};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

deser_prop(P) ->
	?P2R(P).

handle_cast(can_start,P) ->
	actordb_core:start_ready(),
	case P#dp.allshards /= undefined of
		true ->
			handle_cast({local_shards_changed,P#dp.allshards,P#dp.localshards},P#dp{can_start = true, allshards = undefined, localshards = undefined});
		false ->
			{noreply,P#dp{can_start = true}}
	end;
handle_cast({local_shards_changed,A,L},#dp{can_start = false} = P) ->
	{noreply,P#dp{allshards = A, localshards = L}};
handle_cast({local_shards_changed,A,L},P) ->
	?ADBG("local_shards_changed ~p ~p~n~p~n~p",[bkdcore:node_name(),A,L,P#dp.movingdone]),
	?ADBG("stillmoving ~p ~p",[bkdcore:node_name(),P#dp.shardstoget]),
	% If first time this message sent, read if any toget in state
	case P#dp.init of
		false ->
			TG = get_toget();
		true ->
			TG = P#dp.shardstoget
	end,
	{noreply,pick_shards(P#dp{init = true,
							shardstoget = TG, 
							% If shard has appeared in global state as being from this node, delete from movingdone
							movingdone = lists:foldl(fun({Name,_,_},Moved) -> 
											[{Nm,Typ,Ndx} || {Nm,Typ,Ndx} <- Moved, Nm /= Name] 
										end,P#dp.movingdone,L)},
					A,L)};
handle_cast(print_info,P) ->
	?AINF("~p",[?R2P(P)]),
	{noreply,P};
handle_cast(_, P) ->
	{noreply, P}.


can_start() ->
	[spawn_monitor(fun() ->  
					case bkdcore:rpc(Nd,{actordb_shardtree,local,[]}) of
						{'EXIT',_} ->
							exit(false);
						_ ->
							exit(true)
					end
			   end) || Nd <- bkdcore:cluster_nodes()],
	case gather_concluded(length(bkdcore:cluster_nodes())) of
		true ->
			gen_server:cast(?MODULE,can_start);
		false ->
			ok
	end.
gather_concluded(0) ->
	true;
gather_concluded(N) ->
	receive
		{'DOWN',_Monitor,_,_PID,true} ->
			gather_concluded(N-1);
		{'DOWN',_Monitor,_,_PID,false} ->
			false;
		{'DOWN',_,_,_,_} ->
			false
	end.

handle_info(can_start,P) ->
	case P#dp.can_start of
		true ->
			ok;
		false ->
			spawn(fun() -> can_start() end),
			erlang:send_after(300,self(),can_start)
	end,
	{noreply,P};
handle_info({bkdcore_sharedstate,Nd,State},P) ->
	case State of
		init ->
			bkdcore_sharedstate:app_vote_done(actordb,Nd);
		reconnect ->
			bkdcore_sharedstate:app_vote_done(actordb,Nd);
		_ ->
			ok
	end,
	{noreply,P};
% handle_info({'DOWN',_Monitor,_,PID,Reason},P) ->
% 	case lists:keyfind(PID,1,P#dp.shardstogetpid) of
% 		false ->
% 			{noreply,P};
% 		{PID,_From,_To,_Nd,_Type} ->
% 			?ADBG("Shard down ~p, reason ~p",[_From,Reason]),
% 			{noreply,P#dp{shardstogetpid = lists:keydelete(PID,1,P#dp.shardstogetpid), badstop = Reason /= normal}}
% 	end;
handle_info({bkdcore_sharedstate,cluster_state_change},P) ->
	{noreply,P};
handle_info({bkdcore_sharedstate,global_state_change},P) ->
	?ADBG("Global statechange ~p",[bkdcore:node_name()]),
	{noreply,P};
handle_info({bkdcore_sharedstate,cluster_connected},P) ->
	case get_toget() of
		[_|_] = TG when P#dp.shardstoget == [] ->
			?AINF("cluster_connected shards to get ~p",[TG]),
			{noreply,P#dp{shardstoget = TG, init = true}};
		_ ->
			{noreply,P}
	end;
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info(check_steal,P) ->
	erlang:send_after(10000,self(),check_steal),
	?ADBG("Check steal ~p ~p",[P#dp.shardstoget, P#dp.shards_splitting]),
	handle_info(check_splits,start_shards(P));
handle_info(check_splits,P) ->
	case ok of
		_ when P#dp.shards_splitting /= [] ->
			{noreply,start_splits(P,P#dp.local_shards)};
		_ ->
			{noreply,P}
	end;
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info(Msg, P) -> 
	?AINF("shardmngr unhandled msg ~p ~p",[Msg,bkdcore:node_name()]),
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	erlang:send_after(10000,self(),check_steal),
	erlang:send_after(400,self(),can_start),
	ok = bkdcore_sharedstate:register_app(?MODULE,{?MODULE,whereis,[]}),
	{ok,#dp{}}.

whereis() ->
	case whereis(?MODULE) of
		undefined ->
			case butil:is_app_running(actordb_core) of
				true ->
					timer:sleep(10),
					whereis();
				false ->
					undefined
			end;
		P ->
			P
	end.

store_toget(TG) ->
	ok = bkdcore_sharedstate:set_cluster_state(shardmngr,{shardstoget,bkdcore:node_name()},TG),
	ok.
get_toget() ->
	case bkdcore_sharedstate:get_cluster_state(shardmngr,{shardstoget,bkdcore:node_name()}) of
		undefined ->
			[];
		R ->
			R
	end.



start_shards(P) ->
	[ 
		[actordb_shard:start_steal(Nd,From,Type) || Type <- Types] 
		|| {From,_To,Nd,Types} <- P#dp.shardstoget
	],
	% butil:sparsemap(fun({From,To,Nd,Types}) ->
	% 		?AINF("start_stealshard ~p ~p ~p",[From,To,Nd]),
	% 	 	X = butil:sparsemap(fun(Type) -> 
	% 				case actordb_shard:start_steal(Nd,From,Type) of
	% 					{ok,_Pid} ->
	% 						% erlang:monitor(process,Pid),
	% 				 		{{Type,From},To,Nd};
	% 					_E ->
	% 						?AERR("Start steal error ~p",[_E]),
	% 						undefined
	% 				end
	% 		% 	Pid ->
	% 		% 		{Pid,From,To,Nd,Type}
	% 		% end
	% 	end,actordb_util:actor_types()),
	% 	case X of
	% 		[] ->
	% 			undefined;
	% 		_L ->
	% 			% L
	% 			undefined
	% 	end
	% 		end,P#dp.shardstoget),
		P.



start_splits(P,Local) ->
	?AINF("Start split shards ~p",[Local]),
	Splits =  butil:sparsemap(fun({From,To,Nd}) ->
					 SplitPoint = From + ((To-From) div 2),
					 
					 ShardsPerType = butil:sparsemap(fun(Type) -> 
					 	?AINF("start_splitshard ~p ~p ~p",[From,To,Nd]),
					 	% distreg:whereis({shard,Type,SplitPoint})
					 	case actordb_shard:try_whereis(SplitPoint,Type) of
					 		undefined ->
								case catch actordb_shard:start_split(From,Type,SplitPoint) of
									{ok,_Pid} ->
										{SplitPoint,Type};
									_Err ->
										?AINF("SPLIT ERR ~p",[_Err]),
										undefined
								end;
							_ ->
								undefined
						end
					end,actordb_util:actor_types()),
					ShardsPerType
	end,Local),
	P#dp{local_shards = Local,
	     shards_splitting = P#dp.shards_splitting ++ (lists:flatten(Splits) -- P#dp.shards_splitting)}.


% Called on node without enough shards. Will start stealing shards from other nodes if it can find any.
pick_shards(P,All,Local) ->
	case ok of
		_ when P#dp.shardstoget == [], P#dp.movingdone == [] ->
			?ADBG("Started move shards ~p",[bkdcore:node_name()]),
			case get_toget() of
				[_|_] = TG ->
					ok;
				_ ->
					TG = [{From,To,Nd,actordb_schema:types()} || {From,To,Nd} <- split_shards(All,Local)]
			end,
			?AINF("shardstoget ~p",[TG]),
			case TG of
				[] ->
					case length(Local) =< ?MIN_SHARDS of
						true ->
							start_splits(P,Local);
						false ->
							P
					end;
				_ ->
					{Started,NotStarted} = lists:foldl(fun({From,To,Nd,Types},{Started,Not}) -> 
						case bkdcore:rpc(Nd,actordb_shardmngr,steal_shard,[bkdcore:node_name(),From,Nd]) of
							ok ->
								?AINF("steal_shard success ~p ~p",[From,Nd]),
								{[{From,To,Nd,Types}|Started],Not};
							Err ->
								?AINF("Unable to take shard ~p ~p ~p",[From,Nd,Err]),
								{Started,[{From,To,Nd,Types}|Not]}
						end
					end,
					{[],[]},TG),
				case NotStarted of
					[] ->
						TGFinal = Started;
					_ ->
						% TGFinal = lists:subtract(TG,NotStarted),
						TGFinal = [{From,To,Nd,Types} || {From,To,Nd,Types} <- TG, lists:keyfind(From,1,NotStarted) == false],
						store_toget(TGFinal)
				end,
				start_shards(P#dp{shardstoget = TGFinal})
			end;
		_ ->
			P
	end.


% New node. Other nodes in network have shards, this one might not have enough (or any). 
% Decide which shards to move over to this node.
% We want to avoid having neighbouring shards on the same node. So if a node we might be taking shards from has neighbours,
%  pick one of them unless that would create a neighbour on node we are picking for.
% Returns: [{From,To,CurrentNode},...]
split_shards(L,Existing) ->
	AllNodes = lists:flatten([bkdcore:nodelist(G)|| G <- bkdcore:groups_of_type(cluster)]),
	split_shards(bkdcore:node_name(),AllNodes,L,Existing).
split_shards(Me,AllNodes,L,Existing) ->
	ExistingSize = lists:sum([To-From || {From,To,_Nd} <- Existing]),
	Grouped = butil:keygroup(3,[{F,T,Nd} || {F,T,Nd} <- L, Nd /= Me]),
	% ?AINF("split_shards grouped ~p",[Grouped]),

	% Convert to [{NodeName,NamespaceSize,Shards}]
	%  NamespaceSize is sum of all shard sizes
	%  Shards is list of shards for node. 
	% Sorted so that shards will be taken from nodes with highest NamespaceSizes
	Sorted = lists:reverse(lists:keysort(2,
		[{
		   Node,
		   lists:sum([To - From || {From,To,_} <- NodeShards]),
	     lists:reverse(lists:keysort(1,NodeShards))
	   } || {Node,NodeShards} <- Grouped])),
	SizeGoal = ?NAMESPACE_MAX div length(AllNodes),
	split_shards(ExistingSize,Existing,SizeGoal - ExistingSize,Sorted,[],[]).

split_shards(_,_,_,[],[],[]) ->
	[];
split_shards(_,_,Limit,_,_,[]) when Limit =< 0 ->
	[];
split_shards(_,_,Limit,_,_,L) when Limit =< 0 ->
	L;
split_shards(ExistingSize,Existing,Limit,[{Node,Size,Shards}|T],PrevNodes,L) ->
	case length(Shards) > ?MIN_SHARDS andalso 
		   length(Existing)+1 < length(Shards) andalso 
		   ExistingSize < Size of
		true ->
			case shard_candidates(Shards,Existing,L,[]) of
				[] ->
					?AINF("No direct candidates ~p",[Node]),
					case [{SF,ST,SN} || {SF,ST,SN} <- Shards, has_neighbour(SF,ST,Existing) == false, 
													has_neighbour(SF,ST,L) == false] of
						[{From,To,_}|_] ->
							?AINF("Picked ~p",[From]),
							ShardRem = lists:keydelete(From,1,Shards);
						_ ->
							Neighbors = [{SF,ST,SN} || {SF,ST,SN} <- Shards, 
											has_neighbour(SF,ST,Existing) orelse has_neighbour(SF,ST,L)],
							?AINF("No candidates at all ~p~nneighbours ~p",[Node,Neighbors]),
							From = To = ShardRem = undefined
					end;
				[{From,To,_}|_] ->
					ShardRem = lists:keydelete(From,1,Shards),
					ok
			end,
			case From of
				undefined ->
					split_shards(ExistingSize,Existing,Limit,T,PrevNodes,L);
				_ ->
					ShardSize = To-From,
					split_shards(ExistingSize,Existing,
									Limit-ShardSize,T,
									[{Node,Size - ShardSize,ShardRem}|PrevNodes], %lists:keydelete(From,1,Shards)
									[{From,To,Node}|L])
			end;
		% Wait for node to duplicate number of shards as it has too few atm.
		false ->
			?AINF("Node has too few shards ~p",[Node]),
			% ?ADBG("Node does not have enough shards ~p ~p ~p",[bkdcore:node_name(),Node,L]),
			L
	end;
split_shards(ExistingSize,Existing,Limit,[],[_|_] = Prev,L) ->
	split_shards(ExistingSize,Existing,Limit,lists:reverse(lists:keysort(2,Prev)),[],L);
split_shards(_,_,_,[],[],L) ->
	L.

% If node we are picking from has neighbouring shards, those are primary candidates.
shard_candidates([{From,To,Nd}|T],Existing,PickedAlready,L) ->
	case has_neighbour(From,To,T) of
		true ->
			% This shard has a neighbour, but must not have a neighbour in among
			%  existing shards for node we are picking for.
			case has_neighbour(From,To,Existing) of
				false ->
					case has_neighbour(From,To,PickedAlready) of
						false ->
							shard_candidates(T,Existing,[{From,To,Nd}|PickedAlready],[{From,To,Nd}|L]);
						true ->
							shard_candidates(T,Existing,PickedAlready,L)
					end;
				true ->
					shard_candidates(T,Existing,PickedAlready,L)
			end;
		false ->
			shard_candidates(T,Existing,PickedAlready,L)
	end;
shard_candidates([],_,_,L) ->
	L.

% Does L have neighbor of shard that starts with From
has_neighbour(From,To,L) ->
	case butil:findtrue(fun({F1,T1,_}) -> To+1 == F1 orelse From-1 == T1 end,L) of
		false ->
			false;
		_ ->
			true
	end.


