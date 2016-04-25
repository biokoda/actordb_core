% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_shardmvr).
-behaviour(gen_server).
-define(LAGERDBG,true).
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([print_info/0,reload/0,deser_prop/1]).
-export([local_shards_changed/2,shard_moved/3,shard_has_split/3]).
-include_lib("actordb_core/include/actordb.hrl").
-include_lib("kernel/include/file.hrl").
-compile(export_all).

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
% [{SplitPoint,From,To,Node,[Type1,Type2,..]},..]
	shardstoget = [],
	% [{From,Type,Nd}]
	movingdone = [],
	local_shards = [],
	check_steal = true,
	badstop = false}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).


handle_call({shard_moved,Nd,Name,Type},_,P) ->
	?AINF("Shard moved ~p ~p ~p",[Name,Type, P#dp.shardstoget]),
	case lists:keyfind(Name,1,P#dp.shardstoget) of
		{Name,ShardFrom,To,Nd,Types} ->
			case bkdcore:rpc(Nd,{actordb_shardmngr,shard_moved,[Name,Type,bkdcore:node_name()]}) of
				ok ->
					% TG = lists:keydelete(Name,1,P#dp.shardstoget),
					Types1 = lists:delete(Type,Types),
					case Types1 of
						[] ->
							TG = lists:keydelete(Name,1,P#dp.shardstoget);
						_ ->
							TG = [{Name,ShardFrom,To,Nd,Types1}|lists:keydelete(Name,1,P#dp.shardstoget)]
					end,
					?AINF("Shardstoget now ~p~nprev ~p",[TG,P#dp.shardstoget]),
					store_toget(TG),
					{reply,ok,P#dp{movingdone = [{Name,Type,Nd}|P#dp.movingdone], shardstoget = TG}};
				Err ->
					{reply,Err,P}
			end;
		false ->
			% ?AERR("shard_moved that is not set for it"),
			{reply,ok,P}
	end;
handle_call(get_moves,_,P) ->
	{reply,{P#dp.shardstoget,P#dp.movingdone},P};
handle_call(reload, _, P) ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	{reply, ok, ?MODULE:deser_prop(?R2P(P))};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

deser_prop(P) ->
	?P2R(P).

handle_cast(can_start,P) ->
	case actordb_core:start_ready() of
		ok ->
			case P#dp.allshards /= undefined of
				true ->
					handle_cast({local_shards_changed,P#dp.allshards,P#dp.localshards},P#dp{can_start = true});
				false ->
					{noreply,P#dp{can_start = true}}
			end;
		_ ->
			{noreply,P}
	end;
handle_cast({local_shards_changed,A,L},#dp{can_start = false} = P) ->
	{noreply,P#dp{allshards = A, localshards = L}};
handle_cast({local_shards_changed,A,L},P) ->
	?AINF("local_shards_changed ~p ~p~n~p~n~p",[bkdcore:node_name(),A,L,P#dp.movingdone]),
	?AINF("stillmoving ~p ~p",[bkdcore:node_name(),P#dp.shardstoget]),
	% If first time this message sent, read if any toget in state
	case P#dp.shardstoget of
		[] ->
			TG = get_toget();
		TG ->
			ok
	end,
	{noreply,pick_shards(P#dp{shardstoget = TG, allshards = A, localshards = L})};
handle_cast(print_info,P) ->
	?AINF("~p",[?R2P(P)]),
	{noreply,P};
handle_cast(_, P) ->
	{noreply, P}.


can_start() ->
	case catch actordb_shardtree:local() of
		{'EXIT',_} ->
			% ?ADBG("Do not have shardtree"),
			ok;
		_X ->
			case catch actordb_schema:types() of
				{'EXIT',_} ->
					?ADBG("Do not have schema"),
					ok;
				_ ->
					[spawn_monitor(fun() ->  
									?ADBG("Check shard tree on=~p",[Nd]),
									case bkdcore:rpc(Nd,{actordb_shardtree,local,[]}) of
										{'EXIT',_} ->
											?ADBG("Nd=~p, does not have shard tree",[Nd]),
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
					end
			end
	end.
gather_concluded(0) ->
	true;
gather_concluded(N) ->
	receive
		{'DOWN',_Monitor,_,_PID,true} ->
			gather_concluded(N-1);
		{'DOWN',_Monitor,_,_PID,false} ->
			?ADBG("Some node does not have shardtree"),
			false;
		{'DOWN',_,_,_,_} ->
			false
	end.

handle_info(can_start,P) ->
	case P#dp.can_start of
		true ->
			ok;
		false ->
			% ?ADBG("Checking canstart"),
			spawn(fun() -> can_start() end),
			erlang:send_after(300,self(),can_start)
	end,
	{noreply,P};
handle_info({actordb,sharedstate_change},P) ->
	case get_toget() of
		[_|_] = TG when P#dp.shardstoget == [] ->
			?AINF("cluster_connected shards to get ~p",[TG]),
			{noreply,P#dp{shardstoget = TG}};
		_E ->
			{noreply,P}
	end;
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info(check_steal,P) ->
	case ok of
		_ when P#dp.localshards == undefined ->
			erlang:send_after(1000,self(),check_steal),
			{noreply,P};
		_ when P#dp.check_steal == true ->
			erlang:send_after(1000,self(),check_steal),
			{noreply,pick_shards(P)};
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
	erlang:send_after(2000,self(),check_steal),
	erlang:send_after(400,self(),can_start),
	actordb_sharedstate:subscribe_changes(?MODULE),
	{ok,#dp{}}.


store_toget(TG) ->
	ok = actordb_sharedstate:write_cluster(["shardstoget,",bkdcore:node_name()],TG),
	ok.
get_toget() ->
	case actordb_sharedstate:read_cluster(["shardstoget,",bkdcore:node_name()]) of
		undefined ->
			[];
		R ->
			R
	end.



start_shards(P) ->
	[ 
		[spawn(fun() ->
			actordb_shard:start_steal(Nd,From,To,SplitPoint,Type) end) || Type <- Types] 
		|| {SplitPoint,From,To,Nd,Types} <- P#dp.shardstoget
	],
	P.

node_shardsize(Local) ->
	lists:foldl(fun({From,To,_},Sum) -> Sum+(To-From) end, 0, Local).

% Called on node without enough shards. Will start stealing shards from other nodes if it can find any.
pick_shards(P) ->
	% If shard has appeared in global state as being from this node, delete from movingdone
	MD = lists:foldl(fun({Name,_,_},Moved) -> 
		[{Nm,Typ,Ndx} || {Nm,Typ,Ndx} <- Moved, Nm /= Name] 
	end,P#dp.movingdone,P#dp.localshards),
	?ADBG("pick_shards done=~p, loc=~p, all=~p",[MD,P#dp.localshards,P#dp.allshards]),
	pick_shards(P#dp{movingdone = MD},P#dp.allshards,P#dp.localshards).
pick_shards(P,[],_) ->
	P;
pick_shards(#dp{shardstoget = [], movingdone = []} = P,All,Local) ->
	?ADBG("Check move shards ~p, all=~p",[bkdcore:node_name(), All]),
	MySum = node_shardsize(Local),
	case get_toget() of
		[_|_] = TGFinal ->
			start_shards(P#dp{shardstoget = TGFinal});
		_ ->
			case split_shards(All) of
				[] ->
					P#dp{check_steal = false};
				[{_Nd,NdSize,[{FirstFrom,FirstTo,_}|_]}|_] = TG ->
					case NdSize >= MySum+((FirstTo-FirstFrom) div 2) of
						true ->
							?AINF("shardstoget candidates ~p",[TG]),
							TGFinal = try_start_steal(TG),
							start_shards(P#dp{shardstoget = TGFinal});
						false ->
							P#dp{check_steal = false}
					end
			end
	end;
pick_shards(P,_,_) when P#dp.shardstoget /= [] ->
	start_shards(P);
pick_shards(P,_,_) ->
	P.

try_start_steal([{Nd, _Size, [{From,To,Nd}|_]}|T]) ->
	SplitPoint = actordb_util:split_point(From,To),
	case bkdcore:rpc(Nd,actordb_shardmngr,steal_shard,[bkdcore:node_name(),From,Nd]) of
		ok ->
			?AINF("steal_shard success ~p ~p",[From,Nd]),
			[{SplitPoint,From,To,Nd,actordb_util:actor_types()}];
		_ ->
			try_start_steal(T)
	end;
try_start_steal([]) ->
	[].

% Pick one shard from another node to take. Uses filter_largest/4 to take out
%  any nodes that have less than current maximum amount of namespace assigned.
split_shards(L) ->
	Me = bkdcore:node_name(),
	Grouped = butil:keygroup(3,[{F,T,Nd} || {F,T,Nd} <- L, Nd /= Me]),
	% List of nodes with only their largest shards listed.
	Grouped1 = 
		[{
			Node,
			lists:sum([To - From || {From,To,_} <- NodeShards]),
			filter_largest(fun filter_shards/2,NodeShards,0,[])
		} || {Node,NodeShards} <- Grouped],
	% List only nodes with the largest shard space.
	filter_largest(fun filter_nodes/2,Grouped1,0,[]).
	% extract_shards(Grouped2,5).

% extract_shards([],_) ->
% 	[];
% % extract_shards(_,0) ->
% % 	[];
% extract_shards([{_Node,_Size,Shards}|T],N) ->
% 	[hd(Shards)|extract_shards(T,N-1)].


filter_nodes({_Node,Size,_},Max) ->
	case ok of
		_ when Size > Max ->
			{ok,Size};
		_ when Size == Max ->
			ok;
		_ ->
			false
	end.
			

filter_shards({From,To,_},Max) ->
	Size = To - From,
	case ok of
		_ when Size > Max ->
			{ok,Size};
		_ when Size == Max ->
			ok;
		_ ->
			false
	end.

filter_largest(Fun,[H|T],Max,L) ->
	case Fun(H,Max) of
		ok ->
			filter_largest(Fun,T,Max,[H|L]);
		{ok,NMax} ->
			filter_largest(Fun,T,NMax,[H]);
		_ ->
			filter_largest(Fun,T,Max,L)
	end;
filter_largest(_,[],_,L) ->
	L.
