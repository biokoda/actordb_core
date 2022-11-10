% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_latency).
-behaviour(gen_server).
-export([latency/0]).
-export([start/0,stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
-export([tunnel_callback/2, return_call/2, set_run_queue/1]).
-include("actordb.hrl").

% Replication requires some fixed timing values. But those are dangerous in real world. 
% What if nodes are far apart? What if network conditions are bad? What if node is overloaded?
% A node may issue elections or writes because it thinks it needs to resync, which makes a bad
% situation worse. Piling on more work through a bottleneck. 
% To mitigate this we keep track of network latency to nodes in our cluster and we keep track of
% run_queue length on local node.
latency() ->
	case (catch ets:tab2list(latency)) of
		[_|_] = L ->
			% Network latency + scheduling latency
			butil:ds_val(latency,L) + min(3000,butil:ds_val(run_queue,L)*20);
		_ ->
			3000
	end.

set_run_queue(Q) ->
	butil:ds_add(run_queue,Q,latency).

start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).

% Called on remote node through tunnel connection
tunnel_callback(Nd,Time) ->
	rpc:call(Nd,?MODULE,return_call,[node(),Time]).
% Called back on local node
return_call(Nd,Time) ->
	gen_server:cast(?MODULE,{return_call, Nd,Time}).

-record(dp,{interval = [], global_max = 0, global_min = 0, interval_max = 0, nresponses = 0}).

handle_call(print_info,_,P) ->
	?AINF("~p",[P]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast({return_call,_Nd,{MS,S,MiS}},P) ->
	handle_cast({return_call,_Nd,MS*1000000000000 + S*1000000 + MiS},P);
handle_cast({return_call,_Nd,Time},P) ->
	Now = erlang:system_time(micro_seconds),
	% ?AINF("Latency from=~p, is=~p",[_Nd,Now - Time]),
	Latency = min(3000,erlang:abs(Now - Time) div 1000),
	% Time is received from all nodes. Keep the last received one (highest latency)
	case lists:keyfind(Time,1,P#dp.interval) of
		false ->
			Interval = [{Time,Latency}|P#dp.interval];
		{Time,PrevInterval} when PrevInterval < Latency ->
			Interval = lists:keystore(Time,1,P#dp.interval,{Time,Latency});
		{Time,_} ->
			Interval = P#dp.interval
	end,
	case Interval of
		[_A1,_A2,_A3,_A4,_A5,_A6,_A7,_A8,_A9,_A10|_] ->
			{Interval1,_} = lists:split(10,lists:reverse(lists:keysort(1,Interval)));
		Interval1 ->
			ok
	end,
	MaxInInterval = lists:foldl(fun({_,LT},CurMax) -> case CurMax > LT of true -> CurMax; false -> LT end end,
			element(2,hd(Interval1)), Interval1),
	case P#dp.interval_max /= MaxInInterval of
		true ->
			butil:ds_add(latency,MaxInInterval,latency),
			case MaxInInterval > (P#dp.interval_max+10000) andalso MaxInInterval > 20000 of
				true ->
					?AINF("Replication latency increased to ~p ms. High load or slow network.",[MaxInInterval div 1000]);
				false ->
					ok
			end;
		false ->
			ok
	end,
	% ?ADBG("Latency nd=~p latency=~p max_in_interval=~p",[Nd,Latency,MaxInInterval]),
	{noreply,P#dp{global_max = max(Latency,P#dp.global_max),
					interval = Interval1,
					nresponses = P#dp.nresponses + 1,
					interval_max = max(P#dp.interval_max,MaxInInterval),
				  global_min = min(Latency,P#dp.global_min)}};
handle_cast(_, P) ->
	{noreply, P}.

handle_info(latency_check,P) ->
	% Send call to all tunnel connections.
	% Nodes on the other side will do a rpc back with this time.
	% We can keep track of max latency this way.
	% This will affect election timers. Election timer should
	%  not be lower than connection latency.
	erlang:send_after(300,self(),latency_check),
	case nodes() of
		[] ->
			{noreply,P};
		_ ->
			Term = term_to_binary({?MODULE,[node(),erlang:system_time(micro_seconds)]}),
			actordb_sqlite:all_tunnel_call([<<(iolist_size(Term)):16>>,Term]),
			{noreply,P}
	end;
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info(_, P) ->
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init(_) ->
	erlang:send_after(300,self(),latency_check),
	case ets:info(latency,size) of
		undefined ->
			ets:new(latency, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{read_concurrency,true}]),
			butil:ds_add(latency,0,latency),
			butil:ds_add(run_queue,0,latency);
		_ ->
			ok
	end,
	{ok,#dp{}}.
