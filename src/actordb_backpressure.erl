% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_backpressure).
-behaviour(gen_server).
-export([start/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
-export([start_caller/0,start_caller/2,stop_caller/1,sleep_caller/1, is_enabled/0,is_enabled/2,
			inc_callcount/0,inc_callcount/1,dec_callcount/0,dec_callcount/1,call_count/0,call_size/0,
			inc_callsize/1,dec_callsize/1,inc_callsize/2,dec_callsize/2,
			save/3,getval/2,delval/2,has_authentication/3]).
-define(LAGERDBG,true).
-include_lib("actordb.hrl").

-record(caller,{ets, login}).
start_caller() ->
	E = ets:new(callerets,[set,private,{heir,whereis(?MODULE),self()}]),
	butil:ds_add(curcount,0,E),
	butil:ds_add(cursize,0,E),
	#caller{ets = E}.
start_caller(Username, Password) ->
	E = ets:new(callerets,[set,private,{heir,whereis(?MODULE),self()}]),
	Authentication = actordb_sharedstate:read_global_auth(),
	butil:ds_add(auth,Authentication,E),
	butil:ds_add(curcount,0,E),
	butil:ds_add(cursize,0,E),
	#caller{ets = E, login = butil:sha256(Username++";"++Password)}.
stop_caller(P) ->
	Tid = P#caller.ets,
	Size = butil:ds_val(cursize,Tid),
	Count = butil:ds_val(curcount,Tid),
	dec_callcount(Count),
	dec_callsize(Size),
	ets:delete(Tid),
	ok.

% Store data inside connection ets (for instance prepared statement info)
save(#caller{ets = Ets},K,V) ->
	butil:ds_add(K,V,Ets).
getval(#caller{ets = Ets},K) ->
	butil:ds_val(K,Ets).
delval(#caller{ets = Ets},K) ->
	butil:ds_rem(K,Ets).

% High watermark. If more than 2000 calls are waiting for result or
%  more than 32MB of queries is waiting to be processed
%  turn on bp.
is_enabled() ->
	is_enabled(butil:ds_val(global_size,bpcounters),butil:ds_val(global_count,bpcounters)).
is_enabled(GSize,GCount) ->
	GSize > 1024*1024*32 orelse GCount > 2000.

sleep_caller(_P) ->
	case whereis(backpressure_proc) of
		undefined ->
			% Spawn backpressure_proc and wait for it to die
			{Pid,_} = spawn_monitor(fun() ->
								case catch register(backpressure_proc,self()) of
									true ->
										?AINF("Backpressure applied."),
										backpressure_proc();
									_ ->
										% Proc lost the race. Simply link to the real proc so that they die together.
										link(whereis(backpressure_proc)),
										receive
											ok ->
												ok
										end
								end
								end);
		Pid ->
			erlang:monitor(process,Pid)
	end,
	receive
		{'DOWN',_Monitor,_,PID,_Reason} when PID == Pid ->
			ok
	end.

backpressure_proc() ->
	% low watermark 1MB, 300 calls
	case ets:info(bpcounters,size) == undefined orelse call_size() > 1024*1024 orelse call_count() > 300 of
		true ->
			timer:sleep(30),
			backpressure_proc();
		false ->
			?AINF("Backpressure released."),
			exit(continue)
	end.

inc_callcount() ->
	inc_callcount(1).
% Global increment
inc_callcount(N) when is_integer(N) ->
	ets:update_counter(bpcounters,global_count,N);
% Local increment
inc_callcount(P) ->
	ets:update_counter(P#caller.ets,curcount,1).
dec_callcount() ->
	dec_callcount(1).
% Global decrement
dec_callcount(N) when is_integer(N) ->
	ets:update_counter(bpcounters,global_count,-N);
% Local decrement
dec_callcount(P) ->
	ets:update_counter(P#caller.ets,curcount,-1).

inc_callsize(N) ->
	ets:update_counter(bpcounters,global_size,N).
inc_callsize(P,N) ->
	ets:update_counter(P#caller.ets,cursize,N).
dec_callsize(N) ->
	ets:update_counter(bpcounters,global_size,-N).
dec_callsize(P,N) ->
	ets:update_counter(P#caller.ets,cursize,-N).

call_count() ->
	butil:ds_val(global_count,bpcounters).
call_size() ->
	butil:ds_val(global_size,bpcounters).


start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

% No stop because this process must never die.
% stop() ->
% 	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).



-record(dp,{}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).


handle_call(print_info,_,P) ->
	io:format("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast(_, P) ->
	{noreply, P}.

handle_info({'ETS-TRANSFER',Tid,_FromPid,_HeirData},P) ->
	case catch stop_caller(#caller{ets = Tid}) of
		ok ->
			ok;
		Err ->
			?AERR("Cleanup caller error ~p",[Err])
	end,
	{noreply,P};
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
	process_flag(trap_exit,true),
	case ets:info(bpcounters,size) of
		undefined ->
			ets:new(bpcounters, [{write_concurrency,true},named_table,public,set,{heir,whereis(actordb_sup),<<>>}]),
			butil:ds_add(global_count,0,bpcounters),
			butil:ds_add(global_size,0,bpcounters);
		_ ->
			ok
	end,
	{ok,#dp{}}.

has_authentication(P,ActorType,Action)->
	Authentication = butil:ds_val(auth,P#caller.ets),
	Key = lists:keyfind(P#caller.login,3,Authentication),
	case Key of
		false -> false;
		{ActorType,_,_,ActionList} ->
			lists:member(Action,ActionList);
		_ -> false
	end.
