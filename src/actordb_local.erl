% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_local).
-behaviour(gen_server).
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, 
		terminate/2, code_change/3,print_info/0,whereis/0,killactors/0,ulimit/0]).
% Multiupdaters
-export([pick_mupdate/0,mupdate_busy/2,get_mupdaters_state/0,reg_mupdater/2,local_mupdaters/0]).
% Actor activity
-export([actor_started/3,actor_mors/2,actor_cachesize/1,actor_activity/1]).
-define(LAGERDBG,true).
-include_lib("actordb.hrl").
-define(MB,1024*1024).
-define(GB,1024*1024*1024).

killactors() ->
	gen_server:cast(?MODULE,killactors).

% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 
% 						
% 									MULTIUPDATERS
% 
% - public ETS: multiupdaters
% {multiupdate_id,true/false} -> is multiupdater free or not
% 								 multiupdate_id is integer
% {all,[Updaterid1,Updaterid2,...]} -> all ids
% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 

reg_mupdater(Id,Pid) ->
	gen_server:call(?MODULE,{regupdater,Id,Pid}).
pick_mupdate() ->
	case butil:findtrue(fun(Id) -> V = butil:ds_val(Id,multiupdaters), V == true orelse V == undefined end,butil:ds_val(all,multiupdaters)) of
		false ->
			% They are all busy. Pick one at random and queue the request on it.
			actordb:hash_pick([self(),os:timestamp()],butil:ds_val(all,multiupdaters));
		Id ->
			Id
	end.

mupdate_busy(Id,Busy) ->
	butil:ds_add(Id,Busy,multiupdaters).

local_mupdaters() ->
	butil:ds_val(all,multiupdaters).

get_mupdaters_state() ->
	case ets:info(multiupdaters,size) of
		undefined ->
			[];
		_ ->
			case butil:ds_val(all,multiupdaters) of
				undefined ->
					[];
				ALL ->
					[{N,butil:ds_val(N,multiupdaters)} || N <- ALL]
			end
	end.



% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 
% 						
% 									ACTOR ACTIVITY TRACKING
% 
% - public ETS: actoractivity (ordered_set)
%   {now(),Pid} -> activity table of all actors. 
% - public ETS: actorsalive (set)
%   #actor key on pid
%   #actor with pid of actordb_local holds the cachesize sum of all actors
% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 
-record(actor,{pid,name,type,now,mors = master,masternode,cachesize=?DEF_CACHE_PAGES*1024,info = []}).

% called from actor
actor_started(Name,Type,Size) ->
	Now = now(),
	butil:ds_add({Now,self()},actoractivity),
	butil:ds_add(#actor{pid = self(),name = Name, type = Type, now = Now, cachesize = Size},actorsalive),
	ets:update_counter(actorsalive,whereis(?MODULE),{#actor.cachesize,Size}),
	gen_server:cast(?MODULE,{actor_started,self()}),
	Now.

% mors = master/slave
actor_mors(Mors,MasterNode) ->
	ets:update_element(actorsalive,self(),[{#actor.mors,Mors},{#actor.masternode,MasterNode}]),
	DN = bkdcore:dist_name(MasterNode),
	case DN == node() of
		false ->
			case lists:member(DN,nodes()) of
				false ->
					actordb_sqlproc:diepls(self(),not_in_nodes);
				_ ->
					ok
			end;
		_ ->
			ok
	end.

actor_cachesize(Size) ->
	A = butil:ds_val(self(),actorsalive),
	ets:update_element(actorsalive,self(),{#actor.cachesize,Size}),
	ets:update_counter(actorsalive,whereis(?MODULE),{#actor.cachesize,Size - A#actor.cachesize}).

% Call when actor does something. No need for every activity, < 5 times per second at the most.
actor_activity(PrevNow) ->
	Now = now(),
	butil:ds_rem(PrevNow,actoractivity),
	butil:ds_add({Now,self()},actoractivity),
	ets:update_element(actorsalive,self(),{#actor.now,Now}),
	Now.






ulimit() ->
	gen_server:call(?MODULE,ulimit).
start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).



-record(dp,{mupdaters = [], mpids = [], updaters_saved = true, 
% Ulimit and memlimit are checked on startup and will influence how many actors to keep in memory
			ulimit = 1024*100, memlimit = 1024*1024*1024, proclimit, lastcull = {0,0,0}}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	


handle_call({regupdater,Id,Pid},_,P) ->
	erlang:monitor(process,Pid),
	{reply,ok,P#dp{mpids = [{Id,Pid}|lists:keydelete(Id,1,P#dp.mpids)]}};
handle_call(mupdaters,_,P) ->
	{reply,{ok,[{N,butil:ds_val(N,multiupdaters)} || N <- P#dp.mupdaters]},P};
handle_call(ulimit,_,P) ->
	{reply,P#dp.ulimit,P};
handle_call(print_info,_,P) ->
	io:format("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.


handle_cast({actor_started,Pid},P) ->
	erlang:monitor(process,Pid),
	{noreply,P};
handle_cast(killactors,P) ->
	NProc = ets:info(actoractivity,size),
	killactors(NProc,ets:last(actoractivity)),
	{noreply,P};
handle_cast(_, P) ->
	{noreply, P}.

killactors(_,'$end_of_table') ->
	ok;
killactors(N,_) when N =< 0 ->
	ok;
killactors(N,Key) ->
	[{_Now,Pid}] = ets:lookup(actoractivity,Key),
	actordb_sqlproc:diepls(Pid,overlimit),
	killactors(N-1,ets:prev(actoractivity,Key)).


handle_info({'DOWN',_Monitor,_Ref,PID,_Reason}, P) ->
	case butil:ds_val(PID,actorsalive) of
		undefined ->
			case lists:keyfind(PID,2,P#dp.mpids) of
				{Id,PID} ->
					mupdate_busy(Id,false);
				_ ->
					ok
			end,
			{noreply,P#dp{mpids = lists:keydelete(PID,2,P#dp.mpids)}};
		Actor ->
			butil:ds_rem(PID,actorsalive),
			butil:ds_rem(Actor#actor.now,actoractivity),
			ets:update_counter(actorsalive,whereis(?MODULE),{#actor.cachesize,-Actor#actor.cachesize}),
			{noreply,P}
	end;
handle_info(timeout,P) ->
	erlang:send_after(100,self(),timeout),
	handle_info(check_limits,P);
handle_info(check_limits,P) ->
	NProc = ets:info(actoractivity,size),
	Memsize = (butil:ds_val(self(),actorsalive))#actor.cachesize,
	case NProc < P#dp.proclimit andalso Memsize < P#dp.memlimit of
		true ->
			% io:format("NOKILL ~p ~p~n",[Memsize,0.1*P#dp.memlimit]),
			LastCull = P#dp.lastcull;
		false ->
			Now = os:timestamp(),
			case timer:now_diff(Now,P#dp.lastcull) > 1000000 of
				true ->
					?AINF("Killing off inactive actors proc ~p, mem ~p",[{NProc,P#dp.proclimit},{Memsize,P#dp.memlimit}]),
					Killn = NProc - P#dp.proclimit - erlang:round(P#dp.proclimit*0.2),
					LastCull = Now,
					killactors(Killn,ets:last(actoractivity));
				false ->
					LastCull = P#dp.lastcull
			end
	end,
	{noreply,P#dp{lastcull = LastCull}};
handle_info(check_mem,P) ->
	erlang:send_after(10000,self(),check_mem),
	spawn(fun() -> 
			L = memsup:get_system_memory_data(),
			[Free,Total] = butil:ds_vals([free_memory,system_total_memory],L),
			case (Free / Total) < 0.2 of
				true ->
					NProc = ets:info(actoractivity,size),
					killactors(NProc*0.2,ets:last(actoractivity));
				false ->
					ok
			end
	 end),
	{noreply,P};
handle_info({bkdcore_sharedstate,cluster_state_change},P) ->
	handle_info({bkdcore_sharedstate,cluster_connected},P);
handle_info({bkdcore_sharedstate,global_state_change},P) ->
	?AINF("Global statechange ~p",[bkdcore:node_name()]),
	{noreply,P};
handle_info({bkdcore_sharedstate,cluster_connected},P) ->
	case P#dp.mupdaters of
		[] ->
			case bkdcore_sharedstate:get_cluster_state(mupdaters,{mupdaters,bkdcore:node_name()}) of
				nostate ->
					{noreply,P};
				[_|_] = NL ->
					?AINF("Clusterstate mupdaters ~p",[NL]),
					{noreply,P#dp{mupdaters = NL}};
				_ ->
					case create_mupdaters(?MIN_SHARDS*4,[]) of
						[] ->
							erlang:send_after(1000,self(),{bkdcore_sharedstate,cluster_connected}),
							{noreply,P};
						NL ->
							?AINF("Created mupdaters ~p",[NL]),
							% bkdcore_sharedstate:savetermfile(updaters_file(),NL),
							handle_info(save_updaters,P#dp{mupdaters = NL})
					end
			end;
		_ ->
			{noreply,P}
	end;
handle_info(save_updaters,P) ->
	butil:ds_add(all,P#dp.mupdaters,multiupdaters),
	case bkdcore_sharedstate:set_cluster_state(mupdaters,{mupdaters,bkdcore:node_name()},P#dp.mupdaters) of
		ok ->
			{noreply,P#dp{updaters_saved = true}};
		_ ->
			erlang:send_after(1000,self(),save_updaters),
			{noreply,P#dp{updaters_saved = false}}
	end;
handle_info({nodedown, Nd},P) ->
	case bkdcore:name_from_dist_name(Nd) of
		undefined ->
			ok;
		Nm ->
			% Some node has gone down, kill all slaves on this node.
			spawn(fun() -> 
				L = ets:match(actorsalive, #actor{masternode=Nm, pid = '$1', _='_'}),
				[actordb_sqlproc:diepls(Pid,masterdown) || [Pid] <- L]
			end)
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
	net_kernel:monitor_nodes(true),
	erlang:send_after(100,self(),timeout),
	erlang:send_after(10000,self(),check_mem),
	ok = bkdcore_sharedstate:register_app(?MODULE,{?MODULE,whereis,[]}),
	case ets:info(multiupdaters,size) of
		undefined ->
			ets:new(multiupdaters, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{write_concurrency,true}]);
		_ ->
			ok
	end,
	case ets:info(actoractivity,size) of
		undefined ->
			ets:new(actoractivity, [named_table,public,ordered_set,{heir,whereis(actordb_sup),<<>>},{write_concurrency,true}]);
		_ ->
			ok
	end,
	case ets:info(actorsalive,size) of
		undefined ->
			ets:new(actorsalive, [named_table,public,ordered_set,{heir,whereis(actordb_sup),<<>>},
									{write_concurrency,true},{keypos,#actor.pid}]);
		_ ->
			ok
	end,
	butil:ds_add(#actor{pid = self(),cachesize = 0},actorsalive),
	case butil:get_os() of
		win ->
			Ulimit = (#dp{})#dp.ulimit;
		_ ->
			Ulimit = butil:toint(lists:flatten(string:tokens(os:cmd("ulimit -n"),"\n\r")))
	end,
	case memsup:get_memory_data() of
		{0,0,_} ->
			Memlimit1 = (#dp{})#dp.memlimit; 
		{Memlimit1,_,_} ->
			ok
	end,
	case ok of
		_ when Ulimit =< 256 ->
			Proclimit = 100;
		_ when Ulimit =< 1024 ->
			Proclimit = 600;
		_ ->
			Proclimit = Ulimit - 2000
	end,
	case ok of
		_ when Memlimit1 =< ?GB ->
			Memlimit = 200*?MB;
		_ when Memlimit1 =< ?GB*2 ->
			Memlimit = ?GB;
		_ when Memlimit1 =< ?GB*4 ->
			Memlimit = 2*?GB;
		_ ->
			Memlimit = erlang:round(Memlimit1*0.5)
	end,
	{ok,#dp{memlimit = Memlimit, ulimit = Ulimit, proclimit = Proclimit}}.


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

create_mupdaters(0,L) ->
	L;
create_mupdaters(N,L) ->
	case bkdcore_idgen:getid() of
		{ok,Id} ->
			create_mupdaters(N-1,[Id|L]);
		_ ->
			L
	end.


