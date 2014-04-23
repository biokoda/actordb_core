% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_local).
-behaviour(gen_server).
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, 
		terminate/2, code_change/3,print_info/0,killactors/0,ulimit/0]).
% Multiupdaters
-export([pick_mupdate/0,mupdate_busy/2,get_mupdaters_state/0,reg_mupdater/2,local_mupdaters/0]).
% Actor activity
-export([actor_started/3,actor_mors/2,actor_cachesize/1,actor_activity/1]).
-export([subscribe_stat/0,report_write/0, report_read/0,get_nreads/0,get_nactors/0]).
-define(LAGERDBG,true).
-include_lib("actordb.hrl").
-define(MB,1024*1024).
-define(GB,1024*1024*1024).
-define(STATS,runningstats).

killactors() ->
	gen_server:cast(?MODULE,killactors).


% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 
% 
% 						stats
% 
% 	- public ETS: runningstats (?STATS)
% 		[{reads,N} {writes,N},{time_refs,RefFrom,RefTo}]
% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 
subscribe_stat() ->
	gen_server:call(?MODULE,{subscribe_stat,self()}).
report_read() ->
	ets:update_counter(?STATS,reads,1),
	ok.

report_write() ->
	ets:update_counter(?STATS,writes,1),
	ok.

get_nreads() ->
	butil:ds_val(reads,?STATS).
get_nwrites() ->
	butil:ds_val(writes,?STATS).

get_nactors() ->
	case ets:info(actorsalive,size) of
		undefined ->
			0;
		Size ->
			Size
	end.
	

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
	case butil:findtrue(fun(Id) -> V = butil:ds_val(Id,multiupdaters), V == true orelse V == undefined end,
						butil:ds_val(all,multiupdaters)) of
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
% - public ETS: actoractivity (ordered_set) -> ref is always incrementing so is perfect for sort key
%   {make_ref(),Pid} -> activity table of all actors. 
% - public ETS: actorsalive (set)
%   #actor key on pid
%   #actor with pid of actordb_local holds the cachesize sum of all actors
% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % 
-record(actor,{pid,name,type,now,mors = master,masternode,cachesize=?DEF_CACHE_PAGES*1024,info = []}).

% called from actor
actor_started(Name,Type,Size) ->
	Now = make_ref(),
	case get(localstarted) of
		undefined ->
			put(localstarted,true),
			butil:ds_add({Now,self()},actoractivity),
			butil:ds_add(#actor{pid = self(),name = Name, type = Type, now = Now, cachesize = Size},actorsalive),
			ets:update_counter(actorsalive,whereis(?MODULE),{#actor.cachesize,Size}),
			gen_server:cast(?MODULE,{actor_started,self()}),
			Now;
		_ ->
			case butil:ds_val(actorsalive,self()) of
				undefined ->
					erase(localstarted),
					actor_started(Name,Type,Size);
				Ex ->
					Ex#actor.now
			end
	end.

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
	Now = make_ref(),
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
			ulimit = 1024*100, memlimit = 1024*1024*1024, proclimit, lastcull = {0,0,0}, 
			% Every second do make_ref. Since ref is always incrementing it's a simple+fast way
			%  to find out which actors were active during prev second.
			prev_sec_from, prev_sec_to,
			stat_readers = [],prev_reads = 0, prev_writes = 0,
			% slots for 8 raft cluster connections
			% Set element is: NodeName
			raft_connections = {undefined,undefined,undefined,undefined,undefined,undefined,undefined,undefined}}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	


handle_call({regupdater,Id,Pid},_,P) ->
	erlang:monitor(process,Pid),
	{reply,ok,P#dp{mpids = [{Id,Pid}|lists:keydelete(Id,1,P#dp.mpids)]}};
handle_call(mupdaters,_,P) ->
	{reply,{ok,[{N,butil:ds_val(N,multiupdaters)} || N <- P#dp.mupdaters]},P};
handle_call(ulimit,_,P) ->
	{reply,P#dp.ulimit,P};
handle_call({subscribe_stat,Pid},_,P) ->
	{reply,ok,P#dp{stat_readers = [Pid|P#dp.stat_readers]}};
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
% handle_cast({connection_dead,N},P) ->
% 	case element(N,P#dp.raft_connections) of
% 		undefined ->
% 			{noreply,P};
% 		{_Nd,false,_} ->
% 			{noreply,P};
% 		{Nd,true,_} ->
% 			{noreply,P#dp{raft_connections = nodechange(Nd,P#dp.raft_connections,false)}}
% 	end;
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
handle_info(reconnect_raft,P) ->
	erlang:send_after(500,self(),reconnect_raft),
	% {noreply,P#dp{raft_connections = reconnect_raft(P#dp.raft_connections,1)}};
	esqlite3:tcp_reconnect(),
	{noreply,P};
handle_info(read_ref,P) ->
	erlang:send_after(1000,self(),read_ref),
	Ref = make_ref(),
	butil:ds_add(time_refs,{P#dp.prev_sec_to,Ref},?STATS),
	AllReads = get_nreads(),
	AllWrites = get_nwrites(),
	case P#dp.stat_readers of
		[] ->
			SR = [];
		_ ->
			Count = ets:select_count(actoractivity,[{{'$1','_'},[{'>','$1',P#dp.prev_sec_to},{'<','$1',Ref}], [true]}]),
			butil:ds_add(nactive,Count,?STATS),
			SR = [begin Pid ! {doread,AllReads,AllWrites,AllReads - P#dp.prev_reads,AllWrites - P#dp.prev_writes,Count},
					Pid 
		  		  end || Pid <- P#dp.stat_readers, erlang:is_process_alive(Pid)]
	end,
	{noreply,P#dp{prev_sec_to = Ref, prev_sec_from = P#dp.prev_sec_to,
					stat_readers = SR, prev_reads = AllReads, prev_writes = AllWrites}};
handle_info(check_mem,P) ->
	erlang:send_after(5000,self(),check_mem),
	spawn(fun() -> 
			L = memsup:get_system_memory_data(),
			[Free,Total] = butil:ds_vals([free_memory,system_total_memory],L),
			NProc = ets:info(actoractivity,size),
			case is_integer(Total) andalso 
				 is_integer(Free) andalso 
				 Total > 0 andalso 
				 (Free / Total) < 0.2 andalso
				 NProc > 100 of
				true ->
					killactors(NProc*0.2,ets:last(actoractivity));
				false ->
					ok
			end
	 end),
	{noreply,P};
handle_info({bkdcore_sharedstate,cluster_state_change},P) ->
	handle_info({bkdcore_sharedstate,cluster_connected},
		P#dp{raft_connections = store_raft_connection(bkdcore:cluster_nodes(),P#dp.raft_connections)});
handle_info({bkdcore_sharedstate,global_state_change},P) ->
	{noreply,P#dp{raft_connections = store_raft_connection(bkdcore:cluster_nodes(),P#dp.raft_connections)}};
handle_info({bkdcore_sharedstate,cluster_connected},P) ->
	case P#dp.mupdaters of
		[] ->
			case bkdcore_sharedstate:get_cluster_state(mupdaters,{mupdaters,bkdcore:node_name()}) of
				nostate ->
					{noreply,P};
				[_|_] = NL ->
					?AINF("Clusterstate mupdaters ~p",[NL]),
					butil:ds_add(all,NL,multiupdaters),
					{noreply,P#dp{mupdaters = NL}};
				_ ->
					{ok,NumMngrs} = application:get_env(actordb_core,num_transaction_managers),
					case create_mupdaters(NumMngrs,[]) of
						[] ->
							erlang:send_after(1000,self(),{bkdcore_sharedstate,cluster_connected}),
							{noreply,P};
						NL ->
							?AINF("Created mupdaters ~p",[NL]),
							% butil:savetermfile(updaters_file(),NL),
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
			{noreply,P};
		Nm ->
			% Some node has gone down, kill all slaves on this node.
			spawn(fun() -> 
				L = ets:match(actorsalive, #actor{masternode=Nm, pid = '$1', _='_'}),
				[actordb_sqlproc:diepls(Pid,masterdown) || [Pid] <- L]
			end),
			{noreply,P}
	end;
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info(_, P) -> 
	{noreply, P}.


getempty(T,N) ->
	case element(N,T) of
		undefined ->
			N;
		_ ->
			getempty(T,N+1)
	end.

getpos(T,N,Nd) when tuple_size(T) >= N ->
	case element(N,T) of
		Nd when is_binary(Nd) ->
			N;
		_ ->
			getpos(T,N+1,Nd)
	end;
getpos(_,_,_) ->
	undefined.


store_raft_connection([Nd|T],Tuple) ->
	case getpos(Tuple,1,Nd) of
		undefined ->
			Pos = getempty(Tuple,1),
			{IP,Port} = bkdcore:node_address(Nd),
			case esqlite3:tcp_connect_async(IP,Port,[bkdcore:rpccookie(Nd),"tunnelactordb_util"],Pos-1) of
				Ref when is_reference(Ref) ->
					store_raft_connection(T,setelement(Pos,Tuple,Nd));
				_ ->
					?AERR("Unable to establish replication connection to ~p",[Nd]),
					store_raft_connection(T,Tuple)
			end;
		_ ->
			store_raft_connection(T,Tuple)
	end;
store_raft_connection([],T) ->
	T.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init(_) ->
	net_kernel:monitor_nodes(true),
	erlang:send_after(100,self(),timeout),
	erlang:send_after(10000,self(),check_mem),
	erlang:send_after(1000,self(),read_ref),
	erlang:send_after(500,self(),reconnect_raft),
	ok = bkdcore_sharedstate:subscribe_changes(?MODULE),
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
	case ets:info(?STATS,size) of
		undefined ->
			ets:new(?STATS, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},
									{write_concurrency,true}]),
			butil:ds_add(writes,0,?STATS),
			butil:ds_add(reads,0,?STATS);
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
	{ok,#dp{memlimit = Memlimit, ulimit = Ulimit, proclimit = Proclimit, prev_sec_from = make_ref(),prev_sec_to = make_ref()}}.


create_mupdaters(0,L) ->
	L;
create_mupdaters(N,L) ->
	case bkdcore_idgen:getid() of
		{ok,Id} ->
			create_mupdaters(N-1,[Id|L]);
		_ ->
			L
	end.


