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
-export([actor_started/0,actor_mors/2,actor_activity/1]).
-export([subscribe_stat/0,report_write/0, report_read/0,get_nreads/0,get_nwrites/0,get_nactors/0]).
% Ref age
-export([net_changes/0,mod_netchanges/0]).
-define(LAGERDBG,true).
-include_lib("actordb_core/include/actordb.hrl").
-define(MB,1024*1024).
-define(GB,1024*1024*1024).
% -define(STATS,runningstats).
% -define(REF_TIMES,reftimes).
% Stores net changes, current and previous active table.
-define(GLOBAL_INFO,globalinfo).

% Every write/read/significant event on actor is written to current table.
% After 20s:
% - If a previous table exists, add all pids to hibernate table 
%   and send message to processes to go into hibernation. Table is then deleted.
% - Current table becomes previous table
% - New current table is created
-define(CUR_ACTIVE,currently_active).
-define(PREV_ACTIVE,previously_active).
-define(HIBERNATE,hibernate_list).

killactors() ->
	gen_server:cast(?MODULE,killactors).

net_changes() ->
	butil:ds_val(netchanges,?GLOBAL_INFO).
mod_netchanges() ->
	ets:update_counter(?GLOBAL_INFO,netchanges,1).


% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % %
%
% 						stats
%
% 	- public ETS: runningstats (?STATS)
% 		[{reads,N} {writes,N}
% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % %
subscribe_stat() ->
	ok.
% 	gen_server:call(?MODULE,{subscribe_stat,self()}).
report_read() ->
	folsom_metrics_counter:inc(reads, 1).

report_write() ->
	folsom_metrics_counter:inc(writes, 1).

get_nreads() ->
	folsom_metrics_counter:get_value(reads).
% 	butil:ds_val(reads,?STATS).
get_nwrites() ->
	folsom_metrics_counter:get_value(writes).
% 	butil:ds_val(writes,?STATS).

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
% {multiupdate_id,true/false} -> is multiupdater free or not multiupdate_id is integer
% {all,[Updaterid1,Updaterid2,...]} -> all ids
% % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % % %

reg_mupdater(Id,Pid) ->
	gen_server:call(?MODULE,{regupdater,Id,Pid}).
pick_mupdate() ->
	case butil:findtrue(fun(Id) -> V = butil:ds_val(Id,multiupdaters), V == true orelse V == undefined end,
						butil:ds_val(all,multiupdaters)) of
		false ->
			% They are all busy. Pick one at random and queue the request on it.
			actordb:hash_pick(term_to_binary([self(),make_ref(),1234]),butil:ds_val(all,multiupdaters));
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
% -record(actor,{pid,name,type,now,mors = master,masternode,cachesize=?DEF_CACHE_PAGES*?PAGESIZE,info = []}).
-record(actor,{pid,mors = master,masternode}).

% called from actor
actor_started() ->
	butil:ds_add(#actor{pid = self()},actorsalive),
	gen_server:cast(?MODULE,{actor_started,self()}),
	actor_activity(undefined).

% mors = master/slave
actor_mors(Mors,MasterNode) ->
	ets:update_element(actorsalive,self(),[{#actor.mors,Mors},{#actor.masternode,MasterNode}]).
	% DN = bkdcore:dist_name(MasterNode),
	% case DN == node() of
	% 	false ->
	% 		case lists:member(DN,nodes()) of
	% 			false ->
	% 				?AERR("Not in nodes ~p ~p",[DN,nodes()]),
	% 				actordb_sqlproc:diepls(self(),not_in_nodes);
	% 			_ ->
	% 				ok
	% 		end;
	% 	_ ->
	% 		ok
	% end.

% Called when actor does something relevant (write,read,copy).
actor_activity(PrevTable) ->
	case ets:lookup(?GLOBAL_INFO, ?CUR_ACTIVE) of
		[{_,PrevTable}] ->
			PrevTable;
		[{_,NewTable}] ->
			case ets:info(PrevTable,type) of
				undefined ->
					ok;
				_ ->
					(catch ets:delete(PrevTable,{self()}))
			end,
			ets:insert(NewTable,{self()}),
			NewTable
	end.





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
	?AINF("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.


handle_cast({actor_started,Pid},P) ->
	erlang:monitor(process,Pid),
	{noreply,P};
handle_cast(killactors,P) ->
	% NProc = ets:info(actoractivity,size),
	% killactors(NProc,ets:last(actoractivity)),
	{noreply,P};
handle_cast(_, P) ->
	{noreply, P}.

% killactors(_,'$end_of_table') ->
% 	ok;
% killactors(N,_) when N =< 0 ->
% 	ok;
% killactors(N,Key) ->
% 	[{_Now,Pid}] = ets:lookup(actoractivity,Key),
% 	actordb_sqlproc:diepls(Pid,overlimit),
% 	killactors(N-1,ets:prev(actoractivity,Key)).


handle_info({'DOWN',_Monitor,_Ref,PID,_Reason}, P) ->
	start_timer(P),
	case butil:ds_val(PID,actorsalive) of
		undefined ->
			case lists:keyfind(PID,2,P#dp.mpids) of
				{Id,PID} ->
					mupdate_busy(Id,false);
				_ ->
					ok
			end,
			{noreply,P#dp{mpids = lists:keydelete(PID,2,P#dp.mpids)}};
		_Actor ->
			% ?AINF("pid=~p, died=~p",[PID,_Reason]),
			butil:ds_rem(PID,actorsalive),
			case ets:member(?HIBERNATE,PID) of
				true ->
					butil:ds_rem(PID,?HIBERNATE);
				false ->
					Prev = butil:ds_val(?PREV_ACTIVE,?GLOBAL_INFO),
					Cur = butil:ds_val(?CUR_ACTIVE, ?GLOBAL_INFO),
					(catch butil:ds_rem(PID,Prev)),
					(catch butil:ds_rem(PID,Cur))
			end,
			{noreply,P}
	end;
handle_info(reconnect_raft,P) ->
	start_timer(P),
	erlang:send_after(500,self(),reconnect_raft),
	case nodes() of
		[] ->
			ok;
		_ ->
			actordb_sqlite:tcp_reconnect()
	end,
	{noreply,P};
handle_info(switch_cur_active,P) ->
	erlang:send_after(20000,self(),switch_cur_active),

	case butil:ds_val(?PREV_ACTIVE,?GLOBAL_INFO) of
		undefined ->
			ok;
		EtsToHibernate ->
			L = ets:tab2list(EtsToHibernate),
			[Pid ! {hibernate,?HIBERNATE} || {Pid} <- L],
			butil:ds_add(L,?HIBERNATE),
			ets:delete(EtsToHibernate),
			case ets:info(?HIBERNATE,size) of
				N when N > P#dp.proclimit ->
					{ToKill,_} = ets:select(hibernate_list,[{{'$1'},[],['$1']}],P#dp.proclimit),
					[actordb_sqlproc:diepls(Pid,limit) || Pid <- ToKill];
				_ ->
					ok
			end
	end,
	CurEts = butil:ds_val(?CUR_ACTIVE,?GLOBAL_INFO),
	butil:ds_add(?PREV_ACTIVE,CurEts,?GLOBAL_INFO),

	NewEts = ets:new(?CUR_ACTIVE, [public,set,{write_concurrency,true},{heir,whereis(actordb_sup),<<>>}]),
	butil:ds_add(?CUR_ACTIVE,NewEts,?GLOBAL_INFO),


	{noreply,P};
% handle_info(read_ref,P) ->
% 	erlang:send_after(1000,self(),read_ref),
% 	Ref = make_ref(),
% 	AllReads = get_nreads(),
% 	AllWrites = get_nwrites(),
	% case P#dp.stat_readers of
	% 	[] ->
	% 		SR = [];
	% 	_ ->
	% 		% Count = ets:select_count(actoractivity,[{{'$1','_'},[{'>','$1',P#dp.prev_sec_to},{'<','$1',Ref}], [true]}]),
	% 		% butil:ds_add(nactive,Count,?STATS),
	% 		SR = [begin Pid ! {doread,AllReads,AllWrites,AllReads - P#dp.prev_reads,AllWrites - P#dp.prev_writes,Count},
	% 				Pid
	% 	  		  end || Pid <- P#dp.stat_readers, erlang:is_process_alive(Pid)]
	% end,
	% {noreply,P#dp{prev_sec_to = Ref, prev_sec_from = P#dp.prev_sec_to,
	% 				stat_readers = SR, prev_reads = AllReads, prev_writes = AllWrites}};
% handle_info(check_mem,P) ->
% 	erlang:send_after(5000,self(),check_mem),
	% spawn(fun() ->
	% 	L = memsup:get_system_memory_data(),
	% 	[Free,Total,Cached] = butil:ds_vals([free_memory,system_total_memory,cached_memory],L),
	% 	NProc = ets:info(actoractivity,size),
	% 	case is_integer(Total) andalso
	% 		 is_integer(Free) andalso
	% 		 is_integer(Cached) andalso
	% 		 Total > 0 andalso
	% 		 ((Free+Cached) / Total) < 0.2 andalso
	% 		 NProc > 100 of
	% 		true ->
	% 			?AINF("Killing actors, memratio=~p, actors=~p",[Free/Total, NProc]),
	% 			killactors(NProc*0.2,ets:last(actoractivity));
	% 		false ->
	% 			ok
	% 	end
	%  end),
	% {noreply,P};
handle_info({raft_connections,L},P) ->
	{noreply, P#dp{raft_connections = store_raft_connection(L,P#dp.raft_connections)}};
handle_info({actordb,sharedstate_change},P1) ->
	MG1 = actordb_sharedstate:read_global(master_group),
	case lists:member(actordb_conf:node_name(),MG1) of
		true ->
			MG = MG1 -- [actordb_conf:node_name()];
		false ->
			MG = bkdcore:cluster_nodes()
	end,
	?AINF("Storing raft connections ~p ~p",[MG, bkdcore:cluster_nodes()]),
	P = P1#dp{raft_connections = store_raft_connection(MG,P1#dp.raft_connections)},
	case P#dp.mupdaters of
		[] ->
			case actordb_sharedstate:read_cluster(["mupdaters,",bkdcore:node_name()]) of
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
							erlang:send_after(1000,self(),{actordb,sharedstate_change}),
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
	case actordb_sharedstate:write_cluster(["mupdaters,",bkdcore:node_name()],P#dp.mupdaters) of
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
			ets:update_counter(?GLOBAL_INFO,netchanges,1),
			% Some node has gone down, kill all slaves on this node.
			% spawn(fun() ->
			% 	L = ets:match(actorsalive, #actor{masternode=Nm, pid = '$1', _='_'}),
			% 	[actordb_sqlproc:diepls(Pid,nomaster) || [Pid] <- L]
			% end),
			{noreply,P}
	end;
handle_info({nodeup,Nd},P)  ->
	case bkdcore:name_from_dist_name(Nd) of
		undefined ->
			ok;
		_ ->
			ets:update_counter(?GLOBAL_INFO,netchanges,1)
	end,
	{noreply,P};
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
			case lists:member(Nd,bkdcore:cluster_nodes()) of
				true ->
					Type = 1;
				false ->
					Type = 2
			end,
			?AINF("Starting raft connection to ~p",[{Nd,IP,Port}]),
			case actordb_sqlite:tcp_connect_async(IP,Port,[bkdcore:rpccookie(Nd),"tunnel,",actordb_conf:node_name(),",actordb_util"],Pos-1,Type) of
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
	% erlang:send_after(200,self(),{timeout,0}),
	% erlang:send_after(10000,self(),check_mem),
	% erlang:send_after(1000,self(),read_ref),
	erlang:send_after(500,self(),reconnect_raft),
	erlang:send_after(20000,self(),switch_cur_active),
	actordb_sharedstate:subscribe_changes(?MODULE),
	case ets:info(multiupdaters,size) of
		undefined ->
			ets:new(multiupdaters, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{write_concurrency,true}]);
		_ ->
			ok
	end,
	% case ets:info(actoractivity,size) of
	% 	undefined ->
	% 		ets:new(actoractivity, [named_table,public,ordered_set,{heir,whereis(actordb_sup),<<>>},{write_concurrency,true}]);
	% 	_ ->
	% 		ok
	% end,
	case ets:info(actorsalive,size) of
		undefined ->
			ets:new(actorsalive, [named_table,public,ordered_set,{heir,whereis(actordb_sup),<<>>},
				{write_concurrency,true},{keypos,#actor.pid}]);
		_ ->
			ok
	end,
	case ets:info(?HIBERNATE,size) of
		undefined ->
			ets:new(?HIBERNATE, [named_table,public,set,{heir,whereis(actordb_sup),<<>>}]);
		_ ->
			ok
	end,
	case ets:info(?GLOBAL_INFO,size) of
		undefined ->
			ets:new(?GLOBAL_INFO, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{read_concurrency,true}]),
			butil:ds_add(netchanges,0,?GLOBAL_INFO);
		_ ->
			ok
	end,
	CurActiv = ets:new(?CUR_ACTIVE, [public,set,{write_concurrency,true},{heir,whereis(actordb_sup),<<>>}]),
	butil:ds_add(?CUR_ACTIVE,CurActiv,?GLOBAL_INFO),

	folsom_metrics:new_counter(reads),
	folsom_metrics:new_counter(writes),

	butil:ds_add(#actor{pid = 0},actorsalive),
	case butil:get_os() of
		win ->
			Ulimit = (#dp{})#dp.ulimit;
		_ ->
			case catch butil:toint(lists:flatten(string:tokens(os:cmd("ulimit -n"),"\n\r"))) of
				Ulimit when is_integer(Ulimit) ->
					ok;
				_ ->
					Ulimit = 256
			end
	end,
	case memsup:get_memory_data() of
		{0,0,_} ->
			Memlimit1 = (#dp{})#dp.memlimit;
		{Memlimit1,_,_} ->
			ok
	end,
	% case ok of
	% 	_ when Ulimit =< 1024 ->
	% 		Proclimit = erlang:round(Ulimit*0.5);
	% 	_ ->
			% Proclimit = erlang:round(Ulimit*0.8),
	% end,
	case ok of
		_ when Memlimit1 =< ?GB ->
			Proclimit = 20,
			Memlimit = 200*?MB;
		_ when Memlimit1 =< ?GB*2 ->
			Proclimit = 30,
			Memlimit = ?GB;
		_ when Memlimit1 =< ?GB*4 ->
			Proclimit = 100,
			Memlimit = 2*?GB;
		_ ->
			Proclimit = round(Memlimit1 / (8*?MB*2)),
			Memlimit = round(Memlimit1*0.5)
	end,
	P = #dp{memlimit = Memlimit, ulimit = Ulimit, proclimit = Proclimit},
	start_timer(P),
	{ok,P}.


-record(tmr,{proclimit, memlimit, lastcull = {0,0,0},n = 0}).

start_timer(P) ->
	case whereis(short_timer) of
		undefined ->
			spawn_monitor(fun() -> register(short_timer,self()),
				timer(#tmr{proclimit = P#dp.proclimit, memlimit = P#dp.memlimit}) end);
		_ ->
			ok
	end.
timer(P) ->
	receive
	after 200 ->
		% NProc = ets:info(actoractivity,size),
		% Memsize = (butil:ds_val(0,actorsalive))#actor.cachesize,
		% case NProc < P#tmr.proclimit andalso Memsize < P#tmr.memlimit of
		% 	true ->
		% 		% io:format("NOKILL ~p ~p~n",[Memsize,0.1*P#dp.memlimit]),
		% 		LastCull1 = P#tmr.lastcull;
		% 	false ->
				Now = os:timestamp(),
		% 		case timer:now_diff(Now,P#tmr.lastcull) > 1000000 of
		% 			true ->
		% 				?AINF("Killing off inactive actors proc ~p, mem ~p",[{NProc,P#tmr.proclimit},{Memsize,P#tmr.memlimit}]),
		% 				Killn = NProc - P#tmr.proclimit - erlang:round(P#tmr.proclimit*0.2),
						LastCull1 = Now,
		% 				killactors(Killn,ets:last(actoractivity));
		% 			false ->
		% 				LastCull1 = P#tmr.lastcull
		% 		end
		% end,
		timer(P#tmr{lastcull = LastCull1, n = P#tmr.n+1})
	end.

create_mupdaters(0,L) ->
	L;
create_mupdaters(N,L) ->
	case actordb_idgen:getid() of
		{ok,Id} ->
			create_mupdaters(N-1,[Id|L]);
		_E ->
			?AERR("Cant create multiupdater ~p",[_E]),
			L
	end.
