-module(actordb_election).
-behaviour(gen_server).
-export([start/0,stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
-export([whois_leader/1,connect_all/0]).
-include_lib("actordb_sqlproc.hrl").

% This is a global registered process. It executes elections on behalf of sqlproc.

% It brings some order to the election process. Since we can have a large number of actors
% all doing elections (at least on startup), election timeouts can be impossible to set correctly
% and actors will timeout too soon. 


pid() ->
	case global:whereis_name(?MODULE) of
		undefined ->
			start(),
			pid();
		Pid ->
			Pid
	end.

connect_all() ->
	gen_server:cast(pid(),do_connect).

% Returns: pid | nodename | later
% If later set a random timer and call again
% If nodename master has been chosen
% If pid monitor for exit signal.
whois_leader(E) ->
	gen_server:call(pid(),{whois_leader,E#election{candidate = actordb_conf:node_name()}}).

start() ->
	gen_server:start({global,?MODULE},?MODULE, [], []).

stop() ->
	gen_server:call(pid(), stop).

print_info() ->
	gen_server:call(pid(),print_info).


% ETS: [{{Actor,Type},{Master,SinceWhen}},...]
-record(ep,{ets, elections = []}).
-record(e,{id,actor,type,pid,nd,info}).

handle_call({whois_leader, #election{actor = A, type = T} = EI},CallFrom,P) ->
	case butil:ds_val({A,T},P#ep.ets) of
		undefined ->
			case lists:keyfind({A,T},#e.id,P#ep.elections) of
				false ->
					E = #e{id = {A,T}, nd = EI#election.candidate, actor = A, type = T, info = EI},
					{Pid,_} = spawn_monitor(fun() -> doelection(E) end),
					{reply,Pid,P#ep{elections = [E#e{pid = Pid}|P#ep.elections]}};
				E when EI#election.candidate == E#e.nd ->
					{reply, E#e.pid, P};
				E when EI#election.candidate /= E#e.nd ->
					{reply,later,P}
			end;
		{Master,ElectedWhen} ->
			case timer:now_diff(os:timestamp(),ElectedWhen) > 3000000 of
				true ->
					handle_call({whois_leader,EI},CallFrom,P#ep{ets = butil:ds_rem({A,T},P#ep.ets)});
				false ->
					{reply,Master,P}
			end
	end;
handle_call(print_info,_,P) ->
	io:format("~p~n",[P]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast(do_connect,P) ->
	L = bkdcore:cluster_nodes(),
	[spawn(fun() -> net_adm:ping(bkdcore:dist_name(Nd)) end) || Nd <- L],
	{noreply,P};
handle_cast(_, P) ->
	{noreply, P}.

handle_info({'DOWN',_Monitor,_,PID,Reason},P) ->
	case lists:keyfind(PID,#e.pid,P#ep.elections) of
		#e{} = E when element(1,Reason) == leader ->
			{noreply,P#ep{ets = butil:ds_add({E#e.actor,E#e.type},{E#e.nd,os:timestamp()},P#ep.ets),
						  elections = lists:keydelete(E#e.id,#e.id,P#ep.elections)}};
		#e{} = E ->
			{noreply,P#ep{elections = lists:keydelete(E#e.id,#e.id,P#ep.elections)}};
		_ ->
			{noreply,P}
	end;
handle_info(timeout,P) ->
	erlang:send_after(1000,self(),timeout),
	handle_cast(do_connect,P);
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
	erlang:send_after(1000,self(),timeout),
	{ok,#ep{ets = ets:new(elactors,[set,private])}}.


get_followers(E) ->
	receive
		{set_followers,L} ->
			E#election{followers = L}
	after 2000 ->
		exit(timeout)
	end.

doelection(E1) ->
	E = get_followers(E1#e.info),
	ClusterSize = length(E#election.followers) + 1,
	Me = E#election.candidate,
	Msg = {state_rw,{request_vote,Me,E#election.term,E#election.evnum,E#election.evterm}},
	?ADBG("Election for ~p.~p, multicall",[E#election.actor,E#election.type]),
	Start = os:timestamp(),
	case E#election.cbmod of
		actordb_sharedstate ->
			Nodes = actordb_sqlprocutil:follower_nodes(E#election.followers),
			{Results,_GetFailed} = bkdcore_rpc:multicall(Nodes,{actordb_sqlproc,call_slave,
				[E#election.cbmod,E#election.actor,E#election.type,Msg,[{flags,E#election.flags}]]});
		_ ->
			Nodes = [F#flw.distname || F <- E#election.followers],
			{Results,_GetFailed} = rpc:multicall(Nodes,actordb_sqlproc,call_slave,
				[E#election.cbmod,E#election.actor,E#election.type,Msg,[{flags,E#election.flags}]],2000)
	end,
	Stop = os:timestamp(),
	?ADBG("Election took=~p, results ~p failed ~p, contacted ~p",[timer:now_diff(Stop,Start),Results,_GetFailed,Nodes]),

	% Sum votes. Start with 1 (we vote for ourselves)
	case count_votes(Results,{E#election.evnum,E#election.evterm},true,E#election.followers,1) of
		{NumVotes,Followers,AllSynced} when is_integer(NumVotes) ->
			case NumVotes*2 > ClusterSize of
				true ->
					start_election_done(E,{leader,Followers,AllSynced});
				false when (length(Results)+1)*2 =< ClusterSize ->
					% Majority isn't possible anyway.
					start_election_done(E,follower);
				false ->
					start_election_done(E,follower)
			end
	end.
start_election_done(E,X) when is_tuple(X) ->
	?ADBG("Exiting with signal ~p",[X]),
	case E#election.wait of
		true ->
			receive
				exit ->
					exit(X)
				after 300 ->
					?AERR("Wait election write waited too long."),
					exit(X)
			end;
		false ->
			exit(X)
	end;
start_election_done(_P,Signal) ->
	?ADBG("Exiting with signal ~p",[Signal]),
	exit(Signal).

count_votes([{What,Node,_HisLatestTerm,{Num,Term} = NodeNumTerm}|T],NumTerm,AllSynced,Followers,N) ->
	F = lists:keyfind(Node,#flw.node,Followers),
	NF = F#flw{match_index = Num, next_index = Num+1, match_term = Term},
	case What of
		true when AllSynced, NodeNumTerm == NumTerm ->
			count_votes(T,NumTerm,true,lists:keystore(Node,#flw.node,Followers,NF),N+1);
		true ->
			count_votes(T,NodeNumTerm,false,Followers,N+1);
		% outofdate ->
		% 	count_votes(T,NodeNumTerm,false,Followers,N);
		% 	{outofdate,Node,HisLatestTerm};
		_ ->
			count_votes(T,NumTerm,false,Followers,N)
	end;
count_votes([Err|_T],_NumTerm,_AllSynced,_Followers,_N) ->
	% count_votes(T,NumTerm,false,Followers,N);
	exit({failed,Err});
count_votes([],_,AllSynced,F,N) ->
	{N,F,AllSynced}.
