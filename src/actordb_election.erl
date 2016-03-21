-module(actordb_election).
-export([whois_leader/1]).
-include_lib("actordb_sqlproc.hrl").

whois_leader(E) ->
	Home = self(),
	spawn(fun() -> erlang:monitor(process,Home), doelection(Home,E) end).


get_followers(Home,E) ->
	receive
		{set_followers,L} ->
			E#election{followers = L};
		{'DOWN',_Monitor,_,Home,_Reason} ->
			exit(down)
	end.

doelection(Home,E1) ->
	?ADBG("Getfollowers for ~p",[E1]),
	E = get_followers(Home,E1),
	ClusterSize = length(E#election.followers) + 1,
	Me = E#election.candidate,
	Msg = {state_rw,{request_vote,Me,E#election.term,E#election.evnum,E#election.evterm}},
	?ADBG("Election for ~p.~p, multicall",[E#election.actor,E#election.type]),
	Start = actordb_local:elapsed_time(),
	% case E#election.cbmod of
	% 	actordb_sharedstate ->
			Nodes = actordb_sqlprocutil:follower_nodes(E#election.followers),
			{Results,_GetFailed} = bkdcore_rpc:multicall(Nodes,{actordb_sqlproc,call_slave,
				[E#election.cbmod,E#election.actor,E#election.type,Msg,[{flags,E#election.flags}]]}, 200),
	% 	_ ->
	% 		Nodes = [F#flw.distname || F <- E#election.followers, lists:member(F#flw.distname,[node()|nodes()])],
	% 		{Results,_GetFailed} = rpc:multicall(Nodes,actordb_sqlproc,call_slave,
	% 			[E#election.cbmod,E#election.actor,E#election.type,Msg,[{flags,E#election.flags}]],2000)
	% end,
	Stop = actordb_local:elapsed_time(),
	?ADBG("Election took=~p, results ~p failed ~p, contacted ~p",[Stop-Start,Results,_GetFailed,Nodes]),

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
