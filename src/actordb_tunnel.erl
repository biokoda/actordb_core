% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_tunnel).
-behaviour(gen_server).
-export([start/0,stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
-export([]).
-include_lib("actordb_core/include/actordb.hrl").


% Every write thread in actordb_driver uses a TCP "tunnel" to replicate data.
% Generated pages during writes get sent over to nodes in cluster. Write threads are dumb
% and all they do is write to FD. Data is prefixed with info that tells the other side
% what this data is. 
% The receiving node has a circuit breaker (actordb_util:actor_ae_stream) for every actor. 
% If the receiver does not want the data (raft conditions) it will close the receiving process
% thus breaking the circuit and the received data gets sent to a dead process.
% 
% This gen_server creates the actual TCP connection fds and takes them away from erlang runtime.

start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).


-record(dp,{
	% Sockets for every write thread for every driver.
	% #{{ThreadIndex, DriverName, ConnectionSlot} => {Type, Socket}}
	sockets = #{},
	% slots for 8 raft cluster connections
	% Set element is: NodeName
	slots = {undefined,undefined,undefined,undefined,undefined,undefined,undefined,undefined}}).

handle_call(print_info,_,P) ->
	?AINF("~p",[P]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast(_, P) ->
	{noreply, P}.

handle_info({tcpfail,Driver,Thread,Pos}, P) ->
	?AERR("Lost connection to=~p on thread=~p",[element(Pos+1,P#dp.slots), Thread]),
	case maps:get({Thread,Driver,Pos}, P#dp.sockets, undefined) of
		undefined ->
			?AERR("Connection {~p,~p} not found in map=~p",[Thread,Pos,P#dp.sockets]),
			{noreply, P};
		{Type,Sock} when is_port(Sock) ->
			gen_tcp:close(Sock),
			Socks = P#dp.sockets,
			{noreply, P#dp{sockets = Socks#{{Thread, Driver, Pos} => {Type, undefined}}}};
		{_Type, undefined} ->
			{noreply, P}
	end;
handle_info(reconnect_raft,P) ->
	erlang:send_after(500,self(),reconnect_raft),
	{noreply,P#dp{sockets = check_reconnect(P#dp.slots,maps:to_list(P#dp.sockets), P#dp.sockets)}};
handle_info({actordb,sharedstate_change},P) ->
	MG1 = actordb_sharedstate:read_global(master_group),
	case lists:member(actordb_conf:node_name(),MG1) of
		true ->
			MG = MG1 -- [actordb_conf:node_name()];
		false ->
			MG = bkdcore:cluster_nodes()
	end,
	% ?AINF("Storing raft connections ~p ~p",[MG, bkdcore:cluster_nodes()]),
	{Slots, ToConnect} = store_raft_connection(MG,P#dp.slots,[]),
	{noreply, P#dp{slots = Slots, sockets = connect(Slots, ToConnect,P#dp.sockets)}};
handle_info({raft_connections,L},P) ->
	{Slots,ToConnect} = store_raft_connection(L,P#dp.slots,[]),
	{noreply, P#dp{slots = Slots, sockets = connect(Slots, ToConnect,P#dp.sockets)}};
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info(M, P) ->
	?AERR("Invalid msg ~p",[M]),
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init(_) ->
	erlang:send_after(500,self(),reconnect_raft),
	actordb_sharedstate:subscribe_changes(?MODULE),
	ok = actordb_driver:set_tunnel_connector(),
	% ok = aqdrv:set_tunnel_connector(),
	{ok,#dp{}}.

check_reconnect(Slots,[{{Thread, Driver, Pos} = K, {Type,undefined}}|T], Sockets) ->
	Nd = element(Pos+1,Slots),
	{IP,Port} = bkdcore:node_address(Nd),
	case doconnect(IP, Port, Nd) of
		{ok, Sock} ->
			{ok,Fd} = prim_inet:getfd(Sock),
			?AINF("Reconnected to ~p",[Nd]),
			ok = apply(Driver,set_thread_fd,[Thread,Fd,Pos,Type]),
			check_reconnect(Slots,T, Sockets#{K => {Type, Sock}});
		false ->
			check_reconnect(Slots,T, Sockets)
	end;
check_reconnect(Slots,[_|T], S) ->
	check_reconnect(Slots,T, S);
check_reconnect(_,[], S) ->
	S.

connect(Slots,[H|TC], Sockets) ->
	NWThreads = length(actordb_conf:paths()) * actordb_conf:wthreads(),
	ThrL = lists:seq(0,NWThreads-1),
	connect(Slots,TC, connect_threads(Slots,actordb_driver,ThrL, H,Sockets));
connect(_,[],S) ->
	S.

connect_threads(Slots, Driver, [Thread|T], {Nd, Pos, Type} = Info, Sockets) ->
	{IP,Port} = bkdcore:node_address(Nd),
	Nd = element(Pos+1,Slots),
	K = {Thread, Driver, Pos},
	case doconnect(IP, Port, Nd) of
		{ok, Sock} ->
			{ok,Fd} = prim_inet:getfd(Sock),
			ok = apply(Driver,set_thread_fd,[Thread,Fd,Pos,Type]),
			?AINF("Connected to ~p",[Nd]),
			connect_threads(Slots, Driver, T, Info, Sockets#{K => {Type, Sock}});
		false ->
			connect_threads(Slots, Driver, T, Info, Sockets#{K => {Type, undefined}})
	end;
connect_threads(_Slots, _Driver,[],_Info,S) ->
	S.

doconnect(IP, Port, Nd) ->
	case gen_tcp:connect(IP,Port,[{active, false},{packet,4},
			{keepalive,true},{send_timeout,10000}], 500) of
		{ok,S} ->
			inet:setopts(S,[{nodelay, true}]),
			case gen_tcp:send(S,conhdr(Nd)) of
				ok ->
					?AINF("Opened tunnel to ~p",[Nd]),
					ok = prim_inet:ignorefd(S,true),
					{ok,S};
				_ ->
					false
			end;
		_ERR ->
			false
	end.

conhdr(Nd) ->
	[bkdcore:rpccookie(Nd),"tunnel,",actordb_conf:node_name(),",actordb_util"].

store_raft_connection([Nd|T],Tuple,ToConnect) ->
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
			store_raft_connection(T, setelement(Pos,Tuple,Nd), [{Nd, Pos-1, Type}|ToConnect]);
		_ ->
			store_raft_connection(T,Tuple, ToConnect)
	end;
store_raft_connection([],T, ToConnect) ->
	{T,ToConnect}.

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
