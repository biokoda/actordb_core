-module(actordb_test).
-export([batch/0, idtest/0, ins/0, read_timebin/0, 
	loop/1, wal_test/1, q_test/1, q_test/2, client/0, varint/0,
	debug_logging/0, info_logging/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("actordb_sqlproc.hrl").
-define(SOL_SOCKET, 16#ffff).
-define(SO_REUSEPORT, 16#0200).
% misc internal tests
% general tests are in actordb/test/dist_test.erl and run with detest

debug_logging() ->
	switch_backend(debug).
info_logging() ->
	switch_backend(info).
switch_backend(What) ->
	case get_backend() of
		undefined ->
			false;
		Backend ->
			lager:set_loglevel(Backend,What)
	end.
get_backend() ->
	case [{lager_file_backend, Pth} || {lager_file_backend, Pth} <- gen_event:which_handlers(lager_event), 
		string:str(Pth,"console.log") > 0] of
		[Backend|_] ->
			Backend;
		_ ->
			undefined
	end.

varint() ->
	varint(10).
varint(N) when is_integer(N) ->
	case catch actordb_util:varint_dec(actordb_util:varint_enc(N)) of
		{N,_} ->
			varint(vtinc(N));
		_ ->
			throw({fail,N})
	end;
varint(_) ->
	ok.

vtinc(N) when N < 240 ->
	242;
vtinc(242) ->
	1000;
vtinc(1000) ->
	1500;
vtinc(1500) ->
	2000;
vtinc(2000) ->
	4000;
vtinc(4000) ->
	8000;
vtinc(N) when N < 16#ffff ->
	N+16#ffff;
vtinc(N) when N < 16#ffffff ->
	N+16#ffffff;
vtinc(N) when N < 16#ffffffff ->
	N+16#ffffffff;
vtinc(N) when N < 16#ffffffffff ->
	N+16#ffffffffff;
vtinc(N) when N < 16#ffffffffffff ->
	N+16#ffffffffffff;
vtinc(N) when N < 16#ffffffffffffff ->
	N+16#ffffffffffffff;
vtinc(_) ->
	ok.

% udp() ->
% 	Srvrs = [spawn(fun() -> udpsrv(N) end) || N <- lists:seq(1,10)],
% 	timer:sleep(100),
% 	[begin spawn(fun() -> udpclient(N) end) end || N <- lists:seq(1,1000)],
% 	timer:sleep(1000),
% 	[S ! die || S <- Srvrs],
% 	ok.

% udpsrv(N) ->
% 	Opts = [{raw, ?SOL_SOCKET, ?SO_REUSEPORT, <<1:32/native>>},
% 		{active,true},  inet, binary, {recbuf,1024*1024}],
% 	{ok,S} = gen_udp:open(23232,Opts),
% 	io:format("Opened server ~p~n",[N]),
% 	udpsrv(N,S).
% udpsrv(N,S) ->
% 	receive
% 		{udp, S, _IP, Port, Msg} ->
% 			io:format("Received msg=~p, srvid=~p, client_port=~p~n",[Msg, N, Port]),
% 			udpsrv(N,S);
% 		die ->
% 			ok
% 	end.

% udpclient(N) ->
% 	{ok,S} = gen_udp:open(0),
% 	gen_udp:send(S,{127,0,0,1},23232,["sending from ",butil:tolist(N)]).

batch() ->
	Actor = butil:tobin(butil:epochsec()),
	{sql_error,A} = actordb_sqlproc:write({Actor,type1},[create],"insert into tab values (1,'a',2,3);",actordb_actor),
	?AINF("Create actor with error: {sql_error,~p}",[A]),
	% Actor now exists with schema and no data (insert failed)
	Pid = distreg:whereis({Actor,type1}),
	% For every write create ref to match against (id and ref).
	W = [{{self(),make_ref()},#write{sql = "insert into tab values (1,'a',2);"}},
	{{self(),make_ref()},#write{sql = "insert into tab values (2,'b',3);"}},
	{{self(),make_ref()},#write{sql = "insert into tab values (3,'c',4,5);"}},
	{{self(),make_ref()},#write{sql = "insert into tab values (4,'d',5);"}},
	{{self(),make_ref()},#write{sql = "insert into tab values (5,'aa',1);"}}],
	Pid ! {batch,W},
	ok = recbatch(W,1),

	R = [{{self(),make_ref()}, #read{sql = "select * from tab where id=1;"}},
	{{self(),make_ref()}, #read{sql = "select * from tab where id=2;"}},
	{{self(),make_ref()}, #read{sql = "select * from tab where xid=3;"}},
	{{self(),make_ref()}, #read{sql = "select * from tab where id=4;"}},
	{{self(),make_ref()}, #read{sql = "select * from tab where id=5;"}}],
	Pid ! {batch,R},
	ok = recbatch(R,1),

	ok.

recbatch([{{_,Ref},R}|T],Id) ->
	receive
		{Ref,{sql_error,A}} when Id == 3 ->
			?AINF("Correctly received error for invalid sql in batch: ~p",[A]),
			recbatch(T,Id+1);
		{Ref,{ok,{changes,Id,1}}} when element(1,R) == write ->
			?AINF("Received correct write response for id=~p",[Id]),
			recbatch(T,Id+1);
		{Ref,{ok,[{columns,{<<"id">>,<<"txt">>,<<"i">>}},{rows,[{Id,_,_}]}]}} when element(1,R) == read ->
			?AINF("Received correct read response for id=~p",[Id]),
			recbatch(T,Id+1)

		% X when element(1,R) == read ->
		% 	?AERR("REC read: ~p",[X]),
		% 	recbatch(T,Id+1)
	after 1000 ->
		timeout
	end;
recbatch([],_) ->
	ok.


idtest() ->
	N = 100,
	{ok,From} = actordb_idgen:getid(),
	ets:new(idtest,[named_table,public,set,{write_concurrency,true}]),
	Pids = [element(1,spawn_monitor(fun() -> idtest1(From+15000,0) end)) || _ <- lists:seq(1,N)],
	Res = idtest_wait(Pids),
	% io:format("list: ~p~n",[ets:tab2list(idtest)]),
	ets:delete(idtest),
	case Res of
		ok ->
			ok;
		_ ->
			throw(Res)
	end.

idtest_wait([]) ->
	ok;
idtest_wait(Pids) ->
	receive
		{'DOWN',_Monitor,_Ref,PID,Reason} when Reason == normal ->
			% io("Done ~p",[PID]),
			idtest_wait(lists:delete(PID,Pids));
		{'DOWN',_Monitor,_Ref,_PID,Reason} ->
			Reason
	end.

idtest1(Max,Run) when Run rem 1000 == 0 ->
	io:format("~p at ~p~n",[self(),Run]),
	idtest1(Max,Run+1);
idtest1(Max,Run) ->
	{ok,N} = actordb_idgen:getid(),
	case N < Max of
		true ->
			case catch ets:insert_new(idtest,{N,self()}) of
				true ->
					% io:format("ok: ~p me=~p~n",[N,self()]),
					idtest1(Max,Run+1);
				_ ->
					io:format("Failed on ~p, me=~p, ex=~p~n",[N,self(),ets:lookup(idtest,N)]),
					exit(error)
			end;
		false ->
			exit(normal)
	end.

% Tests for actordb_client.
client() ->
	ok = actordb_client:test("myuser","mypass"),
	Param = [[butil:flatnow(),"asdf",3],[butil:flatnow(),"asdf1",4]],
	{ok,{changes,_,_}} = actordb_client:exec_single_param("ax","type1","insert into tab values (?1,?2,?3);",[create],Param),
	 {ok,{changes,_,_}} = actordb_client:exec_single_param("ax","type2","insert into asdf (txt) values (?1);",[create],[[{blob,<<1,2,3>>}]]),
	{ok,{false,[Doc|_]}} = actordb_client:exec_single("ax","type2","select * from asdf;",[]),
	#{id := _, txt := <<1,2,3>>} = Doc,
	{ok,{false,[#{exists := <<"true">>}]}} = actordb_client:exec_single_param("ax","type2","pragma exists;",[],[]).



ins() ->
	{ok,B} = file:read_file("inserts.sql"),
	Lines = binary:split(B,<<"\n">>,[global]),
	S = os:timestamp(),
	ins(Lines),
	io:format("Diff=~p, inserts=~p~n",[timer:now_diff(os:timestamp(),S),length(Lines)]).
ins([<<"[\"ACTORDB QUERY (mapl):\",<<\"",Rem/binary>>|T]) ->
	Sz = (byte_size(Rem)-5),
	<<Useful:Sz/binary,_/binary>> = Rem,
	% io:format("Useful: ~p~n",[Useful]),
	{ok,_} = actordb:exec(Useful),
	ins(T);
ins([<<>>]) ->
	[];
ins([]) ->
	ok.

% Used to parse time file data from actordb_driver.
read_timebin() ->
	{ok,<<Numer:64/unsigned-little,Delim:64/unsigned-little,Times/binary>>} = file:read_file("time.bin"),
	read_timebin(Numer,Delim,Times,[]).

read_timebin(N,D,<<Id,Val:64/unsigned-little,Rem/binary>>,L) ->
	Int = round((Val*N/D)/1000),
	case Id of
		0 ->
			print_times(lists:reverse(L)),
			read_timebin(N,D,Rem,[{0,Int}]);
		_ ->
			read_timebin(N,D,Rem,[{Id,Int}|L])
	end;
read_timebin(_,_,_,_) ->
	ok.

print_times([]) ->
	ok;
print_times([{0,Time}|T]) ->
	io:format("Start:~p~n",[Time]),
	print_times(T,Time,Time).

print_times([{Id,Int}|T],First,Prev) ->
	io:format("~p: ~p first_diff=~p prev_diff=~p~n",[Id,Int, Int-First, Int-Prev]),
	print_times(T,First,Int);
print_times([],_,_) ->
	ok.

% For quick benchmarks.
loop(N) ->
	L = [<<(random:uniform(1000000000)):32,1:32>> || _ <- lists:seq(1,1000)],
	S = os:timestamp(),
	loop1(N,L),
	timer:now_diff(os:timestamp(),S).

loop1(0,_) ->
	ok;
loop1(N,L) ->
	% actordb_util:varint_enc(1000),
	% actordb:exec("actor type1("++integer_to_list(N)++"); select * from tab;"),
	actordb:exec(<<"show schema;">>),
	loop1(N-1,L).


% counters() ->
% 	% folsom_metrics:new_counter(writes),
% 	Pids = [element(1,spawn_monitor(fun() -> counter(0) end)) || _N <- lists:seq(1,1000)],
% 	receive
% 		{'DOWN',_Monitor,_,_PID,Reason} ->
% 			exit(Reason)
% 	after 20000 ->
% 		ok
% 	end,
% 	[P ! stop || P <- Pids],
% 	Ops = rec_counts(0),
% 	io:format("ops=~p, val=~p~n",[Ops, actordb_driver:get_counter(9)]),
% 	ok.
% rec_counts(Ops) ->
% 	receive
% 		{'DOWN',_Monitor,_,_PID,N} ->
% 			rec_counts(Ops+N)
% 		after 2000 ->
% 			Ops
% 	end.
% counter(N) ->
% 	case erlang:process_info(self(),message_queue_len) of
% 		{_,0} ->
% 			actordb_driver:counter_inc(9,N),
% 			actordb_driver:get_counter(9),
% 			counter(N+1);
% 		_ ->
% 			exit(N)
% 	end.


% How fast can we insert data to queue.
q_test(Writers) ->
	q_test(Writers,10000).
q_test(Writers,Timeout) ->
	E = ets:new(walets,[set,public,{write_concurrency,true}]),
	ets:insert(E,{writes,0}),
	% Faster if workers are bound to schedulers.
	SchOnline = erlang:system_info(schedulers_online),
	Pids = [spawn_opt(fun() -> qwriter(E,N) end, [{scheduler, N rem SchOnline}]) || N <- lists:seq(1,Writers)],
	% Pids = [spawn(fun() -> writer(E) end) || _ <- lists:seq(1,Writers)],
	receive
		{'DOWN',_Monitor,_,_PID,Reason} ->
			exit(Reason)
	after Timeout ->
		ok
	end,
	[exit(P,stop) || P <- Pids],
	io:format("writes=~p~n",[butil:ds_val(writes,E)]),
	ok.


qwriter(E,N) ->
	Bytes = iolist_to_binary([butil:iolist_join(lists:duplicate(100,iolist_to_binary(pid_to_list(self()))),"|"),"\n"]),
	qwriter(E,N,Bytes).
qwriter(E,N,Bytes) ->
	actordb_queue:write(N,#{actor => N, flags => [], statements => Bytes}),
	ets:update_counter(E,writes,{2,1}),
	qwriter(E,N,Bytes).


% How fast can multiple writers write to a log file.
wal_test(Writers) ->
	E = ets:new(walets,[set,public,{write_concurrency,true}]),
	ets:insert(E,{offset,0}),
	ets:insert(E,{writes,0}),
	file:delete("logfile"),
	% Faster if workers are bound to schedulers.
	SchOnline = erlang:system_info(schedulers_online),
	Pids = [spawn_opt(fun() -> writer(E) end, [{scheduler, N rem SchOnline}]) || N <- lists:seq(1,Writers)],
	% Pids = [spawn(fun() -> writer(E) end) || _ <- lists:seq(1,Writers)],
	receive
		{'DOWN',_Monitor,_,_PID,Reason} ->
			exit(Reason)
	after 10000 ->
		ok
	end,
	[exit(P,stop) || P <- Pids],
	io:format("Filesize=~pMB, writes=~p~n",[filelib:file_size("logfile") div (1024*1024), butil:ds_val(writes,E)]),
	ok.


writer(E) ->
	{ok,F} = file:open("logfile",[raw,binary,write,read]),
	Bytes = iolist_to_binary([butil:iolist_join(lists:duplicate(100,iolist_to_binary(pid_to_list(self()))),"|"),"\n"]),
	writer(E,Bytes,F).
writer(E,B,F) ->
	Offset = ets:update_counter(E,offset,{2,byte_size(B)}),
	prim_file:pwrite(F,Offset-byte_size(B),B),
	ets:update_counter(E,writes,{2,1}),
	writer(E,B,F).
