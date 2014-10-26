% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(TESTPTH,butil:project_rootpath()++"/testdb/").
-define(NUM_ACTORS,100).

numactors() ->
	case node() == 'testnd@127.0.0.1' orelse element(1,os:type()) == win32 of
		true ->
			?NUM_ACTORS;
		false ->
			5000
	end.

test_real() ->
	basic_write(),
	basic_read().
	% multiupdate_write(),
	% multiupdate_read().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 		All tests are executed in slave nodes of current node.
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  
all_test_() ->
	[
		% fun test_creating_shards/0,
		fun test_parsing/0,
		{setup,	fun single_start/0, fun single_stop/1, fun test_single/1}
		% {setup,	fun onetwo_start/0, fun onetwo_stop/1, fun test_onetwo/1}
		% {setup, fun cluster_start/0, fun cluster_stop/1, fun test_cluster/1}
		% {setup, fun missingn_start/0, fun missingn_stop/1, fun test_missingn/1}
		% {setup,	fun mcluster_start/0,	fun mcluster_stop/1, fun test_mcluster/1}
		% {setup,	fun clusteraddnode_start/0,	fun clusteraddnode_stop/1, fun test_clusteraddnode/1}
		% {setup,	fun clusteradd_start/0,	fun clusteradd_stop/1, fun test_clusteradd/1}
		% {setup,	fun failednodes_start/0, fun failednodes_stop/1, fun test_failednodes/1}
	].

% BP = actordb_backpressure:start_caller().
% actordb_sqlparse:parse_statements(BP,<<"prepare prep (int,text) FOR type1 AS insert into tab1 values(?1,?2);">>).
% actordb_sqlparse:parse_statements(BP,<<"execute prep (1,'abcdefghijwtf''asdddf');">>).
% actordb_sqlparse:parse_statements(BP,<<"prepare delete prep;">>).

test_parsing() ->
	?assertMatch({<<"type">>,$*,[]},
					actordb_sqlparse:split_actor(<<"type(*);">>)),
	?assertMatch({<<"type">>,<<"RES">>,<<"column">>,<<"X">>,[]},
						actordb_sqlparse:split_actor(<<"type(foreach X.column in RES);">>)),
	?assertMatch({<<"type">>,[<<"asdisfpsouf">>,<<"234">>,<<"asdf">>],[]},
						actordb_sqlparse:split_actor(<<"type(asdf,234,asdisfpsouf);">>)),
	?assertMatch({[{{<<"type1">>,<<"RES">>,<<"col">>,<<"X">>,[]},
									  false,
									  [<<"select * from table;">>]}],false},
								actordb_sqlparse:parse_statements(<<"actor type1 ( foreach X.col in RES ) ;",
												"select * from table;">>)),

	?assertMatch({[{{<<"user">>,[<<"denis">>],[]},
						   false,
						   [<<"SELECT * FROM todos;">>]}],
						 false},
						actordb_sqlparse:parse_statements(<<"actor user(denis); SELECT * FROM todos;">>)),
	?assertMatch({[{{<<"type1">>,<<"RES">>,<<"col">>,<<"X">>,[]},
								   false,
								   [[<<"select * from table where id=">>,
								     {<<"X">>,<<"id">>},
								     <<>>,59]]}],
								 false},
								actordb_sqlparse:parse_statements(<<"actor type1 ( foreach X.col in RES );",
														"select * from table where id={{X.id}};">>)),
	?assertMatch({[{{<<"type1">>,<<"RES">>,<<"col">>,<<"X">>,[]},
							   false,
							   [{<<"ABBB">>,
							     [<<"select * from table where id=">>,
							      {<<"X">>,<<"id">>},
							      <<>>,59]}]}],
							 false},
								actordb_sqlparse:parse_statements(<<"actor type1(foreach X.col in RES);",
												"{{ABBB}}select * from table where id={{X.id}};">>)),
	ok.

test_creating_shards() ->
	SingleAllShards = actordb_shardmngr:create_shards([1]),
	?debugFmt("Single all shards ~p",[SingleAllShards]),
	?assertMatch([_,_,_],actordb_shardmvr:split_shards(2,[1,2],SingleAllShards,[])),

	All = actordb_shardmngr:create_shards([1,2]),
	L = actordb_shardmvr:split_shards(3,[1,2,3],All,[]),
	?assertMatch([_,_,_,_],L),
	[?assertEqual(false,actordb_shardmvr:has_neighbour(From,To,lists:keydelete(From,1,L))) || {From,To,_Nd} <- L],
	All1 = lists:foldl(fun({SF,ST,_},A) -> lists:keyreplace(SF,1,A,{SF,ST,3}) end,All,L),
	L1 = actordb_shardmvr:split_shards(4,[1,2,3,4],All1,[]),
	?debugFmt("Replaced all ~n~p",[All1]),
	?debugFmt("Added fourth ~n~p",[lists:foldl(fun({SF,ST,_},A) -> lists:keyreplace(SF,1,A,{SF,ST,4}) end,All1,L1)]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			SINGLE NODE TESTS
% 		tests query operations 1 node cluster
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
single_start() ->
	?debugFmt("Single start",[]),
	basic_init(),
	create_allgroups([[1]]),
	start_slaves([1]),
	ok.
single_stop(_) ->
	stop_slave(1),
	ok.
test_single(_) ->
	[{timeout,20,fun basic_write/0},
		fun basic_read/0,
		% {timeout,10,fun() -> timer:sleep(8000) end},
	  fun basic_read/0,
	  fun basic_write/0,
	  fun multiupdate_write/0,
	  fun multiupdate_read/0,
	  fun kv_readwrite/0,
	  fun basic_write/0,
	  fun basic_read/0,
	  fun copyactor/0
	  	].

ltime() ->
	element(2,lager_util:localtime_ms()).


basic_write() ->
	basic_write(<<"SOME TEXT">>).
basic_write(Txt) ->
	?debugFmt("Basic write",[]),
	[begin
		?debugFmt("~p Write ac~p",[ltime(),N]),
		R = exec(<<"actor type1(ac",(butil:tobin(N))/binary,") create; insert into tab values (",
									(butil:tobin(butil:flatnow()))/binary,",'",Txt/binary,"',1);">>),
		?debugFmt("~p",[R]),
		?assertMatch({ok,_},R)
	end
	 || N <- lists:seq(1,numactors())].
basic_read() ->
	?debugFmt("Basic read",[]),
	% ?debugFmt("~p",[exec(<<"actor type1(ac",(butil:tobin(1))/binary,"); select * from tab1;">>)]),
	[begin
		?debugFmt("~p Read ac~p",[ltime(),N]),
	?assertMatch({ok,[{columns,_},{rows,[{_,<<_/binary>>,_}|_]}]},
			exec(<<"actor type1(ac",(butil:tobin(N))/binary,") create; select * from tab;">>))
	 end
	 || N <- lists:seq(1,numactors())].

copyactor() ->
	?debugFmt("Copy actor",[]),
	?assertMatch({ok,_},exec(["actor type1(newcopy);",
				  "PRAGMA copy=ac1;"])),
	Res = exec(<<"actor type1(newcopy) create; select * from tab;">>),
	% ?debugFmt("Copy from ac1 has data ~p",[Res]),
	?assertMatch({ok,[{columns,_},{rows,[{_,<<_/binary>>,_}|_]}]},
			Res),
	[begin
		?assertMatch({ok,_},exec(["actor type1(newcopy",butil:tolist(N),");",
				  "PRAGMA copy=ac",butil:tolist(N),";"])),
		Res1 = exec(<<"actor type1(newcopy) create; select * from tab;">>),
		?assertMatch({ok,[{columns,_},{rows,[{_,<<_/binary>>,_}|_]}]},
			Res1)
	 end
	 || N <- lists:seq(1,10)].

recoveractor() ->
	?debugFmt("recoveractor",[]),
	% S = ['slave1@127.0.0.1','slave2@127.0.0.1','slave3@127.0.0.1'],
	% rpc:call(findnd(S),actordb,exec,[butil:tobin(Bin)]);
	rpc:call('slave1@127.0.0.1',actordb_sqlproc,stop,[{<<"ac1">>,type1}]),
	rpc:call('slave2@127.0.0.1',actordb_sqlproc,stop,[{<<"ac1">>,type1}]),
	rpc:call('slave3@127.0.0.1',actordb_sqlproc,stop,[{<<"ac1">>,type1}]),
	file:delete([?TESTPTH,"/slave1/actors/ac1.type1"]),
	file:delete([?TESTPTH,"/slave1/actors/ac1.type1-wal"]),
	file:delete([?TESTPTH,"/slave1/actors/ac1.type1-shm"]),
	file:delete([?TESTPTH,"/slave1/actors/ac1.type1-term"]),
	% file:delete([?TESTPTH,"/slave2/actors/ac1.type1"]),
	% file:delete([?TESTPTH,"/slave2/actors/ac1.type1-wal"]),
	% file:delete([?TESTPTH,"/slave2/actors/ac1.type1-shm"]),
	% file:delete([?TESTPTH,"/slave2/actors/ac1.type1-term"]),
	Res = exec(<<"actor type1(ac1) create; select * from tab;">>),
	?debugFmt("After deleting data from 1 node ~p",[Res]),
	?assertMatch({ok,[{columns,_},{rows,[{_,<<_/binary>>,_}|_]}]},
			Res).

multiupdate_write() ->
	?debugFmt("multiupdates",[]),
	% Insert names of 2 actors in table tab2 of actor "all"
	?assertMatch({ok,_},exec(["actor type1(all) create;",
							  "insert into tab2 values (1,'a1');",
							  "insert into tab2 values (2,'a2');"])),
	
	?debugFmt("multiupdate fail insert",[]),
	% Fail test
	?assertMatch(ok,exec(["actor thread(first) create;",
							  "insert into thread values (1,'a1',10);",
							  "actor thread(second) create;",
							  "insert into thread values (1,'a1',10);"])),
	?debugFmt("multiupdates fail",[]),
	?assertMatch(abandoned,exec(["actor thread(first) create;",
							  "update thread set msg='a3' where id=1;",
							  "actor thread(second) create;",
							  "update thread set msg='a3' where i=2;"])),
	?debugFmt("multiupdates still old data",[]),
	?assertMatch({ok,[{columns,{<<"id">>,<<"msg">>,<<"user">>}},
                      {rows,[{1,<<"a1">>,10}]}]},
                 exec(["actor thread(first);select * from thread;"])),
	?assertMatch({ok,[{columns,{<<"id">>,<<"msg">>,<<"user">>}},
                      {rows,[{1,<<"a1">>,10}]}]},
                 exec(["actor thread(second);select * from thread;"])),
	
	?debugFmt("multiupdates foreach insert",[]),
	% Select everything from tab2 for actor "all".
	% Actorname is in .txt column, for every row take that actor and insert value with same unique integer id.
	Res = exec(["actor type1(all);",
				"{{ACTORS}}SELECT * FROM tab2;",
				"actor type1(foreach X.txt in ACTORS) create;",
				"insert into tab2 values ({{uniqid.s}},'{{X.txt}}');"]),
	% ?debugFmt("Res ~p~n",[Res]),
	?assertMatch(ok,Res),

	?debugFmt("multiupdates delete actors",[]),
	?assertMatch(ok,exec(["actor type1(ac100,ac99,ac98,ac97,ac96);PRAGMA delete;"])),
	?debugFmt("Deleting individual actor",[]),
	?assertMatch(ok,exec(["actor type1(ac95);PRAGMA delete;"])),

	?debugFmt("multiupdates creating thread",[]),
	?assertMatch(ok,exec(["actor thread(1) create;",
					"INSERT INTO thread VALUES (100,'message',10);",
					"INSERT INTO thread VALUES (101,'secondmsg',20);",
					"actor user(10) create;",
					"INSERT INTO userinfo VALUES (1,'user1');",
					"actor user(20) create;",
					"INSERT INTO userinfo VALUES (1,'user2');"])),
	ok.
multiupdate_read() ->
	?debugFmt("multiupdate read all type1",[]),
	Res = exec(["actor type1(*);",
				"{{RESULT}}SELECT * FROM tab;"]),
	?assertMatch({ok,[_,_]},Res),
	{ok,[{columns,Cols},{rows,Rows}]} = Res,
	?debugFmt("Result all actors ~p",[{Cols,lists:keysort(4,Rows)}]),
	?assertEqual({<<"id">>,<<"txt">>,<<"i">>,<<"actor">>},Cols),
	% 6 actors were deleted, 2 were added
	?assertEqual((numactors()-6)*2,length(Rows)),

	?debugFmt("multiupdate read thread and user",[]),
	% Add username column to result
	{ok,ResForum} = exec(["actor thread(1);",
				"{{RESULT}}SELECT * FROM thread;",
				"actor user(for X.user in RESULT);",
				"{{A}}SELECT * FROM userinfo WHERE id=1;",
				"{{X.username=A.name}};",
				"{{X.username1=A.name}};"
				]),
	?assertMatch([{columns,{<<"id">>,<<"msg">>,<<"user">>,<<"username">>,<<"username1">>}},
			       {rows,[{101,<<"secondmsg">>,20,<<"user2">>,<<"user2">>},
		  			      {100,<<"message">>,10,<<"user1">>,<<"user1">>}]}],
        ResForum),
	{ok,ResSingle} = exec(["actor thread(1);",
				"{{VAR}}SELECT * FROM thread LIMIT 1;"
				"actor user({{VAR.user}});",
				"{{RESULT}}SELECT * FROM userinfo WHERE id=1;"
				]),
	?assertMatch([{columns,{<<"id">>,<<"name">>}},
			       {rows,[{1,<<"user1">>}]}],
        ResSingle),
	{ok,[{columns,_},{rows,Rows1}]} = exec(["actor type1(*);pragma list;"]),
	Num = numactors()-6+3,
	?assertEqual(Num,length(Rows1)),
	?assertMatch({ok,[{columns,_},{rows,[{Num}]}]},exec(["actor type1(*);pragma count;"])),
	ok.

kv_readwrite() ->
	?debugFmt("~p",[[iolist_to_binary(["actor counters(id",butil:tolist(N),");",
		 "insert into actors values ('id",butil:tolist(N),"',{{hash(id",butil:tolist(N),")}},",
		 	butil:tolist(N),");"])|| N <- lists:seq(1,1)]]),
	[?assertMatch({ok,_},exec(["actor counters(id",butil:tolist(N),");",
		 "insert into actors values ('id",butil:tolist(N),"',{{hash(id",butil:tolist(N),")}},",butil:tolist(N),");"])) 
				|| N <- lists:seq(1,numactors())],
	[?assertMatch({ok,[{columns,_},{rows,[{_,_,N}]}]},
					exec(["actor counters(id",butil:tolist(N),");",
					 "select * from actors where id='id",butil:tolist(N),"';"])) || N <- lists:seq(1,numactors())],
	ReadAll = ["actor counters(*);",
	"{{RESULT}}SELECT * FROM actors;"],
	All = exec(ReadAll),
	?debugFmt("All counters ~p",[All]),
	?debugFmt("Select first 5",[]),
	ReadSome = ["actor counters(id1,id2,id3,id4,id5);",
	"{{RESULT}}SELECT * FROM actors where id='{{curactor}}';"],
	?assertMatch({ok,[{columns,_},
					  {rows,[{<<"id5">>,_,5,<<"id5">>},
					  		  {<<"id4">>,_,4,<<"id4">>},
					  		  {<<"id3">>,_,3,<<"id3">>},
					  		  {<<"id2">>,_,2,<<"id2">>},
					  		  {<<"id1">>,_,1,<<"id1">>}]}]},
			exec(ReadSome)),
	?debugFmt("Increment first 5",[]),
	?assertMatch(ok,exec(["actor counters(id1,id2,id3,id4,id5);",
					"UPDATE actors SET val = val+1 WHERE id='{{curactor}}';"])),
	?debugFmt("Select first 5 again ~p",[exec(ReadSome)]),
	?assertMatch({ok,[{columns,_},
						{rows,[{<<"id5">>,_,6,<<"id5">>},
						  {<<"id4">>,_,5,<<"id4">>},
						  {<<"id3">>,_,4,<<"id3">>},
						  {<<"id2">>,_,3,<<"id2">>},
						  {<<"id1">>,_,2,<<"id1">>}]}]},
			 exec(ReadSome)),
	?debugFmt("delete 5 and 4",[]),
	% Not the right way to delete but it works (not transactional)
	?assertMatch(ok,exec(["actor counters(id5,id4);PRAGMA delete;"])),
	?assertMatch({ok,[{columns,_},
					  {rows,[{<<"id3">>,_,4,<<"id3">>},
						  {<<"id2">>,_,3,<<"id2">>},
						  {<<"id1">>,_,2,<<"id1">>}]}]},
			 exec(ReadSome)),
	?debugFmt("delete 3 and 2",[]),
	% the right way
	?assertMatch(ok,exec(["actor counters(id3,id2);DELETE FROM actors WHERE id='{{curactor}}';"])),
	?debugFmt("read remain",[]),
	?assertMatch({ok,[{columns,_},
					  {rows,[{<<"id1">>,_,2,<<"id1">>}]}]},
			 exec(ReadSome)),
	?assertMatch({ok,[{columns,_},{rows,_}]},All),

	?debugFmt("multiple tables",[]),
	% Multiple tables test
	[?assertMatch({ok,_},exec(["actor filesystem(id",butil:tolist(N),");",
		 "insert into actors values ('id",butil:tolist(N),"',{{hash(id",butil:tolist(N),")}},",butil:tolist(N),");",
		 "insert into users (fileid,uid) values ('id",butil:tolist(N),"',",butil:tolist(N),");"])) 
				|| N <- lists:seq(1,numactors())],

	ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			ADD SECOND NODE
% 	
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
onetwo_start() ->
	basic_init(),
	create_allgroups([[1]]),
	start_slaves([1]),
	ok.
onetwo_stop(_) ->
	stop_slaves([1,2]),
	ok.
test_onetwo(_) ->
	[{timeout,20,fun basic_write/0},
	  fun basic_read/0,
	  {timeout,60,fun test_add_second/0},
	  {timeout,30,fun basic_write/0},
	  fun kv_readwrite/0,
	  fun multiupdate_write/0,
	  fun multiupdate_read/0
	  	].
test_add_second() ->
	create_allgroups([[1,2]]),
	start_slaves([2]),
	timer:sleep(1000),
	?assertMatch(ok,wait_modified_tree(2,[1,2])).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			SINGLE CLUSTER TESTS
% 	Execute queries over cluster with 3 nodes
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
cluster_start() ->
	basic_init(),
	create_allgroups([[1,2,3]]),
	start_slaves([1,2,3]),

	ok.
cluster_stop(_) ->
	% A = rpc:call('slave1@127.0.0.1',fprof,trace,[stop]),
	% B = rpc:call('slave1@127.0.0.1',fprof,profile,[{file,?TESTPTH++"slave1.trace"}]),
	% C = rpc:call('slave1@127.0.0.1',fprof,analyse,[[{dest,?TESTPTH++"slave1.txt"}]]),
	stop_slaves([1,2,3]),
	ok.

% For every slave
% Call slave which sends a tunnel call to all others. Check that it receives back messages from all.
dotunnels() ->
	S = ['slave1@127.0.0.1','slave2@127.0.0.1','slave3@127.0.0.1'],
	[begin
		?debugFmt("Calling ~p",[Nd]),
		rpc:call(Nd,?MODULE,call_tunnels,[]) 
	end || Nd <- S],
	ok.

call_tunnels() ->
	?debugFmt("Call tunnels on ~p",[node()]),
	Term = term_to_binary({?MODULE,[node(),self()]}),
	NSent = esqlite3:all_tunnel_call([<<(iolist_size(Term)):16>>,Term]),
	?debugFmt("Get respones ~p",[NSent]),
	% Presumed 2 threads and 2 nodes
	ok = get_responses(2*2).

get_responses(0) ->
	?debugFmt("received all responses",[]),
	ok;
get_responses(N) ->
	receive
		{callback,Nd} ->
			?debugFmt("On node ~p, calback from Nd ~p, rem ~p",[node(),Nd,N-1]),
			get_responses(N-1)
	end.

tunnel_callback(Nd,Pid) ->
	rpc:call(Nd,erlang,send,[Pid,{callback,node()}]).


test_cluster(_) ->
	[
		% fun() -> timer:sleep(2000), ok end,
		% fun dotunnels/0
	  {timeout,20,fun basic_write/0},
	  fun recoveractor/0,
	  fun basic_read/0,
	  {timeout,10,fun kv_readwrite/0},
	  fun basic_write/0,
	  fun multiupdate_write/0,
	  fun multiupdate_read/0,
	  fun copyactor/0
	 %  fun() -> test_print_end([1,2,3]) end
	 
	 %  {timeout,20,fun() -> timer:sleep(6000),
	 %  			?debugFmt("SLEEP DONE",[]),
	 %  			A = rpc:call('slave1@127.0.0.1',eprof,log,[?TESTPTH++"slave1.eprof.txt"]),
	 %  			B = rpc:call('slave1@127.0.0.1',eprof,stop_profiling,[]),
		% 	C = rpc:call('slave1@127.0.0.1',eprof,analyze,[]),
		% 	?debugFmt("Eprof ~p ~p ~p",[A,B, C]),
		% 	ok end},
		% fun() -> timer:sleep(1000),ok end
			].


missingn_start() ->
	basic_init(),
	create_allgroups([[1,2,3]]),
	start_slaves([1,2,3]),
	ok.
test_missingn(_) ->
	[fun basic_write/0,
	 fun basic_read/0,
	 fun basic_write/0,
	 fun basic_read/0,
	 fun kv_readwrite/0,
	 fun multiupdate_write/0,
	 fun multiupdate_read/0,
	 fun copyactor/0,
	 fun() -> 
	 	?debugFmt("Stopping slave 3",[]),
	 	stop_slaves([3]) 
	 end,
	 fun basic_write/0
	 ].
missingn_stop(_) ->
	stop_slaves([1,2,3]),
	ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			MULTIPLE CLUSTER TESTS
% 	Execute queries over multiple clusters
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
mcluster_start() ->
	basic_init(),
	create_allgroups([[1,2],[3,4]]),
	start_slaves([1,2,3,4]),
	ok.
mcluster_stop(_) ->
	stop_slaves([1,2,3,4]),
	ok.
test_mcluster(_) ->
	[fun basic_write/0,
	 fun basic_read/0,
	 fun basic_write/0,
	 fun basic_read/0,
	 fun kv_readwrite/0,
	 fun multiupdate_write/0,
	 fun multiupdate_read/0,
	 fun copyactor/0
	 ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			ADD NODE TO CLUSTER
% 	Start with a cluster, add a node after initial setup
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clusteraddnode_start() ->
	basic_init(),
	create_allgroups([[1,2]]),
	start_slaves([1,2]),
	ok.
clusteraddnode_stop(_) ->
	stop_slaves([1,2,3]),
	ok.
test_clusteraddnode(_) ->
	[fun basic_write/0,
	  fun basic_read/0,
	  {timeout,60,fun test_add_third/0},
	  {timeout,20,fun basic_read/0},
	  {timeout,20,fun basic_write/0},
	  {timeout, 20, fun kv_readwrite/0},
	  fun multiupdate_write/0,
	  fun multiupdate_read/0,
	  fun() -> test_print_end([1,2,3]) end,
	  fun() -> ?debugFmt("STOPPING SLAVE2",[]), stop_slaves([2]) end,
	  {timeout,20,fun basic_write/0},
	  fun basic_read/0,
	  fun copyactor/0
	  	].
test_add_third() ->
	create_allgroups([[1,2,3]]),
	start_slaves([3]),
	timer:sleep(1000),
	?assertMatch(ok,wait_modified_tree(3,[1,2,3])).

test_print_end(Nodes) ->
	wait_modified_tree(1,Nodes).


wait_modified_tree(Nd) ->
	wait_modified_tree(Nd,[]).
wait_modified_tree(Nd,All) ->
	case rpc:call(fullname(Nd),gen_server,call,[actordb_shardmngr,get_all_shards]) of
		{badrpc,_Err} ->
			?debugFmt("Waiting for shard data from ~p ",[Nd]),
			timer:sleep(1000),
			wait_modified_tree(Nd,All);
		{[_|_] = AllShards1,_Local} ->
			AllShards2 = lists:keysort(1,AllShards1),
			AllShards = [{From,To,To-From,Ndx} || {From,To,Ndx} <- AllShards2],
			?debugFmt("~p allshards ~p",[time(),AllShards]),
			[?debugFmt("~p For nd ~p, beingtaken ~p",[time(),Ndx,
					rpc:call(fullname(Ndx),gen_server,call,[actordb_shardmngr,being_taken])]) || Ndx <- All],
			[?debugFmt("~p For nd ~p, moves ~p",[time(),Ndx,
					rpc:call(fullname(Ndx),gen_server,call,[actordb_shardmvr,get_moves])]) || Ndx <- All],
			case lists:keymember(butil:tobin(slave_name(Nd)),4,AllShards) of
				false ->
					?debugFmt("not member of shard tree",[]),
					% ?debugFmt("Not member of shard tree ~p~nall: ~p~nlocal ~p~nmoves ~p~n",[Nd,All,Local,
					% 	rpc:call(fullname(Nd),gen_server,call,[actordb_shardmvr,get_moves])]),
					timer:sleep(1000),
					wait_modified_tree(Nd,All);
				true ->
					case rpc:call(fullname(Nd),gen_server,call,[actordb_shardmvr,get_moves]) of
						{[],[]} ->
							case lists:filter(fun({_,_,_,SNode}) -> SNode == butil:tobin(slave_name(Nd)) end,AllShards) of
								[_,_,_|_] ->
									ok;
								_X ->
									?debugFmt("get_moves empty, should have 3 shards ~p ~p",[Nd,_X]),
									% ?debugFmt("get_moves wrong num shards ~p~n ~p",[Nd,X]),
									timer:sleep(1000),
									wait_modified_tree(Nd,All)
							end;
						_L ->
							?debugFmt("Still moving processes ~p",[Nd]),
							timer:sleep(1000),
							wait_modified_tree(Nd,All)
					end
			end
	end.

wait_is_ready(Nd) ->
	case rpc:call(fullname(Nd),actordb,is_ready,[]) of
		true ->
			ok;
		_ ->
			timer:sleep(1000),
			wait_is_ready(Nd)
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			ADD CLUSTER TO NETWORK
% 	Start with a cluster, add an additional cluster, wait for shards to move.
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clusteradd_start() ->
	basic_init(),
	create_allgroups([[1,2]]),
	start_slaves([1,2]),
	ok.
clusteradd_stop(_) ->
	stop_slaves([1,2,3,4]),
	ok.
test_clusteradd(_) ->
	[
	 {timeout,10,fun() -> basic_write(butil:tobin(?LINE)) end},
	 {timeout,10,fun basic_read/0},
	 {timeout,10,fun kv_readwrite/0},
	 {timeout,60,fun test_add_cluster/0},
	  {timeout,10,fun() -> basic_write(butil:tobin(?LINE)) end},
	  {timeout,10,fun basic_read/0},
	  {timeout,10,fun multiupdate_write/0},
	  {timeout,10,fun multiupdate_read/0},
	  	% {timeout,10,fun() -> basic_write(butil:tobin(?LINE)) end},
	  fun() -> test_print_end([1,2,3,4]) end
	  	].
test_add_cluster() ->
	create_allgroups([[1,2],[3,4]]),
	start_slaves([3,4]),
	timer:sleep(1000),
	wait_modified_tree(3,[1,2,3,4]),
	wait_modified_tree(4,[1,2,3,4]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			FAILED NODES
% 	Run queries with nodes offline. 
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
failednodes_start() ->
	basic_init(),
	create_allgroups([[1,2,3]]),
	start_slaves([1,2,3]),
	ok.
failednodes_stop(_) ->
	stop_slaves([1,2,3]),
	ok.
test_failednodes(_) ->
	[
	 {timeout,20,fun basic_write/0},
	 {timeout,20,fun basic_write/0},
	 {timeout,20,fun basic_read/0},
	 {timeout,20,fun kv_readwrite/0},
	 {timeout,20,fun multiupdate_write/0},
	 {timeout,20,fun multiupdate_read/0},
	 fun() -> 
	 	?debugFmt("Taking down node 2",[]),
	 	stop_slaves([2]),ok 
	 end,
	 {timeout,60,fun basic_write/0},
	 {timeout,60,fun() -> 
	 	?debugFmt("Starting back node 2",[]),
	 	start_slaves([2]), ok 
	 end},
	 {timeout,60,fun basic_write/0},
	 fun() -> 
	 	?debugFmt("Taking down node 2 and node 3",[]),
	 	stop_slaves([2,3]),ok 
	 end,
	 fun() -> case catch fun basic_write/0 of
	 			_ ->
	 				ok
	 		end end,
	 {timeout,20,fun() ->
	 	?debugFmt("Starting back node 2 and node 3",[]),
	 	start_slaves([2,3]), wait_is_ready(2),wait_is_ready(3), ok 
	 end},
	 {timeout,20,fun basic_write/0},
	 fun() -> test_print_end([1,2,3]) end
	].










%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 	UTILITY FUNCTIONS
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
exec(Bin) ->
	% if testnd then we are running a unit test
	% if not we are running tests manually on live nodes
	case node() == 'testnd@127.0.0.1' orelse element(1,os:type()) == win32 of
		true ->
			S = ['slave1@127.0.0.1','slave2@127.0.0.1','slave3@127.0.0.1'],
			rpc:call(findnd(S),actordb,exec,[butil:tobin(Bin)]);
		false ->
			actordb:exec(Bin)
	end.

findnd([H|T]) ->
	case lists:member(H,nodes(connected)) of
		true ->
			H;
		_ ->
			findnd(T)
	end.

basic_init() ->
	% Start distributed erlang if not active yet.
	% Delete test folder and create it fresh
	case node() == 'nonode@nohost' of
		true ->
			net_kernel:start(['master@127.0.0.1',longnames]);
		_ ->
			ok
	end,
	% Election uses global. If global running on this node instead of one of the slaves
	% nothing will work.
	actordb_election:stop(),
	butil:deldir(?TESTPTH),
	filelib:ensure_dir(?TESTPTH++slave_name(1)++"/etc/"),
	filelib:ensure_dir(?TESTPTH++"etc/").

start_trace(_Opts) ->
	% actordb_shardmngr ! {fprof,Opts}.
	% actordb_shardmngr ! eprof.
	% {ok,P} = eprof:start(),
	% eprof:start_profiling([actordb_shardmngr]).
	ok.


start_slaves(L) ->
	[start_slave(S) || S <- L],
	timer:sleep(3000),
	rpc:call('slave1@127.0.0.1',?MODULE,start_trace,[[start,{file,?TESTPTH++"slave1.trace"}]]),
	% rpc:call('slave1@127.0.0.1',eprof,start,[]),
	case rpc:call('slave1@127.0.0.1',actordb_shardtree,all,[]) of
		{badrpc,_Err} ->
			Init = rpc:call('slave1@127.0.0.1',actordb_cmd,cmd,[init,commit,?TESTPTH++slave_name(1)++"/etc"]);
		_ ->
			Init = rpc:call('slave1@127.0.0.1',actordb_cmd,cmd,[updatenodes,commit,?TESTPTH++slave_name(1)++"/etc"])
	end,
	?debugFmt("Init result ~s",[Init]),
	[wait_tree(fullname(N),10000) || N <- L].
start_slave(N) ->
	{ok,[Paths]} = init:get_argument(pa),
	Name = slave_name(N),
	Cookie = erlang:get_cookie(),
	% {etc,?TESTPTH++Name++"/etc"}
	Opts = [{docompile,false},{autocompile,[]},{rpcport,9050+N}], 
	case Name of
		"slave1" ->
			% file:write_file(?TESTPTH++Name++"/etc/schema.cfg",io_lib:fwrite("~p.~n",[schema()]));
			file:write_file(?TESTPTH++Name++"/etc/schema.yaml",io_lib:fwrite("~s~n",[schema()]));
		_ ->
			ok
	end,
	file:write_file(?TESTPTH++Name++".config",io_lib:fwrite("~p.~n",[[{bkdcore,Opts},
						{actordb_core,[{main_db_folder,?TESTPTH++Name},{extra_db_folders,[]},{mysql_protocol,0}]},
						{lager,[{handlers,setup_loging()}]},
						{sasl,[{errlog_type,error}]}]])),
	% file:write_file(?TESTPTH++Name++"/etc/actordb.cfg",io_lib:fwrite("~p.~n",[[{db_path,?TESTPTH++Name},{level_size,0}]])),
	Param = " -eval \"application:start(actordb_core)\" -pa "++lists:flatten(butil:iolist_join(Paths," "))++
			" -setcookie "++atom_to_list(Cookie)++
			" -config "++?TESTPTH++Name++".config",
	?debugFmt("startparam ~p~n",[Param]),
	{ok,Nd} = slave:start_link('127.0.0.1',butil:toatom(Name),Param),
	Nd.

setup_loging() ->
	{ok,_Handlers} = application:get_env(lager,handlers),
	% [{lager_console_backend,[info,Param]} || {lager_console_backend,[debug,Param]} <- Handlers].
	[{lager_console_backend,[debug,{lager_default_formatter, [time," ",pid," ",node," ",module," ",line,
								" [",severity,"] ", message, "\n"]}]}].

slave_name(N) ->
	"slave"++butil:tolist(N).
fullname(N) ->
	butil:toatom(slave_name(N)++"@127.0.0.1").

wait_tree(Nd,X) when X < 0 ->
	?debugFmt("Timeout waiting for shard for ~p",[Nd]),
	exit(timeout);
wait_tree(Nd,N) ->
	case rpc:call(Nd,actordb_shardtree,all,[]) of
		{badrpc,_Err} ->
			?debugFmt("waiting for shard from ~p ",[Nd]),
			timer:sleep(1000),
			wait_tree(Nd,N-1000);
		Tree ->
			?debugFmt("Have shard tree ~p~n ~p",[Nd,Tree]),
			timer:sleep(1000),
			ok
	end.

stop_slaves(L) ->
	[stop_slave(N) || N <- L].
stop_slave(N) ->
	Nd = fullname(N),
	case lists:member(Nd,nodes(connected)) of
		true ->
			slave:stop(Nd),
			timer:sleep(100),
			stop_slave(N);
		_ ->
			ok
	end.

% param: [1,2,3]  creates nodes [slave1,slave2,slave3]
create_allnodes(Slaves) ->
	{L,_} = lists:foldl(fun(S,{L,C}) ->
		{["- "++butil:tolist(fullname(S))++":"++butil:tolist(9050+C)++"\n"|L],C+1}
	end,
	{[],1},Slaves),
	L.

% [[1,2,3],[4,5,6]]  creates groups [[slave1,slave2,slave3],[slave4,slave5,slave6]]
% Writes the file only to first node so that config is spread to others.
create_allgroups(Groups) ->
	StrNodes = "nodes:\n"++create_allnodes(lists:flatten(Groups)),
	{StrGroups,_} = lists:foldl(fun(G,{L,C}) -> 
						{["- name: "++"grp"++butil:tolist(C)++"\n",
						  "  nodes: ["++butil:iolist_join([slave_name(Nd) || Nd <- G],",")++"]\n",
						  "  type: cluster\n"|L],C+1}
					end,{[],0},Groups),
	?debugFmt("Writing ~p",[?TESTPTH++slave_name(1)++"/etc/nodes.yaml"]),
	file:write_file(?TESTPTH++slave_name(1)++"/etc/nodes.yaml",io_lib:fwrite("~s~n",[StrNodes++"\ngroups:\n"++StrGroups])).

schema() ->
	butil:iolist_join([
		"type1:",
		"- CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT, i INTEGER)",
		"- CREATE TABLE tab1 (id INTEGER PRIMARY KEY, txt TEXT)",
		"- CREATE TABLE tab2 (id INTEGER PRIMARY KEY, txt TEXT)",
		"",
		"thread:",
		"- CREATE TABLE thread (id INTEGER PRIMARY KEY, msg TEXT, user INTEGER);",
		"",
		"user:",
		"- CREATE TABLE userinfo (id INTEGER PRIMARY KEY, name TEXT);",
		"",
		"counters:",
		" type: kv",
		" schema:",
		" - CREATE TABLE actors (id TEXT UNIQUE, hash INTEGER, val INTEGER);"
		"",
		"filesystem:",
		" type: kv",
		" schema:",
		" - CREATE TABLE actors (id TEXT UNIQUE, hash INTEGER, size INTEGER)",
		" - CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, fileid TEXT, uid INTEGER, "++
			"FOREIGN KEY (fileid) REFERENCES actors(id) ON DELETE CASCADE)"
	],"\n").



















%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 		Experiments
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




l(N) ->
	spawn(fun() ->
		_D = <<"actor type1(ac",(butil:tobin(1))/binary,");",
				"insert into tab1 values (",(butil:tobin(butil:flatnow()))/binary,",'",
				(binary:copy(<<"a">>,1024*1))/binary,"');",
				"insert into tab2 values (1,2,3,4,5);",
				"select * from what where id=1 and id=2 or id=10 and id=11;">>,
		
		% cprof:start(),
		Start = now(),
		random:seed(os:timestamp()),
		% {ok,Db,_Schema,_} = actordb_sqlite:init(":memory:",off),
		l1(N,ok),
		% actordb_sqlite:stop(Db),
		% cprof:pause(),
		io:format("Diff ~p~n",[timer:now_diff(now(),Start)])
	end).
	% io:format("~p~n",[cprof:analyse()]).

l1(0,_) ->
	ok;
l1(N,X) ->
	% random:uniform(200),
	% actordb_util:hash(<<"asdf">>),
	% butil:ds_val(twoseconds,reftimes),
	% os:timestamp(),
	% esqlite3:noop(X),
	% actordb_sqlparse:parse_statements(X),
	% base64:encode(X),
	% butil:hex(X),
	% os:timestamp(),
	% file:open(["/Users/sergej/Desktop/","-wal"],[read,binary,raw]),
	l1(N-1,X).


t(Conc,PerWorker) ->
	t(Conc,PerWorker,1).
t(Conc,PerWorker,Size) ->
	spawn(fun() -> runt1(Conc,PerWorker,Size)	end).
% neverend (reload module to kill it)
tnv(C,P,S) ->
	spawn(fun() -> runt(C,P,S)	end).

runt(C,P,S) ->
	runt1(C,P,S),
	runt(C,P,S).

runt1(Concurrency,PerWorker,S) ->
	Start = now(),
	[spawn_monitor(fun() -> {A,B,C} = now(),
							random:seed(A,B,C), 
							run(binary:copy(<<"a">>,1024*S),actordb:start_bp(),N,PerWorker),
							io:format("Done with ~p ~p~n",[N,timer:now_diff(now(),{A,B,C})])
					end)
			 || N <- lists:seq(1,Concurrency)],
	wait_t_response(Concurrency),
	St = now(),
	erase(),
	Time = timer:now_diff(St,Start),
	io:format("~p~n~ps,~pms,~pmics~n", [Time,Time div 1000000, ((Time rem 1000000)  div 1000),
										((Time rem 1000000)  rem 1000)]).

wait_t_response(0) ->
	ok;
wait_t_response(N) ->
	receive
		{'DOWN',_Monitor,_,_PID,_Result} ->
			wait_t_response(N-1)
	end.

run(D,_P,_W,0) ->
	D;
run(D,P,W,N) ->
	% butil:tobin(random:uniform(100000))
		Sql = {[{{<<"type1">>,[<<"ac.",(butil:tobin(W))/binary,".",(butil:tobin(N))/binary>>],[create]},
		   true,
		   [<<"insert into tab values (",(butil:tobin(butil:flatnow()))/binary,",'",D/binary,"',1);">>]}],
		 true},
		 case actordb:exec_bp1(P,byte_size(D),Sql) of
		 % case actordb:exec1(Sql) of
		 	{sleep,_} ->
		 		actordb:sleep_bp(P);
		 		% ok;
		 	_ ->
		 		ok
		 end,
		% exec(<<"actor type1(ac",(butil:tobin(W))/binary,".",(butil:tobin(N))/binary,");",
		% 					"insert into tab1 values (",(butil:tobin(butil:flatnow()))/binary,",'",D/binary,"',1);">>),
		
	run(D,P,W,N-1).


tsingle(N) ->
	spawn_monitor(fun() -> 
			
			file:delete("tt"),
			file:delete("tt-wal"),
			file:delete("tt-shm"),
			{ok,Db,_,_} = actordb_sqlite:init("tt",wal),
			Pragmas = actordb_sqlite:exec(Db,<<"PRAGMA cache_size;PRAGMA mmap_size;PRAGMA page_size;",
								"PRAGMA synchronous=0;PRAGMA locking_mode;">>),
			io:format("PRagmas ~p~n",[Pragmas]),
			% actordb_sqlite:exec(Db,<<"CREATE TABLE tab1 (id INTEGER PRIMARY KEY, txt TEXT);">>)
			Schema = <<"CREATE TABLE tab1 (id INTEGER PRIMARY KEY, txt TEXT);",
						"CREATE TABLE tab2 (id INTEGER PRIMARY KEY, val INTEGER);">>,
			_Schema1 = 
			"CREATE TABLE t_dir (
			  id INTEGER NOT NULL,
			  v INTEGER DEFAULT 1,
			  eid INTEGER NOT NULL,
			  parent INTEGER,
			  name TEXT NOT NULL,
			  tdiff INTEGER,
			  tdel INTEGER,
			  mtime INTEGER NOT NULL,
			  pub INTEGER,
			  root INTEGER NOT NULL,
			  def INTEGER NOT NULL,
			  subs TEXT,
			  children INTEGER DEFAULT 0,
			  fav INTEGER,
			  shared INTEGER,
			  size INTEGER,
			  misc TEXT,
			  created TIMESTAMP DEFAULT (strftime('%s', 'now')),
			  CONSTRAINT pk_dir PRIMARY KEY(id) FOREIGN KEY (parent) REFERENCES t_dir(id) ON UPDATE CASCADE);"++
			"CREATE TABLE t_file (
			  id INTEGER NOT NULL,
			  v INTEGER NULL DEFAULT 1,
			  eid INTEGER NOT NULL,
			  parent INTEGER NOT NULL,
			  name TEXT NULL,
			  tdiff INTEGER NULL,
			  tdel INTEGER NULL,
			  pub INTEGER NULL,
			  size INTEGER NOT NULL,
			  mtime INTEGER NOT NULL,
			  crc INTEGER,
			  crch INTEGER,
			  sha TEXT NULL,
			  rev INTEGER NULL,
			  fav INTEGER NULL,
			  shared INTEGER,
			  misc TEXT,
			  ext TEXT NOT NULL,
			  created TIMESTAMP DEFAULT (strftime('%s', 'now')),
			  CONSTRAINT pk_file PRIMARY KEY(id) FOREIGN KEY (parent) REFERENCES t_dir(id) ON UPDATE CASCADE);"++
			"INSERT OR REPLACE INTO t_dir (id,v,eid,parent,name,tdiff,tdel,mtime,pub,root,def,subs,children,fav,shared,size,misc) "++
			"VALUES ( 1015,1,2,1015,'otp_src_17.1',0,0,0,0,0,0,'',0,0,0,0,"++
				"'836c0000000168026400056d74696d656802680362000007de6108610468036107611b61206a');",
			_Create = actordb_sqlite:exec(Db,Schema),

			Sql = tsingle([],N),
			Start = now(),
			% actordb_sqlite:exec(Db,<<"SAVEPOINT 'aa';">>),
			%% Out1 = esqlite3:bind_insert(<<"INSERT INTO tab1 values(?1,?2);">>,Sql,Db),
			% Out1 = esqlite3:exec_script(<<"_insert into tab1 values (?1,?2);">>,Sql,Db),
			% actordb_sqlite:exec(Db,<<"RELEASE SAVEPOINT 'aa';">>),
			% Out = actordb_sqlite:exec(Db,<<"SELECT count(*) from tab1;">>),
			Out1 = actordb:exec(<<"actor type1(tt) create;",
				"_insert or replace into tab1 values (?1,?2);",
				"_insert or replace into tabx values (?1,?2);">>,[Sql,Sql]),
			io:format("insert ~p~n",[Out1]),
			Out = actordb:exec(<<"actor type1(tt);select count(*) from tab1;select count(*) from tabx;">>),
			io:format("Time ~p~nrows ~p~nWalsize ~p~nSqlsize ~p~n",
				[timer:now_diff(now(),Start),Out,filelib:file_size("tt-wal"),1]) %iolist_size(Sql)
		end),
	receive
		{'DOWN',_Monitor,_,_PID,normal} ->
			ok;
		{'DOWN',_Monitor,_,_PID,Reason} ->
			Reason
		after 1000 ->
			timeout_wait_finish_tsingle
	end.
tsingle(Db,0) ->
	Db;
tsingle(Db,N) ->
	% Sql = 
	% <<
	% "INSERT INTO t_file (id,v,eid,parent,name,tdiff,tdel,pub,size,mtime,crc,crch,sha,rev,fav,shared,misc,ext) VALUES ( ",
	% 	(butil:tobin(N))/binary,
	% 	",1,3,1015,'AUTHORS',0,0,0,601,63570777057,0,0,'',0,0,0,'836a','');">>,
	
	Txt = <<"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA">>,
	% Sql = [<<"insert into tab1 values (">>,butil:tobin(N),$,,$',Txt,$',<<");">>],
	Sql = {tab1,N,Txt},
	tsingle([Sql|Db],N-1).

test_wal() ->
	{PID,_} = spawn_monitor(fun() -> wal_test1() end),
	receive
		{'DOWN',_Monitor,_,PID,normal} ->
			ok;
		{'DOWN',_Monitor,_,PID,Reason} ->
			Reason
		after 1000 ->
			timeout
	end.
wal_test1() ->
	file:delete("tt"),
	{ok,DbPre,_Schema,PageSize} = actordb_sqlite:init("tt",wal),
	{ok,_} = actordb_sqlite:exec(DbPre,<<"CREATE TABLE tab (id TEXT PRIMARY KEY, v INTEGER);INSERT INTO tab VALUES (1,0);">>),
	DbPost = wlcut(DbPre,PageSize,0),
	Print = fun() -> io:format("After ~p~n",[actordb_sqlite:exec(DbPost,"SELECT * FROM tab;")]) end,
	Print(),
	actordb_sqlite:stop(DbPost),
	file:delete("tt"),
	ok.

wlcut(Db,PgSize,N) ->
	{ok,_} = actordb_sqlite:exec(Db,[<<"UPDATE tab SET v=">>,butil:tobin(N), <<" WHERE id=1;">>]),
	{NPrev,NPages} = actordb_sqlite:wal_pages(Db),
	io:format("~p ~p ~p~n",[N,{NPrev,NPages},actordb_sqlite:exec(Db,"SELECT * FROM tab;")]),
	case N of
		10 ->
			% Make the last write magically dissappear.
			{ok,File} = file:open("tt-wal",[write,read,binary,raw]),
			{ok,FileSize} = file:position(File,eof),
			io:format("Filesize ~p, truncating at ~p~n",[FileSize,32+(PgSize+24)*NPrev]),
			{ok,_} = file:position(File,32+(PgSize+24)*NPrev),
			ok = file:truncate(File),
			file:close(File),
			actordb_sqlite:stop(Db),
			{ok,DbPost,_Schema,_PageSize} = actordb_sqlite:init("tt",wal),
			DbPost;
		_ ->
			wlcut(Db,PgSize,N+1)
	end.



filltkv(N) ->
	filltkv(N,binary:copy(<<"a">>,1024*1024)).
filltkv(N,B) when N > 0 ->
	actordb:exec(["ACTOR textkv(",butil:tobin(N),");","INSERT OR IGNORE INTO actors VALUES ('",
					butil:tobin(N),"',","{{hash(",butil:tobin(N),")}},'",B,"')"]),
	filltkv(N-1,B);
filltkv(0,_) ->
	ok.



t(Path) ->
	{ok,F} = file:open([Path,"-wal"],[read,binary,raw]),
	{ok,_WalSize} = file:position(F,eof),
	{ok,_} = file:position(F,{cur,-(4096+40)}),
	{ok,<<A:32,B:32,Evnum:64/big-unsigned,Evterm:64/big-unsigned>>} =
		file:read(F,24),
	file:close(F),
	{A,B,Evnum,Evterm}.
