% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_sqlprocutil).
-compile(export_all).
-include_lib("actordb_sqlproc.hrl").

reply(undefined,_Msg) ->
	ok;
reply(From,Msg) ->
	gen_server:reply(From,Msg).

ae_respond(P,LeaderNode,Success) ->
	Resp = {appendentries_response,bkdcore:node_name(),P#dp.current_term,Success,P#dp.evnum,P#dp.evterm},
	bkdcore:rpc(LeaderNode,{?MODULE,call_master,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
									{state_rw,Resp}]}).

reply_maybe(#dp{callfrom = undefined} = P) ->
	P;
reply_maybe(P) ->
	reply_maybe(P,1,P#dp.follower_indexes).
reply_maybe(P,N,[H|T]) ->
	case H of
		_ when H#flw.next_index > P#dp.evnum ->
			reply_maybe(P,N+1,T);
		_ ->
			reply_maybe(P,N,T)
	end;
reply_maybe(P,N,[]) ->
	case N*2 > (length(P#dp.follower_indexes)+1) of
		true ->
			reply(P#dp.callfrom,P#dp.callres),
			P#dp{callfrom = undefined, callres = undefined};
		false ->
			P
	end.


reopen_db(#dp{mors = master} = P) ->
	case ok of
		% we are master but db not open or open as file descriptor to -wal file
		_ when element(1,P#dp.db) == file_descriptor; P#dp.db == undefined ->
			file:close(P#dp.db),
			{ok,Db,_SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
			P#dp{db = Db, wal_from = wal_from([P#dp.dbpath,"-wal"])};
		_ ->
			case P#dp.wal_from == {0,0} of
				true ->
					P#dp{wal_from = wal_from([P#dp.dbpath,"-wal"])};
				false ->
					P
			end
	end;
reopen_db(P) ->
	case ok of
		_ when element(1,P#dp.db) == connection; P#dp.db == undefined ->
			actordb_sqlite:stop(P#dp.db),
			{ok,F} = file:open([P#dp.dbpath,"-wal"],[read,binary,raw]),
			case file:position(F,eof) of
				{ok,0} ->
					ok = file:write(F,esqlite3:make_wal_header(?PAGESIZE));
				{ok,_WalSize} ->
					ok
			end,
			P#dp{db = F};
		_ ->
			P
	end.

% Find first valid evnum,evterm in wal (from beginning)
wal_from([_|_] = Path) ->
	{ok,F} = file:open(Path,[read,binary,raw]),
	{ok,_} = file:position(F,32),
	wal_from(F);
wal_from(F) ->
	case file:read(F,40) of
		eof ->
			file:close(F),
			{0,0};
		% Non commit page
		{ok,<<_:32,0:32,_/binary>>} ->
			{ok,_} = file:position(F,{cur,?PAGESIZE}),
			wal_from(F);
		% Commit page, we can read evnum evterm safely
		{ok,<<_:32,_:32,Evnum:64/big-unsigned,Evterm:64/big-unsigned,_/binary>>} ->
			file:close(F),
			{Evnum,Evterm}
	end.

try_wal_recover(P,F) when F#flw.file /= undefined ->
	file:close(F#flw.file),
	try_wal_recover(P,F#flw{file = undefined});
try_wal_recover(P,F) ->
	{WalEvfrom,_WalTermfrom} = P#dp.wal_from,
	% Compare match_index not next_index because we need to send prev term as well
	case F#flw.match_index >= WalEvfrom of
		% We can recover from wal if terms match
		true ->
			{File,PrevNum,PrevTerm} = open_wal_at(P,P#flw.next_index),
			case PrevNum == F#flw.match_index andalso PrevTerm == F#flw.match_term of
				true ->
					Res = true,
					NF = F#flw{file = File};
				false ->
					% follower in conflict
					% this will cause a failed appendentries_start and a rewind on follower
					Res = true,
					NF = F#flw{file = File, match_term = PrevTerm, match_index = PrevNum}
			end;
		% Too far behind
		false ->
			Res = false,
			NF = F
	end,
	{Res,
	 P#dp{follower_indexes = lists:keystore(NF#flw.node,#flw.node,P#dp.follower_indexes,NF)},
	 NF}.

continue_maybe(P,F) ->
	case P#dp.evnum >= F#flw.next_index of
		true when F#flw.file == undefined ->
			case try_wal_recover(P,F) of
				{true,NP,NF} ->
					continue_maybe(NP,NF);
				{false,_NP,NF} ->
					ok
			end;
		true ->
			StartRes = bkdcore:rpc(F#flw.node,{?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
						{state_rw,{appendentries_start,P#dp.current_term,bkdcore:node_name(),
									F#flw.match_index,F#flw.match_term,P#dp.commit_index,false}}]}),
			case StartRes of
				false ->
					file:close(F#flw.file),
					NF = F#flw{file = undefined};
				ok ->
					% Send wal
					NF = F,
					send_wal(P,F)
			end;
		% Follower uptodate
		false when F#flw.file == undefined ->
			NF = F;
		false ->
			file:close(F#flw.file),
			NF = F#flw{file = undefined}
	end,
	P#dp{follower_indexes = lists:keystore(F#flw.node,#flw.node,P#dp.follower_indexes,NF)}.

open_wal_at(P,Index) ->
	{ok,F} = file:open([P#dp.dbpath,"-wal"],[read,binary,raw]),
	{ok,_} = file:position(F,32),
	open_wal_at(P,Index,F,undefined,undefined).
open_wal_at(P,Index,F,PrevNum,PrevTerm) ->
	case file:read(F,40) of
		{ok,<<_:32,_:32,Evnum:64/big-unsigned,Evterm:64/big-unsigned,_/binary>>} when Index == Evnum ->
			{ok,_} = file:position(F,{cur,-40}),
			{F,PrevNum,PrevTerm};
		{ok,<<_:32,_:32,Evnum:64/big-unsigned,Evterm:64/big-unsigned,_/binary>>} ->
			{ok,_} = file:position(F,?PAGESIZE),
			open_wal_at(P,Index,F,Evnum,Evterm)
	end.

send_wal(P,#flw{file = File} = F) ->
	{ok,<<Header:40/binary,Page/binary>>} = file:read(F,40+?PAGESIZE),
	case Header of
		<<_:32,Commit:32,Evnum:64/big-unsigned,_:64/big-unsigned,_/binary>> when Evnum == F#flw.next_index ->
			{Compressed1,CompressedSize} = esqlite3:lz4_compress(Page),
			<<Compressed:CompressedSize/binary,_/binary>> = Compressed1,
			WalRes = bkdcore:rpc(F#flw.node,{?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
						{state_rw,{appendentries_wal,P#dp.current_term,Header,Compressed}}]}),
			case WalRes of
				ok when Commit == 0 ->
					send_wal(P,F);
				_ ->
					ok
			end
	end.


append_wal(P,Header,Bin) ->
	ok = file:write(P#dp.db,[Header,esqlite3:lz4_decompress(Bin,?PAGESIZE)]).

% Go back one entry
rewind_wal(P) ->
	case file:position(P#dp.db,{cur,-(?PAGESIZE+40)}) of
		{ok,NPos} ->
			{ok,<<_:32,Commit:32,Evnum:64/unsigned-big,Evterm:64/unsigned-big,_/binary>>} = file:read(P#dp.db,40),
			case ok of
				_ when P#dp.evnum /= Evnum, Commit /= 0 ->
					{ok,_} = file:position(P#dp.db,{cur,?PAGESIZE}),
					file:truncate(P#dp.db),
					P#dp{evnum = Evnum, evterm = Evterm};
				_ ->
					{ok,_} = file:position(P#dp.db,{cur,-40}),
					rewind_wal(P)
			end;
		{error,_} ->
			file:close(P#dp.db),
			file:delete([P#dp.dbpath,"-wal"]),
			% rewind to 0, causing a complete restore from another node
			reopen_db(P#dp{evnum = 0, evterm = 0, db = undefined})
	end.

verify_getdb(Actor,Type,Node1,MasterNode,MeMors,Cb,Evnum,Evcrc) ->
	Ref = make_ref(),
	case MasterNode of
		undefined ->
			CallFunc = call_master;
		_ ->
			CallFunc = call_slave
	end,
	?ADBG("getdb me ~p, getfrom ~p, ref ~p",[{Actor,Type},Node1,Ref]),
	case Node1 of
		{Node,ActorFrom} ->
			RpcRes = bkdcore:rpc(Node,{?MODULE,CallFunc,[Cb,ActorFrom,Type,
											{dbcopy_op,undefined, send_db,{bkdcore:node_name(),Ref,false,Evnum,Evcrc,Actor}}]});
		{split,MFA,Node,ActorFrom} ->
			RpcRes = bkdcore:rpc(Node,{?MODULE,CallFunc,[Cb,ActorFrom,Type,
											{dbcopy_op,undefined, send_db,{bkdcore:node_name(),Ref,{split,MFA},Evnum,Evcrc,Actor}}]});
		{move,_NewShard,Node} ->
			RpcRes = bkdcore:rpc(Node,{?MODULE,CallFunc,[Cb,Actor,Type,
											{dbcopy_op,undefined, send_db,{bkdcore:node_name(),Ref,true,Evnum,Evcrc,Actor}}]});
		Node when is_binary(Node) ->
			RpcRes = bkdcore:rpc(Node,{?MODULE,CallFunc,[Cb,Actor,Type,
											{dbcopy_op,undefined, send_db,{bkdcore:node_name(),Ref,false,Evnum,Evcrc,Actor}}]})
	end,
	% ?AINF("Verify getdb ~p ~p ~p ~p ~p",[Actor,Type,Node1,MasterNode,{element(1,RpcRes),butil:type(element(2,RpcRes))}]),
	case RpcRes of
		{ok,Ref} ->
			% Remote node will start sending db file.
			exit({update_from,Node,MeMors,MasterNode,Ref});
		{ok,Bin} ->
			% db file small enough to be sent directly
			exit({update_direct,MeMors,Bin});
		{wlog,LEvnum,LEvcrc,Sql} ->
			exit({wlog,LEvnum,LEvcrc,Sql});
		{error,enoent} ->
			exit({error,enoent});
		{redirect,RNode} when RNode /= Node ->
			case is_atom(RNode) andalso bkdcore:name_from_dist_name(RNode) /= Node of
				true ->
					verify_getdb(Actor,Type,RNode,MasterNode,MeMors,Cb,Evnum,Evcrc);
				false ->
					exit({redirect,RNode})
			end
	end.

% Check back with multiupdate actor if transaction has been completed, failed or still running.
% Every 100ms.
start_transaction_checker(Id,Uid,Node) ->
	case distreg:whereis({Id,Uid}) of
		undefined ->
			spawn_monitor(fun() -> transaction_checker(Id,Uid,Node) end);
		Pid ->
			{Pid,erlang:monitor(process,Pid)}
	end.
transaction_checker(Id,Uid,Node) ->
	case distreg:reg({Id,Uid}) of
		ok ->
			transaction_checker1(Id,Uid,Node);
		_ ->
			% If process already exists, attach to it and exit with the same exit signal
			Existing1 = distreg:whereis({Id,Uid}),
			case is_pid(Existing1) of
				true ->
					Existing = Existing1,
					erlang:monitor(process,Existing);
				false ->
					Existing = start_transaction_checker(Id,Uid,Node)
			end,
			receive
				{'DOWN',_Monitor,_Ref,Pid,noproc} when Pid == Existing ->
					start_transaction_checker(Id,Uid,Node),
					transaction_checker(Id,Uid,Node);
				{'DOWN',_Monitor,_Ref,Pid,Result} when Pid == Existing ->
					exit(Result)
			end
	end.
transaction_checker1(Id,Uid,Node) ->
	timer:sleep(100),
	Res = actordb:rpc(Node,Uid,{actordb_multiupdate,transaction_state,[Uid,Id]}),
	?ADBG("transaction_check ~p ~p",[{Id,Uid,Node},Res]),
	case Res of
		% Running
		{ok,0} ->
			transaction_checker1(Id,Uid,Node);
		% Done
		{ok,1} ->
			exit(done);
		% Failed
		{ok,-1} ->
			exit(abandonded);
		_ ->
			transaction_checker1(Id,Uid,Node)
	end.


check_redirect(P,Copyfrom) ->
	case Copyfrom of
		{move,NewShard,Node} ->
			case bkdcore:rpc(Node,{?MODULE,call_master,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
											{state_rw,donothing,[]}]}) of
				{redirect,SomeNode} ->
					case lists:member(SomeNode,bkdcore:all_cluster_nodes()) of
						true ->
							{true,NewShard};
						false ->
							case bkdcore:cluster_group(Node) == bkdcore:cluster_group(SomeNode) of
								true ->
									?AERR("Still redirects to local, will retry move. ~p",[{P#dp.actorname,P#dp.actortype}]),
									check_redirect(P,{move,NewShard,SomeNode});
								false ->
									check_redirect(P,{move,NewShard,SomeNode})
							end
					end;
				{error,econnrefused} ->
					throw({econnrefused,Node});
				ok ->
					false
			end;
		{split,{M,F,A},Node,ShardFrom} ->
			case bkdcore:rpc(Node,{?MODULE,call_master,[P#dp.cbmod,ShardFrom,P#dp.actortype,
										{dbcopy_op,undefined,checksplit,{M,F,A}}]}) of
				ok ->
					ok;
				{error,econnrefused} ->
					throw({econnrefused,Node});
				false ->
					false
			end;
		{Copyf1,_,_} when is_tuple(Copyf1) ->
			check_redirect(P,Copyf1);
		_ ->
			copy
	end.


do_copy_reset(MoveType,Copyreset,State) ->
	case Copyreset of
		{Mod,Func,Args} ->
			case apply(Mod,Func,[State|Args]) of
				ok ->
					ResetSql = <<>>;
				ResetSql when is_list(ResetSql); is_binary(ResetSql) ->
					ok
			end;
		ok ->
			ResetSql = <<>>;
		ResetSql when is_list(ResetSql); is_binary(ResetSql) ->
			ok;
		_ ->
			ResetSql = <<>>
	end,
	case MoveType of
		move ->
			<<>>;
		_ ->
			ResetSql
	end.

base_schema(SchemaVers,Type) ->
	base_schema(SchemaVers,Type,undefined).
base_schema(SchemaVers,Type,MovedTo) ->
	case MovedTo of
		undefined ->
			Moved = [];
		_ ->
			Moved = [{?MOVEDTO,MovedTo}]
	end,
	DefVals = [[$(,K,$,,$',butil:tobin(V),$',$)] || {K,V} <- 
		[{?SCHEMA_VERS,SchemaVers},{?ATYPE,Type},{?EVNUM,0},{?EVCRC,0},{?EVTERM,0}|Moved]],
	[(?LOGTABLE),
	 <<"CREATE TABLE __transactions (id INTEGER PRIMARY KEY, tid INTEGER,",
	 	" updater INTEGER, node TEXT,schemavers INTEGER, sql TEXT);",
	 "CREATE TABLE __wlog (id INTEGER PRIMARY KEY, crc INTEGER,prevev INTEGER,prevcrc INTEGER, sql TEXT);",
	 "CREATE TABLE __adb (id INTEGER PRIMARY KEY, val TEXT);">>,
	 <<"INSERT INTO __adb (id,val) VALUES ">>,
	 	butil:iolist_join(DefVals,$,),$;].

do_cb_init(#dp{cbstate = undefined} = P) ->
	case P#dp.movedtonode of
		undefined ->
			do_cb_init(P#dp{cbstate = apply(P#dp.cbmod,cb_startstate,[P#dp.actorname,P#dp.actortype])});
		_ ->
			P#dp.cbstate
	end;
do_cb_init(P) ->
	case apply(P#dp.cbmod,cb_init,[P#dp.cbstate,P#dp.evnum]) of
		{ok,NS} ->
			NS;
		{doread,Sql} ->
			case apply(P#dp.cbmod,cb_init,[P#dp.cbstate,P#dp.evnum,actordb_sqlite:exec(P#dp.db,Sql,read)]) of
				{ok,NS} ->
					NS;
				ok ->
					P#dp.cbstate
			end;
		ok ->
			P#dp.cbstate
	end.

actor_start(P) ->
	actordb_local:actor_started(P#dp.actorname,P#dp.actortype,?PAGESIZE*?DEF_CACHE_PAGES).

start_verify(P,JustStarted) ->
	ClusterNodes = bkdcore:cluster_nodes(),
	case ok of
		_ when P#dp.movedtonode /= undefined; ClusterNodes == [] ->
			P#dp{verified = true,cbstate = do_cb_init(P), activity_now = actor_start(P)};
		% Actor was started with mode slave. It will remain in slave(follower) mode and
		%  not attempt to become candidate for now.
		_ when P#dp.mors == slave, JustStarted ->
			P;
		_ ->
			CurrentTerm = P#dp.current_term+1,
			ok = butil:savetermfile([P#dp.dbpath,"-term"],{bkdcore:node_name(),CurrentTerm}),
			NP = P#dp{current_term = CurrentTerm, voted_for = bkdcore:node_name()},
			{Verifypid,_} = spawn_monitor(fun() -> 
							start_election(NP)
								end),
			NP#dp{verifypid = Verifypid, verified = false, activity_now = actor_start(P)}
	end.
% Call RequestVote RPC on cluster nodes. 
% This should be called in an async process and current_term and voted_for should have
%  been set for this election (incremented current_term, voted_for = Me)
start_election(P) ->
	ConnectedNodes = bkdcore:cluster_nodes_connected(),
	ClusterSize = length(bkdcore:cluster_nodes()) + 1,
	Me = bkdcore:node_name(),
	Msg = {state_rw,{request_vote,Me,P#dp.current_term,P#dp.evnum,P#dp.evterm}},
	{Results,_GetFailed} = rpc:multicall(ConnectedNodes,?MODULE,call_slave,
			[P#dp.cbmod,P#dp.actorname,P#dp.actortype,Msg,[{flags,P#dp.flags}]]),
	% Sum votes. Start with 1 (we vote for ourselves)
	case count_votes(Results,1) of
		{outofdate,Node,_NewerTerm} ->
			rpc:call(bkdcore:dist_name(Node),?MODULE,call_slave,
						[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,doelection}]),
			exit(follower);
		NumVotes when is_integer(NumVotes) ->
			case NumVotes*2 > ClusterSize of
				true ->
					exit(leader);
				false ->
					exit(follower)
			end
	end.
count_votes([{What,Node,HisLatestTerm}|T],N) ->
	case What of
		true ->
			count_votes(T,N+1);
		outofdate ->
			{outofdate,Node,HisLatestTerm};
		% already voted or something crashed
		_ ->
			count_votes(T,N)
	end;
count_votes([],N) ->
	N.

read_num(P) ->
	case P#dp.db of
		undefined ->
			{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal);
		Db ->
			SchemaTables = true
	end,
	case SchemaTables of
		[] ->
			<<>>;
		_ ->
			Res = actordb_sqlite:exec(Db,
						<<"SELECT * FROM __adb WHERE id=",?ANUM/binary,";">>,read),
			case Res of
				{ok,[{columns,_},{rows,[]}]} ->
					<<>>;
				{ok,[{columns,_},{rows,[{_,Num}]}]} ->
					Num;
				{sql_error,{"exec_script",sqlite_error,"no such table: __adb"}} ->
					<<>>
			end
	end.

delactorfile(P) ->
	[Pid ! delete || {_,Pid,_,_} <- P#dp.dbcopy_to],
	?ADBG("delfile ~p ~p ~p",[P#dp.actorname,P#dp.actortype,P#dp.mors]),
	case P#dp.movedtonode of
		undefined ->
			file:delete(P#dp.dbpath),
			file:delete(P#dp.dbpath++"-wal"),
			file:delete(P#dp.dbpath++"-shm");
		_ ->
			% Leave behind redirect marker.
			% Create a file with "1" attached to end
			{ok,Db,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath++"1",off),
			ok = actordb_sqlite:okornot(actordb_sqlite:exec(Db,[<<"BEGIN;">>,base_schema(0,P#dp.actortype,P#dp.movedtonode),
								<<"COMMIT;">>],write)),
			actordb_sqlite:stop(Db),
			% Rename into the actual dbfile (should be atomic op)
			ok = file:rename(P#dp.dbpath++"1",P#dp.dbpath),
			file:delete(P#dp.dbpath++"-wal"),
			file:delete(P#dp.dbpath++"-shm")
	end.


delete_actor(P) ->
	?AINF("deleting actor ~p ~p ~p",[P#dp.actorname,P#dp.dbcopy_to,P#dp.dbcopyref]),
	case (P#dp.flags band ?FLAG_TEST == 0) of
		true ->
			case actordb_shardmngr:find_local_shard(P#dp.actorname,P#dp.actortype) of
				{redirect,Shard,Node} ->
					actordb:rpc(Node,P#dp.actorname,{actordb_shard,del_actor,[Shard,P#dp.actorname,P#dp.actortype]});
				undefined ->
					{Shard,_,Node} = actordb_shardmngr:find_global_shard(P#dp.actorname),
					actordb:rpc(Node,P#dp.actorname,{actordb_shard,del_actor,[Shard,P#dp.actorname,P#dp.actortype]});
				Shard ->
					ok = actordb_shard:del_actor(Shard,P#dp.actorname,P#dp.actortype)
			end,
			actordb_events:actor_deleted(P#dp.actorname,P#dp.actortype,read_num(P));
		_ ->
			ok
	end,
	empty_queue(P#dp.callqueue,{error,deleted}),
	actordb_sqlite:stop(P#dp.db),
	delactorfile(P).
empty_queue(Q,ReplyMsg) ->
	case queue:is_empty(Q) of
		true ->
			ok;
		false ->
			{{value,Call},CQ} = queue:out_r(Q),
			{From,_Msg} = Call,
			reply(From,ReplyMsg),
			empty_queue(CQ,ReplyMsg)
	end.

semicolon(<<>>) ->
	<<>>;
semicolon(<<_/binary>> = Sql) ->
	case binary:last(Sql) of
		$; ->
			Sql;
		_ ->
			[Sql,$;]
	end;
semicolon(S) ->
	S.
% Set on firt write and not changed after. This is used to prevent a case of an actor
% getting deleted, but later created new. A server that was offline during delete missed
% the delete call and relies on actordb_events.
actornum(#dp{evnum = 0} = P) ->
	ActorNum = butil:md5(term_to_binary({P#dp.actorname,P#dp.actortype,os:timestamp(),make_ref()})),
	[<<"$INSERT OR REPLACE INTO __adb VALUES (">>,?ANUM,",'",butil:tobin(ActorNum),<<"');">>];
actornum(_) ->
	<<>>.

has_schema_updated(P,Sql) ->
	Schema = actordb_schema:num(),
	case P#dp.schemanum == Schema orelse P#dp.transactionid /= undefined orelse Sql == delete of
		true ->
			ok;
		false ->
			case apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,P#dp.schemavers]) of
				{_,[]} ->
					ok;
				{NewVers,SchemaUpdate} ->
					?ADBG("updating schema ~p ~p",[?R2P(P),SchemaUpdate]),
					{NewVers,iolist_to_binary([SchemaUpdate,
						<<"UPDATE __adb SET val='",(butil:tobin(NewVers))/binary,"' WHERE id=",?SCHEMA_VERS/binary,";">>,Sql])}
			end
	end.



nodes_for_replication(P) ->
	ReplicatingTo = [Nd || {Nd,_,_,_} <- P#dp.dbcopy_to],
	% Nodes we are replicating DB to will eventually get the data. So do not send the write now since it will be sent
	%  over with db copy.
	ClusterNodes = lists:subtract(bkdcore:cluster_nodes(),ReplicatingTo),
	LenCluster = length(ClusterNodes),
	ConnectedNodes = lists:subtract(bkdcore:cluster_nodes_connected(), ReplicatingTo),
	LenConnected = length(ConnectedNodes),
	{ConnectedNodes,LenCluster,LenConnected}.


% If called on slave, first check if master nodes is even alive.
% If not stop this process. It will cause actor to get started again and likely master node will
% be set for this node.
redirect_master(P) ->
	case lists:member(P#dp.masternodedist,nodes()) of
		true ->
			{reply,{redirect,P#dp.masternode},P};
		false ->
			{stop,normal,P}
	end.


parse_opts(P,[H|T]) ->
	case H of
		{actor,Name} ->
			parse_opts(P#dp{actorname = Name},T);
		{type,Type} when is_atom(Type) ->
			parse_opts(P#dp{actortype = Type},T);
		{mod,Mod} ->
			parse_opts(P#dp{cbmod = Mod},T);
		{state,S} ->
			parse_opts(P#dp{cbstate = S},T);
		{slave,true} ->
			parse_opts(P#dp{mors = slave},T);
		{slave,false} ->
			parse_opts(P#dp{mors = master,masternode = bkdcore:node_name(),masternodedist = node()},T);
		{copyfrom,Node} ->
			parse_opts(P#dp{copyfrom = Node},T);
		{copyreset,What} ->
			case What of
				false ->
					parse_opts(P#dp{copyreset = false},T);
				true ->
					parse_opts(P#dp{copyreset = <<>>},T);
				Mod ->
					parse_opts(P#dp{copyreset = Mod},T)
			end;
		{queue,Q} ->
			parse_opts(P#dp{callqueue = Q},T);
		{flags,F} ->
			parse_opts(P#dp{flags = P#dp.flags bor F},T);
		create ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_CREATE},T);
		actornum ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_ACTORNUM},T);
		exists ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_EXISTS},T);
		noverify ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_NOVERIFY},T);
		test ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_TEST},T);
		lock ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_STARTLOCK},T);
		nohibernate ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_NOHIBERNATE},T);
		_ ->
			parse_opts(P,T)
	end;
parse_opts(P,[]) ->
	Name = {P#dp.actorname,P#dp.actortype},
	case distreg:reg(self(),Name) of
		ok ->
			DbPath = lists:flatten(apply(P#dp.cbmod,cb_path,
									[P#dp.cbstate,P#dp.actorname,P#dp.actortype]))++
									butil:tolist(P#dp.actorname)++"."++butil:tolist(P#dp.actortype),
			P#dp{dbpath = DbPath};
		name_exists ->
			{registered,distreg:whereis(Name)}
	end.







%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 
% 							Copy full DB from one node to another.
% 
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
dbcopy_op_call({dbcopy_op,_,send_db,Data} = Msg,CallFrom,P) ->
	% Send database to another node.
	% This gets called from that node.
	% If we have sql in wlog, send missing wlog. If not send entire db.
	% File is sent in 1MB chunks. If db is =< 1MB, send the file directly.
	% If file > 1MB, switch to journal mode wal. This will caue writes to go to a WAL file not into the db.
	%  Which means the sqlite file can be read from safely.
	{Node,Ref,IsMove,_RemoteEvNum,_RemoteEvCrc,ActornameToCopyto} = Data,
	case P#dp.verified of
		true ->
			% case P#dp.wlog_status of
			% 	?WLOG_ACTIVE when IsMove == false, RemoteEvNum > 0 ->
			% 		case actordb_sqlite:exec(P#dp.db,["SELECT id FROM __wlog WHERE id=",butil:tobin(RemoteEvNum)," AND ",
			% 										" crc=",butil:tobin(RemoteEvCrc)]) of
			% 			{ok,[{columns,_},{rows,[{_}]}]} ->
			% 				?ADBG("dbcopy sending from wlog ~p",[{P#dp.actorname,P#dp.actortype}]),
			% 				SendFromWlog = true;
			% 			_ ->
			% 				SS = ["SELECT min(id) FROM __wlog WHERE prevev=",butil:tobin(RemoteEvNum)," AND ",
			% 										" prevcrc=",butil:tobin(RemoteEvCrc)],
			% 				case actordb_sqlite:exec(P#dp.db,SS) of
			% 					{ok,[{columns,_},{rows,[{_}]}]} ->
			% 						?ADBG("dbcopy sending from wlog ~p",[{P#dp.actorname,P#dp.actortype}]),
			% 						SendFromWlog = true;
			% 					_ ->
			% 						SendFromWlog = false
			% 				end
			% 		end;
			% 	_ ->
			% 		SendFromWlog = false
			% end,
			% case SendFromWlog of
			% 	true ->
			% 		Me = self(),
			% 		case lists:keyfind(Ref,3,P#dp.dbcopy_to) of
			% 			false ->
			% 				?COPYTRASH,
			% 				Db = P#dp.db,
			% 				{Pid,_} = spawn_monitor(fun() -> 
			% 						wlog_dbcopy(P#dp{dbcopy_to = Node, dbcopyref = Ref,
			% 										evnum = RemoteEvNum},Me,ActornameToCopyto) end),
			% 				{reply,{ok,Ref},check_timer(P#dp{db = Db,
			% 												dbcopy_to = [{Node,Pid,Ref,IsMove}|P#dp.dbcopy_to], 
			% 												activity = P#dp.activity + 1})};
			% 			{_,_Pid,Ref,_} ->
			% 				?DBG("senddb already exists with same ref!"),
			% 				{reply,{ok,Ref},P}
			% 		end;
			% 	false ->
					case file:read_file_info(P#dp.dbpath) of
						{ok,I} when I#file_info.size > 1024*1024 orelse P#dp.dbcopy_to /= [] orelse 
										IsMove /= false  ->
							?ADBG("senddb myname ~p, remotename ~p info ~p, copyto already ~p",
									[{P#dp.actorname,P#dp.actortype},ActornameToCopyto,
															{Node,Ref,IsMove,I#file_info.size},P#dp.dbcopy_to]),
							?DBLOG(P#dp.db,"senddb to ~p ~p",[ActornameToCopyto,Node]),
							Me = self(),
							case lists:keyfind(Ref,3,P#dp.dbcopy_to) of
								false ->
									?COPYTRASH,
									Db = P#dp.db,
									{Pid,_} = spawn_monitor(fun() -> 
											dbcopy(P#dp{dbcopy_to = Node, dbcopyref = Ref},Me,ActornameToCopyto) end),
									{reply,{ok,Ref},P#dp{db = Db,
														dbcopy_to = [{Node,Pid,Ref,IsMove}|P#dp.dbcopy_to], 
														activity = P#dp.activity + 1}};
								{_,_Pid,Ref,_} ->
									?DBG("senddb already exists with same ref!"),
									{reply,{ok,Ref},P}
							end;
						{ok,_I} ->
							{ok,Bin} = file:read_file(P#dp.dbpath),
							{reply,{ok,Bin},P};
						{error,enoent} ->
							?AINF("enoent during senddb to ~p ~p",[Node,?R2P(P)]),
							reply(CallFrom,{error,enoent}),
							{stop,normal,P};
						Err ->
							{reply,Err,P}
					end;
			% end;
		_ when P#dp.masternode /= undefined ->
			case P#dp.masternode == bkdcore:node_name() of
				true ->
					{noreply,P#dp{callqueue = queue:in_r({CallFrom,Msg},P#dp.callqueue)}};
				_ ->
					?ADBG("redirect not master node"),
					redirect_master(P)
			end;
		false ->
			{noreply,P#dp{callqueue = queue:in_r({CallFrom,Msg},P#dp.callqueue)}}
	end;
dbcopy_op_call({dbcopy_op,From1,wlog_read,Data} = Msg,CallFrom,P) ->
	{FromPid,Ref} = From1,
	IdFrom = butil:tobin(Data),
	case actordb_sqlite:exec(P#dp.db,[<<"SELECT sum(length(sql)) FROM __wlog WHERE id>">>,IdFrom," and id<",IdFrom,
												"+100;"]) of
		{ok,[{columns,_},{rows,[{N}]}]} when is_integer(N), N < 1024*1024*10 ->
			{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(P#dp.db,
										[<<"SELECT * FROM __wlog WHERE id>">>,IdFrom," and id<",IdFrom,
												"+100;"]),
			case length(Rows) < 100 of
				true ->
					{reply,{ok,done,Rows},P#dp{locked = butil:lists_add({FromPid,Ref},P#dp.locked)}};
				false ->
					{reply,{ok,continue,Rows}}
			end;
		{ok,[{columns,_},{rows,[{N}]}]} when is_integer(N) ->
			{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(P#dp.db,
										[<<"SELECT * FROM __wlog WHERE id>">>,IdFrom," and id<",IdFrom,
												"+10;"]),
			case length(Rows) < 10 of
				true ->
					{reply,{ok,done,Rows},P#dp{locked = butil:lists_add({FromPid,Ref},P#dp.locked)}};
				false ->
					{reply,{ok,continue,Rows},P}
			end;
		{ok,[{columns,_},{rows,[{undefined}]}]} ->
			case P#dp.transactionid of
				undefined ->
					{reply,done,P#dp{locked = butil:lists_add({FromPid,Ref},P#dp.locked)}};
				_ ->
					{noreply,P#dp{callqueue = queue:in_r({CallFrom,Msg},P#dp.callqueue)}}
			end
	end;
dbcopy_op_call({dbcopy_op,From1,wal_read,Data} = Msg,CallFrom,P) ->
	{FromPid,Ref} = From1,
	Size = filelib:file_size([P#dp.dbpath,"-wal"]),
	case Size =< Data of
		true when P#dp.transactionid == undefined ->
			{reply,{[P#dp.dbpath,"-wal"],Size},P#dp{locked = butil:lists_add({FromPid,Ref},P#dp.locked)}};
		true ->
			{noreply,P#dp{callqueue = queue:in_r({CallFrom,Msg},P#dp.callqueue)}};
		false ->
			?DBG("wal_size ~p",[{From1,Data}]), 
			{reply,{[P#dp.dbpath,"-wal"],Size},P}
	end;
dbcopy_op_call({dbcopy_op,_,checksplit,Data},_,P) ->
			{M,F,A} = Data,
			{reply,apply(M,F,[P#dp.cbstate,check|A]),P};
		% wlog_unneeded ->
		% 	?ADBG("Received wlog_unneeded ~p, have it? ~p",[{P#dp.actorname,P#dp.actortype},P#dp.wlog_status]),
		% 	% Data is either ?WLOG_NONE or ?WLOG_ABANDONDED
		% 	case ok of
		% 		_ when (P#dp.wlog_status == ?WLOG_ACTIVE orelse P#dp.wlog_status == ?WLOG_ABANDONDED) 
		% 					andalso P#dp.dbcopy_to == [] ->
		% 			ok = actordb_sqlite:exec(P#dp.db,[<<"$INSERT OR REPLACE INTO __adb VALUES (">>,
		% 							?WLOG_STATUS,$,,butil:tobin(Data),<<");">>,
		% 							<<"$DELETE * FROM __wlog;">>]),
		% 			{reply,ok,P#dp{wlog_status = ?WLOG_NONE, wlog_len = 0}};
		% 		_ ->
		% 			{reply,ok,P}
		% 	end;
		% Copy done ok, release lock.
dbcopy_op_call({dbcopy_op,From1,unlock,Data},CallFrom,P) ->
	% For unlock data = copyref
	case lists:keyfind(Data,2,P#dp.locked) of
		false ->
			?AERR("Unlock attempt on non existing lock ~p ~p, locks ~p",[{P#dp.actorname,P#dp.actortype},Data,P#dp.locked]),
			{reply,false,P};
		{wait_copy,Data,IsMove,Node,_TimeOfLock} ->
			?DBG("Actor unlocked ~p ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.evnum,Data,P#dp.dbcopy_to]),
			DbCopyTo = lists:keydelete(Data,3,P#dp.dbcopy_to),
			case IsMove of
				true ->
					actordb_sqlproc:write_call({undefined,0,{moved,Node},undefined},CallFrom,
										P#dp{locked = lists:keydelete(Data,2,P#dp.locked),dbcopy_to = DbCopyTo});
				_ ->
					self() ! doqueue,
					WithoutLock = [Tuple || Tuple <- P#dp.locked, element(2,Tuple) /= Data],
					
					case IsMove of
						{split,{M,F,A}} ->
							case apply(M,F,[P#dp.cbstate,split|A]) of
								{ok,Sql} ->
									NS = P#dp.cbstate;
								{ok,Sql,NS} ->
									ok
							end,
							WriteMsg = {undefined,erlang:crc32(Sql),Sql,undefined},
							case ok of
								_ when WithoutLock == [], DbCopyTo == [] ->
									actordb_sqlproc:write_call(WriteMsg,CallFrom,P#dp{locked = WithoutLock, 
												dbcopy_to = DbCopyTo,
												cbstate = NS});
								_ ->
									CQ = queue:in_r({CallFrom,{write,WriteMsg}},P#dp.callqueue),
									{noreply,P#dp{locked = WithoutLock,cbstate = NS, dbcopy_to = DbCopyTo,callqueue = CQ}}
							end;
						_ ->
							case ok of
								_ when WithoutLock == [], DbCopyTo == [] ->
									% case P#dp.wlog_status of
									% 	?WLOG_ACTIVE ->
									% 		can_stop_wlog(P);
									% 	_ ->
									% 		ok
									% end,
									{reply,ok,P#dp{locked = WithoutLock, 
												dbcopy_to = DbCopyTo}};
								_ ->
									{reply,ok,P#dp{locked = WithoutLock, dbcopy_to = DbCopyTo}}
							end
					end
			end;
		{_FromPid,Data} ->
			% Check if race condition
			case lists:keyfind(Data,3,P#dp.dbcopy_to) of
				{Node,_PID,Data,IsMove} ->
					dbcopy_op_call({dbcopy_op,From1,unlock,Data},CallFrom,
							P#dp{locked = [{wait_copy,Data,IsMove,Node,os:timestamp()}|P#dp.locked]});
				false ->
					?AERR("dbcopy_to does not contain ref ~p, ~p",[Data,P#dp.dbcopy_to]),
					{reply,false,P}
			end
	end.

dbcopy(P,Home,ActorTo) ->
	{ok,F} = file:open(P#dp.dbpath,[read,binary,raw]),
	dbcopy(P,Home,ActorTo,F,0,db).
dbcopy(P,Home,ActorTo,F,Offset,wal) ->
	still_alive(P,Home,ActorTo),
	case gen_server:call(Home,{dbcopy_op,{self(),P#dp.dbcopyref},wal_read,Offset}) of
		{_Walname,Offset} ->
			?ADBG("dbsend done ",[]),
			% case rpc(P#dp.dbcopy_to,{?MODULE,call_slave,[P#dp.cbmod,ActorTo,P#dp.actortype,{db_chunk,P#dp.dbcopyref,<<>>,done}]}) of
			exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,<<>>,done,original]}));
		{_Walname,Walsize} when Offset > Walsize ->
			?AERR("Offset larger than walsize ~p ~p",[{ActorTo,P#dp.actortype},Offset,Walsize]),
			% case rpc(P#dp.dbcopy_to,{?MODULE,call_slave,[P#dp.cbmod,ActorTo,P#dp.actortype,{db_chunk,P#dp.dbcopyref,<<>>,done}]}) of
			exit(copyfail);
		{Walname,Walsize} ->
			Readnum = min(1024*1024,Walsize-Offset),
			case Offset of
				0 ->
					{ok,F1} = file:open(Walname,[read,binary,raw]);
				_ ->
					F1 = F
			end,
			{ok,Bin} = file:read(F1,Readnum),
			?ADBG("dbsend wal ~p",[{Walname,Walsize}]),
			% ok = rpc(P#dp.dbcopy_to,{?MODULE,call_slave,[P#dp.cbmod,ActorTo,P#dp.actortype,{db_chunk,P#dp.dbcopyref,Bin,wal}]}),
			ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,Bin,wal,original]}),
			dbcopy(P,Home,ActorTo,F1,Offset+Readnum,wal)
	end;
dbcopy(P,Home,ActorTo,F,0,db) ->
	still_alive(P,Home,ActorTo),
	{ok,Bin} = file:read(F,1024*1024),
	% ok = rpc(P#dp.dbcopy_to,{?MODULE,call_slave,[P#dp.cbmod,ActorTo,P#dp.actortype,{db_chunk,P#dp.dbcopyref,Bin,db}]}),
	?ADBG("dbsend ~p ~p",[P#dp.dbcopyref,byte_size(Bin)]),
	ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,Bin,db,original]}),
	case byte_size(Bin) == 1024*1024 of
		true ->
			dbcopy(P,Home,ActorTo,F,0,db);
		false ->
			file:close(F),
			dbcopy(P,Home,ActorTo,undefined,0,wal)
	end.

dbcopy_send(_P,Ref,Bin,Status,Origin) ->
	F = fun(_F,N) when N < 0 ->
			{error,timeout};
			(F,N) ->
		case distreg:whereis({copyproc,Ref}) of
			undefined ->
				timer:sleep(30),
				F(F,N-30);
			Pid ->
				{ok,Pid}
		end
	end,
	{ok,Pid} = F(F,5000),
	MonRef = erlang:monitor(process,Pid),
	Pid ! {Ref,self(),Bin,Status,Origin},
	receive
		{'DOWN',MonRef,_,Pid,Reason} ->
			erlang:demonitor(MonRef),
			{error,Reason};
		{Ref,Pid,Response} ->
			erlang:demonitor(MonRef),
			Response
		after 10000 ->
			erlang:demonitor(MonRef),
			{error,timeout}
	end.


start_copyrec(P) ->
	StartRef = make_ref(),
	Home = self(),
	true = is_reference(P#dp.dbcopyref),
	spawn(fun() ->
		case distreg:reg(self(),{copyproc,P#dp.dbcopyref}) of
			ok ->
				?ADBG("Started copyrec ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.dbcopyref,P#dp.copyfrom]),
				Home ! {StartRef,self()},
				case ok of
					% if copyfrom binary, it's a restore within a cluster.
					% if copyfrom tuple, it's moving/copying from one cluster to another 
					%  or one actor to another.
					_ when P#dp.mors == master, is_tuple(P#dp.copyfrom) ->
						{ConnectedNodes1,LenCluster,LenConnected} = nodes_for_replication(P),
						ConnectedNodes = [bkdcore:name_from_dist_name(Nd) || Nd <- ConnectedNodes1],
						case LenCluster == 0 of
							true ->
								ok;
							_ ->
								true = LenConnected*2 > LenCluster
						end,
						StartOpt = [{actor,P#dp.actorname},{type,P#dp.actortype},{mod,P#dp.cbmod},lock,nohibernate,
													{lockinfo,dbcopy,{P#dp.dbcopyref,P#dp.cbstate,
																	  P#dp.copyfrom,P#dp.copyreset}}],
						[{ok,_} = rpc(Nd,{actordb_sqlproc,start_copylock,
									[{P#dp.actorname,P#dp.actortype},StartOpt]}) || Nd <- ConnectedNodes];
					_ ->
						ConnectedNodes = []
				end,
				dbcopy_receive(P,undefined,undefined,ConnectedNodes);
			name_exists ->
				Home ! {StartRef,distreg:whereis({copyproc,P#dp.dbcopyref})}
		end
	end),
	receive
		{StartRef,Pid1} ->
			{ok,Pid1}
	after 2000 ->
		exit(dbcopy_receive_error)
	end.

dbcopy_receive(P,F,CurStatus,ChildNodes) ->
	receive
		{Ref,Source,Bin,Status,Origin} when Ref == P#dp.dbcopyref ->
			case Origin of
				original ->
					[ok = rpc(Nd,{?MODULE,dbcopy_send,[P,Ref,Bin,Status,master]}) || Nd <- ChildNodes];
				master ->
					ok
			end,
			case CurStatus == Status of
				true when Status == sql ->
					{EvNum,Crc,Sql} = Bin,
					ok = actordb_sqlite:okornot(actordb_sqlite:exec(F,[<<"$SAVEPOINT 'adb';">>,
											 Sql,
											 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
											 <<"$UPDATE __adb SET val='">>,butil:tobin(Crc),<<"' WHERE id=">>,?EVCRC,";",
											 <<"$RELEASE SAVEPOINT 'adb';">>])),
					F1 = F;
				true ->
					ok = file:write(F,Bin),
					F1 = F;
				false when Status == sql ->
					{ok,Db,_SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
					{EvNum,Crc,Sql} = Bin,
					ok = actordb_sqlite:okornot(actordb_sqlite:exec(Db,[<<"$SAVEPOINT 'adb';">>,
						 Sql,
						 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
						 <<"$UPDATE __adb SET val='">>,butil:tobin(Crc),<<"' WHERE id=">>,?EVCRC,";",
						 <<"$RELEASE SAVEPOINT 'adb';">>])),
					F1 = Db;
				false when Status == db ->
					file:delete(P#dp.dbpath++"-wal"),
					file:delete(P#dp.dbpath++"-shm"),
					{ok,F1} = file:open(P#dp.dbpath,[write,raw]),
					ok = file:write(F1,Bin);
				false when Status == wal ->
					ok = file:close(F),
					{ok,F1} = file:open(P#dp.dbpath++"-wal",[write,raw]),
					ok = file:write(F1,Bin);
				false when Status == done ->
					case ok of
						_ when element(1,F) == connection ->
							actordb_sqlite:stop(F);
						_ ->
							file:close(F)
					end,
					F1 = undefined,
					{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
					case SchemaTables of
						[] ->
							?AERR("DB open after move without schema? ~p ~p",[P#dp.actorname,P#dp.actortype]),
							actordb_sqlite:stop(Db),
							actordb_sqlite:move_to_trash(P#dp.dbpath),
							exit(copynoschema);
						_ ->
							?ADBG("Copyreceive done ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},
								 {Origin,P#dp.copyfrom},actordb_sqlite:exec(Db,"SELECT * FROM __adb;")]),
							ok = actordb_sqlite:okornot(actordb_sqlite:exec(Db,
											<<"INSERT OR REPLACE INTO __adb (id,val) VALUES (",?COPYFROM/binary,",
											'",(base64:encode(term_to_binary({P#dp.copyfrom,P#dp.copyreset,
																				P#dp.cbstate})))/binary,"');">>,write)),
							actordb_sqlite:stop(Db),
							Source ! {Ref,self(),ok},
							case Origin of
								original ->
									callback_unlock(P);
								_ ->
									exit(ok)
							end
					end
			end,
			Source ! {Ref,self(),ok},
			dbcopy_receive(P,F1,Status,ChildNodes);
		X ->
			?AERR("dpcopy_receive ~p received invalid msg ~p",[P#dp.dbcopyref,X])
	after 30000 ->
		exit(timeout_db_receive)
	end.

callback_unlock(P) ->
	case P#dp.copyfrom of
		{move,_NewShard,Node} ->
			ActorName = P#dp.actorname;
		{split,_MFA,Node,ActorName} ->
			ok;
		{Node,ActorName} ->
			ok;
		Node when is_binary(Node) ->
			true = Node /= bkdcore:node_name(),
			ActorName = P#dp.actorname
	end,
	% Unlock database on source side
	case rpc(Node,{?MODULE,call_master,[P#dp.cbmod,ActorName,
								P#dp.actortype,{dbcopy_op,undefined,unlock,P#dp.dbcopyref}]}) of
		ok ->
			exit(ok);
		{ok,_} ->
			exit(ok);
		{redirect,Somenode} ->
			case lists:member(Somenode,bkdcore:all_cluster_nodes()) of
				true ->
					exit(ok);
				false ->
					exit({unlock_invalid_redirect,Somenode})
			end;
		{error,Err} ->
			?AERR("Failed to execute dbunlock ~p",[Err]),
			exit(failed_unlock)
	end.

still_alive(P,Home,ActorTo) ->
	case erlang:is_process_alive(Home) of
		true ->
			ok;
		false ->
			receive
				delete ->
					?ADBG("Actor deleted during copy ~p",[Home]),
					ok = rpc(P#dp.dbcopy_to,{?MODULE,call_slave,[P#dp.cbmod,ActorTo,P#dp.actortype,
																	{db_chunk,P#dp.dbcopyref,<<>>,delete}]})
			after 0 ->
				?AERR("dbcopy home proc is dead ~p",[Home]),
				exit(actorprocdead)
			end
	end.

rpc(localhost,{M,F,A}) ->
	apply(M,F,A);
rpc(Nd,MFA) ->
	% Me = self(),
	% F = fun(F) ->
	% 		receive
	% 			done ->
	% 				ok;
	% 			{'DOWN',_MonRef,_,Me,_Reason} ->
	% 				ok
	% 			after 1000 ->
	% 				?AINF("Rpc waiting on ~p ~p",[Nd,MFA]),
	% 				F(F)
	% 		end
	% 	end,
	% Printer = spawn(fun() -> erlang:monitor(process,Me), F(F) end),
	Res = bkdcore:rpc(Nd,MFA),
	% Printer ! done,
	Res.

