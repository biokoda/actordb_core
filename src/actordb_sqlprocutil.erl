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

ae_respond(P,LeaderNode,Success,PrevEvnum,AEType) ->
	Resp = {appendentries_response,actordb_conf:node_name(),P#dp.current_term,Success,P#dp.evnum,P#dp.evterm,PrevEvnum,AEType},
	bkdcore:rpc(LeaderNode,{actordb_sqlproc,call_master,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
									{state_rw,Resp}]}).

reply_maybe(#dp{callfrom = undefined, callres = undefined} = P) ->
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
		% If transaction active or copy/move actor, we can continue operation now because it has been safely replicated.
		true when P#dp.transactioninfo /= undefined; element(1,P#dp.callfrom) == exec ->
			% Now it's time to execute second stage of transaction.
			% This means actually executing the transaction sql, without releasing savepoint.
			case P#dp.transactioninfo /= undefined of
				true ->
					{Sql,EvNumNew,NewVers} = P#dp.transactioninfo,
					{Tid,Updaterid,_} = P#dp.transactionid,
					case Sql of
						<<"delete">> ->
							Res = ok;
						_ ->
							NewSql = [Sql,<<"$DELETE FROM __transactions WHERE tid=">>,(butil:tobin(Tid)),
												<<" AND updater=">>,(butil:tobin(Updaterid)),";"],
							% Execute transaction sql and at the same time delete transaction sql from table.
							% No release savepoint yet. That comes in transaction confirm.
							ComplSql = 
									[<<"$SAVEPOINT 'adb';">>,
									 NewSql,
									 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNumNew),<<"' WHERE id=">>,?EVNUM,";",
									 <<"$UPDATE __adb SET val='">>,butil:tobin(P#dp.current_term),<<"' WHERE id=">>,?EVTERM,";"
									 ],
							VarHeader = term_to_binary({P#dp.current_term,actordb_conf:node_name(),P#dp.evnum,P#dp.evterm}),
							Res = actordb_sqlite:exec(P#dp.db,ComplSql,P#dp.evterm,EvNumNew,VarHeader)
					end;
				false ->
					NewVers = P#dp.schemavers,
					Res = P#dp.callres
			end,
			% We can safely reply result of actual transaction sql. Even though it is not replicated completely yet.
			% Because write to __transactions is safely replicated, transaction is guaranteed to be executed
			%  before next write or read.
			case P#dp.callfrom of
				{exec,From,{move,Node}} ->
					NewActor = P#dp.actorname,
					IsMove = true,
					Msg = {move,actordb_conf:node_name()};
				{exec,From,{split,MFA,Node,OldActor,NewActor}} ->
					IsMove = {split,MFA},
					% Change node name back to this node, so that copy knows where split is from.
					Msg = {split,MFA,actordb_conf:node_name(),OldActor,NewActor};
				_ ->
					IsMove = Msg = Node = NewActor = undefined,
					From = P#dp.callfrom
			end,
			?ADBG("Reply transaction=~p res=~p",[P#dp.transactioninfo,Res]),
			reply(From,Res),
			% If write not ok it should never reach this point anyway.
			case actordb_sqlite:okornot(Res) of
				Something when Something /= ok, P#dp.transactionid /= undefined ->
					Me = self(),
					spawn(fun() -> gen_server:call(Me,{commit,false,P#dp.transactionid}) end);
				_ ->
					ok
			end,
			NP = do_cb_init(P#dp{callfrom = undefined, callres = undefined, schemavers = NewVers,activity = make_ref()}),
			case Msg of
				undefined ->
					NP;
				_ ->
					Ref = make_ref(),
					case actordb:rpc(Node,NewActor,{actordb_sqlproc,call_master,[P#dp.cbmod,NewActor,P#dp.actortype,
										{dbcopy,{start_receive,Msg,Ref}},[{lockinfo,wait}]]}) of
						ok ->
							{reply,_,NP1} = dbcopy_call({send_db,{Node,Ref,IsMove,NewActor}},From,NP),
							NP1;
						_ ->
							% Unable to start copy/move operation. Store it for later.
							NP#dp{copylater = {os:timestamp(),Msg}}
					end
			end;
		true ->
			?ADBG("Reply ok ~p ~p",[{P#dp.actorname,P#dp.actortype},{P#dp.callfrom,P#dp.callres}]),
			reply(P#dp.callfrom,P#dp.callres),
			do_cb_init(P#dp{callfrom = undefined, callres = undefined});
		false ->
			% ?ADBG("Reply NOT FINAL ~p evnum ~p followers ~p",
				% [{P#dp.actorname,P#dp.actortype},P#dp.evnum,[F#flw.next_index || F <- P#dp.follower_indexes]]),
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
			{ok,F} = file:open([P#dp.dbpath,"-wal"],[read,write,binary,raw]),
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
	case file:open(Path,[read,binary,raw]) of
		{ok,F} ->
			{ok,_} = file:position(F,32),
			wal_from(F);
		_ ->
			{0,0}
	end;
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
	?AINF("Try_wal_recover ~p for=~p",[{P#dp.actorname,P#dp.actortype},F#flw.node]),
	{WalEvfrom,_WalTermfrom} = P#dp.wal_from,
	% Compare match_index not next_index because we need to send prev term as well
	case F#flw.match_index >= WalEvfrom of
		% We can recover from wal if terms match
		true ->
			{File,PrevNum,PrevTerm} = open_wal_at(P,F#flw.next_index),
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
		% Too far behind. Send entire db.
		false ->
			Res = false,
			NF = F
	end,
	{Res,store_follower(P,NF),NF}.

open_wal_at(P,Index) ->
	{ok,F} = file:open([P#dp.dbpath,"-wal"],[read,binary,raw]),
	{ok,_} = file:position(F,32),
	open_wal_at(P,Index,F,undefined,undefined).
open_wal_at(P,Index,F,PrevNum,PrevTerm) ->
	case file:read(F,40) of
		{ok,<<_:32,_:32,Evnum:64/big-unsigned,Evterm:64/big-unsigned,_/binary>>} when Index == Evnum ->
			{ok,_} = file:position(F,{cur,-40}),
			case PrevNum == undefined of
				true ->
					{F,Evnum,Evterm};
				false ->
					{F,PrevNum,PrevTerm}
			end;
		{ok,<<_:32,_:32,Evnum:64/big-unsigned,Evterm:64/big-unsigned,_/binary>>} ->
			{ok,_NPos} = file:position(F,{cur,?PAGESIZE}),
			?ADBG("open wal at ~p ~p",[{P#dp.actorname,P#dp.actortype}, _NPos]),
			open_wal_at(P,Index,F,Evnum,Evterm)
	end.


continue_maybe(P,F,AEType) ->
	% Check if follower behind
	?AINF("Continue maybe ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},F#flw.node,{P#dp.evnum,F#flw.next_index}]),
	case P#dp.evnum >= F#flw.next_index of
		true when F#flw.file == undefined ->
			case AEType of
				% If head, then this was successful write from log head. But follower seems to be behind, this is because
				%  there was a new write before this response reached leader.
				head ->
					store_follower(P,F);
				_ ->
					{true,NP,NF} = try_wal_recover(P,F),
					continue_maybe(NP,NF,AEType)
			end;
		true ->
			StartRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
						{state_rw,{appendentries_start,P#dp.current_term,actordb_conf:node_name(),
									F#flw.match_index,F#flw.match_term,recover}}]}),
			case StartRes of
				false ->
					% to be continued in appendentries_response
					file:close(F#flw.file),
					store_follower(P,F#flw{file = undefined, wait_for_response_since = make_ref()});
				ok ->
					% Send wal
					send_wal(P,F),
					store_follower(P,F#flw{wait_for_response_since = make_ref()})
			end;
		% Follower uptodate, close file if open
		false when F#flw.file == undefined ->
			store_follower(P,F);
		false ->
			file:close(F#flw.file),
			store_follower(P,F#flw{file = undefined})
	end.

store_follower(P,NF) ->
	P#dp{follower_indexes = lists:keystore(NF#flw.node,#flw.node,P#dp.follower_indexes,NF)}.


% Read until commit set in header.
send_wal(P,#flw{file = File} = F) ->
	{ok,<<Header:40/binary,Page/binary>>} = file:read(File,40+?PAGESIZE),
	case Header of
		<<_:32,Commit:32,Evnum:64/big-unsigned,_:64/big-unsigned,_/binary>> when Evnum == F#flw.next_index ->
			WalRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
						{state_rw,{appendentries_wal,P#dp.current_term,Header,Page,recover}}]}),
			case WalRes of
				ok when Commit == 0 ->
					send_wal(P,F);
				ok ->
					ok;
				_ ->
					error
			end
	end.


append_wal(P,Header,Bin) ->
	ok = file:write(P#dp.db,[Header,esqlite3:lz4_decompress(Bin,?PAGESIZE)]).

% Go back one entry
rewind_wal(P) ->
	case file:position(P#dp.db,{cur,-(?PAGESIZE+40)}) of
		{ok,_NPos} ->
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

save_term(P) ->
	ok = butil:savetermfile([P#dp.dbpath,"-term"],{P#dp.voted_for,P#dp.current_term}),
	P.


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
			case bkdcore:rpc(Node,{actordb_sqlproc,call_master,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
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
		{split,{M,F,A},Node,ShardFrom,_ShardNew} ->
			case bkdcore:rpc(Node,{actordb_sqlproc,call_master,[P#dp.cbmod,ShardFrom,P#dp.actortype,
										{dbcopy,{checksplit,{M,F,A}}}]}) of
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


do_copy_reset(Copyreset,State) ->
	case Copyreset of
		{Mod,Func,Args} ->
			case apply(Mod,Func,[State|Args]) of
				ok ->
					<<>>;
				ResetSql when is_list(ResetSql); is_binary(ResetSql) ->
					ResetSql
			end;
		ok ->
			<<>>;
		ResetSql when is_list(ResetSql); is_binary(ResetSql) ->
			ResetSql;
		_ ->
			<<>>
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
		[{?SCHEMA_VERS,SchemaVers},{?ATYPE,Type},{?EVNUM,0},{?EVTERM,0}|Moved]],
	[<<"CREATE TABLE __transactions (id INTEGER PRIMARY KEY, tid INTEGER,",
	 	" updater INTEGER, node TEXT,schemavers INTEGER, sql TEXT);",
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
do_cb_init(#dp{cbinit = false} = P) ->
	S = P#dp.cbstate,
	case apply(P#dp.cbmod,cb_init,[S,P#dp.evnum]) of
		{ok,NS} ->
			P#dp{cbstate = NS, cbinit = true};
		{doread,Sql} ->
			case apply(P#dp.cbmod,cb_init,[S,P#dp.evnum,actordb_sqlite:exec(P#dp.db,Sql,read)]) of
				{ok,NS} ->
					P#dp{cbstate = NS,cbinit = true};
				ok ->
					P#dp{cbinit = true}
			end;
		ok ->
			P#dp{cbinit = true}
	end;
do_cb_init(P) ->
	P.


actor_start(P) ->
	actordb_local:actor_started(P#dp.actorname,P#dp.actortype,?PAGESIZE*?DEF_CACHE_PAGES).

start_verify(P,JustStarted) ->
	case ok of
		_ when P#dp.movedtonode /= undefined ->
			P#dp{verified = true};
		% Actor was started with mode slave. It will remain in slave(follower) mode and
		%  not attempt to become candidate for now.
		_ when P#dp.mors == slave, JustStarted ->
			P;
		_ ->
			CurrentTerm = P#dp.current_term+1,
			ok = butil:savetermfile([P#dp.dbpath,"-term"],{actordb_conf:node_name(),CurrentTerm}),
			NP = P#dp{current_term = CurrentTerm, voted_for = actordb_conf:node_name()},
			{Verifypid,_} = spawn_monitor(fun() -> 
							start_election(NP)
								end),
			NP#dp{electionpid = Verifypid, verified = false, activity_now = actor_start(P)}
	end.
% Call RequestVote RPC on cluster nodes. 
% This should be called in an async process and current_term and voted_for should have
%  been set for this election (incremented current_term, voted_for = Me)
start_election(P) ->
	ConnectedNodes = bkdcore:cluster_nodes_connected(),
	ClusterSize = length(bkdcore:cluster_nodes()) + 1,
	Me = actordb_conf:node_name(),
	Msg = {state_rw,{request_vote,Me,P#dp.current_term,P#dp.evnum,P#dp.evterm}},
	{Results,_GetFailed} = rpc:multicall(ConnectedNodes,actordb_sqlproc,call_slave,
			[P#dp.cbmod,P#dp.actorname,P#dp.actortype,Msg,[{flags,P#dp.flags}]]),
	?AINF("Election for ~p, results ~p",[{P#dp.actorname,P#dp.actortype},Results]),
	% Sum votes. Start with 1 (we vote for ourselves)
	case count_votes(Results,1) of
		{outofdate,Node,_NewerTerm} ->
			rpc:call(bkdcore:dist_name(Node),actordb_sqlproc,call_slave,
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


post_election_sql(P,[],undefined,SqlIn,Callfrom) ->
	case iolist_size(SqlIn) of
		0 ->
			case P#dp.evnum of
				0 ->
					{SchemaVers,Schema} = apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,0]),
					NP = P#dp{schemavers = SchemaVers},
					% Set on firt write and not changed after. This is used to prevent a case of an actor
					% getting deleted, but later created new. A server that was offline during delete missed
					% the delete call and relies on actordb_events.
					ActorNum = actordb_util:hash(term_to_binary({P#dp.actorname,P#dp.actortype,os:timestamp(),make_ref()})),
					Sql = [actordb_sqlprocutil:base_schema(SchemaVers,P#dp.actortype),
								 Schema,
							<<"$INSERT OR REPLACE INTO __adb VALUES (">>,?ANUM,",'",butil:tobin(ActorNum),<<"');">>];
				_ ->
					case apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,P#dp.schemavers]) of
						{_,[]} ->
							NP = P,
							Sql = <<>>;
						{SchemaVers,Schema} ->
							NP = P#dp{schemavers = SchemaVers},
							Sql = [Schema,
									<<"UPDATE __adb SET val='">>,(butil:tobin(SchemaVers)),
										<<"' WHERE id=",?SCHEMA_VERS/binary,";">>]
					end
			end,
			{NP,Sql,Callfrom};
		_ ->
			{P,SqlIn,Callfrom}
	end;
post_election_sql(P,[{1,Tid,Updid,Node,SchemaVers,MSql1}],undefined,Sql,Callfrom) ->
	case base64:decode(MSql1) of
		<<"delete">> ->
			Sql = delete;
		Sql1 ->
			Sql = [Sql,Sql1]
	end,
	% Put empty sql in transactioninfo. Since sqls get appended for existing transactions in write_call.
	% This way sql does not get written twice to it.
	ReplSql = {<<>>,P#dp.evnum+1,SchemaVers},
	Transid = {Tid,Updid,Node},
	NP = P#dp{transactioninfo = ReplSql, 
				transactionid = Transid, 
				schemavers = SchemaVers},
	{NP,Sql,Callfrom};
post_election_sql(P,[],Copyfrom,SqlIn,_) ->
	CleanupSql = [<<"DELETE FROM __adb WHERE id=">>,(?COPYFROM),";"],
	case Copyfrom of
		{move,NewShard,MoveTo} ->
			case lists:member(MoveTo,bkdcore:all_cluster_nodes()) of
				% This is node where actor moved to.
				true ->
					Sql1 = CleanupSql,
					Callfrom = undefined,
					MovedToNode = P#dp.movedtonode,
					ok = actordb_shard:reg_actor(NewShard,P#dp.actorname,P#dp.actortype);
				% This is node where actor moved from. Check if data is on the other node. If not start copy again.
				false ->
					Num = actordb_sqlprocutil:read_num(P),
					true = Num /= <<>>,
					case actordb:rpc(MoveTo,P#dp.actorname,
							{actordb_sqlproc,try_actornum,[P#dp.actorname,P#dp.actortype,P#dp.cbmod]}) of
						% Num matches with local num. Actor moved successfully. 
						{_,Num} ->
							MovedToNode = MoveTo,
							Sql1 = delete,
							Callfrom = undefined;
						{error,_} ->
							MovedToNode = P#dp.movedtonode,
							Sql1 = CleanupSql,
							Callfrom = undefined,
							exit(unable_to_verify_actormove);
						% No number - there is nothing on other node. Restart move (continued in reply_maybe).
						{APath,ANum} when APath == ""; ANum == <<>> ->
							Sql1 = "",
							MovedToNode = P#dp.movedtonode,
							Callfrom =  {exec,undefined,{move,MoveTo}};
						% Different num. Do not do anything. 
						{_,_} ->
							MovedToNode = P#dp.movedtonode,
							Sql1 = CleanupSql,
							Callfrom = undefined
					end
			end,
			ResetSql = [];
		{split,Mfa,Node,ActorFrom,ActorTo} ->
			{M,F,A} = Mfa,
			MovedToNode = P#dp.movedtonode,
			case apply(M,F,[P#dp.cbstate,check|A]) of
				% Split returned ok on old actor (unlock call was executed successfully).
				ok when P#dp.actorname == ActorFrom ->
					Sql1 = CleanupSql,
					ResetSql = [],
					Callfrom = undefined;
				% If split done and we are on new actor.
				_ when ActorFrom /= P#dp.actorname ->
					Sql1 = CleanupSql,
					Callfrom = undefined,
					ResetSql = actordb_sqlprocutil:do_copy_reset(P#dp.copyreset,P#dp.cbstate);
				% It was not completed. Do it again.
				_ when P#dp.actorname == ActorFrom ->
					Sql1 = [],
					ResetSql = [],
					Callfrom = {exec,undefined,{split,Mfa,Node,ActorFrom,ActorTo}};
				_ ->
					Sql1 = ResetSql = [],
					Callfrom = undefined,
					exit(wait_for_split)
			end
	end,
	case Sql1 of
		delete ->
			Sql = delete;
		_ ->
			Sql = [SqlIn,Sql1,ResetSql]
	end,
	{P#dp{copyfrom = undefined, copyreset = undefined, movedtonode = MovedToNode},Sql,Callfrom};
post_election_sql(P,Transaction,Copyfrom,Sql,Callfrom) when Transaction /= [], Copyfrom /= undefined ->
	% Combine sqls for transaction and copy.
	case post_election_sql(P,Transaction,undefined,Sql,Callfrom) of
		{NP1,delete,_} ->
			{NP1,delete,Callfrom};
		{NP1,Sql1,_} ->
			post_election_sql(NP1,[],Copyfrom,Sql1,Callfrom)
	end.


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
			file:delete(P#dp.dbpath++"-term"),
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
			file:delete(P#dp.dbpath++"-term"),
			file:delete(P#dp.dbpath++"-shm")
	end.

do_checkpoint(P) ->
	case P#dp.mors of
		master ->
			actordb_sqlite:checkpoint(P#dp.db),
			[rpc:cast(bkdcore:dist_name(F#flw.node),
						actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,checkpoint}])
					 || F <- P#dp.follower_indexes, F#flw.match_index == P#dp.evnum];
		_ ->
			{ok,Db,_SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
			actordb_sqlite:checkpoint(Db),
			actordb_sqlite:stop(Db)
	end.

delete_actor(P) ->
	?AINF("deleting actor ~p ~p ~p",[P#dp.actorname,P#dp.dbcopy_to,P#dp.dbcopyref]),
	case (P#dp.flags band ?FLAG_TEST == 0) andalso P#dp.movedtonode == undefined of
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
	case P#dp.follower_indexes of
		[] ->
			ok;
		_ ->
			{_,_} = rpc:multicall(nodes(),actordb_sqlproc,call_slave,
							[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,{delete,P#dp.movedtonode}},[]])
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
			parse_opts(P#dp{mors = master,masternode = actordb_conf:node_name(),masternodedist = node()},T);
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
% Initialize call on node that is source of copy
dbcopy_call({send_db,{Node,Ref,IsMove,ActornameToCopyto}} = Msg,CallFrom,P) ->
	% Send database to another node.
	% This gets called from that node.
	% File is sent in 1MB chunks. If db is =< 1MB, send the file directly.
	% If file > 1MB, switch to journal mode wal. This will caue writes to go to a WAL file not into the db.
	%  Which means the sqlite file can be read from safely.
	case P#dp.verified of
		true ->
			?ADBG("senddb myname ~p, remotename ~p info ~p, copyto already ~p",
					[{P#dp.actorname,P#dp.actortype},ActornameToCopyto,
											{Node,Ref,IsMove},P#dp.dbcopy_to]),
			Me = self(),
			case lists:keyfind(Ref,3,P#dp.dbcopy_to) of
				false ->
					Db = P#dp.db,
					{Pid,_} = spawn_monitor(fun() -> 
							dbcopy(P#dp{dbcopy_to = Node, dbcopyref = Ref},Me,ActornameToCopyto) end),
					{reply,{ok,Ref},P#dp{db = Db,
										dbcopy_to = [{Node,Pid,Ref,IsMove}|P#dp.dbcopy_to], 
										activity = make_ref()}};
				{_,_Pid,Ref,_} ->
					?DBG("senddb already exists with same ref!"),
					{reply,{ok,Ref},P}
			end;
		_ when P#dp.masternode /= undefined ->
			case P#dp.masternode == actordb_conf:node_name() of
				true ->
					{noreply,P#dp{callqueue = queue:in_r({CallFrom,{dbcopy,Msg}},P#dp.callqueue)}};
				_ ->
					?ADBG("redirect not master node"),
					redirect_master(P)
			end;
		false ->
			{noreply,P#dp{callqueue = queue:in_r({CallFrom,{dbcopy,Msg}},P#dp.callqueue)}}
	end;
% Initial call on node that is destination of copy
dbcopy_call({start_receive,Copyfrom,Ref},_,P) ->
	?ADBG("start receive entire db ~p",[{P#dp.actorname,P#dp.actortype}]),
	% If move, split or copy actor to a new actor this must be called on master.
	case is_tuple(Copyfrom) of
		true when P#dp.mors /= master ->
			redirect_master(P);
		_ ->
			actordb_sqlite:stop(P#dp.db),
			{ok,RecvPid} = start_copyrec(P#dp{copyfrom = Copyfrom, dbcopyref = Ref}),
			erlang:monitor(process,RecvPid),

			{reply,ok,P#dp{db = undefined,dbcopyref = Ref, copyfrom = Copyfrom, copyproc = RecvPid}}
	end;
% Read chunk of wal log.
dbcopy_call({wal_read,From1,Data} = Msg,CallFrom,P) ->
	{FromPid,Ref} = From1,
	Size = filelib:file_size([P#dp.dbpath,"-wal"]),
	case Size =< Data of
		true when P#dp.transactionid == undefined ->
			{reply,{[P#dp.dbpath,"-wal"],Size},P#dp{locked = butil:lists_add({FromPid,Ref},P#dp.locked)}};
		true ->
			{noreply,P#dp{callqueue = queue:in_r({CallFrom,{dbcopy,Msg}},P#dp.callqueue)}};
		false ->
			?DBG("wal_size ~p",[{From1,Data}]), 
			{reply,{[P#dp.dbpath,"-wal"],Size},P}
	end;
dbcopy_call({checksplit,Data},_,P) ->
	{M,F,A} = Data,
	{reply,apply(M,F,[P#dp.cbstate,check|A]),P};
% Final call when copy done
dbcopy_call({unlock,Data},CallFrom,P) ->
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
					actordb_sqlproc:write_call({undefined,{moved,Node},undefined},CallFrom,
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
							WriteMsg = {undefined,[Sql,<<"DELETE FROM __adb WHERE id=">>,(?COPYFROM),";"],undefined},
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
					dbcopy_call({unlock,Data},CallFrom,
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
	case gen_server:call(Home,{dbcopy,{wal_read,{self(),P#dp.dbcopyref},Offset}}) of
		{_Walname,Offset} ->
			?ADBG("dbsend done ",[]),
			exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,<<>>,done,original]}));
		{_Walname,Walsize} when Offset > Walsize ->
			?AERR("Offset larger than walsize ~p ~p",[{ActorTo,P#dp.actortype},Offset,Walsize]),
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
			ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,Bin,wal,original]}),
			dbcopy(P,Home,ActorTo,F1,Offset+Readnum,wal)
	end;
dbcopy(P,Home,ActorTo,F,0,db) ->
	still_alive(P,Home,ActorTo),
	{ok,Bin} = file:read(F,1024*1024),
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
						ConnectedNodes = [bkdcore:name_from_dist_name(Nd) || Nd <- bkdcore:cluster_nodes_connected()],
						case ConnectedNodes of
							[] ->
								ok;
							_ ->
								true = length(ConnectedNodes)*2 > length(bkdcore:cluster_nodes())
						end,
						StartOpt = [{actor,P#dp.actorname},{type,P#dp.actortype},{mod,P#dp.cbmod},lock,nohibernate,{slave,true},
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
				true ->
					ok = file:write(F,Bin),
					F1 = F;
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
		{move,Node} ->
			ActorName = P#dp.actorname;
		{move,_NewShard,Node} ->
			ActorName = P#dp.actorname;
		{split,_MFA,Node,ActorName,_Myname} ->
			ok;
		{Node,ActorName} ->
			ok;
		Node when is_binary(Node) ->
			true = Node /= actordb_conf:node_name(),
			ActorName = P#dp.actorname
	end,
	% Unlock database on source side. Once unlocked move/copy is complete.
	case rpc(Node,{actordb_sqlproc,call_master,[P#dp.cbmod,ActorName,
								P#dp.actortype,{dbcopy,{unlock,P#dp.dbcopyref}}]}) of
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
					ok = rpc(P#dp.dbcopy_to,{actordb_sqlproc,call_slave,[P#dp.cbmod,ActorTo,P#dp.actortype,
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

