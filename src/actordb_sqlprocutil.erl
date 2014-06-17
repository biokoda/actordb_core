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
	bkdcore_rpc:cast(LeaderNode,{actordb_sqlproc,call,[{P#dp.actorname,P#dp.actortype},[],
									{state_rw,Resp},P#dp.cbmod]}).

append_wal(P,Header,Bin) ->
	ok = file:write(P#dp.db,[Header,esqlite3:lz4_decompress(Bin,?PAGESIZE)]).

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
							case ok of
								_ when Tid == 0, Updaterid == 0 ->
									self() ! commit_transaction,
									Res = ok;
								_ ->
									Res = {actordb_conf:node_name(),ok}
							end;
						_ when P#dp.follower_indexes /= [] ->
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
							Res1 = actordb_sqlite:exec(P#dp.db,ComplSql,P#dp.evterm,EvNumNew,VarHeader),
							Res = {actordb_conf:node_name(),Res1},
							case actordb_sqlite:okornot(Res1) of
								Something when Something /= ok, P#dp.transactionid /= undefined ->
									Me = self(),
									spawn(fun() -> gen_server:call(Me,{commit,false,P#dp.transactionid}) end);
								_ ->
									ok
							end;
						_ ->
							Res = {actordb_conf:node_name(),ok}
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
			?DBG("Reply transaction=~p res=~p from=~p",[P#dp.transactioninfo,Res,From]),
			reply(From,Res),
			NP = do_cb(P#dp{callfrom = undefined, callres = undefined, 
									schemavers = NewVers,activity = make_ref()}),
			case Msg of
				undefined ->
					NP;
				_ ->
					Ref = make_ref(),
					case actordb:rpc(Node,NewActor,{actordb_sqlproc,call,[{NewActor,P#dp.actortype},[{lockinfo,wait},lock],
										{dbcopy,{start_receive,Msg,Ref}},P#dp.cbmod]}) of
						ok ->
							{reply,_,NP1} = dbcopy_call({send_db,{Node,Ref,IsMove,NewActor}},From,NP),
							NP1;
						_ ->
							% Unable to start copy/move operation. Store it for later.
							NP#dp{copylater = {os:timestamp(),Msg}}
					end
			end;
		true ->
			?DBG("Reply ok ~p",[{P#dp.callfrom,P#dp.callres}]),
			reply(P#dp.callfrom,P#dp.callres),
			do_cb(P#dp{callfrom = undefined, callres = undefined});
		false ->
			% ?DBG("Reply NOT FINAL evnum ~p followers ~p",
				% [P#dp.evnum,[F#flw.next_index || F <- P#dp.follower_indexes]]),
			P
	end.



reopen_db(#dp{mors = master} = P) ->
	case ok of
		% we are master but db not open or open as file descriptor to -wal file
		_ when element(1,P#dp.db) == file_descriptor; P#dp.db == undefined ->
			file:close(P#dp.db),
			init_opendb(P);
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

init_opendb(P) ->
	{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
	NP = P#dp{db = Db},
	case SchemaTables of
		[_|_] ->
			?DBG("Opening HAVE schema",[]),
			{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(Db,
					<<"SELECT * FROM __adb;">>,read),
			Evnum = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
			Vers = butil:toint(butil:ds_val(?SCHEMA_VERSI,Rows)),
			MovedToNode1 = butil:ds_val(?MOVEDTOI,Rows),
			EvTerm = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),
			set_followers(true,NP#dp{evnum = Evnum, schemavers = Vers,
						wal_from = wal_from([P#dp.dbpath,"-wal"]),
						evterm = EvTerm,
						movedtonode = MovedToNode1});
		[] -> 
			?DBG("Opening NO schema",[]),
			set_followers(false,NP)
	end.

set_followers(HaveSchema,P) ->
	case apply(P#dp.cbmod,cb_nodelist,[P#dp.cbstate,HaveSchema]) of
		{read,Sql} ->
			{ok,NS,NL} = apply(P#dp.cbmod,cb_nodelist,[P#dp.cbstate,HaveSchema,
								actordb_sqlite:exec(P#dp.db,Sql,read)]);
		{ok,NS,NL} ->
			ok
	end,
	P#dp{cbstate = NS,follower_indexes = P#dp.follower_indexes ++ 
			[#flw{node = Nd,match_index = 0,next_index = P#dp.evnum+1} || Nd <- NL, 
					lists:keymember(Nd,#flw.node,P#dp.follower_indexes) == false]}.

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
try_wal_recover(#dp{wal_from = {0,0}} = P,F) ->
	case wal_from([P#dp.dbpath,"-wal"]) of
		{0,0} ->
			{false,store_follower(P,F),F};
		WF ->
			try_wal_recover(P#dp{wal_from = WF},F)
	end;
try_wal_recover(P,F) ->
	?DBG("Try_wal_recover for=~p, myev=~p MatchIndex=~p MyEvFrom=~p",
		[F#flw.node,P#dp.evnum,F#flw.match_index,element(1,P#dp.wal_from)]),
	{WalEvfrom,_WalTermfrom} = P#dp.wal_from,
	% Compare match_index not next_index because we need to send prev term as well
	case F#flw.match_index >= WalEvfrom andalso F#flw.match_index > 0 andalso F#flw.match_index < P#dp.evnum of
		% We can recover from wal if terms match
		true ->
			{File,PrevNum,PrevTerm} = open_wal_at(P,F#flw.next_index),
			case PrevNum == 1 andalso F#flw.match_index == 0 of
				true ->
					Res = true,
					NF = F#flw{file = File};
				_ ->
					case PrevNum == F#flw.match_index andalso PrevTerm == F#flw.match_term of
						true ->
							Res = true,
							NF = F#flw{file = File};
						false ->
							% follower in conflict
							% this will cause a failed appendentries_start and a rewind on follower
							Res = true,
							NF = F#flw{file = File, match_term = PrevTerm, match_index = PrevNum}
					end
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
			?DBG("open wal, atoffset=~p, looking_for_index=~p, went_past=~p",[_NPos,Index,Evnum]),
			open_wal_at(P,Index,F,Evnum,Evterm)
	end.


continue_maybe(P,F,AEType) ->
	% Check if follower behind
	?DBG("Continue maybe ~p {MyEvnum,NextIndex}=~p havefile=~p",
			[F#flw.node,{P#dp.evnum,F#flw.next_index},F#flw.file /= undefined]),
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
			?DBG("Sending AE start on evnum=~p",[F#flw.match_index]),
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
					case send_wal(P,F) of
						wal_corruption ->
							store_follower(P,F#flw{wait_for_response_since = make_ref()});
						_ ->
							?DBG("Sent AE on evnum=~p",[F#flw.match_index]),
							store_follower(P,F#flw{wait_for_response_since = make_ref()})
					end
			end;
		% Follower uptodate, close file if open
		false when F#flw.file == undefined ->
			store_follower(P,F);
		false ->
			file:close(F#flw.file),
			bkdcore_rpc:cast(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,recovered}]}),
			store_follower(P,F#flw{file = undefined})
	end.

store_follower(P,NF) ->
	P#dp{activity = make_ref(),follower_indexes = lists:keystore(NF#flw.node,#flw.node,P#dp.follower_indexes,NF)}.


% Read until commit set in header.
send_wal(P,#flw{file = File} = F) ->
	{ok,<<Header:40/binary,Page/binary>>} = file:read(File,40+?PAGESIZE),
     {HC1,HC2} = esqlite3:wal_checksum(Header,0,0,32),
     {C1,C2} = esqlite3:wal_checksum(Page,HC1,HC2,byte_size(Page)),
	case Header of
		<<_:32,Commit:32,Evnum:64/big-unsigned,_:64/big-unsigned,
		  _:32,_:32,Chk1:32/unsigned-big,Chk2:32/unsigned-big,_/binary>> when Evnum == F#flw.next_index ->
			case ok of
				_ when C1 == Chk1, C2 == Chk2 ->
					{Compressed,CompressedSize} = esqlite3:lz4_compress(Page),
					<<PageCompressed:CompressedSize/binary,_/binary>> = Compressed,
					WalRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
								{state_rw,{appendentries_wal,P#dp.current_term,Header,PageCompressed,recover}}]}),
					case WalRes of
						ok when Commit == 0 ->
							send_wal(P,F);
						ok ->
							ok;
						_ ->
							error
					end;
				_ ->
					?ERR("DETECTED DISK CORRUPTION ON WAL LOG! Stepping down as leader for and truncating log"),
					file:position(File,{cur,-?PAGESIZE-40}),
					file:truncate(File),
					exit(wal_corruption)
			end
	end.


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
	ok = butil:savetermfile([P#dp.dbpath,"-term"],{P#dp.voted_for,P#dp.current_term,P#dp.evnum}),
	P.


% Result: done | abandoned
transaction_done(Id,Uid,Result) ->
	case distreg:whereis({Id,Uid}) of
		undefined ->
			ok;
		Pid ->
			exit(Pid,Result)
	end.

% Check back with multiupdate actor if transaction has been completed, failed or still running.
% Every 100ms.
start_transaction_checker(0,0,<<>>) ->
	{undefined,undefined};
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
	Res = actordb:rpc(Node,Uid,{actordb_multiupdate,transaction_state,[Uid,Id]}),
	?ADBG("transaction_check ~p ~p",[{Id,Uid,Node},Res]),
	case Res of
		% Running
		{ok,0} ->
			timer:sleep(100),
			transaction_checker1(Id,Uid,Node);
		% Done
		{ok,1} ->
			exit(done);
		% Failed
		{ok,-1} ->
			exit(abandoned);
		_ ->
			transaction_checker1(Id,Uid,Node)
	end.


check_redirect(P,Copyfrom) ->
	case Copyfrom of
		{move,NewShard,Node} ->
			case bkdcore:rpc(Node,{actordb_sqlproc,call,[{P#dp.actorname,P#dp.actortype},[],
											{state_rw,donothing},P#dp.cbmod]}) of
				{redirect,SomeNode} ->
					case lists:member(SomeNode,bkdcore:all_cluster_nodes()) of
						true ->
							{true,NewShard};
						false ->
							case bkdcore:cluster_group(Node) == bkdcore:cluster_group(SomeNode) of
								true ->
									?ERR("Still redirects to local, will retry move."),
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
			case bkdcore:rpc(Node,{actordb_sqlproc,call,[{ShardFrom,P#dp.actortype},[],
										{dbcopy,{checksplit,{M,F,A}}},P#dp.cbmod]}) of
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
				{ok,NS} ->
					{ok,NS};
				_ ->
					{ok,State}
			end;
		_ ->
			{ok,State}
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
	[<<"$CREATE TABLE __transactions (id INTEGER PRIMARY KEY, tid INTEGER,",
	 	" updater INTEGER, node TEXT,schemavers INTEGER, sql TEXT);",
	 "$CREATE TABLE __adb (id INTEGER PRIMARY KEY, val TEXT);">>,
	 <<"$INSERT INTO __adb (id,val) VALUES ">>,
	 	butil:iolist_join(DefVals,$,),$;].

do_cb(#dp{cbstate = undefined} = P) ->
	case P#dp.movedtonode of
		undefined ->
			do_cb(P#dp{cbstate = apply(P#dp.cbmod,cb_startstate,[P#dp.actorname,P#dp.actortype])});
		_ ->
			P#dp.cbstate
	end;
do_cb(#dp{cbinit = false} = P) ->
	S = P#dp.cbstate,
	case apply(P#dp.cbmod,cb_init,[S,P#dp.evnum]) of
		{ok,NS} ->
			do_cb(P#dp{cbstate = NS, cbinit = true});
		{doread,Sql} ->
			case apply(P#dp.cbmod,cb_init,[S,P#dp.evnum,actordb_sqlite:exec(P#dp.db,Sql,read)]) of
				{ok,NS} ->
					do_cb(P#dp{cbstate = NS,cbinit = true});
				ok ->
					do_cb(P#dp{cbinit = true})
			end;
		ok ->
			do_cb(P#dp{cbinit = true})
	end;
do_cb(P) ->
	case apply(P#dp.cbmod,cb_write_done,[P#dp.cbstate,P#dp.evnum]) of
		{ok,NS} ->
			P#dp{cbstate = NS};
		ok ->
			P
	end.


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
		_ when is_pid(P#dp.election) ->
			P;
		_ ->
			case JustStarted == force orelse timer:now_diff(os:timestamp(),P#dp.election) > 500000 of
				true ->
					CurrentTerm = P#dp.current_term+1,
					ok = butil:savetermfile([P#dp.dbpath,"-term"],{actordb_conf:node_name(),CurrentTerm}),
					NP = reopen_db(P#dp{current_term = CurrentTerm, voted_for = actordb_conf:node_name(), mors = master, verified = false}),
					{Verifypid,_} = spawn_monitor(fun() -> 
									start_election(NP)
										end),
					NP#dp{election = Verifypid, verified = false, activity = make_ref()};
				false ->
					?DBG("Election just done, ignoring call."),
					P
			end
	end.
follower_nodes(L) ->
	[F#flw.node || F <- L].
% Call RequestVote RPC on cluster nodes. 
% This should be called in an async process and current_term and voted_for should have
%  been set for this election (incremented current_term, voted_for = Me)
start_election(P) ->
	ClusterSize = length(P#dp.follower_indexes) + 1,
	Me = actordb_conf:node_name(),
	Msg = {state_rw,{request_vote,Me,P#dp.current_term,P#dp.evnum,P#dp.evterm}},
	Nodes = follower_nodes(P#dp.follower_indexes),
	?DBG("Election, multicall to ~p",[Nodes]),
	{Results,_GetFailed} = bkdcore_rpc:multicall(Nodes,{actordb_sqlproc,call_slave,
			[P#dp.cbmod,P#dp.actorname,P#dp.actortype,Msg,[{flags,P#dp.flags band (bnot ?FLAG_WAIT_ELECTION)}]]}),
	?DBG("Election, results ~p failed ~p, contacted ~p",[Results,_GetFailed,Nodes]),

	% Sum votes. Start with 1 (we vote for ourselves)
	case count_votes(Results,1) of
		{outofdate,Node,_NewerTerm} ->
			send_doelection(Node,P),
			start_election_done(P,follower);
		NumVotes when is_integer(NumVotes) ->
			case NumVotes*2 > ClusterSize of
				true ->
					start_election_done(P,leader);
				false when (length(Results)+1)*2 =< ClusterSize ->
					% Majority isn't possible anyway.
					start_election_done(P,follower);
				false ->
					% Majority is possible.
					% This election failed. Check if any of the nodes said they will try to get elected.
					case [true || {_What,_Node,_HisLatestTerm,true} <- Results] of
						% If none, sort nodes by name. Pick the one after me and tell him to try to get elected.
						% If he fails, he will pick the node after him. One must succeed because majority is online.
						[] ->
							SortedNodes = lists:sort([actordb_conf:node_name()|[Nd || {_What,Nd,_HisLatestTerm,_} <- Results]]),
							case butil:lists_split_at(actordb_conf:node_name(),SortedNodes) of
								{_,[Next|_]} ->
									send_doelection(Next,P);
								{[Next|_],_} ->
									send_doelection(Next,P)
							end;
						_ ->
							ok
					end,
					start_election_done(P,follower)
			end
	end.
start_election_done(P,leader) ->
	?DBG("Exiting with signal ~p",[leader]),
	case P#dp.flags band ?FLAG_WAIT_ELECTION > 0 of
		true ->
			receive
				exit ->
					exit(leader)
				after 300 ->
					?ERR("Wait election write waited too long."),
					exit(leader)
			end;
		false ->
			exit(leader)
	end;
start_election_done(P,Signal) ->
	?DBG("Exiting with signal ~p",[Signal]),
	exit(Signal).

send_doelection(Node,P) ->
	DoElectionMsg = [P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,doelection}],
	bkdcore_rpc:call(Node,{actordb_sqlproc,call_slave,DoElectionMsg}).
count_votes([{What,Node,HisLatestTerm,_WillHeDoElection}|T],N) ->
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


post_election_sql(P,[],undefined,SqlIn,Callfrom1) ->
	case iolist_size(SqlIn) of
		0 ->
			% If actor is starting with a write, we can incorporate the actual write to post election sql.
			% This is why wait_election flag is added at actordb_sqlproc:write.
			QueueEmpty = queue:is_empty(P#dp.callqueue),
			case Callfrom1 of
				undefined when QueueEmpty == false ->
					case queue:out_r(P#dp.callqueue) of
						{{value,{Callfrom,{write,{undefined,CallWrite,undefined}}}},CQ} ->
							ok;
						_ ->
							CallWrite = <<>>,
							CQ = P#dp.callqueue,
							Callfrom = Callfrom1
					end;
				Callfrom ->
					CallWrite = <<>>,
					CQ = P#dp.callqueue
			end,
			?DBG("Adding write to post election sql ~p",[CallWrite]),
			case P#dp.schemavers of
				undefined ->
					{SchemaVers,Schema} = apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,0]),
					NP = P#dp{schemavers = SchemaVers},
					% Set on firt write and not changed after. This is used to prevent a case of an actor
					% getting deleted, but later created new. A server that was offline during delete missed
					% the delete call and relies on actordb_events.
					ActorNum = actordb_util:hash(term_to_binary({P#dp.actorname,P#dp.actortype,os:timestamp(),make_ref()})),
					Sql = [base_schema(SchemaVers,P#dp.actortype),
								 Schema,CallWrite,
							<<"$INSERT OR REPLACE INTO __adb VALUES (">>,?ANUM,",'",butil:tobin(ActorNum),<<"');">>];
				_ ->
					case apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,P#dp.schemavers]) of
						{_,[]} ->
							NP = P,
							Sql = CallWrite;
						{SchemaVers,Schema} ->
							NP = P#dp{schemavers = SchemaVers},
							Sql = [Schema,CallWrite,
									<<"$UPDATE __adb SET val='">>,(butil:tobin(SchemaVers)),
										<<"' WHERE id=",?SCHEMA_VERS/binary,";">>]
					end
			end,
			{NP#dp{callqueue = CQ},Sql,Callfrom};
		_ ->
			{P,SqlIn,Callfrom1}
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
					?DBG("Calling shard to signal move done"),
					Sql1 = CleanupSql,
					Callfrom = undefined,
					MovedToNode = P#dp.movedtonode,
					ok = actordb_shard:reg_actor(NewShard,P#dp.actorname,P#dp.actortype);
				% This is node where actor moved from. Check if data is on the other node. If not start copy again.
				false ->
					?DBG("Have move data on init, check if moved over to ~p",[MoveTo]),
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
			NS = P#dp.cbstate;
		{split,Mfa,Node,ActorFrom,ActorTo} ->
			?INF("Split done, moved out of ~p",[ActorFrom]),
			{M,F,A} = Mfa,
			MovedToNode = P#dp.movedtonode,
			case apply(M,F,[P#dp.cbstate,check|A]) of
				% Split returned ok on old actor (unlock call was executed successfully).
				ok when P#dp.actorname == ActorFrom ->
					?INF("Split done on new node"),
					Sql1 = CleanupSql,
					NS = P#dp.cbstate,
					Callfrom = undefined;
				% If split done and we are on new actor.
				_ when ActorFrom /= P#dp.actorname ->
					?INF("executing reset on new node after copy"),
					Sql1 = CleanupSql,
					Callfrom = undefined,
					{ok,NS} = do_copy_reset(P#dp.copyreset,P#dp.cbstate);
				% It was not completed. Do it again.
				_ when P#dp.actorname == ActorFrom ->
					?ERR("Check split failed on original actor retry."),
					Sql1 = [],
					NS = P#dp.cbstate,
					Callfrom = {exec,undefined,{split,Mfa,Node,ActorFrom,ActorTo}};
				_ ->
					Sql1 = NS = [],
					Callfrom = undefined,
					exit(wait_for_split)
			end
	end,
	case Sql1 of
		delete ->
			Sql = delete;
		_ ->
			Sql = [SqlIn,Sql1]
	end,
	{P#dp{copyfrom = undefined, copyreset = undefined, movedtonode = MovedToNode, cbstate = NS},Sql,Callfrom};
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
				{sql_error,{"exec_script",sqlite_error,"no such table: __adb"},_} ->
					<<>>
			end
	end.

delactorfile(P) ->
	[Pid ! delete || {_,Pid,_,_} <- P#dp.dbcopy_to],
	?DBG("delfile master=~p, ismoved=~p",[P#dp.mors,P#dp.movedtonode]),
	% Term files are not deleted. This is because of deleted actors. If a node was offline
	%  when an actor was deleted, then the actor was created anew still while offline, 
	%  this will keep the term and evnum number higher than that old file and raft logic will overwrite that data.
	save_term(P),
	case P#dp.movedtonode of
		undefined ->
			file:delete(P#dp.dbpath),
			% file:delete(P#dp.dbpath++"-term"),
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
			% file:delete(P#dp.dbpath++"-term"),
			file:delete(P#dp.dbpath++"-shm")
	end.

do_checkpoint(P) ->
	case P#dp.mors of
		master ->
			actordb_sqlite:checkpoint(P#dp.db),
			[bkdcore_rpc:cast(F#flw.node,
						{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,checkpoint}]})
					 || F <- P#dp.follower_indexes, F#flw.match_index == P#dp.evnum];
		_ ->
			{ok,Db,_SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
			actordb_sqlite:checkpoint(Db),
			actordb_sqlite:stop(Db)
	end.

delete_actor(P) ->
	?DBG("deleting actor ~p ~p ~p",[P#dp.actorname,P#dp.dbcopy_to,P#dp.dbcopyref]),
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
			end;
			% actordb_events:actor_deleted(P#dp.actorname,P#dp.actortype,read_num(P));
		_ ->
			ok
	end,
	case P#dp.follower_indexes of
		[] ->
			ok;
		_ ->
			{_,_} = bkdcore_rpc:multicall(follower_nodes(P#dp.follower_indexes),{actordb_sqlproc,call_slave,
							[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,{delete,P#dp.movedtonode}}]})
	end,
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
	case catch actordb_schema:num() of
		Schema ->
			ok
	end,
	case P#dp.schemanum == Schema orelse P#dp.transactionid /= undefined orelse Sql == delete of
		true ->
			ok;
		false ->
			case apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,P#dp.schemavers]) of
				{_,[]} ->
					ok;
				{NewVers,SchemaUpdate} ->
					?DBG("updating schema ~p ~p",[?R2P(P),SchemaUpdate]),
					{NewVers,iolist_to_binary([SchemaUpdate,
						<<"UPDATE __adb SET val='",(butil:tobin(NewVers))/binary,"' WHERE id=",?SCHEMA_VERS/binary,";">>,Sql])}
			end
	end.


% If called on slave, first check if master nodes is even alive.
% If not stop this process. It will cause actor to get started again and likely master node will
% be set for this node.
redirect_master(P) ->
	case lists:member(P#dp.masternodedist,nodes()) of
		true ->
			{reply,{redirect,P#dp.masternode},P};
		false ->
			case lists:member(P#dp.masternode,bkdcore:all_cluster_nodes()) of
				false ->
					{reply,{redirect,P#dp.masternode},P};
				true ->
					{stop,normal,P}
			end
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
		wait_election ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_WAIT_ELECTION},T);
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
			P#dp{dbpath = DbPath,activity_now = actordb_sqlprocutil:actor_start(P)};
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
			?DBG("senddb myname, remotename ~p info ~p, copyto already ~p",
					[ActornameToCopyto,{Node,Ref,IsMove},P#dp.dbcopy_to]),
			Me = self(),
			case lists:keyfind(Ref,#cpto.ref,P#dp.dbcopy_to) of
				false ->
					Db = P#dp.db,
					{Pid,_} = spawn_monitor(fun() -> 
							dbcopy(P#dp{dbcopy_to = Node, dbcopyref = Ref},Me,ActornameToCopyto) end),
					{reply,{ok,Ref},P#dp{db = Db,
										dbcopy_to = [#cpto{node = Node, pid = Pid, ref = Ref, ismove = IsMove, 
															actorname = ActornameToCopyto}|P#dp.dbcopy_to], 
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
					?DBG("redirect not master node"),
					redirect_master(P)
			end;
		false ->
			{noreply,P#dp{callqueue = queue:in_r({CallFrom,{dbcopy,Msg}},P#dp.callqueue)}}
	end;
% Initial call on node that is destination of copy
dbcopy_call({start_receive,Copyfrom,Ref},_,P) ->
	?DBG("start receive entire db",[]),
	% If move, split or copy actor to a new actor this must be called on master.
	case is_tuple(Copyfrom) of
		true when P#dp.mors /= master ->
			redirect_master(P);
		_ ->
			actordb_sqlite:stop(P#dp.db),
			{ok,RecvPid} = start_copyrec(P#dp{copyfrom = Copyfrom, dbcopyref = Ref}),

			{reply,ok,P#dp{db = undefined,dbcopyref = Ref, copyfrom = Copyfrom, copyproc = RecvPid}}
	end;
% Read chunk of wal log.
dbcopy_call({wal_read,From1,Data} = Msg,CallFrom,P) ->
	{FromPid,Ref} = From1,
	Size = filelib:file_size([P#dp.dbpath,"-wal"]),
	case Size =< Data of
		true when P#dp.transactionid == undefined ->
			{reply,{[P#dp.dbpath,"-wal"],Size,P#dp.evnum,P#dp.current_term},
				P#dp{locked = butil:lists_add(#lck{pid = FromPid,ref = Ref},P#dp.locked)}};
		true ->
			{noreply,P#dp{callqueue = queue:in_r({CallFrom,{dbcopy,Msg}},P#dp.callqueue)}};
		false ->
			?DBG("wal_size ~p",[{From1,Data}]), 
			{reply,{[P#dp.dbpath,"-wal"],Size,P#dp.evnum,P#dp.current_term},P}
	end;
dbcopy_call({checksplit,Data},_,P) ->
	{M,F,A} = Data,
	{reply,apply(M,F,[P#dp.cbstate,check|A]),P};
% Final call when copy done
dbcopy_call({unlock,Data},CallFrom,P) ->
	% For unlock data = copyref
	case lists:keyfind(Data,#lck.ref,P#dp.locked) of
		false ->
			?ERR("Unlock attempt on non existing lock ~p, locks ~p",[Data,P#dp.locked]),
			{reply,false,P};
		% {wait_copy,Data,IsMove,Node,_TimeOfLock} ->
		LC when LC#lck.time /= undefined ->
			?DBG("Actor unlocked ~p ~p ~p",[P#dp.evnum,Data,P#dp.dbcopy_to]),
			DbCopyTo = lists:keydelete(Data,#cpto.ref,P#dp.dbcopy_to),
			WithoutLock = lists:keydelete(Data,#lck.ref,P#dp.locked),
			case LC#lck.ismove of
				true ->
					actordb_sqlproc:write_call({undefined,{moved,LC#lck.node},undefined},CallFrom,
										P#dp{locked = WithoutLock,dbcopy_to = DbCopyTo});
				_ ->
					self() ! doqueue,
					case LC#lck.ismove of
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
									?DBG("Queing write call"),
									CQ = queue:in_r({CallFrom,{write,WriteMsg}},P#dp.callqueue),
									{noreply,P#dp{locked = WithoutLock,cbstate = NS, dbcopy_to = DbCopyTo,callqueue = CQ}}
							end;
						_ ->
							?DBG("Copy done"),
							NP = P#dp{locked = WithoutLock, dbcopy_to = DbCopyTo},
							case LC#lck.actorname == P#dp.actorname of
								true ->
									case lists:keyfind(LC#lck.node,#flw.node,P#dp.follower_indexes) of
										% Flw when is_tuple(Flw), WithoutLock == [], DbCopyTo == [] ->
										% 	{reply,ok,start_verify(reply_maybe(store_follower(NP,
										% 				Flw#flw{match_index = P#dp.evnum, next_index = P#dp.evnum+1})),force)};
										Flw when is_tuple(Flw) ->
											CQ = queue:in_r({CallFrom,{write,{undefined,<<>>,undefined}}},P#dp.callqueue),
											{reply,ok,reply_maybe(store_follower(NP#dp{callqueue = CQ},
														Flw#flw{match_index = P#dp.evnum, match_term = P#dp.current_term, next_index = P#dp.evnum+1}))};
										_ ->
											{reply,ok,NP}
									end;
								false ->
									{reply,ok,NP}
							end
					end
			end;
		LC ->
			% Check if race condition
			case lists:keyfind(Data,#cpto.ref,P#dp.dbcopy_to) of
				false ->
					?ERR("dbcopy_to does not contain ref ~p, ~p",[Data,P#dp.dbcopy_to]),
					{reply,false,P};
				Cpto ->
					NLC = LC#lck{actorname = Cpto#cpto.actorname, node = Cpto#cpto.node, 
									ismove = Cpto#cpto.ismove, time = os:timestamp()},
					dbcopy_call({unlock,Data},CallFrom,
							P#dp{locked = lists:keystore(LC#lck.ref,#lck.ref,P#dp.locked,NLC)})
			end
	end.

dbcopy(P,Home,ActorTo) ->
	{ok,F} = file:open(P#dp.dbpath,[read,binary,raw]),
	dbcopy(P,Home,ActorTo,F,0,db).
dbcopy(P,Home,ActorTo,F,Offset,wal) ->
	still_alive(P,Home,ActorTo),
	case gen_server:call(Home,{dbcopy,{wal_read,{self(),P#dp.dbcopyref},Offset}}) of
		{_Walname,Offset,Evnum,Evterm} ->
			?DBG("dbsend done ",[]),
			Param = [{bin,<<>>},{status,done},{origin,original},{evnum,Evnum},{evterm,Evterm}],
			exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}));
		{_Walname,Walsize,_,_} when Offset > Walsize ->
			?ERR("Offset larger than walsize ~p ~p",[{ActorTo,P#dp.actortype},Offset,Walsize]),
			exit(copyfail);
		{Walname,Walsize,Evnum,Evterm} ->
			Readnum = min(1024*1024,Walsize-Offset),
			case Offset of
				0 ->
					{ok,F1} = file:open(Walname,[read,binary,raw]);
				_ ->
					F1 = F
			end,
			{ok,Bin} = file:read(F1,Readnum),
			?DBG("dbsend wal ~p",[{Walname,Walsize}]),
			Param = [{bin,Bin},{status,wal},{origin,original},{evnum,Evnum},{evterm,Evterm}],
			ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}),
			dbcopy(P,Home,ActorTo,F1,Offset+Readnum,wal)
	end;
dbcopy(P,Home,ActorTo,F,0,db) ->
	still_alive(P,Home,ActorTo),
	{ok,Bin} = file:read(F,1024*1024),
	?DBG("dbsend ~p ~p",[P#dp.dbcopyref,byte_size(Bin)]),
	Param = [{bin,Bin},{status,db},{origin,original}],
	ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}),
	case byte_size(Bin) == 1024*1024 of
		true ->
			dbcopy(P,Home,ActorTo,F,0,db);
		false ->
			file:close(F),
			dbcopy(P,Home,ActorTo,undefined,0,wal)
	end.

dbcopy_send(Ref,Param) ->
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
	Pid ! {Ref,self(),Param},
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
	spawn_monitor(fun() ->
		case distreg:reg(self(),{copyproc,P#dp.dbcopyref}) of
			ok ->
				?DBG("Started copyrec copyref=~p copyfrom=~p",[P#dp.dbcopyref,P#dp.copyfrom]),
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
								case (length(ConnectedNodes)+1)*2 > (length(bkdcore:cluster_nodes())+1) of
									true ->
										true;
									false ->
										exit(nomajority)
								end
						end,
						StartOpt = [{actor,P#dp.actorname},{type,P#dp.actortype},{mod,P#dp.cbmod},lock,nohibernate,{slave,true},
													{lockinfo,dbcopy,{P#dp.dbcopyref,P#dp.cbstate,
																	  P#dp.copyfrom,P#dp.copyreset}}],
						[{ok,_} = rpc(Nd,{actordb_sqlproc,start_copylock,
									[{P#dp.actorname,P#dp.actortype},StartOpt]}) || Nd <- ConnectedNodes];
					_ ->
						ConnectedNodes = []
				end,
				dbcopy_receive(Home,P,undefined,undefined,ConnectedNodes);
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

dbcopy_receive(Home,P,F,CurStatus,ChildNodes) ->
	receive
		{Ref,Source,Param} when Ref == P#dp.dbcopyref ->
			[Bin,Status,Origin,Evnum,Evterm] = butil:ds_vals([bin,status,origin,evnum,evterm],Param),
			case Origin of
				original ->
					Param1 = [{bin,Bin},{status,Status},{origin,master},	{evnum,Evnum},{evterm,Evterm}],
					[ok = rpc(Nd,{?MODULE,dbcopy_send,[Ref,Param1]}) || Nd <- ChildNodes];
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
					case is_integer(Evnum) of
						true ->
							ok = butil:savetermfile([P#dp.dbpath,"-term"],{undefined,Evterm,Evnum});
						false ->
							ok
					end,
					case SchemaTables of
						[] ->
							?ERR("DB open after move without schema?",[]),
							actordb_sqlite:stop(Db),
							actordb_sqlite:move_to_trash(P#dp.dbpath),
							exit(copynoschema);
						_ ->
							?DBG("Copyreceive done ~p ~p",[
								 {Origin,P#dp.copyfrom},actordb_sqlite:exec(Db,"SELECT * FROM __adb;")]),
							actordb_sqlite:stop(Db),
							Source ! {Ref,self(),ok},
							case Origin of
								original ->
									% callback_unlock(Home,Evnum,Evterm,P);
									exit(unlock);
								_ ->
									exit(ok)
							end
					end
			end,
			Source ! {Ref,self(),ok},
			dbcopy_receive(Home,P,F1,Status,ChildNodes);
		X ->
			?ERR("dpcopy_receive ~p received invalid msg ~p",[P#dp.dbcopyref,X])
	after 30000 ->
		exit(timeout_db_receive)
	end.

callback_unlock(P) ->
	?DBG("Callback unlock ~p",[P#dp.copyfrom]),
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
	case rpc(Node,{actordb_sqlproc,call,[{ActorName,P#dp.actortype},[],{dbcopy,{unlock,P#dp.dbcopyref}},P#dp.cbmod,onlylocal]}) of
		ok ->
			ok;
		{ok,_} ->
			ok;
		{redirect,Somenode} ->
			case lists:member(Somenode,bkdcore:all_cluster_nodes()) of
				true ->
					ok;
				false ->
					{unlock_invalid_redirect,Somenode}
			end;
		{error,Err} ->
			?ERR("Failed to execute dbunlock ~p",[Err]),
			failed_unlock
	end.

still_alive(P,Home,ActorTo) ->
	case erlang:is_process_alive(Home) of
		true ->
			ok;
		false ->
			receive
				delete ->
					?DBG("Actor deleted during copy ~p",[Home]),
					ok = rpc(P#dp.dbcopy_to,{actordb_sqlproc,call_slave,[P#dp.cbmod,ActorTo,P#dp.actortype,
																	{db_chunk,P#dp.dbcopyref,<<>>,delete}]})
			after 0 ->
				?ERR("dbcopy home proc is dead ~p",[Home]),
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
	% 				?INF("Rpc waiting on ~p ~p",[Nd,MFA]),
	% 				F(F)
	% 		end
	% 	end,
	% Printer = spawn(fun() -> erlang:monitor(process,Me), F(F) end),
	Res = bkdcore:rpc(Nd,MFA),
	% Printer ! done,
	Res.

