% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_sqlprocutil).
-compile(export_all).
-include_lib("actordb_sqlproc.hrl").


static_sqls() ->
	% Prepared statements which will be referenced by their index in the tuple.
	% Used for common queries and they only get parsed once per actor.
	{
	% #s00;
	"SAVEPOINT 'adb';",
	% #s01;
	"RELEASE SAVEPOINT 'adb';",
	% #s02;
	"INSERT OR REPLACE INTO __adb (id,val) VALUES (?1,?2);",
	% #s03;
	"INSERT OR REPLACE INTO __transactions (id,tid,updater,node,schemavers,sql) VALUES (1,?1,?2,?3,?4,?5);",
	% #s04;
	"DELETE FROM __transactions WHERE tid=?1 AND updater=?2;",
	% #s05;
	"INSERT OR REPLACE INTO actors VALUES (?1,?2);",
	% #d06;
	"SELECT * FROM transactions WHERE id=?1;",
	% #d07; -> d means it returns result
	"INSERT INTO transactions (commited) VALUES (0);",
	% #s08;
	"UPDATE transactions SET commited=1 WHERE id=?1 AND (commited=0 OR commited=1);",
	% #s09;
	"INSERT OR REPLACE INTO terms VALUES (?1,?2,?3,?4,?5,?6);",
	% #d10;
	"SELECT * FROM terms WHERE actor=?1 AND type=?2;"
	}.

reply(undefined,_Msg) ->
	ok;
reply([_|_] = From,Msg) ->
	[gen_server:reply(F,Msg) || F <- From];
reply(From,Msg) ->
	gen_server:reply(From,Msg).

ae_respond(P,undefined,_Success,_PrevEvnum,_AEType,_CallCount) ->
	?ERR("Unable to respond for AE because leader is gone"),
	ok;
ae_respond(P,LeaderNode,Success,PrevEvnum,AEType,CallCount) ->
	Resp = {appendentries_response,actordb_conf:node_name(),P#dp.current_term,Success,P#dp.evnum,P#dp.evterm,PrevEvnum,AEType,CallCount},
	bkdcore_rpc:cast(LeaderNode,{actordb_sqlproc,call,[{P#dp.actorname,P#dp.actortype},[nostart],
									{state_rw,Resp},P#dp.cbmod]}).

append_wal(P,Header,Bin) ->
	actordb_driver:inject_page(P#dp.db,Bin,Header).

reply_maybe(#dp{callfrom = undefined, callres = undefined} = P) ->
	doqueue(P);
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
	% case N == length(P#dp.follower_indexes)+1 of
		% If transaction active or copy/move actor, we can continue operation now because it has been safely replicated.
		true when P#dp.transactioninfo /= undefined; element(1,P#dp.callfrom) == exec ->
			Me = self(),
			% Now it's time to execute second stage of transaction.
			% This means actually executing the transaction sql, without releasing savepoint.
			case P#dp.transactioninfo /= undefined of
				true ->
					{Sql,EvNumNew,NewVers} = P#dp.transactioninfo,
					{Tid,Updaterid,_} = P#dp.transactionid,
					% case Sql of
					% 	<<"delete">> ->
					% 		case ok of
					% 			_ when Tid == 0, Updaterid == 0 ->
					% 				self() ! commit_transaction,
					% 				Res = ok;
					% 			_ ->
					% 				Res = {actordb_conf:node_name(),ok}
					% 		end;
					case ok of
						_ when P#dp.follower_indexes /= [] ->
							case Sql of
								<<"delete">> ->
									Sql1 = [],
									AdbRecs = [?MOVEDTOI,<<"$deleted$">>];
								_ ->
									Sql1 = Sql,
									AdbRecs = []
							end,
							NewSql = [Sql1,<<"$DELETE FROM __transactions WHERE tid=">>,(butil:tobin(Tid)),
												<<" AND updater=">>,(butil:tobin(Updaterid)),";"],
							% Execute transaction sql and at the same time delete transaction sql from table.
							% No release savepoint yet. That comes in transaction confirm.
							ComplSql =
									[%<<"$SAVEPOINT 'adb';">>,
									<<"#s00;">>,
									 NewSql,
									 % <<"$UPDATE __adb SET val='">>,butil:tobin(EvNumNew),<<"' WHERE id=">>,?EVNUM,";",
									 % <<"$UPDATE __adb SET val='">>,butil:tobin(P#dp.current_term),<<"' WHERE id=">>,?EVTERM,";"
									 <<"#s02;">>
									 ],
							VarHeader = create_var_header(P),
							Records = [[[?EVNUMI,butil:tobin(EvNumNew)],[?EVTERMI,butil:tobin(P#dp.current_term)]]++AdbRecs],
							Res1 = actordb_sqlite:exec(P#dp.db,ComplSql,Records,P#dp.evterm,EvNumNew,VarHeader),
							Res = {actordb_conf:node_name(),Res1},
							case actordb_sqlite:okornot(Res1) of
								Something when Something /= ok, P#dp.transactionid /= undefined ->
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
			case P#dp.movedtonode of
				deleted ->
					actordb_sqlprocutil:delete_actor(P),
					spawn(fun() -> actordb_sqlproc:stop(Me) end);
				_ ->
					ok
			end,
			reply(From,Res),
			NP = doqueue(do_cb(P#dp{callfrom = undefined, callres = undefined,
									schemavers = NewVers,activity = make_ref()})),
			case Msg of
				undefined ->
					checkpoint(NP);
				_ ->
					Ref = make_ref(),
					case actordb:rpc(Node,NewActor,{actordb_sqlproc,call,[{NewActor,P#dp.actortype},[{lockinfo,wait},lock],
										{dbcopy,{start_receive,Msg,Ref}},P#dp.cbmod]}) of
						ok ->
							{reply,_,NP1} = dbcopy_call({send_db,{Node,Ref,IsMove,NewActor}},From,NP),
							NP1;
						_ ->
							erlang:send_after(3000,self(),retry_copy),
							% Unable to start copy/move operation. Store it for later.
							NP#dp{copylater = {os:timestamp(),Msg}}
					end
			end;
		true ->
			?DBG("Reply ok ~p",[{P#dp.callfrom,P#dp.callres}]),
			case P#dp.movedtonode of
				deleted ->
					Me = self(),
					actordb_sqlprocutil:delete_actor(P),
					spawn(fun() -> actordb_sqlproc:stop(Me) end);
				_ ->
					ok
			end,
			reply(P#dp.callfrom,P#dp.callres),
			doqueue(checkpoint(do_cb(P#dp{callfrom = undefined, callres = undefined})));
		false ->
			% ?DBG("Reply NOT FINAL evnum ~p followers ~p",
				% [P#dp.evnum,[F#flw.next_index || F <- P#dp.follower_indexes]]),
			P
	end.

% We are sending prevevnum and prevevterm, #dp.evterm is less than #dp.current_term only
%  when election is won and this is first write after it.
create_var_header(P) ->
	term_to_binary({P#dp.current_term,actordb_conf:node_name(),P#dp.evnum,P#dp.evterm,follower_call_counts(P)}).
create_var_header_with_db(P) ->
	% case filelib:file_size(P#dp.fullpath) of
	% 	?PAGESIZE ->
	% 		{ok,Dbfile} = prim_file:read_file(P#dp.fullpath),
	% 		{Compressed,CompressedSize} = actordb_sqlite:lz4_compress(Dbfile),
	% 		<<DbCompressed:CompressedSize/binary,_/binary>> = Compressed,
	% 		term_to_binary({P#dp.current_term,actordb_conf:node_name(),
	% 								P#dp.evnum,P#dp.evterm,follower_call_counts(P),DbCompressed});
	% 	Size ->
			% ?ERR("DB not pagesize, can not replicate the base db ~p",[Size]),
			create_var_header(P).
	% end.

follower_call_counts(P) ->
	[{F#flw.node,{F#flw.match_index,F#flw.match_term}} || F <- P#dp.follower_indexes].


send_empty_ae(P,<<_/binary>> = Nm) ->
	send_empty_ae(P,lists:keyfind(Nm,#flw.node,P#dp.follower_indexes));
send_empty_ae(P,F) ->
	?DBG("sending empty ae to ~p",[F#flw.node]),
	bkdcore_rpc:cast(F#flw.node,
			{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
			 {state_rw,{appendentries_start,P#dp.current_term,actordb_conf:node_name(),
			 F#flw.match_index,F#flw.match_term,empty,{F#flw.match_index,F#flw.match_term}}}]}),
	F#flw{wait_for_response_since = os:timestamp()}.

reopen_db(#dp{mors = master} = P) ->
	case ok of
		_ when P#dp.db == undefined  ->
			init_opendb(P);
		_ ->
			case actordb_sqlite:exec(P#dp.db,<<"SeLECT * FROM __adb;">>,read) of
				{ok,[{columns,_},{rows,[_|_] = Rows}]} ->
					read_db_state(P,Rows);
				_XX ->
					?DBG("Read __adb result ~p",[_XX]),
					P
			end
	end;
reopen_db(P) ->
	case ok of
		_ when P#dp.db == undefined  ->
			NP = init_opendb(P),
			actordb_sqlite:replicate_opts(NP#dp.db,<<>>),
			NP;
		_ ->
			actordb_sqlite:replicate_opts(P#dp.db,<<>>),
			P
	end.

init_opendb(P) ->
	{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
	NP = P#dp{db = Db},
	case SchemaTables of
		[_|_] ->
			?DBG("Opening HAVE schema",[]),
			read_db_state(NP);
		[] ->
			?DBG("Opening NO schema",[]),
			set_followers(false,NP)
	end.

read_db_state(P) ->
	{ok,[{columns,_},{rows,[_|_] = Rows}]} = actordb_sqlite:exec(P#dp.db,
			<<"sELECT * FROM __adb;">>,read),
	read_db_state(P,Rows).
read_db_state(P,Rows) ->
	?DBG("Adb rows ~p",[Rows]),
	Evnum = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
	Vers = butil:toint(butil:ds_val(?SCHEMA_VERSI,Rows)),
	MovedToNode1 = butil:ds_val(?MOVEDTOI,Rows),
	case MovedToNode1 of
		<<"$deleted$">> ->
			delete_actor(P),
			MovedToNode = deleted;
		<<>> ->
			MovedToNode = undefined;
		MovedToNode ->
			ok
	end,
	% ?DBG("Opening with moved=~p",[MovedToNode]),
	EvTerm = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),
	% BaseVers = butil:toint(butil:ds_val(?BASE_SCHEMA_VERSI,Rows,0)),
	set_followers(true,P#dp{evnum = Evnum, schemavers = Vers,
				evterm = EvTerm,
				movedtonode = MovedToNode}).

actually_delete(P) ->
	% -> This function should no longer get called. With updated driver actor is actually safely deleted
	%  on first call.
	ok = actordb_driver:wal_rewind(P#dp.db,0),
	[].
	% {ok,[{columns,_},{rows,Tables}]} = actordb_sqlite:exec(P#dp.db,<<"SELECT NAME FROM sqlite_master WHERE type='table';">>,read),
	% Drops = [<<"$DROP TABLE ",Name/binary,";">> || {Name} <- Tables, Name /= <<"__adb">> andalso Name /= <<"sqlite_sequence">>],
	% ?DBG("Drop tables in deleted=~p",[Drops]),
	% #write{sql = ["$INSERT OR REPLACE INTO __adb (id,val) VALUES (",?MOVEDTO,",'');",
	% 			  "$INSERT OR REPLACE INTO __adb (id,val) VALUES (",?BASE_SCHEMA_VERS,",0);",Drops], records = []}.

set_followers(HaveSchema,P) ->
	case apply(P#dp.cbmod,cb_nodelist,[P#dp.cbstate,HaveSchema]) of
		{read,Sql} ->
			{ok,NS,NL} = apply(P#dp.cbmod,cb_nodelist,[P#dp.cbstate,HaveSchema,
								actordb_sqlite:exec(P#dp.db,Sql,read)]);
		{ok,NS,NL} ->
			ok
	end,
	P#dp{cbstate = NS,follower_indexes = P#dp.follower_indexes ++
			[#flw{node = Nd,distname = bkdcore:dist_name(Nd),match_index = 0,next_index = P#dp.evnum+1} || Nd <- NL,
					lists:keymember(Nd,#flw.node,P#dp.follower_indexes) == false]}.

try_wal_recover(P,F) when F#flw.file /= undefined ->
	case F#flw.file of
		{iter,_} ->
			actordb_driver:iterate_close(F#flw.file);
		_ ->
			ok
	end,
	try_wal_recover(P,F#flw{file = undefined});
try_wal_recover(P,#flw{match_index = 0, match_term = 0} = F) ->
	{false,P,F};
try_wal_recover(P,F) ->
	% case F#flw.match_index of
	% 	0 ->
	% 		Evterm = 1,
	% 		Evnum = 1;
	% 	Evnum ->
	% 		Evterm = F#flw.match_term
	% end,
	case actordb_driver:iterate_db(P#dp.db,F#flw.match_index,F#flw.match_term) of
		{ok,Iter2,Bin,Head,Done} ->
			% , match_term = Evterm, match_index = Evnum
			NF = F#flw{file = Iter2, pagebuf = {Bin,Head,Done}},
			{true,store_follower(P,NF),NF};
		% Term conflict. We found evnum, but term is different. Store this term for follower and send it.
		% Follower will reject and rewind and they will move one write back.
		{ok,Term} ->
			NF = F#flw{match_term = Term, file = 1},
			{false,store_follower(P,NF),NF};
		done ->
			{false,P,F}
	end.

continue_maybe(P,F,true) ->
	store_follower(P,F);
continue_maybe(P,F,SuccessHead) ->
	% Check if follower behind
	?DBG("Continue maybe ~p {MyEvnum,NextIndex}=~p havefile=~p",
			[F#flw.node,{P#dp.evnum,F#flw.next_index},F#flw.file /= undefined]),
	case P#dp.evnum >= F#flw.next_index of
		true when F#flw.file == undefined ->
			case try_wal_recover(P,F) of
				{true,NP,NF} ->
					continue_maybe(NP,NF,SuccessHead);
				{false,NP,_} ->
					?DBG("sending entire db to ~p",[F#flw.node]),
					Ref = make_ref(),
					case bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,
										[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
										{dbcopy,{start_receive,actordb_conf:node_name(),Ref}}]}) of
						ok ->
							{reply,_,NP1} = dbcopy_call({send_db,{F#flw.node,Ref,false,P#dp.actorname}},undefined,NP),
							NP1;
						_Err ->
							?ERR("Error sending db ~p",[_Err]),
							NP
					end
			end;
		true ->
			?DBG("Sending AE start on evnum=~p, matchindex=~p, matchterm=~p",[F#flw.next_index,F#flw.match_index,F#flw.match_term]),
			StartRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
				{state_rw,{appendentries_start,P#dp.current_term,actordb_conf:node_name(),
							F#flw.match_index,F#flw.match_term,recover,{F#flw.match_index,F#flw.match_term}}}]}),
			case StartRes of
				ok when F#flw.file /= undefined ->
					% Send wal
					SendResp = (catch send_wal(P,F)),
					case F#flw.file of
						{iter,_} ->
							actordb_driver:iterate_close(F#flw.file);
						_ ->
							ok
					end,
					case SendResp of
						wal_corruption ->
							store_follower(P,F#flw{file = undefined, wait_for_response_since = os:timestamp()});
						error ->
							?ERR("Error iterating wal"),
							store_follower(P,F#flw{file = undefined, wait_for_response_since = os:timestamp()});
						NF when element(1,NF) == flw ->
							?DBG("Sent AE on evnum=~p",[F#flw.next_index]),
							bkdcore_rpc:cast(NF#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,recovered}]}),
							store_follower(P,NF#flw{file = undefined, wait_for_response_since = os:timestamp()})
					end;
				% to be continued in appendentries_response
				_ ->
					case F#flw.file of
						{iter,_} ->
							actordb_driver:iterate_close(F#flw.file);
						_ ->
							ok
					end,
					store_follower(P,F#flw{file = undefined, pagebuf = <<>>, wait_for_response_since = os:timestamp()})
			end;
		% Follower uptodate, close file if open
		false when F#flw.file == undefined ->
			store_follower(P,F);
		false ->
			case F#flw.file of
				{iter,_} ->
					actordb_driver:iterate_close(F#flw.file);
				_ ->
					ok
			end,
			bkdcore_rpc:cast(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,recovered}]}),
			store_follower(P,F#flw{file = undefined, pagebuf = <<>>})
	end.

store_follower(P,#flw{distname = undefined} = F) ->
	store_follower(P,F#flw{distname = bkdcore:dist_name(F#flw.node)});
store_follower(P,NF) ->
	P#dp{activity = make_ref(),follower_indexes = lists:keystore(NF#flw.node,#flw.node,P#dp.follower_indexes,NF)}.


% Read until commit set in header.
send_wal(P,#flw{file = {iter,_}} = F) ->
	case F#flw.pagebuf of
		<<>> ->
			case actordb_driver:iterate_db(P#dp.db,F#flw.file) of
				% {ok,Iter,<<Header:144/binary,Page/binary>>,_IsLastWal} ->
				{ok,Iter,PageCompressed,Header,Commit} ->
					ok;
				done ->
					?ERR("Detected disk corruption while trying to replicate to stale follower. Stepping down as leader."),
					throw(wal_corruption),
					Commit = Iter = Header = PageCompressed = undefined
			end;
		{PageCompressed,Header,Commit} ->
			Iter = F#flw.file
	end,
	<<ET:64,EN:64,Pgno:32,_:32>> = Header,
	?DBG("send_wal et=~p, en=~p, pgno=~p, commit=~p",[ET,EN,Pgno,Commit]),
	WalRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
				{state_rw,{appendentries_wal,P#dp.current_term,Header,PageCompressed,recover,{F#flw.match_index,F#flw.match_term}}},
				[nostart]]}),
	case WalRes == ok orelse WalRes == done of
		true when Commit == 0 ->
			send_wal(P,F#flw{file = Iter, pagebuf = <<>>});
		true ->
			F#flw{file = Iter, pagebuf = <<>>};
		_ ->
			error
	end.

% Go back one entry
rewind_wal(P) ->
	actordb_driver:wal_rewind(P#dp.db,P#dp.evnum),
	% case actordb_driver:wal_rewind(P#dp.db,P#dp.evnum) of
	% 	{ok,0} ->
	% 		P;
	% 	{ok,_} ->
	Sql = <<"SElECT * FROM __adb where id in (",(?EVNUM)/binary,",",(?EVTERM)/binary,");">>,
	{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(P#dp.db, Sql,read),
	Evnum = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
	EvTerm = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),
	P#dp{evnum = Evnum, evterm = EvTerm}.
	% end.

save_term(P) ->
	store_term(P,P#dp.voted_for,P#dp.current_term,P#dp.evnum,P#dp.evterm),
	P.
store_term(P, undefined, CurrentTerm, _EN, _ET) ->
	store_term(P, <<>>, CurrentTerm, _EN, _ET);
store_term(#dp{db = undefined} = P, VotedFor, CurrentTerm, _EN, _ET) ->
	ok = actordb_driver:term_store(P#dp.dbpath, CurrentTerm, VotedFor,actordb_util:hash(P#dp.dbpath));
store_term(P,VotedFor,CurrentTerm,_Evnum,_EvTerm) ->
	ok = actordb_driver:term_store(P#dp.db, CurrentTerm, VotedFor).

doqueue(P) when P#dp.callres == undefined, P#dp.verified /= false, P#dp.transactionid == undefined, P#dp.locked == [] ->
	case queue:is_empty(P#dp.callqueue) of
		true ->
			% ?INF("Queue empty"),
			case apply(P#dp.cbmod,cb_idle,[P#dp.cbstate]) of
				{ok,NS} ->
					P#dp{cbstate = NS};
				_ ->
					P
			end;
		false ->
			{{value,Call},CQ} = queue:out_r(P#dp.callqueue),
			{From,Msg} = Call,
			case actordb_sqlproc:handle_call(Msg,From,P#dp{callqueue = CQ}) of
				{reply,Res,NP} ->
					reply(From,Res),
					doqueue(NP);
				{stop,_,NP} ->
					?DBG("Doqueue for call stop ~p",[Msg]),
					self() ! stop,
					NP;
				% If call returns noreply, it will continue processing later.
				{noreply,NP} ->
					% We may have just inserted the same call back in the queue. If we did, it
					%  is placed in the wrong position. It should be in rear not front. So that
					%  we continue with this call next time we try to execute queue.
					% If we were to leave it as is, process might execute calls in a different order
					%  than it received them.
					case queue:is_empty(NP#dp.callqueue) of
						false ->
							{{value,Call1},CQ1} = queue:out(NP#dp.callqueue),
							case Call1 == Call of
								true ->
									NP#dp{callqueue = queue:in(Call,CQ1)};
								false ->
									NP
							end;
						_ ->
							NP
					end
			end
	end;
doqueue(P) ->
	% ?INF("Queue notyet ~p",[{P#dp.callres,P#dp.verified,P#dp.transactionid,P#dp.locked}]),
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
			% Moved = [{?MOVEDTO,MovedTo}]
			Moved = [[?MOVEDTO,MovedTo]]
	end,
	% DefVals = [[$(,K,$,,$',butil:tobin(V),$',$)] || {K,V} <-
	% 	[{?SCHEMA_VERS,SchemaVers},{?ATYPE,Type},{?EVNUM,0},{?EVTERM,0}|Moved]],
	DefVals = [[?SCHEMA_VERSI,butil:tobin(SchemaVers)],[?ATYPEI,Type],[?EVNUMI,<<"0">>],[?EVTERMI,<<"0">>]|Moved],
	{[<<"$CREATE TABLE IF NOT EXISTS __transactions (id INTEGER PRIMARY KEY, tid INTEGER,",
		 	" updater INTEGER, node TEXT,schemavers INTEGER, sql TEXT);",
		 "$CREATE TABLE IF NOT EXISTS __adb (id INTEGER PRIMARY KEY, val TEXT);">>],DefVals}.
	 % <<"$INSERT INTO __adb (id,val) VALUES ">>,
	 % 	butil:iolist_join(DefVals,$,),$;].

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

election_timer(undefined) ->
	Latency = actordb_latency:latency(),
	% run_queue is a good indicator of system overload.
	Fixed = max(300,Latency),
	T = Fixed+random:uniform(Fixed),
	?ADBG("Relection try in ~p, replication latency ~p",[T,Latency]),
	erlang:send_after(T,self(),{doelection,Latency,os:timestamp()});
election_timer(T) ->
	case is_reference(T) andalso erlang:read_timer(T) /= false of
		true ->
			T;
		false when is_pid(T) ->
			?ADBG("Election pid active"),
			T;
		_ ->
			election_timer(undefined)
	end.

actor_start(P) ->
	actordb_local:actor_started(P#dp.actorname,P#dp.actortype,?PAGESIZE*?DEF_CACHE_PAGES).


% Returns:
% synced -> do nothing, do not set timer again
% resync -> run election immediately
% wait_longer -> wait for 3s and run election
% timer -> run normal timer again
check_for_resync(P,L,Action) ->
	check_for_resync1(P,L,Action,os:timestamp()).
check_for_resync1(P, [F|L],Action,Now) when F#flw.match_index == P#dp.evnum, F#flw.wait_for_response_since == undefined ->
	check_for_resync1(P,L,Action,Now);
check_for_resync1(_,[],Action,_) ->
	Action;
check_for_resync1(P,[F|L],_Action,Now) ->
	IsAlive = lists:member(F#flw.distname,nodes()),
	case F#flw.wait_for_response_since of
		undefined ->
			Wait = 0;
		_ ->
			Wait = timer:now_diff(Now,F#flw.wait_for_response_since)
	end,
	?DBG("check_resync nd=~p, alive=~p",[F#flw.node,IsAlive]),
	LastSeen = timer:now_diff(Now,F#flw.last_seen),
	case ok of
		_ when Wait > 1000000, IsAlive ->
			resync;
		_ when LastSeen > 1000000, F#flw.match_index /= P#dp.evnum, IsAlive ->
			resync;
		_ when IsAlive == false, LastSeen > 3000000 ->
			resync;
		_ when IsAlive == false ->
			check_for_resync1(P,L,wait_longer,Now);
		_ ->
			check_for_resync1(P,L,timer,Now)
	end.


start_verify(P,JustStarted) ->
	case ok of
		_ when is_binary(P#dp.movedtonode) ->
			P#dp{verified = true};
		% Actor was started with mode slave. It will remain in slave(follower) mode and
		%  not attempt to become candidate for now.
		_ when P#dp.mors == slave, JustStarted ->
			P;
		_ when is_pid(P#dp.election); is_reference(P#dp.election) ->
			P;
		_ ->
			?DBG("Trying to get elected"),
			CurrentTerm = P#dp.current_term+1,
			Me = actordb_conf:node_name(),
			E = #election{actor = P#dp.actorname, type = P#dp.actortype, candidate = Me,
						wait = P#dp.flags band ?FLAG_WAIT_ELECTION > 0, term = CurrentTerm,evnum = P#dp.evnum,
						evterm = P#dp.evterm, flags = P#dp.flags band (bnot ?FLAG_WAIT_ELECTION),
						followers = P#dp.follower_indexes, cbmod = P#dp.cbmod},
			case actordb_election:whois_leader(E) of
				Result when is_pid(Result); Result == Me ->
					store_term(P,actordb_conf:node_name(),CurrentTerm,P#dp.evnum,P#dp.evterm),
					NP = set_followers(P#dp.schemavers /= undefined,
							reopen_db(P#dp{current_term = CurrentTerm, voted_for = Me,
									mors = master, verified = false})),
					case ok of
						_ when is_pid(Result) ->
							Verifypid = Result,
							erlang:monitor(process,Verifypid),
							Verifypid ! {set_followers,NP#dp.follower_indexes};
						_ when is_binary(Result) ->
							Verifypid = self(),
							self() ! {'DOWN',make_ref(),process,self(),{leader,P#dp.follower_indexes,false}}
					end,
					NP#dp{election = Verifypid, verified = false, activity = make_ref()};
				LeaderNode when is_binary(LeaderNode) ->
					?DBG("Received leader ~p",[LeaderNode]),
					DistName = bkdcore:dist_name(LeaderNode),
					case lists:member(DistName,nodes()) of
						true ->
							actordb_local:actor_mors(slave,LeaderNode),
							doqueue(reopen_db(P#dp{masternode = LeaderNode, election = undefined,
								masternodedist = DistName, mors = slave,
								callfrom = undefined, callres = undefined,
								verified = true, activity = make_ref()}));
						_ ->
							P#dp{election = election_timer(P#dp.election)}
					end;
				Err ->
					?DBG("Election try result ~p",[Err]),
					P#dp{election = election_timer(P#dp.election)}
			end
	end.
follower_nodes(L) ->
	[F#flw.node || F <- L].
% Call RequestVote RPC on cluster nodes.
% This should be called in an async process and current_term and voted_for should have
%  been set for this election (incremented current_term, voted_for = Me)
% start_election(P) ->
% 	ClusterSize = length(P#dp.follower_indexes) + 1,
% 	Me = actordb_conf:node_name(),
% 	Msg = {state_rw,{request_vote,Me,P#dp.current_term,P#dp.evnum,P#dp.evterm}},
% 	Nodes = follower_nodes(P#dp.follower_indexes),
% 	?DBG("Election, multicall to ~p",[Nodes]),
% 	Start = os:timestamp(),
% 	{Results,_GetFailed} = bkdcore_rpc:multicall(Nodes,{actordb_sqlproc,call_slave,
% 			[P#dp.cbmod,P#dp.actorname,P#dp.actortype,Msg,[{flags,P#dp.flags band (bnot ?FLAG_WAIT_ELECTION)}]]}),
% 	Stop = os:timestamp(),
% 	?DBG("Election took=~p, results ~p failed ~p, contacted ~p",[timer:now_diff(Stop,Start),Results,_GetFailed,Nodes]),

% 	% Sum votes. Start with 1 (we vote for ourselves)
% 	case count_votes(Results,{P#dp.evnum,P#dp.evterm},true,P#dp.follower_indexes,1) of
% 		% {outofdate,_Node,_NewerTerm} ->
% 		% 	% send_doelection(Node,P),
% 		% 	start_election_done(P,follower);
% 		{NumVotes,Followers,AllSynced} when is_integer(NumVotes) ->
% 			case NumVotes*2 > ClusterSize of
% 				true ->
% 					start_election_done(P,{leader,Followers,AllSynced});
% 				false when (length(Results)+1)*2 =< ClusterSize ->
% 					% Majority isn't possible anyway.
% 					start_election_done(P,follower);
% 				false ->
% 					% Majority is possible.
% 					% This election failed. Check if any of the nodes said they will try to get elected.
% 					% case [true || {_What,_Node,_HisLatestTerm,true} <- Results] of
% 					% 	% If none, sort nodes by name. Pick the one after me and tell him to try to get elected.
% 					% 	% If he fails, he will pick the node after him. One must succeed because majority is online.
% 					% 	[] ->
% 					% 		SortedNodes = lists:sort([actordb_conf:node_name()|[Nd || {_What,Nd,_HisLatestTerm,_} <- Results]]),
% 					% 		case butil:lists_split_at(actordb_conf:node_name(),SortedNodes) of
% 					% 			{_,[Next|_]} ->
% 					% 				send_doelection(Next,P);
% 					% 			{[Next|_],_} ->
% 					% 				send_doelection(Next,P)
% 					% 		end;
% 					% 	_ ->
% 					% 		ok
% 					% end,
% 					start_election_done(P,follower)
% 			end
% 	end.
% start_election_done(P,X) when is_tuple(X) ->
% 	?DBG("Exiting with signal ~p",[X]),
% 	case P#dp.flags band ?FLAG_WAIT_ELECTION > 0 of
% 		true ->
% 			receive
% 				exit ->
% 					exit(X)
% 				after 300 ->
% 					?ERR("Wait election write waited too long."),
% 					exit(X)
% 			end;
% 		false ->
% 			exit(X)
% 	end;
% start_election_done(P,Signal) ->
% 	?DBG("Exiting with signal ~p",[Signal]),
% 	exit(Signal).

% send_doelection(Node,P) ->
% 	DoElectionMsg = [P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,doelection}],
% 	bkdcore_rpc:call(Node,{actordb_sqlproc,call_slave,DoElectionMsg}).
% count_votes([{What,Node,_HisLatestTerm,{Num,Term} = NodeNumTerm}|T],NumTerm,AllSynced,Followers,N) ->
% 	F = lists:keyfind(Node,#flw.node,Followers),
% 	NF = F#flw{match_index = Num, next_index = Num+1, match_term = Term},
% 	case What of
% 		true when AllSynced, NodeNumTerm == NumTerm ->
% 			count_votes(T,NumTerm,true,lists:keystore(Node,#flw.node,Followers,NF),N+1);
% 		true ->
% 			count_votes(T,NodeNumTerm,false,Followers,N+1);
% 		% outofdate ->
% 		% 	count_votes(T,NodeNumTerm,false,Followers,N);
% 		% 	{outofdate,Node,HisLatestTerm};
% 		_ ->
% 			count_votes(T,NumTerm,false,Followers,N)
% 	end;
% count_votes([Err|_T],_NumTerm,_AllSynced,_Followers,_N) ->
% 	% count_votes(T,NumTerm,false,Followers,N);
% 	exit({failed,Err});
% count_votes([],_,AllSynced,F,N) ->
% 	{N,F,AllSynced}.


% SqlIn - only has anything in it if this is recreate after delete
post_election_sql(P,[],undefined,SqlIn,Callfrom1) ->
	% If actor is starting with a write, we can incorporate the actual write to post election sql.
	% This is why wait_election flag is added at actordb_sqlproc:write.
	QueueEmpty = queue:is_empty(P#dp.callqueue),
	case Callfrom1 of
		undefined when QueueEmpty == false ->
			case queue:out_r(P#dp.callqueue) of
				{{value,{Callfrom,#write{sql = <<_/binary>> = CallWrite1, records = CallRecords}}},CQ} ->
					CallWrite = semicolon(CallWrite1);
				_ ->
					CallRecords = [],
					CallWrite = <<>>,
					CQ = P#dp.callqueue,
					Callfrom = Callfrom1
			end;
		Callfrom ->
			CallRecords = [],
			CallWrite = <<>>,
			CQ = P#dp.callqueue
	end,
	?DBG("Adding write to post election sql schemavers=~p",[P#dp.schemavers]),
	case P#dp.schemavers of
		undefined ->
			{SchemaVers,Schema} = apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,0]),
			NP = P#dp{schemavers = SchemaVers},
			case P#dp.follower_indexes of
				[] ->
					Flags = P#dp.flags;
				_ ->
					Flags = P#dp.flags bor ?FLAG_SEND_DB
			end,
			ActorNum = actordb_util:hash(term_to_binary({P#dp.actorname,P#dp.actortype,os:timestamp(),make_ref()})),
			{BS,Records1} = base_schema(SchemaVers,P#dp.actortype),
			?DBG("Adding base schema ~p",[BS]),
			AdbRecords = Records1 ++ [[?ANUMI,butil:tobin(ActorNum)]],
			Sql = [SqlIn,BS,Schema,CallWrite
					];
		_ ->
			AdbRecords = [],
			Flags = P#dp.flags,
			case apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,P#dp.schemavers]) of
				{_,[]} ->
					NP = P,
					Sql = CallWrite;
				{SchemaVers,Schema} ->
					NP = P#dp{schemavers = SchemaVers},
					Sql = [SqlIn,Schema,CallWrite,
							<<"$UPDATE __adb SET val='">>,(butil:tobin(SchemaVers)),
								<<"' WHERE id=",?SCHEMA_VERS/binary,";">>]
			end
	end,
	{NP#dp{callqueue = CQ, flags = Flags},Sql,CallRecords,AdbRecords,Callfrom};
post_election_sql(P,[{1,Tid,Updid,Node,SchemaVers,MSql1}],undefined,SqlIn,Callfrom) ->
	case base64:decode(MSql1) of
		<<"delete">> ->
			Sql = delete;
		Sql1 ->
			Sql = [SqlIn,Sql1]
	end,
	% Put empty sql in transactioninfo. Since sqls get appended for existing transactions in write_call.
	% This way sql does not get written twice to it.
	ReplSql = {<<>>,P#dp.evnum+1,SchemaVers},
	Transid = {Tid,Updid,Node},
	NP = P#dp{transactioninfo = ReplSql,
				transactionid = Transid,
				schemavers = SchemaVers},
	{NP,Sql,[],[],Callfrom};
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
	{P#dp{copyfrom = undefined, copyreset = undefined, movedtonode = MovedToNode, cbstate = NS},Sql,[],[],Callfrom};
post_election_sql(P,Transaction,Copyfrom,Sql,Callfrom) when Transaction /= [], Copyfrom /= undefined ->
	% Combine sqls for transaction and copy.
	case post_election_sql(P,Transaction,undefined,Sql,Callfrom) of
		{NP1,delete,_} ->
			{NP1,delete,[],[],Callfrom};
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
						<<"SELeCT * FROM __adb WHERE id=",?ANUM/binary,";">>,read),
			case Res of
				{ok,[{columns,_},{rows,[]}]} ->
					<<>>;
				{ok,[{columns,_},{rows,[{_,Num}]}]} ->
					Num;
				{sql_error,{"exec_script",sqlite_error,"no such table: __adb"},_} ->
					<<>>
			end
	end.

% Checkpoint should not be called too often (wasting resources and no replication space),
% or not often enough (too much disk space wasted).
% The main problem scenario is one of the nodes being offline. Then it can fall far behind.
% Current formula is max(XPages,X*DBSIZE). Default values: XPages=5000, X=0.1
% This way if a large DB, replication space is max 10%. Or if small DB, max 5000 pages.
% Page is 4096 bytes max, but with compression it is usually much smaller.
checkpoint(P) when P#dp.last_checkpoint == P#dp.evnum orelse P#dp.last_checkpoint+6 =< P#dp.evnum ->
	case P#dp.mors of
		master ->
			case [F || F <- P#dp.follower_indexes, F#flw.next_index =< P#dp.last_checkpoint] of
				[] ->
					actordb_driver:checkpoint(P#dp.db,P#dp.evnum-3);
				_ ->
					% One of the followers is at least 6 events behind
					{_,_,_,MxPage,AllPages,_,_} = actordb_driver:actor_info(P#dp.dbpath,actordb_util:hash(P#dp.dbpath)),
					{MxPages,MxFract} = actordb_conf:replication_space(),
					Diff = AllPages - MxPage,
					case Diff > max(MxPages,0.1*MxFract) of
						true ->
							actordb_driver:checkpoint(P#dp.db,P#dp.evnum-3);
						false ->
							ok
					end
			end;
		slave ->
			actordb_driver:checkpoint(P#dp.db,P#dp.evnum-3)
	end,
	% This way checkpoint gets executed every 6 writes.
	P#dp{last_checkpoint = P#dp.evnum};
checkpoint(P) ->
	P.

delete_actor(P) ->
	?DBG("deleting actor ~p ~p ~p",[P#dp.actorname,P#dp.dbcopy_to,P#dp.dbcopyref]),
	case ((P#dp.flags band ?FLAG_TEST == 0) andalso P#dp.movedtonode == undefined) orelse P#dp.movedtonode == deleted of
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
	% ?DBG("Deleted from shard ~p",[P#dp.follower_indexes]),
	ok = actordb_driver:wal_rewind(P#dp.db,0),
	case P#dp.follower_indexes of
		[] ->
			ok;
		_ ->
			{_,_} = bkdcore_rpc:multicall(follower_nodes(P#dp.follower_indexes),{actordb_sqlproc,call_slave,
							[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,{delete,deleted}}]})
	% 		{_,_} = bkdcore_rpc:multicall(follower_nodes(P#dp.follower_indexes),{actordb_sqlproc,stop,
	% 						[{P#dp.actorname,P#dp.actortype}]})
	end,
	actordb_sqlite:stop(P#dp.db).
	% delactorfile(P).
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
semicolon([<<_/binary>> = Bin]) ->
	semicolon(Bin);
semicolon([C]) when is_integer(C) ->
	case C of
		$; ->
			[C];
		_ ->
			[C,$;]
	end;
semicolon([C]) when is_list(C) ->
	semicolon(C);
semicolon([H|T]) ->
	[H|semicolon(T)];
semicolon([]) ->
	[].

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
						<<"UPDATE __adb SET val='",(butil:tobin(NewVers))/binary,"' WHERE id=",?SCHEMA_VERS/binary,";">>])}
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
		no_election_timeout ->
			parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_NO_ELECTION_TIMEOUT},T);
		nostart ->
			{stop,nostart};
		_ ->
			parse_opts(P,T)
	end;
parse_opts(P,[]) ->
	Name = {P#dp.actorname,P#dp.actortype},
	case distreg:reg(self(),Name) of
		ok ->
			DbPath = lists:flatten(apply(P#dp.cbmod,cb_path,
									[P#dp.cbstate,P#dp.actorname,P#dp.actortype]))++
									butil:encode_percent(butil:tolist(P#dp.actorname))++"."++
									butil:encode_percent(butil:tolist(P#dp.actortype)),
			P#dp{dbpath = DbPath,activity_now = actor_start(P), netchanges = actordb_local:net_changes()};
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
check_locks(P,[H|T],L) when is_tuple(H#lck.time) ->
	case timer:now_diff(os:timestamp(),H#lck.time) > 3000000 of
		true ->
			?ERR("Abandoned lock ~p ~p ~p",[P#dp.actorname,H#lck.node,H#lck.ref]),
			check_locks(P,T,L);
		false ->
			check_locks(P,T,[H|L])
	end;
check_locks(P,[H|T],L) ->
	check_locks(P,T,[H|L]);
check_locks(P,[],L) ->
	doqueue(P#dp{locked = L}).

retry_copy(#dp{copylater = undefined} = P) ->
	P;
retry_copy(P) ->
	{LastTry,Copy} = P#dp.copylater,
	case timer:now_diff(os:timestamp(),LastTry) > 1000000*3 of
		true ->
			case Copy of
				{move,Node} ->
					NewActor = P#dp.actorname,
					IsMove = true,
					Msg = {move,actordb_conf:node_name()};
				{split,MFA,Node,OldActor,NewActor} ->
					IsMove = {split,MFA},
					Msg = {split,MFA,actordb_conf:node_name(),OldActor,NewActor}
			end,
			Ref = make_ref(),
			case actordb:rpc(Node,NewActor,{?MODULE,call,[{NewActor,P#dp.actortype},[{lockinfo,wait},lock],
														{dbcopy,{start_receive,Msg,Ref}},P#dp.cbmod]}) of
				ok ->
					{reply,_,NP1} = actordb_sqlprocutil:dbcopy_call({send_db,{Node,Ref,IsMove,NewActor}},undefined,P),
					NP1#dp{copylater = undefined};
				_ ->
					erlang:send_after(3000,self(),retry_copy),
					P#dp{copylater = {os:timestamp(),Msg}}
			end;
		false ->
			P
	end.

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
dbcopy_call({start_receive,Copyfrom,Ref},_,#dp{db = cleared} = P) ->
	?DBG("start receive entire db ~p ~p",[Copyfrom,P#dp.db]),
	% clean up any old sqlite handles.
	garbage_collect(),
	% If move, split or copy actor to a new actor this must be called on master.
	case is_tuple(Copyfrom) of
		true when P#dp.mors /= master ->
			redirect_master(P);
		_ ->
			{ok,RecvPid} = start_copyrec(P#dp{copyfrom = Copyfrom, dbcopyref = Ref}),
			{reply,ok,P#dp{db = undefined, mors = slave,dbcopyref = Ref, copyfrom = Copyfrom, copyproc = RecvPid, election = undefined}}
	end;
dbcopy_call({start_receive,Copyfrom,Ref},CF,P) ->
	% first clear existing db
	case P#dp.db of
		_ when element(1,P#dp.db) == actordb_driver ->
			N = actordb_driver:wal_rewind(P#dp.db,0),
			?DBG("Start receive rewinding data result=~p",[N]);
		_ when P#dp.db == undefined ->
			{ok,Db,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
			N = actordb_driver:wal_rewind(Db,0),
			?DBG("Start receive open and rewind data result=~p",[N])
	end,
	dbcopy_call({start_receive,Copyfrom,Ref},CF,P#dp{db = cleared});
% Read chunk of wal log.
dbcopy_call({wal_read,From1,done},_CallFrom,P) ->
	{FromPid,Ref} = From1,
	erlang:send_after(1000,self(),check_locks),
	{reply,ok,P#dp{locked = butil:lists_add(#lck{pid = FromPid,ref = Ref},P#dp.locked)}};
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
					actordb_sqlproc:write_call(#write{sql = {moved,LC#lck.node}},CallFrom,
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
							WriteMsg = #write{sql = [Sql,<<"DELETE FROM __adb WHERE id=">>,(?COPYFROM),";"]},
							case ok of
								_ when WithoutLock == [], DbCopyTo == [] ->
									actordb_sqlproc:write_call(WriteMsg,CallFrom,P#dp{locked = WithoutLock,
												dbcopy_to = DbCopyTo,
												cbstate = NS});
								_ ->
									?DBG("Queing write call"),
									CQ = queue:in_r({CallFrom,WriteMsg},P#dp.callqueue),
									{noreply,P#dp{locked = WithoutLock,cbstate = NS, dbcopy_to = DbCopyTo,callqueue = CQ}}
							end;
						_ ->
							?DBG("Copy done at evnum=~p",[P#dp.evnum]),
							NP = P#dp{locked = WithoutLock, dbcopy_to = DbCopyTo},
							case LC#lck.actorname == P#dp.actorname of
								true ->
									case lists:keyfind(LC#lck.node,#flw.node,P#dp.follower_indexes) of
										% Flw when is_tuple(Flw), WithoutLock == [], DbCopyTo == [] ->
										% 	{reply,ok,start_verify(reply_maybe(store_follower(NP,
										% 				Flw#flw{match_index = P#dp.evnum, next_index = P#dp.evnum+1})),force)};
										Flw when is_tuple(Flw) ->
											CQ = queue:in_r({CallFrom,#write{sql = <<>>}},P#dp.callqueue),
											{reply,ok,reply_maybe(store_follower(NP#dp{callqueue = CQ},
														Flw#flw{match_index = P#dp.evnum, match_term = P#dp.evterm,
																	next_index = P#dp.evnum+1}))};
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

dbcopy_done(P,Head,Home) ->
	gen_server:call(Home,{dbcopy,{wal_read,{self(),P#dp.dbcopyref},done}}),
	Param = [{bin,{<<>>,Head}},{status,done},{origin,original},{curterm,P#dp.current_term}],
	exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]})).
dbcopy_do(P,Bin) ->
	Param = [{bin,Bin},{status,wal},{origin,original},{curterm,P#dp.current_term}],
	ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}).

dbcopy(P,Home,ActorTo) ->
	ok = actordb_driver:checkpoint_lock(P#dp.db,1),
	Me = self(),
	spawn(fun() ->
		erlang:monitor(process,Me),
		receive
			{'DOWN',_Ref,_,_Me,_} ->
				actordb_driver:checkpoint_lock(P#dp.db,0)
		end
	end),
	dbcopy(P,Home,ActorTo,undefined,0,wal).
dbcopy(P,Home,ActorTo,{iter,_} = Iter,_Bin,wal) ->
	case actordb_driver:iterate_db(P#dp.db,Iter) of
		{ok,Iter1,Bin1,Head,Done} when Done == 0 ->
			dbcopy_do(P,{Bin1,Head}),
			dbcopy(P,Home,ActorTo,Iter1,<<>>,wal);
		{ok,_Iter1,Bin1,Head,Done} when Done > 0 ->
			dbcopy_do(P,{Bin1,Head}),
			dbcopy_done(P,Head,Home)
	end;
dbcopy(P,Home,ActorTo,_F,_Offset,wal) ->
	still_alive(P,Home,ActorTo),
	case actordb_driver:iterate_db(P#dp.db,0,0) of
		{ok,Iter,Bin,Head,Done} when Done == 0 ->
			dbcopy_do(P,{Bin,Head}),
			dbcopy(P,Home,ActorTo,Iter,Iter,wal);
		{ok,_Iter,Bin,Head,Done} when Done > 0 ->
			dbcopy_do(P,{Bin,Head}),
			dbcopy_done(P,Head,Home)
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
									{lockinfo,dbcopy,{P#dp.dbcopyref,P#dp.cbstate,P#dp.copyfrom,P#dp.copyreset}}],
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
			[{Bin,Header},Status,Origin] = butil:ds_vals([bin,status,origin],Param),
			<<Evterm:64/unsigned, Evnum:64/unsigned,_/binary>> = Header,
			% ?DBG("copy_receive size=~p, origin=~p, status=~p",[byte_size(Bin),Origin,Status]),
			case Origin of
				original ->
					Param1 = [{bin,{Bin,Header}},{status,Status},{origin,master},	{evnum,Evnum},{evterm,Evterm}],
					[ok = rpc(Nd,{?MODULE,dbcopy_send,[Ref,Param1]}) || Nd <- ChildNodes];
				master ->
					ok
			end,
			case CurStatus == Status of
				true ->
					?DBG("Inject page evterm=~p evnum=~p",[Evterm,Evnum]),
					ok = actordb_driver:inject_page(F,Bin,Header),
					F1 = F;
				false when Status == wal ->
					?DBG("Opening new at ~p",[P#dp.dbpath]),
					{ok,F1,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
					?DBG("Inject page evterm=~p evnum=~p",[Evterm,Evnum]),
					ok = actordb_driver:inject_page(F1,Bin,Header);
				false when Status == done ->
					F1 = undefined,
					case actordb_sqlite:exec(F,<<"select name, sql from sqlite_master where type='table';">>,read) of
						{ok,[{columns,_},{rows,SchemaTables}]} ->
							ok;
						_X ->
							SchemaTables = []
					end,
					Db = F,
					Sql = <<"SELECT * FROM __adb where id in (",(?EVNUM)/binary,",",(?EVTERM)/binary,");">>,
					{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(Db, Sql,read),
					Evnum1 = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
					Evterm1 = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),
					<<HEvterm:64,HEvnum:64,_/binary>> = Header,
					?DBG("Storing evnum=~p, evterm=~p, curterm=~p, hevterm=~p,hevnum=~p",[Evnum1,Evterm1,HEvterm,HEvnum,butil:ds_val(curterm,Param)]),
					store_term(P#dp{db = Db},undefined,butil:ds_val(curterm,Param,Evterm1),Evnum1,Evterm1),
					case SchemaTables of
						[] ->
							?ERR("DB open after move without schema?",[]),
							actordb_driver:wal_rewind(Db,0),
							actordb_sqlite:stop(Db),
							% actordb_sqlite:move_to_trash(P#dp.fullpath),
							exit(copynoschema);
						_ ->
							?DBG("Copyreceive done ~p ~p ~p",[SchemaTables,
								 {Origin,P#dp.copyfrom},actordb_sqlite:exec(Db,"SELEcT * FROM __adb;")]),
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
	?DBG("Callback unlock, copyfrom=~p",[P#dp.copyfrom]),
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
