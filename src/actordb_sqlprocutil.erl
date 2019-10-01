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
	"UPDATE transactions SET commited=1 WHERE id=?1 AND (commited=0 OR commited=1);"
	}.

reply(undefined,_Msg) ->
	ok;
reply([batch|From],ok) ->
	[reply(F,ok) || F <- From];
	% reply_tuple(From,1,Msg);
reply([batch|From],{ok,Msg}) ->
	reply_tuple(From,1,Msg);
reply([batch|From],{error,Msg}) ->
	reply(From,{error,Msg});
reply([_|_] = From,Msg) ->
	[gen_server:reply(F,Msg) || F <- From, is_atom(F) == false];
reply(From,Msg) ->
	gen_server:reply(From,Msg).

reply_tuple([H|T],Pos,Msg) ->
	reply(H,actordb_sqlite:exec_res({ok,element(Pos,Msg)},"")),
	reply_tuple(T,Pos+1,Msg);
reply_tuple([],_,_) ->
	ok.

% shards/kv can have reads that turn into writes, or have extra data to return along with read.
read_reply(P,undefined,_,_) ->
	P;
read_reply(P,[H|T],Pos,Res) ->
	?DBG("read_reply"),
	case H of
		{tuple,What,From} ->
			reply(From,{What,actordb_sqlite:exec_res({ok, element(Pos,Res)})}),
			read_reply(P,T,Pos+1,Res);
		{mod,{Mod,Func,Args},From} ->
			case apply(Mod,Func,[P#dp.cbstate,actordb_sqlite:exec_res({ok, element(Pos,Res)})|Args]) of
				{write,Write} ->
					case Write of
						_ when is_binary(Write); is_list(Write) ->
							{noreply,NP} = actordb_sqlproc:write_call(#write{sql = iolist_to_binary(Write)},From,P);
						{_,_,_} ->
							{noreply,NP} = actordb_sqlproc:write_call(#write{mfa = Write},From,P)
					end,
					read_reply(NP,T,Pos+1,Res);
				{write,Write,NS} ->
					case Write of
						_ when is_binary(Write); is_list(Write) ->
							{noreply,NP} = actordb_sqlproc:write_call(#write{sql = iolist_to_binary(Write)},
									From,P#dp{cbstate = NS});
						{_,_,_} ->
							{noreply,NP} =
								actordb_sqlproc:write_call(#write{mfa = Write},From,P#dp{cbstate = NS})
					end,
					read_reply(NP,T,Pos+1,Res);
				{reply_write,Reply,Write,NS} ->
					reply(From,Reply),
					case Write of
						_ when is_binary(Write); is_list(Write) ->
							{noreply,NP} = actordb_sqlproc:write_call(
								#write{sql = iolist_to_binary(Write)},undefined,P#dp{cbstate = NS});
						{_,_,_} ->
							{noreply,NP} = actordb_sqlproc:write_call(
								#write{mfa = Write},undefined,P#dp{cbstate = NS})
					end,
					read_reply(NP,T,Pos+1,Res);
				{reply,What,NS} ->
					reply(From,What),
					read_reply(P#dp{cbstate = NS},T,Pos+1,Res);
				{reply,What} ->
					reply(From,What),
					read_reply(P,T,Pos+1,Res)
			end;
		From ->
			reply(From,actordb_sqlite:exec_res({ok, element(Pos,Res)})),
			read_reply(P,T,Pos+1,Res)
	end;
read_reply(P,[],_,_) ->
	P.

ae_respond(P,undefined,_Success,_PrevEvnum,_AEType,_CallCount) ->
	?ERR("Unable to respond for AE because leader is gone"),
	ok;
ae_respond(P,LeaderNode,Success,PrevEvnum,AEType,CallCount) ->
	Resp = {appendentries_response,actordb_conf:node_name(),
		P#dp.current_term,Success,P#dp.evnum,P#dp.evterm,PrevEvnum,AEType,CallCount},
	bkdcore_rpc:cast(LeaderNode,{actordb_sqlproc,call,[{P#dp.actorname,P#dp.actortype},[nostart],
		{state_rw,Resp},P#dp.cbmod]}).

append_wal(#dp{db = queue} = P,Header,Bin) ->
	actordb_queue:cb_inject_page(P#dp.cbstate,Bin,Header);
append_wal(P,Header,Bin) ->
	actordb_sqlite:inject_page(P,Bin,Header).

reply_maybe(#dp{callfrom = undefined, callres = undefined} = P) ->
	doqueue(P);
reply_maybe(P) ->
	reply_maybe(P,1,1,P#dp.followers).
reply_maybe(P,NReplicated,NNodes,[H|T]) ->
	case H of
		_ when H#flw.next_index > P#dp.evnum ->
			reply_maybe(P,NReplicated+1,NNodes+1,T);
		_ ->
			reply_maybe(P,NReplicated,NNodes+1,T)
	end;
% reply_maybe(P,NReplicated,NFollowers,[]) when NReplicated == NFollowers, P#dp.flags bor ?FLAG_REPORT_SYNC ->
% 	ok = actordb_catchup:synced(P#dp.actorname,P#dp.actortype),
% 	reply_maybe(P#dp{flags = P#dp.flags band (bnot ?FLAG_REPORT_SYNC)}, NReplicated,NFollowers,[]);
reply_maybe(#dp{callfrom = undefined, callres = undefined} = P,_,_,[]) ->
	doqueue(P);
reply_maybe(P,NReplicated,NNodes,[]) ->
	% ?DBG("reply_maybe ~p",[P#dp.callfrom]),
	case P#dp.callfrom of
		[_|_] ->
			Exec = [CC || CC <- P#dp.callfrom, element(1,CC) == exec];
		_ when element(1,P#dp.callfrom) == exec ->
			Exec = [P#dp.callfrom];
		_ ->
			Exec = []
	end,
	case NReplicated*2 > NNodes of
	% case N == length(P#dp.followers)+1 of
		% If transaction active or copy/move actor, we can continue operation now because it has been safely replicated.
		true when P#dp.transactioninfo /= undefined; Exec /= [] ->
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
						_ when P#dp.followers /= [] ->
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
									 <<"#s02;">>
									 ],
							VarHeader = create_var_header(P),
							Records = [[[?EVNUMI,butil:tobin(EvNumNew)],
								[?EVTERMI,butil:tobin(P#dp.current_term)]]++AdbRecs],
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
			case Exec of
				[{exec,From1,{move,Node} = OrigMsg}] ->
					NewActor = P#dp.actorname,
					From = [case CC of _ when element(1,CC) == exec -> From1; _ -> CC end || CC <- P#dp.callfrom],
					IsMove = true,
					Msg = {move,actordb_conf:node_name()};
				[{exec,From1,{split,MFA,Node,OldActor,NewActor} = OrigMsg}] ->
					IsMove = {split,MFA},
					From = [case CC of _ when element(1,CC) == exec -> From1; _ -> CC end || CC <- P#dp.callfrom],
					% Change node name back to this node, so that copy knows where split is from.
					Msg = {split,MFA,actordb_conf:node_name(),OldActor,NewActor};
				_ ->
					OrigMsg = IsMove = Msg = Node = NewActor = undefined,
					From = P#dp.callfrom
			end,
			?DBG("Reply transaction=~p, id=~p res=~p from=~p",[P#dp.transactioninfo,P#dp.transactionid,Res,From]),
			case P#dp.movedtonode of
				deleted ->
					actordb_sqlprocutil:delete_actor(P),
					spawn(fun() -> actordb_sqlproc:stop(Me) end);
				_ ->
					case actordb_conf:sync() of
						true ->
							% Some time goes by between write and replication.
							% We are syncing when replication is done.
							% Another actor may already have synced and this will be a noop.
							actordb_sqlite:fsync(P);
						_ ->
							ok
					end
			end,
			reply(From,Res),
			RR = P#dp.rasync,
			read_reply(P, RR#ai.callfrom, 1, RR#ai.wait),
			case P#dp.transactioninfo of
				undefined ->
					actordb_sqlite:replication_done(P);
				_ ->
					ok
			end,
			BD = P#dp.wasync,
			NP = doqueue(do_cb(P#dp{callfrom = undefined, callres = undefined,
				rasync = RR#ai{wait = undefined, callfrom = undefined},
				wasync = BD#ai{nreplies = BD#ai.nreplies + 1},
				schemavers = NewVers})),
			case Msg of
				undefined ->
					checkpoint(NP);
				_ ->
					Ref = make_ref(),
					RpcParam = [{NewActor,P#dp.actortype},[{lockinfo,wait},lock],
						{dbcopy,{start_receive,Msg,Ref}},P#dp.cbmod],
					case actordb:rpc(Node,NewActor,{actordb_sqlproc,call,RpcParam}) of
						ok ->
							?DBG("dbcopy call ok"),
							{reply,_,NP1} = dbcopy_call({send_db,{Node,Ref,IsMove,NewActor}},From,NP),
							NP1;
						Err ->
							?DBG("dbcopy call retry_later=~p",[Err]),
							erlang:send_after(3000,self(),retry_copy),
							% Unable to start copy/move operation. Store it for later.
							NP#dp{copylater = {actordb_local:elapsed_time(),OrigMsg}}
					end
			end;
		true ->
			?DBG("Reply ok ~p ~p",[{P#dp.callfrom,P#dp.callres},{NReplicated*2, NNodes}]),
			BD = P#dp.wasync,
			RR = P#dp.rasync,
			case P#dp.movedtonode of
				deleted ->
					Moved = deleted,
					Me = self(),
					actordb_sqlprocutil:delete_actor(P),
					spawn(fun() -> actordb_sqlproc:stop(Me) end);
				{moved,Nd} ->
					Moved = Nd,
					Me = self(),
					actordb_sqlprocutil:delete_actor(P),
					spawn(fun() -> actordb_sqlproc:stop(Me) end);
				_ ->
					Moved = P#dp.movedtonode,
					case actordb_conf:sync() /= true andalso P#dp.force_sync of
						true ->
							% Some time goes by between write and replication.
							% We are syncing when replication is done.
							% Another actor may already have synced and this will be a noop.
							actordb_sqlite:fsync(P);
						_ ->
							ok
					end
			end,
			reply(P#dp.callfrom,P#dp.callres),
			read_reply(P, RR#ai.callfrom, 1, RR#ai.wait),
			actordb_sqlite:replication_done(P),
			doqueue(checkpoint(do_cb(P#dp{movedtonode = Moved, callfrom = undefined, callres = undefined,
				force_sync = false,
				rasync = RR#ai{wait = undefined, callfrom = undefined},
				wasync = BD#ai{nreplies = BD#ai.nreplies + 1}})));
		false ->
			% ?DBG("Reply NOT FINAL evnum ~p followers ~p",
				% [P#dp.evnum,[F#flw.next_index || F <- P#dp.followers]]),
			P
	end.

% We are sending prevevnum and prevevterm, #dp.evterm is less than #dp.current_term only
%  when election is won and this is first write after it.
% create_var_header(#dp{db = queue} = P) ->
% 	{P#dp.current_term,actordb_conf:node_name(),P#dp.evnum,P#dp.evterm,follower_call_counts(P)};
create_var_header(P) ->
	term_to_binary({P#dp.current_term,actordb_conf:node_name(),P#dp.evnum,P#dp.evterm,follower_call_counts(P)}).

follower_call_counts(P) ->
	[{F#flw.node,{F#flw.match_index,F#flw.match_term}} || F <- P#dp.followers].


send_empty_ae(P,<<_/binary>> = Nm) ->
	send_empty_ae(P,lists:keyfind(Nm,#flw.node,P#dp.followers));
send_empty_ae(P,F) ->
	?DBG("sending empty ae to ~p",[F#flw.node]),
	case [C || C <- P#dp.dbcopy_to, C#cpto.node == F#flw.node, C#cpto.actorname == P#dp.actorname] of
		[] ->
			bkdcore_rpc:cast(F#flw.node,
			{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
			 {state_rw,{appendentries_start,P#dp.current_term,actordb_conf:node_name(),
			 F#flw.match_index,F#flw.match_term,empty,{F#flw.match_index,F#flw.match_term}}}]}),
			F#flw{wait_for_response_since = actordb_local:elapsed_time()};
		_ ->
			?DBG("dbcopy active to node"),
			F
	end.

reopen_db(#dp{mors = master} = P) ->
	case ok of
		_ when P#dp.db == undefined; P#dp.db == queue  ->
			init_opendb(P);
		_ ->
			?DBG("reopen_db"),
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
		_ when P#dp.db == queue ->
			init_opendb(P#dp{cbstate = actordb_queue:cb_replicate_opts(P#dp.cbstate,<<>>)});
		_ when P#dp.db == undefined; P#dp.db == queue  ->
			NP = init_opendb(P),
			actordb_sqlite:replicate_opts(NP,<<>>),
			NP;
		_ ->
			actordb_sqlite:replicate_opts(P,<<>>),
			P
	end.

init_opendb(P) ->
	case P#dp.db of
		undefined when P#dp.dbpath /= queue ->
			{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
			NP = P#dp{db = Db};
		undefined ->
			SchemaTables = [ok || actordb_queue:cb_actor_info(P#dp.cbstate) /= undefined],
			NP = P#dp{db = queue};
		_ when element(1,P#dp.db) == actordb_driver ->
			Sql = <<"select name, sql from sqlite_master where type='table';">>,
			{ok,[[{columns,_},{rows,SchemaTables}]]} = actordb_sqlite:exec(P#dp.db,Sql),
			NP = P;
		_ ->
			% If not driver, it is queue. No schema there.
			SchemaTables = [ok || actordb_queue:cb_actor_info(P#dp.cbstate) /= undefined],
			NP = P
	end,
	case SchemaTables of
		[_|_] ->
			?DBG("Opening HAVE schema",[]),
			read_db_state(NP);
		[] ->
			?DBG("Opening NO schema",[]),
			set_followers(false,NP)
	end.

read_db_state(P) when element(1,P#dp.db) == actordb_driver ->
	{ok,[{columns,_},{rows,[_|_] = Rows}]} = actordb_sqlite:exec(P#dp.db,
			<<"sELECT * FROM __adb;">>,read),
	read_db_state(P,Rows);
read_db_state(#dp{db = queue} = P) ->
	{_,_,_InProg,_MxPage,_AllPages,Term,Evnum}
		= actordb_queue:cb_actor_info(P#dp.cbstate),
	read_db_state(P,[{?SCHEMA_VERS,1},{?EVTERMI,Term},{?EVNUMI,Evnum}]).
read_db_state(P,Rows) ->
	?DBG("Adb rows ~p",[Rows]),
	% Every read parameter should have default val.
	% This may be read on an actor that just has redirect marker.
	Evnum = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
	Vers = butil:toint(butil:ds_val(?SCHEMA_VERSI,Rows, 0)),
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
	ok = actordb_sqlite:wal_rewind(P,0),
	[].

set_followers(HaveSchema,P) ->
	case apply(P#dp.cbmod,cb_nodelist,[P#dp.cbstate,HaveSchema]) of
		{read,Sql} ->
			{ok,NS,NL} = apply(P#dp.cbmod,cb_nodelist,[P#dp.cbstate,HaveSchema,
								actordb_sqlite:exec(P#dp.db,Sql,read)]);
		{ok,NS,NL} ->
			ok
	end,
	P#dp{cbstate = NS,followers = P#dp.followers ++
			[#flw{node = Nd,distname = bkdcore:dist_name(Nd),match_index = 0,
			next_index = P#dp.evnum+1} || Nd <- NL,
			lists:keymember(Nd,#flw.node,P#dp.followers) == false]}.

try_wal_recover(P,F) when F#flw.file /= undefined ->
	case F#flw.file of
		{iter,_} ->
			actordb_sqlite:iterate_close(F#flw.file);
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
	case actordb_sqlite:iterate_db(P,F#flw.match_term,F#flw.match_index) of
		{ok,Iter2,Bin,Head,Done} ->
			?DBG("try_wal_recover success",[]),
			NF = F#flw{file = Iter2, pagebuf = {Bin,Head,Done}},
			{true,store_follower(P,NF),NF};
		% Term conflict. We found evnum, but term is different. Store this term for follower and send it.
		% Follower will reject and rewind and they will move one write back.
		{ok,Term} ->
			?DBG("try_wal_recover, rewind necessary=~p",[Term]),
			NF = F#flw{match_term = Term, file = 1},
			{true,store_follower(P,NF),NF};
		done ->
			?DBG("try_wal_recover done"),
			{false,P,F}
	end.

% continue_maybe(P,F,true) ->
% 	store_follower(P,F);
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
							Send = {send_db,{F#flw.node,Ref,false,P#dp.actorname}},
							{reply,_,NP1} = dbcopy_call(Send,undefined,NP),
							NP1;
						_Err ->
							?ERR("Error sending db ~p",[_Err]),
							NP
					end
			end;
		true ->
			?DBG("Sending AE start on evnum=~p, matchindex=~p, matchterm=~p",
				[F#flw.next_index,F#flw.match_index,F#flw.match_term]),
			StartRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
				{state_rw,{appendentries_start,P#dp.current_term,actordb_conf:node_name(),
							F#flw.match_index,F#flw.match_term,recover,{F#flw.match_index,F#flw.match_term}}}]}),
			case StartRes of
				ok when F#flw.file /= undefined ->
					% Send wal
					SendResp = (catch send_wal(P,F)),
					case F#flw.file of
						{iter,_} ->
							actordb_sqlite:iterate_close(F#flw.file);
						_ ->
							ok
					end,
					case SendResp of
						wal_corruption ->
							store_follower(P,F#flw{file = undefined, wait_for_response_since = actordb_local:elapsed_time()});
						error ->
							?ERR("Error iterating wal"),
							store_follower(P,F#flw{file = undefined, wait_for_response_since = actordb_local:elapsed_time()});
						NF when element(1,NF) == flw ->
							?DBG("Sent AE on evnum=~p",[F#flw.next_index]),
							store_follower(P,NF#flw{file = undefined, inrecovery = true,
								wait_for_response_since = actordb_local:elapsed_time()})
					end;
				% to be continued in appendentries_response
				_ ->
					case F#flw.file of
						{iter,_} ->
							actordb_sqlite:iterate_close(F#flw.file);
						_ ->
							ok
					end,
					store_follower(P,F#flw{file = undefined, pagebuf = <<>>, wait_for_response_since = actordb_local:elapsed_time()})
			end;
		% Follower uptodate, close file if open
		false when F#flw.file == undefined ->
			store_follower(P,F);
		false ->
			case F#flw.file of
				{iter,_} ->
					actordb_sqlite:iterate_close(F#flw.file);
				_ ->
					ok
			end,
			store_follower(P,F#flw{file = undefined, pagebuf = <<>>})
	end.

store_follower(P,#flw{distname = undefined} = F) ->
	store_follower(P,F#flw{distname = bkdcore:dist_name(F#flw.node)});
store_follower(P,NF) when NF#flw.next_index > P#dp.evnum, NF#flw.inrecovery ->
	CP = [P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,recovered}],
	bkdcore_rpc:cast(NF#flw.node,{actordb_sqlproc,call_slave,CP}),
	store_follower(P,NF#flw{inrecovery = false});
store_follower(P,NF) ->
	P#dp{followers = lists:keystore(NF#flw.node,#flw.node,P#dp.followers,NF)}.


% Read until commit set in header.
send_wal(P,F) ->
	send_wal(P,F,[],[],0).
send_wal(P,#flw{file = {iter,_}} = F,HeaderBuf, PageBuf,BufSize) ->
	case F#flw.pagebuf of
		<<>> ->
			case actordb_sqlite:iterate_db(P,F#flw.file) of
				% {ok,Iter,<<Header:144/binary,Page/binary>>,_IsLastWal} ->
				{ok,Iter,PageCompressed,Header,Commit} ->
					ok;
				done ->
					?ERR("Disk corruption while trying to replicate to stale follower. Stepping down as leader."),
					throw(wal_corruption),
					Commit = Iter = Header = PageCompressed = undefined
			end;
		{PageCompressed,Header,Commit} ->
			Iter = F#flw.file
	end,
	case Commit of
		0 when BufSize < 1024*128 ->
			?DBG("Buffering page ~p",[byte_size(PageCompressed)]),
			send_wal(P,F#flw{pagebuf = <<>>, file = Iter},[Header|HeaderBuf],[PageCompressed|PageBuf],BufSize+byte_size(PageCompressed));
		_ when Commit > 0; BufSize >= 1024*128 ->
			<<ET:64,EN:64,Pgno:32,_:32>> = Header,
			?DBG("send_wal et=~p, en=~p, pgno=~p, commit=~p, bufsize=~p",[ET,EN,Pgno,Commit,BufSize]),
			WalRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
						{state_rw,{appendentries_wal,
							lists:reverse([Header|HeaderBuf]),lists:reverse([PageCompressed|PageBuf]),
							recover,{F#flw.match_index,F#flw.match_term}}},
						[nostart]]}),
			?DBG("send_wal resp=~p",[WalRes]),
			case WalRes == ok orelse WalRes == done of
				% If successful response, always set last_seen so we know node active.
				true when Commit == 0 ->
					send_wal(P,F#flw{file = Iter, pagebuf = <<>>, last_seen = actordb_local:elapsed_time()},[],[],0);
				true ->
					F#flw{file = Iter, pagebuf = <<>>, last_seen = actordb_local:elapsed_time()};
				_ ->
					error
			end
	end.

% Go back one entry
rewind_wal(P) ->
	{ok,NS,Evnum,EvTerm} = actordb_sqlite:wal_rewind(P,P#dp.evnum),
	P#dp{evnum = Evnum, evterm = EvTerm, cbstate = NS}.

save_term(P) ->
	store_term(P,P#dp.voted_for,P#dp.current_term,P#dp.evnum,P#dp.evterm).
store_term(P, undefined, CurrentTerm, _EN, _ET) ->
	store_term(P, <<>>, CurrentTerm, _EN, _ET);
store_term(#dp{dbpath = queue} = P, VotedFor, CurrentTerm, _EN,_ET) ->
	{ok,NS} = actordb_queue:cb_term_store(P#dp.cbstate, CurrentTerm, VotedFor),
	P#dp{cbstate = NS};
store_term(P, VotedFor, CurrentTerm, _EN, _ET) ->
	actordb_sqlite:term_store(P, CurrentTerm, VotedFor),
	P.
% store_term(P,VotedFor,CurrentTerm,_Evnum,_EvTerm) ->
% 	actordb_sqlite:term_store(P, CurrentTerm, VotedFor).

statequeue(#dp{wasync = #ai{wait = WRef}} = P) when is_reference(WRef) ->
	% Statequeue may trigger doqueue, which may trigger a write
	% We must stop executing statequeue now, otherwise an endless loop is created.
	erlang:send_after(100,self(),statequeue),
	P;
statequeue(P) ->
	case queue:is_empty(P#dp.statequeue) of
		true ->
			P;
		false ->
			{{value,Call},CQ} = queue:out_r(P#dp.statequeue),
			{From,Msg} = Call,
			?DBG("statequeue pop from=~p, msg=~p",[From,Msg]),
			case actordb_sqlproc:handle_call(Msg,From,P#dp{statequeue = CQ}) of
				{reply,Res,NP} ->
					reply(From,Res),
					statequeue(NP);
				{reply,Res,NP,_} ->
					reply(From,Res),
					statequeue(NP);
				% state_rw always responds, sometimes before returning
				{noreply,NP} ->
					statequeue(NP);
				{noreply,NP,_} ->
					statequeue(NP)
			end
	end.

has_schema_updated(P,Sql) ->
	case catch actordb_schema:num() of
		Schema ->
			ok
	end,
	case is_integer(P#dp.schemanum) andalso
			(P#dp.schemanum == Schema orelse
			P#dp.transactionid /= undefined orelse
			Sql == delete) of
		true when P#dp.schemavers /= undefined ->
			ok;
		_ ->
			case P#dp.schemavers of
				undefined ->
					Vers = 0;
				Vers ->
					ok
			end,
			case apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,Vers]) of
				{_,[]} when is_integer(P#dp.schemanum) == false ->
					{ok,Schema};
				{_,[]} ->
					ok;
				{NewVers,SchemaUpdate} ->
					case Vers of
						0 ->
							{BS,Records1} = base_schema(NewVers,P#dp.actortype),
							{NewVers,[BS,SchemaUpdate],Records1};
						_ ->
							?DBG("updating schema ~p ~p",[?R2P(P),SchemaUpdate]),
							{NewVers,iolist_to_binary([SchemaUpdate,
							<<"UPDATE __adb SET val='",(butil:tobin(NewVers))/binary,
							"' WHERE id=",?SCHEMA_VERS/binary,";">>])}
					end
			end
	end.

exec_writes(#dp{verified = true, wasync = W} = P) when W#ai.buffer /= [] ->
	actordb_sqlproc:write_call1(#write{sql = W#ai.buffer, records = W#ai.buffer_recs},W#ai.buffer_cf,W#ai.buffer_nv,P);
exec_writes(P) ->
	P.

% We must have successfully replicated at least one write before executing reads.
exec_reads(#dp{verified = true, rasync = R, wasync = #ai{nreplies = NR}} = P) when R#ai.buffer /= [], NR > 0 ->
	case has_schema_updated(P,[]) of
		ok ->
			exec_writes(actordb_sqlproc:read_call1(R#ai.safe_read, R#ai.buffer,R#ai.buffer_recs,R#ai.buffer_cf,P));
		{ok,SchemaNum} ->
			exec_writes(actordb_sqlproc:read_call1(R#ai.safe_read, R#ai.buffer,R#ai.buffer_recs,R#ai.buffer_cf,P#dp{schemanum = SchemaNum}));
		SUR ->
			% Skip read for now because we must execute schema change first
			exec_writes(schema_change(P, SUR))
	end;
exec_reads(P) ->
	exec_writes(schema_change(P)).

schema_change(#dp{mors = slave} = P) ->
	P;
schema_change(P) ->
	schema_change(P,has_schema_updated(P,[])).
schema_change(P,ok) ->
	P;
schema_change(P,{ok,SN}) ->
	P#dp{schemanum = SN};
schema_change(P,{NewVers,Sql,Recs}) ->
	{noreply,NP} = actordb_sqlproc:write_call(#write{sql = Sql, newvers = NewVers, records = Recs}, undefined,P),
	NP#dp{schemanum = actordb_schema:num()};
schema_change(P,{NewVers,Sql}) ->
	{noreply,NP} = actordb_sqlproc:write_call(#write{sql = Sql, newvers = NewVers}, undefined,P),
	NP#dp{schemanum = actordb_schema:num()}.

net_changes(#dp{wasync = W} = P) ->
	case P#dp.netchanges == actordb_local:net_changes() of
		true ->
			P;
		false when W#ai.buffer == [], P#dp.mors == master, P#dp.movedtonode == undefined ->
			% start_verify(P#dp{netchanges = actordb_local:net_changes()},false)
			{noreply,NP} = actordb_sqlproc:write_call(#write{sql = []},undefined, P),
			NP#dp{netchanges = actordb_local:net_changes()};
		false ->
			P#dp{netchanges = actordb_local:net_changes()}
	end.

smallest_transaction({FromS,Smallest},[{FromH,#write{transaction = Trans} = H}|T],Extracted) when Trans /= undefined ->
	% ?ADBG("comparing transactions cur=~p, with=~p",[Smallest#write.transaction, Trans]),
	case element(2,Smallest#write.transaction) > element(2,Trans) of
		true ->
			smallest_transaction({FromH,H},T,[{FromS,Smallest}|Extracted]);
		false ->
			smallest_transaction({FromS,Smallest},T,[{FromH,H}|Extracted])
	end;
smallest_transaction(S,[H|T],E) ->
	% ?ADBG("Ignoring for smallest compare ~p",[H]),
	smallest_transaction(S,T,[H|E]);
smallest_transaction(S,[],E) ->
	{S,E}.

doqueue(P) ->
	doqueue(P,[]).
% Execute queued calls and execute reads/writes. handle_call just batched r/w's together.
doqueue(#dp{verified = true,callres = undefined, callfrom = undefined,transactionid = undefined,locked = [],
		wasync = #ai{wait = undefined, nreplies = NR}} = P, Skipped) ->
	NetChanges = actordb_local:net_changes(),
	% ?DBG("doqueue"),
	case ok of
		_ when NetChanges /= P#dp.netchanges ->
			?DBG("Rerun election due to net change"),
			Now = actordb_local:elapsed_time(),
			Interval = election_timer_interval(),
			case P#dp.election_timer of
				undefined when Now - P#dp.last_vote_event > Interval*2 ->
					self() ! doelection,
					P#dp{verified = false, netchanges = NetChanges};
				_ ->
					P#dp{verified = false, netchanges = NetChanges, election_timer = election_timer(P#dp.election_timer)}
			end;
		_ ->
			case queue:is_empty(P#dp.callqueue) of
				true ->
					case apply(P#dp.cbmod,cb_idle,[P#dp.cbstate]) of
						{ok,NS} ->
							exec_reads(net_changes(appendqueue(P#dp{cbstate = NS},Skipped)));
						_ ->
							exec_reads(net_changes(appendqueue(P,Skipped)))
					end;
				false ->
					{{value,Call},CQ} = queue:out_r(P#dp.callqueue),
					{From,Msg} = Call,
					Res = case Msg of
						#write{} when Msg#write.transaction /= undefined ->
							case NR > 0 of
								true ->
									SkippedNew = Skipped,
									?DBG("Executing transaction ~p",[Msg#write.transaction]),
									{{FromSmallest,Smallest},Extracted} = smallest_transaction({From,Msg},queue:to_list(CQ),[]),
									actordb_sqlproc:write_call1(Smallest,FromSmallest,P#dp.schemavers,
												P#dp{callqueue = queue:from_list(Extracted)});
								false ->
									% Not ready to process transaction yet.
									SkippedNew = [{From,Msg}|Skipped],
									{noreply,P#dp{callqueue = CQ}}
							end;
						#write{} ->
							SkippedNew = Skipped,
							actordb_sqlproc:write_call(Msg,From,P#dp{callqueue = CQ});
						#read{} ->
							SkippedNew = Skipped,
							actordb_sqlproc:read_call(Msg,From,P#dp{callqueue = CQ});
						{move,NewShard,Node,CopyReset,CbState} ->
							SkippedNew = Skipped,
							?INF("Received move call to=~p",[{NewShard,Node}]),
							% Call to move this actor to another cluster.
							% First store the intent to move with all needed data.
							% This way even if a node chrashes, the actor will attempt to move on next startup.
							% When write done, reply to caller and start with move process (in ..util:reply_maybe.
							Sql = <<"$INSERT INTO __adb (id,val) VALUES (",?COPYFROM/binary,",'",
									(base64:encode(term_to_binary({{move,NewShard,Node},CopyReset,CbState})))/binary,"');">>,
							actordb_sqlproc:write_call(#write{sql = Sql},{exec,From,{move,Node}},
								actordb_sqlprocutil:set_followers(true,P#dp{callqueue = CQ}));
						{split,MFA,Node,OldActor,NewActor,CopyReset,CbState} ->
							SkippedNew = Skipped,
							% Similar to above. Both have just insert and not insert and replace because
							%  we can only do one move/split at a time. It makes no sense to do both at the same time.
							% So rely on DB to return error for these conflicting calls.
							SplitBin = term_to_binary({{split,MFA,Node,OldActor,NewActor},CopyReset,CbState}),
							Sql = <<"$INSERT INTO __adb (id,val) VALUES (",?COPYFROM/binary,",'",
									(base64:encode(SplitBin))/binary,"');">>,
							% Split is called when shards are moving around (nodes were added).
							% If different number of nodes in cluster, we need
							%  to have an updated list of nodes.
							actordb_sqlproc:write_call(#write{sql = Sql},{exec,From,{split,MFA,Node,OldActor,NewActor}},
								actordb_sqlprocutil:set_followers(true,P#dp{callqueue = CQ}));
						{copy,{Node,OldActor,NewActor}} ->
							SkippedNew = Skipped,
							Ref = make_ref(),
							case actordb:rpc(Node,NewActor,{actordb_sqlproc,call,[{NewActor,P#dp.actortype},[{lockinfo,wait},lock],
											{dbcopy,{start_receive,{actordb_conf:node_name(),OldActor},Ref}},P#dp.cbmod]}) of
								ok ->
									dbcopy_call({send_db,{Node,Ref,false,NewActor}},From,P#dp{callqueue = CQ});
								Err ->
									{reply, Err,P#dp{callqueue = CQ}}
							end;
						report_synced ->
							SkippedNew = Skipped,
							{reply,ok,P#dp{callqueue = CQ, flags = P#dp.flags bor ?FLAG_REPORT_SYNC}};
						delete ->
							SkippedNew = Skipped,
							{reply,ok,P#dp{movedtonode = deleted, callqueue = CQ}};
						stop ->
							SkippedNew = Skipped,
							?DBG("Received stop call"),
							{stop, stopped, P};
						noop ->
							SkippedNew = Skipped,
							{reply, noop, P#dp{callqueue = CQ}};
						Msg ->
							SkippedNew = Skipped,
							% ?DBG("cb_call ~p",[{P#dp.cbmod,Msg}]),
							case apply(P#dp.cbmod,cb_call,[Msg,From,P#dp.cbstate]) of
								{write,Sql,NS} ->
									actordb_sqlproc:write_call(#write{sql = Sql},From,P#dp{cbstate = NS, callqueue = CQ});
								{reply,Resp,S} ->
									{reply,Resp,P#dp{cbstate = S, callqueue = CQ}};
								{reply,Resp} ->
									{reply,Resp,P#dp{callqueue = CQ}}
							end
					end,
					case Res of
						{noreply,NP} ->
							doqueue(NP,SkippedNew);
						{noreply,NP,_} ->
							doqueue(NP,SkippedNew);
						{reply,X,NP} ->
							reply(From,X),
							doqueue(NP,SkippedNew);
						{reply,X,NP,_} ->
							reply(From,X),
							doqueue(NP,SkippedNew);
						{stop,_,NP} ->
							self() ! stop,
							NP
					end
			end
	end;
doqueue(P,[]) ->
	% ?DBG("doqueue can't execute: verified=~p, callres=~p, transid=~p, locked=~p",
	% 	[P#dp.verified,P#dp.callres,P#dp.transactionid,P#dp.locked]),
	P;
doqueue(P,Skipped) ->
	appendqueue(P,Skipped).

appendqueue(P,[Skipped|T]) ->
	appendqueue(P#dp{callqueue = queue:in_r(Skipped,P#dp.callqueue)},T);
appendqueue(P,[]) ->
	P.


% Result: done | abandoned
transaction_done(Id,Uid,Result) ->
	case distreg:whereis({Id,Uid}) of
		undefined ->
			ok;
		Pid ->
			exit(Pid,Result)
	end.
transaction_overruled(Id,Uid) ->
	case distreg:whereis({Id,Uid}) of
		undefined ->
			ignore;
		Pid ->
			Pid ! overruled
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
transaction_checker1(Id,Uid,Node1) ->
	case bkdcore:node_address(Node1) of
		undefined ->
			Node = actordb_conf:node_name();
		_ ->
			Node = Node1
	end,
	Res = actordb:rpc(Node,Uid,{actordb_multiupdate,transaction_state,[Uid,Id]}),
	?ADBG("transaction_check ~p ~p",[{Id,Uid,Node},Res]),
	case Res of
		% Running
		{ok,0} ->
			receive
				overruled ->
					self() ! overruled,
					actordb:rpc(Node,Uid,{actordb_multiupdate,overruled,[Uid,Id]})
			after 100 ->
				ok
			end,
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

	% ,[?EVNUMI,<<"0">>],[?EVTERMI,<<"0">>]
	ActorNum = actordb_util:hash(term_to_binary({Type,os:timestamp(),make_ref()})),
	DefVals = [[[?SCHEMA_VERSI,butil:tobin(SchemaVers)],[?ANUMI,butil:tobin(ActorNum)],[?ATYPEI,Type]|Moved]],
	{[<<"$CREATE TABLE IF NOT EXISTS __transactions (id INTEGER PRIMARY KEY, tid INTEGER,",
		 	" updater INTEGER, node TEXT,schemavers INTEGER, sql TEXT);",
		 "$CREATE TABLE IF NOT EXISTS __adb (id INTEGER PRIMARY KEY, val TEXT);#s02;">>],DefVals}.
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
	case apply(P#dp.cbmod,cb_write_done,[P#dp.cbstate,P#dp.evterm,P#dp.evnum]) of
		{ok,NS} ->
			P#dp{cbstate = NS};
		ok ->
			P
	end.

election_timer_interval() ->
	election_timer_interval(actordb_latency:latency()).
election_timer_interval(Latency) ->
	max(300,Latency).

election_timer(undefined,undefined) ->
	election_timer(actordb_local:elapsed_time(),undefined);
election_timer(Now,undefined) ->
	Latency = actordb_latency:latency(),
	Fixed = election_timer_interval(),
	T = (Fixed div 2) + rand:uniform(Fixed),
	?ADBG("Relection try in ~p, replication latency ~p",[T,Latency]),
	{timer, erlang:send_after(T,self(),{doelection,Latency,Now})};
election_timer(_Now,{election,_,_} = E) ->
	E;
election_timer(Now,{timer,T}) ->
	case erlang:read_timer(T) /= false of
		true ->
			{timer, T};
		_ ->
			election_timer(Now,undefined)
	end.
election_timer(undefined) ->
	election_timer(undefined,undefined);
election_timer(T) ->
	election_timer(undefined,T).

actor_start(_P) ->
	actordb_local:actor_started().

is_alive(F) ->
	case lists:member(F#flw.distname,nodes()) of
		true ->
			true;
		false ->
			bkdcore_rpc:is_connected(F#flw.node)
	end.

% Will categorize followers then decide what to do.
% Categories:
% - synced: all is perfect
% - waiting: we are waiting for response, nothing is taking unusually long
% - delayed: we are waiting for response, it is taking unusually long
% - dead: node is offline from what we see
follower_check(P) ->
	follower_check(P,P#dp.followers,[],[],[],[]).
follower_check(P,[F|T],Synced,Waiting,Delayed,Dead) when F#flw.match_index == P#dp.evnum,
		F#flw.wait_for_response_since == undefined ->
	follower_check(P,T,[F|Synced],Waiting,Delayed,Dead);
follower_check(P,[F|T],Synced,Waiting,Delayed,Dead) ->
	Addr = bkdcore:node_address(F#flw.node),
	IsAlive = is_alive(F),
	Now = actordb_local:elapsed_time(),
	case F#flw.wait_for_response_since of
		undefined ->
			Wait = 0;
		_ ->
			Wait = Now-F#flw.wait_for_response_since
	end,
	?DBG("check_resync nd=~p, alive=~p",[F#flw.node,IsAlive]),
	LastSeen = Now-F#flw.last_seen,
	Latency = actordb_latency:latency(),
	case ok of
		_ when Addr == undefined ->
			follower_check(P#dp{followers =
				lists:keydelete(F#flw.node,#flw.node,P#dp.followers)}, T, Synced,Waiting,Delayed,Dead);
		_ when IsAlive == false ->
			follower_check(P,T,Synced,Waiting,Delayed,[F|Dead]);
		_ when Wait > (1000+Latency) ->
			follower_check(P,T,Synced,Waiting,[F|Delayed],Dead);
		_ when LastSeen > (1000+Latency), F#flw.match_index /= P#dp.evnum ->
			follower_check(P,T,Synced,Waiting,[F|Delayed],Dead);
		_ ->
			follower_check(P,T,Synced,[F|Waiting],Delayed,Dead)
	end;
follower_check(P,[],Synced,Waiting,Delayed,Dead) ->
	{P,[Synced,Waiting,Delayed,Dead]}.

follower_check_handle({P,Res}) ->
	follower_check_handle(P,Res);
follower_check_handle(P) ->
	follower_check_handle(follower_check(P)).
follower_check_handle(P,[S,W,D,Dead]) ->
	follower_check_handle(P,S,W,D,Dead).
follower_check_handle(P,_Synced,[],[],[]) ->
	case ok of
		_ when P#dp.movedtonode == deleted ->
			{stop,normal,P};
		_ when P#dp.flags band ?FLAG_REPORT_SYNC > 0 ->
			ok = actordb_catchup:synced(P#dp.actorname,P#dp.actortype),
			{noreply,P#dp{flags = P#dp.flags band (bnot ?FLAG_REPORT_SYNC), election_timer = undefined}};
		_ ->
			{noreply,P#dp{election_timer = undefined}}
	end;
follower_check_handle(P,_Synced,_Waiting,[],[]) ->
	{noreply,P#dp{election_timer = election_timer(undefined)}};
% Some nodes are delayed unreasonably long.
follower_check_handle(P,_Synced,_Waiting,_Delayed,[]) ->
	?DBG("Have delayed nodes: ~p",[_Delayed]),
	self() ! doqueue,
	Now = actordb_local:elapsed_time(),
	case Now - P#dp.last_write_at of
		Diff when Diff > 1000 ->
			actordb_sqlproc:write_call(#write{sql = []},undefined,P);
		_ ->
			{noreply,P#dp{election_timer = election_timer(undefined)}}
	end;
follower_check_handle(P,Synced,Waiting,Delayed,Dead) ->
	% Some node is not reponding. Report to catchup.
	actordb_catchup:report(P#dp.actorname,P#dp.actortype),
	case length(Synced)+length(Waiting)+length(Delayed)+1 > length(Dead) of
		true ->
			% We can still continue.
			follower_check_handle(P,Synced,Waiting,Delayed,[]);
		false ->
			% We can not continue. Step down.
			% Any pending writes/reads will return error, unless nodes come back online fast enough.
			{noreply,P#dp{verified = false, mors = slave, masternode = undefined,
				masternodedist = undefined,
				without_master_since = actordb_local:elapsed_time(),
				election_timer = election_timer(undefined)}}
	end.


start_verify(P,JustStarted) ->
	case ok of
		_ when is_binary(P#dp.movedtonode) ->
			P#dp{verified = true};
		% Actor was started with mode slave. It will remain in slave(follower) mode and
		%  not attempt to become candidate for now.
		_ when P#dp.mors == slave, JustStarted ->
			P;
		_ when is_tuple(P#dp.election_timer) ->
			P;
		_ ->
			?DBG("Trying to get elected ~p",[P#dp.schemavers]),
			CurrentTerm = P#dp.current_term+1,
			Me = actordb_conf:node_name(),
			NP = set_followers(P#dp.schemavers /= undefined,
					reopen_db(P#dp{current_term = CurrentTerm, voted_for = Me,
						mors = master, verified = false})),
			Msg = {state_rw,{request_vote,Me,CurrentTerm,P#dp.evnum,P#dp.evterm}},
			Call = {actordb_sqlproc,call_slave,
				[P#dp.cbmod,P#dp.actorname,P#dp.actortype,Msg,[{flags,P#dp.flags band (bnot ?FLAG_WAIT_ELECTION)}]]},
			ElecRef = make_ref(),
			NF = [Flw#flw{election_rpc_ref = bkdcore_rpc:async_call(ElecRef,Flw#flw.node,Call)} || Flw <- NP#dp.followers],
			store_term(P,actordb_conf:node_name(),CurrentTerm,P#dp.evnum,P#dp.evterm),
			TimerDur = max(200,actordb_latency:latency()),
			case NF of
				[] ->
					TimerRef = ElecRef,
					self() ! {election_timeout, ElecRef};
				_ ->
					TimerRef = erlang:send_after(TimerDur,self(),{election_timeout,ElecRef})
			end,
			NP#dp{election_timer = {election,TimerRef, ElecRef}, verified = false, followers = NF}
	end.
follower_nodes(L) ->
	[F#flw.node || F <- L].


% SqlIn - only has anything in it if this is recreate after delete
post_election_sql(P,[],undefined,SqlIn,Callfrom1) ->
	{P,SqlIn,[],Callfrom1};
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
	{NP,Sql,[],Callfrom};
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
				% This is node where actor moved from. Check if data is on the other node.
				% If not start copy again.
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
	{P#dp{copyfrom = undefined, copyreset = undefined,
		movedtonode = MovedToNode, cbstate = NS},Sql,[],Callfrom};
post_election_sql(P,Transaction,Copyfrom,Sql,Callfrom) when Transaction /= [], Copyfrom /= undefined ->
	% Combine sqls for transaction and copy.
	case post_election_sql(P,Transaction,undefined,Sql,Callfrom) of
		{NP1,delete,_} ->
			{NP1,delete,[],Callfrom};
		{NP1,Sql1,_} ->
			post_election_sql(NP1,[],Copyfrom,Sql1,Callfrom)
	end.


read_num(P) ->
	case P#dp.db of
		queue ->
			Db = SchemaTables = [];
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
checkpoint(#dp{mors = master, followers = []} = P) when P#dp.last_checkpoint+6 =< P#dp.evnum ->
	?DBG("Single node checkpoint"),
	actordb_sqlite:checkpoint(P,P#dp.evnum-3),
	P#dp{last_checkpoint = P#dp.evnum-3};
checkpoint(P) when P#dp.last_checkpoint+6 =< P#dp.evnum ->
	?DBG("Attempt checkpoint ~p ~p",[P#dp.last_checkpoint, P#dp.evnum]),
	case P#dp.mors of
		master ->
			case [F || F <- P#dp.followers, F#flw.next_index =< P#dp.last_checkpoint andalso F#flw.next_index > 0] of
				[] ->
					?DBG("No followers far behind"),
					Msg = {state_rw,checkpoint},
					inform_followers(P,Msg),
					actordb_sqlite:checkpoint(P,P#dp.evnum-3);
				_ ->
					% One of the followers is at least 6 events behind
					{_,_,_,MxPage,AllPages,_,_} = actordb_sqlite:actor_info(P),
					{MxPages,MxFract} = actordb_conf:replication_space(),
					Diff = AllPages - MxPage,
					?DBG("checkpoint allpages=~p, mxpage=~p, max=~p",[AllPages,MxPage,max(MxPages,0.1*MxFract)]),
					case Diff > max(MxPages,0.1*MxFract) of
						true ->
							Msg = {state_rw,checkpoint},
							inform_followers(P,Msg),
							actordb_sqlite:checkpoint(P,P#dp.evnum-3);
						false ->
							ok
					end
			end;
		slave ->
			?DBG("Slave checkpoint"),
			actordb_sqlite:checkpoint(P,P#dp.evnum-3)
	end,
	% This way checkpoint gets executed every 6 writes.
	P#dp{last_checkpoint = P#dp.evnum};
checkpoint(P) ->
	% ?DBG("Not doing checkpoint ~p ~p",[P#dp.last_checkpoint, P#dp.evnum]),
	P.

inform_followers(P,Msg) ->
	[begin
		bkdcore_rpc:cast(F#flw.node,
			{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,Msg]})
	end || F <- P#dp.followers].

moved_replace(P,Node) ->
	?INF("Replacing actor with redirect marker to node=~p",[Node]),
	Sql = ["CREATE TABLE IF NOT EXISTS __adb (id INTEGER PRIMARY KEY, val TEXT);",
		<<"INSERT OR REPLACE INTO __adb (id,val) VALUES (">>,?MOVEDTO,",'",Node,"');"],
	actordb_sqlite:wal_rewind(P#dp.db,0,Sql),
	ok.

delete_actor(P) ->
	?DBG("deleting actor ~p ~p ~p",[P#dp.actorname,P#dp.dbcopy_to,P#dp.dbcopyref]),
	case ((P#dp.flags band ?FLAG_TEST == 0) andalso
		P#dp.movedtonode == undefined) orelse
		P#dp.movedtonode == deleted of
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
	% ?DBG("Deleted from shard ~p",[P#dp.followers]),
	case P#dp.movedtonode of
		{moved,Moved} ->
			moved_replace(P,Moved);
		_ ->
			ok = actordb_sqlite:wal_rewind(P#dp.db,0)
	end,
	case P#dp.followers of
		[] ->
			ok;
		_ ->
			spawn(fun() ->
			{_,_} = bkdcore_rpc:multicall(follower_nodes(P#dp.followers),{actordb_sqlproc,call_slave,
				[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,{delete,P#dp.movedtonode}}]})
			end)
	% 		{_,_} = bkdcore_rpc:multicall(follower_nodes(P#dp.followers),{actordb_sqlproc,stop,
	% 						[{P#dp.actorname,P#dp.actortype}]})
	end,
	actordb_sqlite:stop(P).
	% delactorfile(P).
empty_queue(A,R,Q,ReplyMsg) ->
	case queue:is_empty(Q) of
		true ->
			[reply(F,ReplyMsg) || F <- A#ai.buffer_cf],
			case R#ai.callfrom of
				[_|_] ->
					[reply(F,ReplyMsg) || F <- R#ai.callfrom];
				_ ->
					ok
			end;
		false ->
			{{value,Call},CQ} = queue:out_r(Q),
			{From,_Msg} = Call,
			reply(From,ReplyMsg),
			empty_queue(A,R,CQ,ReplyMsg)
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

parse_opts(P,[{actor,Name}|T]) ->
	parse_opts(P#dp{actorname = Name},T);
parse_opts(P,[{type,Type}|T]) ->
	parse_opts(P#dp{actortype = Type},T);
parse_opts(P,[{mod,Mod}|T]) ->
	parse_opts(P#dp{cbmod = Mod},T);
parse_opts(P,[{state,S}|T]) ->
	parse_opts(P#dp{cbstate = S},T);
parse_opts(P,[{slave,true}|T]) ->
	parse_opts(P#dp{mors = slave},T);
parse_opts(P,[{slave,false}|T]) ->
	parse_opts(P#dp{mors = master,masternode = actordb_conf:node_name(),masternodedist = node()},T);
parse_opts(P,[{copyfrom,Node}|T]) ->
	parse_opts(P#dp{copyfrom = Node},T);
parse_opts(P,[{copyreset,What}|T]) ->
	case What of
		false ->
			parse_opts(P#dp{copyreset = false},T);
		true ->
			parse_opts(P#dp{copyreset = <<>>},T);
		Mod ->
			parse_opts(P#dp{copyreset = Mod},T)
	end;
parse_opts(P,[{queue,Q}|T]) ->
	parse_opts(P#dp{callqueue = Q},T);
parse_opts(P,[{wasync,W}|T]) ->
	parse_opts(P#dp{wasync = W},T);
parse_opts(P,[{rasync,R}|T]) ->
	parse_opts(P#dp{rasync = R},T);
parse_opts(P,[{flags,F}|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor F},T);
parse_opts(P,[create|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_CREATE},T);
parse_opts(P,[actornum|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_ACTORNUM},T);
parse_opts(P,[exists|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_EXISTS},T);
parse_opts(P,[noverify|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_NOVERIFY},T);
parse_opts(P,[test|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_TEST},T);
parse_opts(P,[lock|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_STARTLOCK},T);
parse_opts(P,[nohibernate|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_NOHIBERNATE},T);
parse_opts(P,[wait_election|T]) ->
	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_WAIT_ELECTION},T);
% parse_opts(P,[no_election_timeout|T]) ->
% 	parse_opts(P#dp{flags = P#dp.flags bor ?FLAG_NO_ELECTION_TIMEOUT},T);
parse_opts(_,[nostart|_]) ->
	{stop,nostart};
parse_opts(P,[_|T]) ->
	parse_opts(P,T);
parse_opts(P,[]) ->
	Name = {P#dp.actorname,P#dp.actortype},
	case distreg:reg(self(),Name) of
		ok ->
			case apply(P#dp.cbmod,cb_path,[P#dp.cbstate,P#dp.actorname,P#dp.actortype]) of
				Pth when is_list(Pth) ->
					DbPath = lists:flatten(Pth)++
						butil:encode_percent(butil:tolist(P#dp.actorname))++"."++
						butil:encode_percent(butil:tolist(P#dp.actortype));
				DbPath ->
					ok
			end,
			P#dp{dbpath = DbPath, activity = actor_start(P),
				netchanges = actordb_local:net_changes(),
				cbstate = apply(P#dp.cbmod,cb_init_engine,[P#dp.cbstate])};
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
	case actordb_local:elapsed_time()-H#lck.time > 3000 of
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
	case actordb_local:elapsed_time()-LastTry > 1000*2.5 of
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
			case actordb:rpc(Node,NewActor,{actordb_sqlproc,call,[{NewActor,P#dp.actortype},[{lockinfo,wait},lock],
					{dbcopy,{start_receive,Msg,Ref}},P#dp.cbmod]}) of
				ok ->
					?INF("Retry copy now"),
					SDB = {send_db,{Node,Ref,IsMove,NewActor}},
					{reply,_,NP1} = actordb_sqlprocutil:dbcopy_call(SDB,undefined,P),
					NP1#dp{copylater = undefined};
				Err ->
					?INF("retry_copy in 3s, err=~p",[Err]),
					erlang:send_after(3000,self(),retry_copy),
					P#dp{copylater = {actordb_local:elapsed_time(),Copy}}
			end;
		false ->
			?INF("retry_copy in 3s",[]),
			erlang:send_after(3000,self(),retry_copy),
			P
	end.

% Initialize call on node that is source of copy
dbcopy_call({send_db,{Node,Ref,IsMove,ActornameToCopyto}},_CallFrom,P) ->
	% Send database to another node.
	% This gets called from that node.
	true = P#dp.verified,
	?DBG("senddb myname, remotename ~p info ~p, copyto already ~p",
			[ActornameToCopyto,{Node,Ref,IsMove},P#dp.dbcopy_to]),
	Me = self(),
	case lists:keyfind(Ref,#cpto.ref,P#dp.dbcopy_to) of
		false ->
			Db = P#dp.db,
			{Pid,_} = spawn_monitor(fun() ->
					dbcopy_start(P#dp{dbcopy_to = Node, dbcopyref = Ref},Me,ActornameToCopyto) end),
			erlang:send_after(1000,self(),copy_timer),
			{reply,{ok,Ref},P#dp{db = Db,
					dbcopy_to = [#cpto{node = Node, pid = Pid, ref = Ref, ismove = IsMove,
					actorname = ActornameToCopyto}|P#dp.dbcopy_to]}};
		{_,_Pid,Ref,_} ->
			?DBG("senddb already exists with same ref!"),
			{reply,{ok,Ref},P}
	end;
dbcopy_call({reached_end,{FromPid,Ref,Evnum}},_,P) ->
	erlang:send_after(1000,self(),check_locks),
	?DBG("dbcopy reached_end? my=~p, in=~p",[P#dp.evnum, Evnum]),
	?DBG("actor_info ~p",[actordb_sqlite:actor_info(P)]),
	{reply,P#dp.evnum > Evnum,P#dp{locked = butil:lists_add(#lck{pid = FromPid,ref = Ref},P#dp.locked)}};
% Initial call on node that is destination of copy
dbcopy_call({start_receive,Copyfrom,Ref},_,P) ->
	?DBG("start receive entire db ~p ~p",[Copyfrom,P#dp.db]),
	case P#dp.db of
		_ when P#dp.db == queue ->
			Db = P#dp.db;
		_ when element(1,P#dp.db) == actordb_driver ->
			N = actordb_sqlite:wal_rewind(P,0),
			?DBG("Start receive rewinding data result=~p",[N]),
			Db = P#dp.db;
		_ when P#dp.db == undefined ->
			{ok,Db,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
			N = actordb_sqlite:wal_rewind(Db,0),
			?DBG("Start receive open and rewind data result=~p",[N])
	end,
	% If move, split or copy actor to a new actor this must be called on master.
	case is_tuple(Copyfrom) of
		true when P#dp.mors /= master ->
			redirect_master(P);
		_ ->
			{ok,RecvPid} = start_copyrec(P#dp{db = Db, copyfrom = Copyfrom, dbcopyref = Ref}),
			{reply,ok,P#dp{db = Db, mors = slave,dbcopyref = Ref,
				copyfrom = Copyfrom, copyproc = RecvPid, election_timer = undefined}}
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
					actordb_sqlproc:write_call(#write{sql = {moved,LC#lck.node}},CallFrom,
						P#dp{locked = WithoutLock,dbcopy_to = DbCopyTo});
				_ ->
					case LC#lck.ismove of
						{split,{M,F,A}} ->
							case apply(M,F,[P#dp.cbstate,split|A]) of
								{ok,Sql} ->
									NS = P#dp.cbstate;
								{ok,Sql,NS} ->
									ok
							end,
							WriteMsg = #write{sql = [Sql,<<"DELETE FROM __adb WHERE id=">>,(?COPYFROM),";"]},
							actordb_sqlproc:write_call(WriteMsg,CallFrom,P#dp{locked = WithoutLock,
								dbcopy_to = DbCopyTo,
								cbstate = NS});
						_ ->
							?DBG("Copy done at evnum=~p",[P#dp.evnum]),
							NP = P#dp{locked = WithoutLock, dbcopy_to = DbCopyTo},
							case LC#lck.actorname == P#dp.actorname of
								true ->
									case lists:keyfind(LC#lck.node,#flw.node,P#dp.followers) of
										Flw when is_tuple(Flw) ->
											{noreply,NP1} = actordb_sqlproc:write_call(#write{sql = <<>>},CallFrom,NP),
											{reply,ok,reply_maybe(store_follower(NP1,
												Flw#flw{match_index = P#dp.evnum, match_term = P#dp.evterm,
														next_index = P#dp.evnum+1})), 0};
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
									ismove = Cpto#cpto.ismove, time = actordb_local:elapsed_time()},
					dbcopy_call({unlock,Data},CallFrom,
							P#dp{locked = lists:keystore(LC#lck.ref,#lck.ref,P#dp.locked,NLC)})
			end
	end.


dbcopy_start(P,Home,ActorTo) ->
	?DBG("dbcopy_start, to=~p",[ActorTo]),
	ok = actordb_sqlite:checkpoint_lock(P,1),
	Me = self(),
	spawn(fun() ->
		erlang:monitor(process,Me),
		receive
			{'DOWN',_Ref,_,_Me,_} ->
				actordb_sqlite:checkpoint_lock(P,0)
		end
	end),
	dbcopy(P#dp{evnum = 0, evterm = 0},Home,ActorTo,undefined,[],[],0).

dbcopy_done(P,<<>>,_Home,_ActorTo) ->
	% This should only happen from imports from pre-lmdb versions of actordb
	Param = [{bin,{<<>>,<<>>}},{status,done},{origin,original},{curterm,P#dp.current_term}],
	exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}));
dbcopy_done(P,Head,Home,ActorTo) ->
	{Evterm,Evnum} = read_header(Head),
	?DBG("DBCOPY DONE! term=~p, num=~p",[Evterm,Evnum]),
	case gen_server:call(Home,{dbcopy,{reached_end,{self(),P#dp.dbcopyref,Evnum}}}, infinity) of
		% false = nothing more to send
		false ->
			Param = [{bin,{<<>>,Head}},{status,done},{origin,original},{curterm,P#dp.current_term}],
			exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}));
		% true = continue at evnum where we left off.
		true ->
			dbcopy(P#dp{evterm = Evterm, evnum = Evnum},Home,ActorTo,undefined,[],[],0)
	end.
dbcopy_do(P,Bin) ->
	Param = [{bin,Bin},{status,wal},{origin,original},{curterm,P#dp.current_term}],
	ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}).

dbcopy(P,Home,ActorTo,{iter,_} = Iter, BufHead, BufBin, SizeBuf) ->
	case actordb_sqlite:iterate_db(P,Iter) of
		{ok,Iter1,Bin,Head,Done} when Done == 0, SizeBuf < 1024*128 ->
			dbcopy(P,Home,ActorTo,Iter1,[Head|BufHead],[Bin|BufBin],SizeBuf+byte_size(Bin));
		{ok,Iter1,Bin,Head,Done} when Done > 0; SizeBuf >= 1024*128 ->
			dbcopy_do(P,{lists:reverse([Bin|BufBin]),lists:reverse([Head|BufHead])}),
			case Done of
				0 ->
					dbcopy(P,Home,ActorTo,Iter1,[],[],0);
				_ ->
					dbcopy_done(P,Head,Home,ActorTo)
			end
	end;
dbcopy(P,Home,ActorTo,_F, BufHead,BufBin,SizeBuf) ->
	still_alive(P,Home,ActorTo),
	% Evterm/evnum is set to 0 for first loop around, then to evterm/evnum of last run.
	% Do this as long as there are events to replicate. We have to reach the end sometime.
	case actordb_sqlite:iterate_db(P,P#dp.evterm,P#dp.evnum) of
		{ok,Iter,Bin,Head,Done} when Done == 0 ->
			% dbcopy_do(P,{Bin,Head}),
			dbcopy(P,Home,ActorTo,Iter, [Head|BufHead], [Bin|BufBin],SizeBuf+byte_size(Bin));
		{ok,_Iter,Bin,Head,Done} when Done > 0 ->
			dbcopy_do(P,{Bin,Head}),
			dbcopy_done(P,Head,Home,ActorTo);
		done when BufHead == [], BufBin == [] ->
			dbcopy_done(P,<<>>,Home,ActorTo)
	end.

% Executed on node that is receiving db.
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
			erlang:demonitor(MonRef,[flush]),
			{error,Reason};
		{Ref,Pid,Response} ->
			erlang:demonitor(MonRef,[flush]),
			Response
		after 10000 ->
			erlang:demonitor(MonRef,[flush]),
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
						CN = [bkdcore:name_from_dist_name(Nd) || Nd <- bkdcore:cluster_nodes_connected()],
						case CN of
							[] ->
								ok;
							_ ->
								case (length(CN)+1)*2 > (length(bkdcore:cluster_nodes())+1) of
									true ->
										true;
									false ->
										exit(nomajority)
								end
						end,
						StartOpt = [{actor,P#dp.actorname},{type,P#dp.actortype},{mod,P#dp.cbmod},lock,nohibernate,{slave,true},
									{lockinfo,dbcopy,{P#dp.dbcopyref,P#dp.cbstate,P#dp.copyfrom,P#dp.copyreset}}],
						[{ok,_} = rpc(Nd,{actordb_sqlproc,start_copylock,
									[{P#dp.actorname,P#dp.actortype},StartOpt]}) || Nd <- CN];
					_ ->
						CN = []
				end,
				dbcopy_receive(Home,P,undefined,CN);
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

dbcopy_inject(Db,[Bin|BT],[Header|HT]) ->
	dbcopy_inject(Db,Bin,Header),
	dbcopy_inject(Db,BT,HT);
dbcopy_inject(_,[],[]) ->
	ok;
dbcopy_inject(Db,Bin,Header) ->
	ok = actordb_sqlite:inject_page(Db,Bin,Header).

read_header([Header|_]) ->
	read_header(Header);
read_header(<<Evterm:64,Evnum:64,_/binary>>) ->
	{Evterm,Evnum}.

dbcopy_receive(Home,P,CurStatus,ChildNodes) ->
	receive
		{Ref,Source,Param} when Ref == P#dp.dbcopyref ->
			[{Bin,Header},Status,Origin] = butil:ds_vals([bin,status,origin],Param),
			case Header of
				<<>> ->
					Evterm = Evnum = done;
				_ ->
					{Evterm,Evnum} = read_header(Header)
			end,
			% ?DBG("copy_receive size=~p, origin=~p, status=~p",[byte_size(Bin),Origin,Status]),
			case Origin of
				original ->
					Param1 = [{bin,{Bin,Header}},{status,Status},{origin,master},{evnum,Evnum},{evterm,Evterm}],
					[ok = rpc(Nd,{?MODULE,dbcopy_send,[Ref,Param1]}) || Nd <- ChildNodes];
				master ->
					ok
			end,
			case CurStatus == Status of
				true ->
					?DBG("Inject page evterm=~p evnum=~p",[Evterm,Evnum]),
					ok = dbcopy_inject(P#dp.db,Bin,Header);
				false when Status == wal ->
					?DBG("Opening new at ~p",[P#dp.dbpath]),
					?DBG("Inject page evterm=~p evnum=~p",[Evterm,Evnum]),
					ok = dbcopy_inject(P#dp.db,Bin,Header);
				false when Status == done ->
					SqlSchema = <<"select name, sql from sqlite_master where type='table';">>,
					case actordb_sqlite:exec(P#dp.db,SqlSchema,read) of
						{ok,[{columns,_},{rows,SchemaTables}]} ->
							ok;
						_X ->
							SchemaTables = []
					end,
					Sql = <<"SELECT * FROM __adb where id in (",(?EVNUM)/binary,",",(?EVTERM)/binary,");">>,
					{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(P#dp.db, Sql,read),
					Evnum1 = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
					Evterm1 = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),
					?DBG("Storing evnum=~p, evterm=~p, curterm=~p, hevterm=~p,hevnum=~p",
						[Evnum1,Evterm1,Evterm,Evnum,butil:ds_val(curterm,Param)]),
					store_term(P,undefined,butil:ds_val(curterm,Param,Evterm1),Evnum1,Evterm1),
					case SchemaTables of
						[] ->
							?ERR("DB open after move without schema?",[]),
							actordb_sqlite:wal_rewind(P,0),
							% actordb_sqlite:move_to_trash(P#dp.fullpath),
							exit(copynoschema);
						_ ->
							?DBG("Copyreceive done ~p ~p ~p",[SchemaTables,
								 {Origin,P#dp.copyfrom},actordb_sqlite:exec(P#dp.db,"SELEcT * FROM __adb;")]),
							Source ! {Ref,self(),ok},
							case Origin of
								original ->
									case P#dp.copyfrom of
										{<<_/binary>> = _Node,<<_/binary>> = _ActorName} ->
											ok = actordb_util:reg_actor(P#dp.actorname,P#dp.actortype);
										_ ->
											ok
									end,
									% callback_unlock(Home,Evnum,Evterm,P);
									exit(unlock);
								_ ->
									exit(ok)
							end
					end
			end,
			Source ! {Ref,self(),ok},
			dbcopy_receive(Home,P,Status,ChildNodes);
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
	DBC = {dbcopy,{unlock,P#dp.dbcopyref}},
	case rpc(Node,{actordb_sqlproc,call,[{ActorName,P#dp.actortype},[],DBC,P#dp.cbmod,onlylocal]}) of
		ok ->
			ok;
		false ->
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
