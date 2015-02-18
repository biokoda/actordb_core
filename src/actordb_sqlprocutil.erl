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

ae_respond(P,LeaderNode,Success,PrevEvnum,AEType,CallCount) ->
	Resp = {appendentries_response,actordb_conf:node_name(),P#dp.current_term,Success,P#dp.evnum,P#dp.evterm,PrevEvnum,AEType,CallCount},
	bkdcore_rpc:cast(LeaderNode,{actordb_sqlproc,call,[{P#dp.actorname,P#dp.actortype},[nostart],
									{state_rw,Resp},P#dp.cbmod]}).

append_wal(P,Header,Bin1) ->
	Bin = actordb_sqlite:lz4_decompress(Bin1,?PAGESIZE),
	case element(1,P#dp.db) of
		actordb_driver ->
			actordb_driver:inject_page(P#dp.db,Bin,Header);
		_ ->
			case file:write(P#dp.db,[Header,Bin]) of
				ok ->
					ok;
				Err ->
					?AERR("Append wal failed ~p, state ~p",[Err,P]),
					throw(cant_append)
			end
	end.

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
									Recs = [[[?MOVEDTOI,<<"$deleted$">>]]],
									Sql1 = <<"#s02;">>;
								_ ->
									Recs = [],
									Sql1 = Sql
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
							Records = Recs++[[[?EVNUMI,butil:tobin(EvNumNew)],[?EVTERMI,butil:tobin(P#dp.current_term)]]],
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
					actordb_sqlprocutil:delete_actor(P);
					% spawn(fun() -> actordb_sqlproc:stop(Me) end);
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
					%Me = self(),
					actordb_sqlprocutil:delete_actor(P);
					% spawn(fun() -> actordb_sqlproc:stop(Me) end);
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
	case filelib:file_size(P#dp.fullpath) of
		?PAGESIZE ->
			{ok,Dbfile} = prim_file:read_file(P#dp.fullpath),
			{Compressed,CompressedSize} = actordb_sqlite:lz4_compress(Dbfile),
			<<DbCompressed:CompressedSize/binary,_/binary>> = Compressed,
			term_to_binary({P#dp.current_term,actordb_conf:node_name(),
									P#dp.evnum,P#dp.evterm,follower_call_counts(P),DbCompressed});
		Size ->
			?ERR("DB not pagesize, can not replicate the base db ~p",[Size]),
			create_var_header(P)
	end.

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
	F#flw{wait_for_response_since = make_ref()}.

reopen_db(#dp{mors = master} = P) ->
	Driver = actordb_conf:driver(),
	case ok of
		_ when Driver == actordb_driver, P#dp.db == undefined  ->
			init_opendb(P);
		_ when Driver == actordb_driver ->
			case actordb_sqlite:exec(P#dp.db,<<"SeLECT * FROM __adb;">>,read) of
				{ok,[{columns,_},{rows,[_|_] = Rows}]} ->
					read_db_state(P,Rows);
				_XX ->
					?DBG("Read __adb result ~p",[_XX]),
					P
			end;
		% we are master but db not open or open as file descriptor to -wal file
		_ when element(1,P#dp.db) == file_descriptor; P#dp.db == undefined ->
			file:close(P#dp.db),
			garbage_collect(),
			init_opendb(P);
		_ ->
			case P#dp.wal_from == {0,0} of
				true ->
					P#dp{wal_from = wal_from([P#dp.fullpath,"-wal"])};
				false ->
					P
			end
	end;
reopen_db(P) ->
	Driver = actordb_conf:driver(),
	case ok of
		_ when Driver == actordb_driver, P#dp.db == undefined  ->
			NP = init_opendb(P),
			actordb_sqlite:replicate_opts(NP#dp.db,<<>>),
			NP;
		_ when Driver == actordb_driver ->
			actordb_sqlite:replicate_opts(P#dp.db,<<>>),
			P;
		_ when element(1,P#dp.db) == connection; P#dp.db == undefined ->
			actordb_sqlite:stop(P#dp.db),
			{ok,F} = file:open([P#dp.fullpath,"-wal"],[read,write,binary,raw]),
			case file:position(F,eof) of
				{ok,0} ->
					ok = file:write(F,actordb_sqlite:make_wal_header(?PAGESIZE));
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
				wal_from = wal_from([P#dp.fullpath,"-wal"]),
				evterm = EvTerm,
				movedtonode = MovedToNode}).

actually_delete(P) ->
	% Delete just means adding deleted flag. But if we get a query with create, this means
	% we must delete all data. Drop everything except __adb. This way evnum and evterm continue where they left off.
	{ok,[{columns,_},{rows,Tables}]} = actordb_sqlite:exec(P#dp.db,<<"SELECT NAME FROM sqlite_master WHERE type='table';">>,read),
	Drops = [<<"$DROP TABLE ",Name/binary,";">> || {Name} <- Tables, Name /= <<"__adb">>],
	?DBG("Drop tables in deleted=~p",[Drops]),
	#write{sql = ["$INSERT OR REPLACE INTO __adb (id,val) VALUES (",?MOVEDTO,",'');",
				  "$INSERT OR REPLACE INTO __adb (id,val) VALUES (",?BASE_SCHEMA_VERS,",0);",Drops], records = []}.

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

% Find first valid evnum,evterm in wal (from beginning)
wal_from([_|_] = Path) ->
	case actordb_conf:driver() of
		actordb_driver ->
			{0,0};
		_ ->
			case file:open(Path,[read,binary,raw]) of
				{ok,F} ->
					{ok,_} = file:position(F,32),
					wal_from(F);
				_ ->
					{0,0}
			end
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
	case F#flw.file of
		{iter,_} ->
			ok;
		_ ->
			file:close(F#flw.file)
	end,
	try_wal_recover(P,F#flw{file = undefined});
try_wal_recover(#dp{wal_from = {0,0}} = P,F) ->
	case actordb_conf:driver() of
		actordb_driver ->
			case F#flw.match_index of
				0 ->
					Evnum = 1;
				Evnum ->
					ok
			end,
			case actordb_driver:iterate_wal(P#dp.db,Evnum) of
				{ok,Iter2,Bin,_IsActiveWal} ->
					NF = F#flw{file = Iter2,pagebuf = Bin},
					{true,store_follower(P,NF),NF};
				done ->
					{false,P,F}
			end;
		_ ->
			case wal_from([P#dp.fullpath,"-wal"]) of
				{0,0} ->
					{false,store_follower(P,F),F};
				WF ->
					try_wal_recover(P#dp{wal_from = WF},F)
			end
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
			?INF("Too far behind, can not recover from wal match=~p, wal_from=~p, evnum=~p, sending entire db to node.",
					[F#flw.match_index,WalEvfrom,P#dp.evnum]),
			Res = false,
			NF = F
	end,
	{Res,store_follower(P,NF),NF}.

open_wal_at(P,Index) ->
	{ok,F} = file:open([P#dp.fullpath,"-wal"],[read,binary,raw]),
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
			?DBG("Sending AE start on evnum=~p",[F#flw.next_index]),
			StartRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
				{state_rw,{appendentries_start,P#dp.current_term,actordb_conf:node_name(),
							F#flw.match_index,F#flw.match_term,recover,{F#flw.match_index,F#flw.match_term}}}]}),
			case StartRes of
				ok ->
					% Send wal
					case catch send_wal(P,F) of
						wal_corruption ->
							store_follower(P,F#flw{wait_for_response_since = make_ref()});
						error ->
							?ERR("Error iterating wal"),
							store_follower(P,F#flw{wait_for_response_since = make_ref()});
						NF when element(1,NF) == flw ->
							?DBG("Sent AE on evnum=~p",[F#flw.next_index]),
							store_follower(P,NF#flw{wait_for_response_since = make_ref()})
					end;
				% to be continued in appendentries_response
				_ ->
					file:close(F#flw.file),
					store_follower(P,F#flw{file = undefined, pagebuf = <<>>, wait_for_response_since = make_ref()})
			end;
		% Follower uptodate, close file if open
		false when F#flw.file == undefined ->
			store_follower(P,F);
		false ->
			case element(1,F#flw.file) of
				iter ->
					actordb_driver:iterate_close(F#flw.file);
				_ ->
					file:close(F#flw.file)
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
			case actordb_driver:iterate_wal(P#dp.db,F#flw.file) of
				{ok,Iter,<<Header:144/binary,Page/binary>>,_IsLastWal} ->
					ok;
				done ->
					?ERR("Detected disk corruption while trying to replicate to stale follower. Stepping down as leader."),
					throw(wal_corruption),
					Iter = Header = Page = undefined
			end;
		<<Header:144/binary,Page/binary>> ->
			Iter = F#flw.file
	end,
	<<_:32,Commit:32,_/binary>> = Header,
	{Compressed,CompressedSize} = actordb_sqlite:lz4_compress(Page),
	<<PageCompressed:CompressedSize/binary,_/binary>> = Compressed,
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
	end;
send_wal(P,#flw{file = File} = F) ->
	{ok,<<Header:40/binary,Page/binary>>} = file:read(File,40+?PAGESIZE),
     {HC1,HC2} = actordb_sqlite:wal_checksum(Header,0,0,32),
     {C1,C2} = actordb_sqlite:wal_checksum(Page,HC1,HC2,byte_size(Page)),
	case Header of
		<<_:32,Commit:32,Evnum:64/big-unsigned,_:64/big-unsigned,
		  _:32,_:32,Chk1:32/unsigned-big,Chk2:32/unsigned-big,_/binary>> when Evnum == F#flw.next_index ->
			case ok of
				_ when C1 == Chk1, C2 == Chk2 ->
					{Compressed,CompressedSize} = actordb_sqlite:lz4_compress(Page),
					<<PageCompressed:CompressedSize/binary,_/binary>> = Compressed,
					WalRes = bkdcore:rpc(F#flw.node,{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
								{state_rw,{appendentries_wal,P#dp.current_term,Header,PageCompressed,recover,{F#flw.match_index,F#flw.match_term}}},
								[nostart]]}),
					case WalRes == ok orelse WalRes == done of
						true when Commit == 0 ->
							send_wal(P,F);
						true ->
							F;
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
rewind_wal(P) when element(1,P#dp.db) == actordb_driver ->
	case actordb_driver:wal_rewind(P#dp.db,P#dp.evnum) of
		{ok,0} ->
			P;
		{ok,_} ->
			Sql = <<"SElECT * FROM __adb where id in (",(?EVNUM)/binary,",",(?EVTERM)/binary,");">>,
			{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(P#dp.db, Sql,read),
			Evnum = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
			EvTerm = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),
			P#dp{evnum = Evnum, evterm = EvTerm}
	end;
rewind_wal(P) ->
	?DBG("Rewinding wal"),
	case file:position(P#dp.db,{cur,-(?PAGESIZE+40)}) of
		{ok,_NPos} ->
			{ok,<<_:32,Commit:32,Evnum:64/unsigned-big,Evterm:64/unsigned-big,_/binary>>} = file:read(P#dp.db,40),
			case ok of
				_ when P#dp.evnum /= Evnum, Commit /= 0 ->
					?DBG("Rewind at ~p",[Evnum]),
					{ok,_} = file:position(P#dp.db,{cur,?PAGESIZE}),
					file:truncate(P#dp.db),
					P#dp{evnum = Evnum, evterm = Evterm};
				_ ->
					{ok,_} = file:position(P#dp.db,{cur,-40}),
					rewind_wal(P)
			end;
		{error,_} ->
			?DBG("Resetting to zero"),
			file:close(P#dp.db),
			file:delete([P#dp.fullpath,"-wal"]),
			% rewind to 0, causing a complete restore from another node
			reopen_db(P#dp{evnum = 0, evterm = 0, db = undefined})
	end.

save_term(P) ->
	% ok = butil:savetermfile([P#dp.fullpath,"-term"],{P#dp.voted_for,P#dp.current_term,P#dp.evnum,P#dp.evterm}),
	%ok = actordb_termstore:store_term_info(P#dp.actorname,P#dp.actortype,P#dp.voted_for,P#dp.current_term,P#dp.evnum,P#dp.evterm),
	store_term(P,P#dp.voted_for,P#dp.current_term,P#dp.evnum,P#dp.evterm),
	P.
store_term(P,VotedFor,CurrentTerm,Evnum,EvTerm) ->
	case actordb_conf:termdb() of
		false ->
			ok = butil:savetermfile([P#dp.fullpath,"-term"],{VotedFor,CurrentTerm,Evnum,EvTerm});
		true ->
			ok = actordb_termstore:store_term_info(P#dp.actorname,P#dp.actortype,VotedFor,CurrentTerm,Evnum,EvTerm)
	end.

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
	DefVals = [[[?SCHEMA_VERSI,SchemaVers],[?ATYPEI,Type],[?EVNUMI,0],[?EVTERMI,0]|Moved]],
	{[<<"$CREATE TABLE IF NOT EXISTS __transactions (id INTEGER PRIMARY KEY, tid INTEGER,",
		 	" updater INTEGER, node TEXT,schemavers INTEGER, sql TEXT);",
		 "$CREATE TABLE IF NOT EXISTS __adb (id INTEGER PRIMARY KEY, val TEXT);">>,
		 "#s02;"],DefVals}.
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
	Latency = butil:ds_val(latency,latency),
	Fixed = max(300,Latency),
	T = Fixed+random:uniform(Fixed),
	?ADBG("Relection try in ~p, replication latency ~p",[T,Latency]),
	erlang:send_after(T,self(),{doelection,Latency,make_ref()});
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
check_for_resync(P, [F|L],Action) when F#flw.match_index == P#dp.evnum, F#flw.wait_for_response_since == undefined ->
	check_for_resync(P,L,Action);
check_for_resync(_,[],Action) ->
	Action;
check_for_resync(P,[F|L],_Action) ->
	IsAlive = lists:member(F#flw.distname,nodes()),
	case F#flw.wait_for_response_since of
		undefined ->
			Wait = 0;
		_ ->
			Wait = actordb_local:min_ref_age(F#flw.wait_for_response_since)
	end,
	case F#flw.last_seen of
		undefined ->
			LastSeen = 30000;
		_ ->
			LastSeen = actordb_local:min_ref_age(F#flw.last_seen)
	end,
	case ok of
		_ when Wait > 1000, IsAlive ->
			resync;
		_ when LastSeen > 1000, F#flw.match_index /= P#dp.evnum, IsAlive ->
			resync;
		_ when IsAlive == false ->
			check_for_resync(P,L,wait_longer);
		_ ->
			check_for_resync(P,L,timer)
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
					% ok = butil:savetermfile([P#dp.fullpath,"-term"],{actordb_conf:node_name(),CurrentTerm,P#dp.evnum,P#dp.evterm}),
					% ok = actordb_termstore:store_term_info(P#dp.actorname,P#dp.actortype,actordb_conf:node_name(),
					%	 CurrentTerm,P#dp.evnum,P#dp.evterm),
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
								masternodedist = DistName, 
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
			Records = Records1++CallRecords++[[[?ANUMI,butil:tobin(ActorNum)]]],
			Sql = [SqlIn,BS,Schema,CallWrite,
					"#s02;"
					];
		_ ->
			Records = [],
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
	{NP#dp{callqueue = CQ, flags = Flags},Sql,Records,Callfrom};
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
	{P#dp{copyfrom = undefined, copyreset = undefined, movedtonode = MovedToNode, cbstate = NS},Sql,[],Callfrom};
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

% delactorfile(P) ->
% 	[Pid ! delete || {_,Pid,_,_} <- P#dp.dbcopy_to],
% 	?DBG("delfile master=~p, ismoved=~p",[P#dp.mors,P#dp.movedtonode]),
% 	% Term files are not deleted. This is because of deleted actors. If a node was offline
% 	%  when an actor was deleted, then the actor was created anew still while offline, 
% 	%  this will keep the term and evnum number higher than that old file and raft logic will overwrite that data.
% 	save_term(P),
% 	case P#dp.movedtonode of
% 		undefined ->
% 			case actordb_conf:driver() of
% 				actordb_driver ->
% 					actordb_driver:delete_actor(P#dp.db),
% 					ok;
% 				_ ->
% 					file:delete(P#dp.fullpath),
% 					file:delete(P#dp.fullpath++"-wal"),
% 					file:delete(P#dp.fullpath++"-shm")
% 			end;
% 		_ ->
% 			% Leave behind redirect marker.
% 			% Create a file with "1" attached to end
% 			{ok,Db,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath++"1",off),
% 			{Sql,Records} = base_schema(0,P#dp.actortype,P#dp.movedtonode),
% 			ok = actordb_sqlite:okornot(actordb_sqlite:exec(Db,[<<"BEGIN;">>,Sql,<<"COMMIT;">>],Records,write)),
% 			actordb_sqlite:stop(Db),
% 			% Rename into the actual dbfile (should be atomic op)
% 			ok = file:rename(P#dp.fullpath++"1",P#dp.fullpath),
% 			file:delete(P#dp.fullpath++"-wal"),
% 			% file:delete(P#dp.fullpath++"-term"),
% 			file:delete(P#dp.fullpath++"-shm")
% 	end.

checkpoint(P) ->
	case actordb_conf:driver() of
		actordb_driver ->
			P;
		_ ->
			{_,NPages} = esqlite3:wal_pages(P#dp.db),
			DbSize = NPages*(?PAGESIZE+40),
			case DbSize > 1024*1024 andalso P#dp.dbcopyref == undefined andalso P#dp.dbcopy_to == [] of
				true ->
					NotSynced = lists:foldl(fun(F,Count) ->
						case F#flw.match_index /= P#dp.evnum of
							true ->
								Count+1;
							false ->
								Count
						end 
					end,0,P#dp.follower_indexes),
					case NotSynced of
						0 ->
							P#dp{wal_from = do_checkpoint(P)};
						% If nodes arent synced, tolerate 30MB of wal size.
						_ when DbSize >= 1024*1024*30 ->
							P#dp{wal_from = do_checkpoint(P)};
						_ ->
							P
					end;
				false ->
					P
			end
	end.

do_checkpoint(P) ->
	case actordb_conf:driver() of
		actordb_driver ->
			{0,0};
		_ ->
			% Only do one check point at a time.
			case catch register(checkpointer,self()) of
				true ->
					?DBG("Executing checkpoint ~p",[P#dp.mors]),
					case P#dp.mors of
						master ->
							actordb_sqlite:checkpoint(P#dp.db),
							save_term(P),
							unregister(checkpointer),
							[bkdcore_rpc:cast(F#flw.node,
										{actordb_sqlproc,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{state_rw,checkpoint}]})
									 || F <- P#dp.follower_indexes, F#flw.match_index == P#dp.evnum];
						_ ->
							{ok,Db,_SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
							actordb_sqlite:checkpoint(Db),
							unregister(checkpointer),
							save_term(P),
							actordb_sqlite:stop(Db)
					end,
					{0,0};
				_ ->
					?DBG("Delaying checkpoint ~p",[P#dp.mors]),
					case P#dp.mors of
						master ->
							ok;
						_ ->
							erlang:send_after(100,self(),do_checkpoint)
					end,
					P#dp.wal_from
			end
	end.

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
			case actordb_conf:driver() of
				actordb_driver ->
					ThreadNum = actordb_util:hash(DbPath) rem length(actordb_conf:paths()),
					FullPath = lists:nth(ThreadNum+1,actordb_conf:paths())++"/"++DbPath;
				_ ->
					FullPath = DbPath
			end,
			P#dp{dbpath = DbPath,fullpath = FullPath,activity_now = actor_start(P), netchanges = actordb_local:net_changes()};
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
		_ when element(1,P#dp.db) == connection ->
			actordb_sqlite:stop(P#dp.db);
		_ when P#dp.db == undefined ->
			{ok,Db,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
			N = actordb_driver:wal_rewind(Db,0),
			?DBG("Start receive open and rewind data result=~p",[N])
	end,
	dbcopy_call({start_receive,Copyfrom,Ref},CF,P#dp{db = cleared});
% Read chunk of wal log.
dbcopy_call({wal_read,From1,Data},_CallFrom,P) ->
	{FromPid,Ref} = From1,
	Size = filelib:file_size([P#dp.fullpath,"-wal"]),
	case ok of
		_ when Data == done orelse Size =< Data -> %when P#dp.transactionid == undefined ->
			erlang:send_after(1000,self(),check_locks),
			{reply,{[P#dp.fullpath,"-wal"],Size,P#dp.evnum,P#dp.current_term},
				P#dp{locked = butil:lists_add(#lck{pid = FromPid,ref = Ref},P#dp.locked)}};
		% true ->
		% 	{noreply,P#dp{callqueue = queue:in_r({CallFrom,{dbcopy,Msg}},P#dp.callqueue)}};
		_ ->
			?DBG("wal_size from=~p, insize=~p, filesize=~p",[From1,Data,Size]), 
			{reply,{[P#dp.fullpath,"-wal"],Size,P#dp.evnum,P#dp.current_term},P}
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

dbcopy_done(P,Home) ->
	gen_server:call(Home,{dbcopy,{wal_read,{self(),P#dp.dbcopyref},done}}),
	Param = [{bin,<<>>},{status,done},{origin,original},{evnum,0},{evterm,0},{curterm,P#dp.current_term}],
	exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]})).
dbcopy_do(P,Bin) ->
	Param = [{bin,Bin},{status,wal},{origin,original},{evnum,0},{evterm,0},{curterm,P#dp.current_term}],
	ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}).

dbcopy(P,Home,ActorTo) ->
	{ok,F} = file:open(P#dp.fullpath,[read,binary,raw]),
	ok = actordb_driver:checkpoint_lock(P#dp.db,1),
	Me = self(),
	spawn(fun() ->
		erlang:monitor(process,Me),
		receive
			{'DOWN',_Ref,_,_Me,_} ->
				actordb_driver:checkpoint_lock(P#dp.db,0)
		end
	end),
	dbcopy(P,Home,ActorTo,F,0,db).
dbcopy(P,Home,ActorTo,{iter,_} = Iter,_Bin,wal) ->
	% case Bin of
	% 	<<>> ->
			case actordb_driver:iterate_wal(P#dp.db,Iter) of
				{ok,Iter1,Bin1,_} ->
					dbcopy_do(P,Bin1),
					dbcopy(P,Home,ActorTo,Iter1,<<>>,wal);
				done ->
					dbcopy_done(P,Home)
			end;
	% 	_ ->
	% 		dbcopy_do(P,Bin),
	% 		dbcopy(P,Home,ActorTo,Iter,<<>>,wal)
	% end;
dbcopy(P,Home,ActorTo,F,Offset,wal) ->
	still_alive(P,Home,ActorTo),
	case actordb_conf:driver() of
		actordb_driver ->
			case actordb_driver:iterate_wal(P#dp.db,Offset) of
				{ok,Iter,Bin,_} ->
					dbcopy_do(P,Bin),
					dbcopy(P,Home,ActorTo,Iter,Iter,wal);
				done ->
					dbcopy_done(P,Home)
			end;
		_ ->
			WR = gen_server:call(Home,{dbcopy,{wal_read,{self(),P#dp.dbcopyref},Offset}}),
			case WR of
				{_Walname,Offset,Evnum,Evterm} ->
					?DBG("dbsend done ",[]),
					Param = [{bin,<<>>},{status,done},{origin,original},{evnum,Evnum},{evterm,Evterm}],
					exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P#dp.dbcopyref,Param]}));
				{_Walname,Walsize,_,_} when Offset > Walsize ->
					?ERR("Offset larger than walsize ~p ~p ~p ~p",[ActorTo,P#dp.actortype,Offset,Walsize]),
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
			end
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
			% ?DBG("copy_receive size=~p, origin=~p, status=~p",[byte_size(Bin),Origin,Status]),
			case Origin of
				original ->
					Param1 = [{bin,Bin},{status,Status},{origin,master},	{evnum,Evnum},{evterm,Evterm}],
					[ok = rpc(Nd,{?MODULE,dbcopy_send,[Ref,Param1]}) || Nd <- ChildNodes];
				master ->
					ok
			end,
			case CurStatus == Status of
				true ->
					case element(1,F) of
						file_descriptor ->
							ok = file:write(F,Bin);
						_ ->
							<<_:8/binary,Evn:64,_/binary>> = Bin,
							?DBG("Inject page evnum ~p",[Evn]),
							ok = actordb_driver:inject_page(F,Bin)
					end,
					F1 = F;
				false when Status == db ->
					file:delete(P#dp.fullpath++"-wal"),
					file:delete(P#dp.fullpath++"-shm"),
					{ok,F1} = file:open(P#dp.fullpath,[write,raw]),
					?DBG("Writing db file ~p",[byte_size(Bin)]),
					ok = file:write(F1,Bin);
				false when Status == wal ->
					ok = file:close(F),
					case actordb_conf:driver() of
						actordb_driver ->
							?DBG("Opening new at ~p",[P#dp.dbpath]),
							{ok,F1,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
							% ok = actordb_sqlite:exec(F1,<<"$CREATE TABLE IF NOT EXISTS __adb (id INTEGER PRIMARY KEY, val TEXT);">>,write),
							<<_:8/binary,Evn:64,_/binary>> = Bin,
							?DBG("Inject page evnum ~p",[Evn]),
							ok = actordb_driver:inject_page(F1,Bin);
						_ ->
							{ok,F1} = file:open(P#dp.fullpath++"-wal",[write,raw]),
							ok = file:write(F1,Bin)
					end;
				false when Status == done ->
					actordb_sqlite:stop(F),
					F1 = undefined,
					case ok of
						_ when element(1,F) == actordb_driver ->
							case actordb_sqlite:exec(F,<<"select name, sql from sqlite_master where type='table';">>,read) of
								{ok,[{columns,_},{rows,SchemaTables}]} ->
									ok;
								_X ->
									SchemaTables = []
							end,
							Db = F;
						_ ->
							{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal)
					end,
					Drv = actordb_conf:driver(),
					case ok of
						_ when Drv == actordb_driver ->
							Sql = <<"SELECT * FROM __adb where id in (",(?EVNUM)/binary,",",(?EVTERM)/binary,");">>,
							{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(Db, Sql,read),
							Evnum1 = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
							Evterm1 = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),
							?DBG("Storing evnum=~p, evterm=~p, curterm=~p",[Evnum1,Evterm1,butil:ds_val(curterm,Param)]),
							% ok = butil:savetermfile([P#dp.fullpath,"-term"],{undefined,butil:ds_val(curterm,Param,Evterm1),Evnum1,Evterm1});
							% ok = actordb_termstore:store_term_info(P#dp.actorname,P#dp.actortype,undefined,
							% 	butil:ds_val(curterm,Param,Evterm1),Evnum1,Evterm1);
							store_term(P,undefined,butil:ds_val(curterm,Param,Evterm1),Evnum1,Evterm1);
						_ when is_integer(Evnum) ->
							% ok = butil:savetermfile([P#dp.fullpath,"-term"],{undefined,Evterm,Evnum,Evterm});
							% ok = actordb_termstore:store_term_info(P#dp.actorname,P#dp.actortype,undefined,
							% 	Evterm,Evnum,Evterm);
							store_term(P,undefined,Evterm,Evnum,Evterm);
						false ->
							ok
					end,
					case SchemaTables of
						[] ->
							?ERR("DB open after move without schema?",[]),
							actordb_sqlite:stop(Db),
							actordb_sqlite:move_to_trash(P#dp.fullpath),
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

