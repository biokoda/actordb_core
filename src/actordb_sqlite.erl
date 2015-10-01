% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_sqlite).
-export([init/1,init/2,init/3,exec/2,exec/3,exec/4,exec/5,exec/6,okornot/1,
exec_async/2,exec_async/3,exec_async/4,exec_async/5,exec_async/6,
stop/1,close/1,checkpoint/2,rollback/1,
lz4_compress/1,lz4_decompress/2,replicate_opts/3,replicate_opts/2,parse_helper/2,
all_tunnel_call/1,tcp_reconnect/0,exec_res/2,exec_res/1,
tcp_connect_async/5,store_prepared_table/2, wal_rewind/2, term_store/3, actor_info/1,replication_done/1,
iterate_close/1, inject_page/3,fsync/1, iterate_db/2, iterate_db/3, checkpoint_lock/2]).
% -include_lib("actordb_core/include/actordb.hrl").
-include_lib("actordb_sqlproc.hrl").

% Interface module to storage/sql engine. Either actordb_driver or queue.

init(Path) ->
	init(Path,wal).
init(Path,JournalMode) ->
	init(Path,JournalMode,actordb_util:hash(Path)).
init(queue,_,_) ->
	{ok,queue,[1],?PAGESIZE};
init(Path,JournalMode,Thread) ->
			% "$PRAGMA cache_size=",(butil:tobin(?DEF_CACHE_PAGES))/binary,";"
	Sql = <<"select name, sql from sqlite_master where type='table';">>,
	case actordb_driver:open(Path,Thread,Sql,JournalMode) of
		{ok,Db,{ok,[[{columns,_},{rows,Tables}]]}} ->
			{ok,Db,Tables,?PAGESIZE};
		Err ->
			?AERR("Unable to open sqlite file at ~p error ~p",[Path,Err]),
			{error,Err}
	end.

wal_rewind(P,Evnum) when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:wal_rewind(P#dp.db,Evnum);
wal_rewind(#dp{dbpath = queue} = P,Evnum) ->
	actordb_queue:cb_wal_rewind(P#dp.cbstate,Evnum);
wal_rewind(Db,Evnum) when element(1,Db) == actordb_driver ->
	actordb_driver:wal_rewind(Db,Evnum).

term_store(P,CurrentTerm,VotedFor) when element(1,P#dp.db) == actordb_driver ->
	ok = actordb_driver:term_store(P#dp.db, CurrentTerm, VotedFor);
term_store(Db,CurrentTerm,VotedFor) when element(1,Db) == actordb_driver ->
	ok = actordb_driver:term_store(Db, CurrentTerm, VotedFor);
term_store(P, CurrentTerm, VotedFor) when is_list(P#dp.dbpath) ->
	ok = actordb_driver:term_store(P#dp.dbpath, CurrentTerm, VotedFor,actordb_util:hash(P#dp.dbpath)).

actor_info(P) when is_list(P#dp.dbpath) ->
	actordb_driver:actor_info(P#dp.dbpath,actordb_util:hash(P#dp.dbpath));
actor_info(#dp{dbpath = queue} = P) ->
	actordb_queue:cb_actor_info(P#dp.cbstate);
actor_info(Pth) when is_list(Pth) ->
	actordb_driver:actor_info(Pth,actordb_util:hash(Pth)).

lz4_compress(Bin) ->
	actordb_driver:lz4_compress(Bin).
lz4_decompress(Bin,Size) ->
	actordb_driver:lz4_decompress(Bin,Size).

replicate_opts(P,Bin,Type) when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:replicate_opts(P#dp.db,Bin,Type);
replicate_opts(#dp{dbpath = queue} = P, Bin, Type) ->
	actordb_queue:cb_replicate_opts(P#dp.cbstate, Bin, Type);
replicate_opts(Db,Bin,Type) when element(1,Db) == actordb_driver ->
	actordb_driver:replicate_opts(Db,Bin,Type).

replicate_opts(P,Bin) when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:replicate_opts(P#dp.db,Bin);
replicate_opts(#dp{dbpath = queue} = P, Bin) ->
	actordb_queue:cb_replicate_opts(P#dp.cbstate, Bin);
replicate_opts(Db,Bin) when element(1,Db) == actordb_driver ->
	actordb_driver:replicate_opts(Db,Bin).

replication_done(P) when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:replication_done(P#dp.db);
replication_done(#dp{dbpath = queue} = P) ->
	actordb_queue:cb_replication_done(P#dp.cbstate);
replication_done(Db) when element(1,Db) == actordb_driver ->
	actordb_driver:replication_done(Db).

parse_helper(Bin,Offset) ->
	actordb_driver:parse_helper(Bin,Offset).

all_tunnel_call(Bin) ->
	actordb_driver:all_tunnel_call(Bin).

tcp_reconnect() ->
	actordb_driver:tcp_reconnect().

tcp_connect_async(IP,Port,Bin,Pos,Type) ->
	actordb_driver:tcp_connect_async(IP,Port,Bin,Pos,Type).

store_prepared_table(Vers,Sqls) ->
	actordb_driver:store_prepared_table(Vers,Sqls).

rollback(P) when element(1,P#dp.db) == actordb_driver ->
	rollback(P#dp.db);
rollback(Db) when element(1,Db) == actordb_driver ->
	okornot(exec(Db,<<"ROLLBACK;">>)).

checkpoint(P,Pos)  when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:checkpoint(P#dp.db,Pos);
checkpoint(Db,Pos) when element(1,Db) == actordb_driver ->
	actordb_driver:checkpoint(Db,Pos);
checkpoint(#dp{db = queue} = _P, _) ->
	ok;
checkpoint(queue, _) ->
	ok.

checkpoint_lock(P,N)  when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:checkpoint_lock(P#dp.db,N);
checkpoint_lock(Db,N) when element(1,Db) == actordb_driver ->
	actordb_driver:checkpoint_lock(Db,N);
checkpoint_lock(#dp{db = queue} = _P,_)  ->
	ok;
checkpoint_lock(queue,_) ->
	ok.

iterate_db(P,Iter)  when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:iterate_db(P#dp.db,Iter);
iterate_db(Db,Iter) when element(1,Db) == actordb_driver ->
	actordb_driver:iterate_db(Db,Iter).

iterate_db(P,Term,Evnum)  when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:iterate_db(P#dp.db,Term,Evnum);
iterate_db(Db,Term,Evnum) when element(1,Db) == actordb_driver ->
	actordb_driver:iterate_db(Db,Term,Evnum).

iterate_close(I) when element(1,I) == iter ->
	actordb_driver:iterate_close(I).

inject_page(P,Bin,Header) when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:inject_page(P#dp.db,Bin,Header);
inject_page(#dp{dbpath = queue} = P, Bin, Header) ->
	actordb_queue:cb_inject_page(P#dp.cbstate,Bin,Header);
inject_page(Db,Bin,Header) when element(1,Db) == actordb_driver ->
	actordb_driver:inject_page(Db,Bin,Header).

fsync(P) when element(1,P#dp.db) == actordb_driver ->
	actordb_driver:fsync(P#dp.db);
fsync(#dp{dbpath = queue} = P) ->
	actordb_queue:cb_fsync(P#dp.cbstate);
fsync(Db) when element(1,Db) == actordb_driver ->
	actordb_driver:fsync(Db).

close(Db) ->
	stop(Db).
stop(undefined) ->
	ok;
stop(P) when element(1,P#dp.db) == actordb_driver ->
	stop(P#dp.db);
stop(Db) when element(1,Db) == actordb_driver ->
	actordb_driver:close(Db),
	ok;
stop(_) ->
	ok.

exec(Db,Sql,read) ->
	actordb_local:report_read(),
	% exec(Db,S);
	Res = actordb_driver:exec_read(Sql,Db,actordb_conf:query_timeout()),
	exec_res(Res,Sql);
exec(Db,S,write) ->
	actordb_local:report_write(),
	exec(Db,S);
exec(Db,Sql,Records) ->
	Res = actordb_driver:exec_script(Sql,Records,Db,actordb_conf:query_timeout()),
	exec_res(Res,Sql).
exec(Db,Sql,Records,read) ->
	actordb_local:report_write(),
	% exec(Db,S,Records);
	Res = actordb_driver:exec_read(Sql,Records,Db,actordb_conf:query_timeout()),
	exec_res(Res,Sql);
exec(Db,S,Records,write) ->
	actordb_local:report_write(),
	exec(Db,S,Records).
exec(Db,Sql) ->
	Res = actordb_driver:exec_script(Sql,Db,actordb_conf:query_timeout()),
	exec_res(Res,Sql).
exec(Db,Sql,Evterm,Evnum,VarHeader) ->
	actordb_local:report_write(),
	Res =  actordb_driver:exec_script(Sql,Db,actordb_conf:query_timeout(),Evterm,Evnum,VarHeader),
	exec_res(Res,Sql).
exec(Db,Sql,[],Evterm,Evnum,VarHeader) ->
	exec(Db,Sql,Evterm,Evnum,VarHeader);
exec(Db,Sql,Records,Evterm,Evnum,VarHeader) ->
	actordb_local:report_write(),
	Res =  actordb_driver:exec_script(Sql,Records,Db,actordb_conf:query_timeout(),Evterm,Evnum,VarHeader),
	exec_res(Res,Sql).

exec_async(Db,Sql,read) ->
	actordb_local:report_read(),
	actordb_driver:exec_read_async(Sql,Db);
exec_async(Db,S,write) ->
	actordb_local:report_write(),
	exec_async(Db,S);
exec_async(Db,Sql,Records) ->
	actordb_driver:exec_script_async(Sql,Records,Db).
exec_async(Db,Sql,Records,read) ->
	actordb_local:report_write(),
	actordb_driver:exec_read_async(Sql,Records,Db);
exec_async(Db,S,Records,write) ->
	actordb_local:report_write(),
	exec_async(Db,S,Records).
exec_async(Db,Sql) ->
	actordb_driver:exec_script_async(Sql,Db).
exec_async(Db,Sql,Evterm,Evnum,VarHeader) ->
	actordb_local:report_write(),
	actordb_driver:exec_script_async(Sql,Db,Evterm,Evnum,VarHeader).
exec_async(Db,Sql,[],Evterm,Evnum,VarHeader) ->
	exec_async(Db,Sql,Evterm,Evnum,VarHeader);
exec_async(Db,Sql,Records,Evterm,Evnum,VarHeader) ->
	actordb_local:report_write(),
	actordb_driver:exec_script_async(Sql,Records,Db,Evterm,Evnum,VarHeader).

exec_res(Res) ->
	exec_res(Res,[]).
exec_res(Res,Sql) ->
	case Res of
		{ok,[[{columns,_},_] = Res1]} ->
			{ok,Res1};
		{ok,[{changes,Id,Rows}]} ->
			{ok,{changes,Id,Rows}};
		{ok,[ok]} ->
			ok;
		{ok,[]} ->
			ok;
		{ok,T} when is_tuple(T) ->
			{ok,T};
		{error,Msg} ->
			{sql_error,Msg,Sql};
		ok ->
			ok;
		X ->
			X
	end.


okornot(Res) ->
	case Res of
		ok ->
			ok;
		{changes,_} ->
			ok;
		{ok,_} ->
			ok;
		{sql_error,Err,_Sql} ->
			?ADBG("Sql failed ~p ~p",[Err,_Sql]),
			{sql_error,Err,_Sql};
		{sql_error,Err} ->
			{sql_error,Err,<<>>}
	end.
