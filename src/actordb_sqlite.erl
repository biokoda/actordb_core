% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sqlite).
-export([init/1,init/2,init/3,exec/2,exec/3,exec/4,exec/5,exec/6,okornot/1,
		exec_async/2,exec_async/3,exec_async/4,exec_async/5,exec_async/6,
		stop/1,close/1,checkpoint/1,rollback/1,
		lz4_compress/1,lz4_decompress/2,replicate_opts/3,replicate_opts/2,parse_helper/2,
		all_tunnel_call/1,tcp_reconnect/0,exec_res/2,exec_res/1,
		tcp_connect_async/5,store_prepared_table/2]).
-include("actordb.hrl").

init(Path) ->
	init(Path,wal).
init(Path,JournalMode) ->
	init(Path,JournalMode,actordb_util:hash(Path)).
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

lz4_compress(Bin) ->
	actordb_driver:lz4_compress(Bin).
lz4_decompress(Bin,Size) ->
	actordb_driver:lz4_decompress(Bin,Size).

replicate_opts(Db,Bin,Type) ->
	actordb_driver:replicate_opts(Db,Bin,Type).

replicate_opts(Db,Bin) ->
	actordb_driver:replicate_opts(Db,Bin).

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

rollback(Db) ->
	okornot(exec(Db,<<"ROLLBACK;">>)).

checkpoint(Db) when element(1,Db) == connection ->
	exec(Db,<<"PRAGMA wal_checkpoint;">>).

close(Db) ->
	stop(Db).
stop(undefined) ->
	ok;
stop(Db) when element(1,Db) == actordb_driver ->
	actordb_driver:close(Db),
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
exec(Db,Sql,Evnum,Evterm,VarHeader) ->
	actordb_local:report_write(),
	Res =  actordb_driver:exec_script(Sql,Db,actordb_conf:query_timeout(),Evnum,Evterm,VarHeader),
	exec_res(Res,Sql).
exec(Db,Sql,[],Evnum,Evterm,VarHeader) ->
	exec(Db,Sql,Evnum,Evterm,VarHeader);
exec(Db,Sql,Records,Evnum,Evterm,VarHeader) ->
	actordb_local:report_write(),
	Res =  actordb_driver:exec_script(Sql,Records,Db,actordb_conf:query_timeout(),Evnum,Evterm,VarHeader),
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
exec_async(Db,Sql,Evnum,Evterm,VarHeader) ->
	actordb_local:report_write(),
	actordb_driver:exec_script_async(Sql,Db,Evnum,Evterm,VarHeader).
exec_async(Db,Sql,[],Evnum,Evterm,VarHeader) ->
	exec_async(Db,Sql,Evnum,Evterm,VarHeader);
exec_async(Db,Sql,Records,Evnum,Evterm,VarHeader) ->
	actordb_local:report_write(),
	actordb_driver:exec_script_async(Sql,Records,Db,Evnum,Evterm,VarHeader).

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
