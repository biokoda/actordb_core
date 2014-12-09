% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sqlite).
-export([init/1,init/2,init/3,exec/2,exec/3,exec/4,exec/5,exec/6,set_pragmas/2,set_pragmas/3,okornot/1,
		 stop/1,close/1,checkpoint/1,move_to_trash/1,copy_to_trash/1,rollback/1]).  %wal_pages/1
-include("actordb.hrl").

init(Path) ->
	init(Path,wal).
init(Path,JournalMode) ->
	init(Path,JournalMode,actordb_util:hash(Path)).
init(Path,JournalMode,Thread) ->
	case actordb_conf:driver() of
		actordb_driver ->
			Sql = <<"select name, sql from sqlite_master where type='table';",
					"$PRAGMA cache_size=",(butil:tobin(?DEF_CACHE_PAGES))/binary,";">>,
			case actordb_driver:open(Path,Thread,Sql) of
				{ok,Db,[_,[{columns,_},{rows,Tables}]]} ->
					{ok,Db,Tables,?PAGESIZE};
				Err ->
					?AERR("Unable to open sqlite file at ~p error ~p",[Path,Err]),
					case move_to_trash(Path) of
						ok ->
							init(Path,JournalMode,Thread);
						Err1 ->
							?AERR("Unable to move to trash err ~p",[Err1]),
							Err
					end
			end;
		_ ->
			Sql = <<"select name, sql from sqlite_master where type='table';",
					"PRAGMA page_size=",(butil:tobin(?PAGESIZE))/binary,";"
					% Exclusive locking is faster but unrealiable. 
					% "$PRAGMA locking_mode=EXCLUSIVE;",
					"$PRAGMA foreign_keys=1;",
					"$PRAGMA synchronous=",(actordb_conf:sync())/binary,";",
					"$PRAGMA cache_size=",(butil:tobin(?DEF_CACHE_PAGES))/binary,";",
					"$PRAGMA journal_mode=",(butil:tobin(JournalMode))/binary,";",
					"$PRAGMA journal_size_limit=0;">>,
			case esqlite3:open(Path,Thread,Sql) of
				{ok,Db,[_,[{columns,_},{rows,Tables}]]} ->
					{ok,Db,Tables,4096};
				% Unable to open sqlite file.
				% Crash if file permission problem.
				% If file corrupted, move to trash folder (one or two levels above current folder) and open again.
				Err ->
					?AERR("Unable to open sqlite file at ~p error ~p",[Path,Err]),
					case move_to_trash(Path) of
						ok ->
							init(Path,JournalMode,Thread);
						Err1 ->
							?AERR("Unable to move to trash err ~p",[Err1]),
							Err
					end
			end
	end.

rollback(Db) when element(1,Db) == connection ->
	okornot(exec(Db,<<"ROLLBACK;">>));
rollback(_) ->
	% actordb_driver does rollback automaticaly on errors
	ok.

move_to_trash(Path) ->
	case actordb_conf:level_size() of
		0 -> 
			Trash = filename:join(lists:reverse(tl(lists:reverse(filename:split(filename:dirname(Path))))))++
						"/trash/"++filename:basename(Path);
		_ ->
			Trash = filename:join(lists:reverse(tl(tl(lists:reverse(filename:split(filename:dirname(Path)))))))++
						"/trash/"++filename:basename(Path)
	end,
	filelib:ensure_dir(Trash),
	file:rename(Path,Trash).
copy_to_trash(Path) ->
	case actordb_conf:level_size() of
		0 -> 
			Trash = filename:join(lists:reverse(tl(lists:reverse(filename:split(filename:dirname(Path))))))++
						"/trash/"++filename:basename(Path);
		_ ->
			Trash = filename:join(lists:reverse(tl(tl(lists:reverse(filename:split(filename:dirname(Path)))))))++
						"/trash/"++filename:basename(Path)
	end,
	filelib:ensure_dir(Trash),
	file:copy(Path,Trash).

checkpoint(Db) ->
	exec(Db,<<"PRAGMA wal_checkpoint;">>).

close(Db) ->
	stop(Db).
stop(undefined) ->
	ok;
stop(Db) when element(1,Db) == connection ->
	esqlite3:close(Db),
	ok;
stop(Db) when element(1,Db) == actordb_driver ->
	actordb_driver:close(Db),
	ok;
stop(Db) when element(1,Db) == file_descriptor ->
	file:close(Db).

% wal_pages(Db) ->
% 	esqlite3:wal_pages(Db).

set_pragmas(Db,JournalMode) ->
	set_pragmas(Db,JournalMode,actordb_conf:sync()).
set_pragmas(Db,JournalMode,Sync) when element(1,Db) == connection ->
	_Setpragma = exec(Db,<<%"PRAGMA locking_mode=EXCLUSIVE;",
						 "PRAGMA synchronous=",(butil:tobin(Sync))/binary,";",
						"PRAGMA journal_mode=",(butil:tobin(JournalMode))/binary,";",
						"PRAGMA journal_size_limit=0;">>),
	ok;
set_pragmas(_,_,_) ->
	ok.


exec(Db,S,read) ->
	actordb_local:report_read(),
	exec(Db,S);
exec(Db,S,write) ->
	actordb_local:report_write(),
	exec(Db,S);
exec(Db,Sql,Records) when element(1,Db) == connection ->
	Res = esqlite3:exec_script(Sql,Records,Db,actordb_conf:query_timeout()),
	exec_res(Res,Sql);
exec(Db,Sql,Records) ->
	Res = actordb_driver:exec_script(Sql,Records,Db,actordb_conf:query_timeout()),
	exec_res(Res,Sql).
exec(Db,S,Records,read) ->
	actordb_local:report_write(),
	exec(Db,S,Records);
exec(Db,S,Records,write) ->
	actordb_local:report_write(),
	exec(Db,S,Records).
exec(Db,Sql)  when element(1,Db) == connection ->
	Res = esqlite3:exec_script(Sql,Db,actordb_conf:query_timeout()),
	exec_res(Res,Sql);
exec(Db,Sql) ->
	Res = actordb_driver:exec_script(Sql,Db,actordb_conf:query_timeout()),
	exec_res(Res,Sql).
exec(Db,Sql,Evnum,Evterm,VarHeader) when element(1,Db) == connection ->
	actordb_local:report_write(),
	Res =  esqlite3:exec_script(Sql,Db,actordb_conf:query_timeout(),Evnum,Evterm,VarHeader),
	exec_res(Res,Sql);
exec(Db,Sql,Evnum,Evterm,VarHeader) ->
	actordb_local:report_write(),
	Res =  actordb_driver:exec_script(Sql,Db,actordb_conf:query_timeout(),Evnum,Evterm,VarHeader),
	exec_res(Res,Sql).
exec(Db,Sql,[],Evnum,Evterm,VarHeader) ->
	exec(Db,Sql,Evnum,Evterm,VarHeader);
exec(Db,Sql,Records,Evnum,Evterm,VarHeader) when element(1,Db) == connection ->
	actordb_local:report_write(),
	Res =  esqlite3:exec_script(Sql,Records,Db,actordb_conf:query_timeout(),Evnum,Evterm,VarHeader),
	exec_res(Res,Sql);
exec(Db,Sql,Records,Evnum,Evterm,VarHeader) ->
	actordb_local:report_write(),
	Res =  actordb_driver:exec_script(Sql,Records,Db,actordb_conf:query_timeout(),Evnum,Evterm,VarHeader),
	exec_res(Res,Sql).


exec_res(Res,Sql) ->
	case Res of
		{ok,[[{columns,_},_] = Res1]} ->
			{ok,Res1};
		{ok,[{rowid,X}]} -> % not used anymore
			{ok,{rowid,X}};
		{ok,[{changes,Id,Rows}]} ->
			{ok,{changes,Id,Rows}};
		{ok,[ok]} ->
			ok;
		{ok,[]} ->
			ok;
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
		{rowid,_} ->
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
