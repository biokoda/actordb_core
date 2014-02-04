% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sqlite).
-export([init/1,init/2,init/3,exec/2,set_pragmas/2,set_pragmas/3,stop/1,close/1,checkpoint/1,move_to_trash/1,copy_to_trash/1]). 
-include("actordb.hrl").

init(Path) ->
	init(Path,wal).
init(Path,JournalMode) ->
	init(Path,JournalMode,actordb_util:hash(Path)).
init(Path,JournalMode,Thread) ->
	Sql = <<"select name, sql from sqlite_master where type='table';",
			"PRAGMA page_size;"
			% Exclusive locking is faster but unrealiable. 
			% "$PRAGMA locking_mode=EXCLUSIVE;",
			"$PRAGMA foreign_keys=1;",
			"$PRAGMA synchronous=",(actordb_conf:sync())/binary,";",
			"$PRAGMA cache_size=",(butil:tobin(?DEF_CACHE_PAGES))/binary,";",
			"$PRAGMA journal_mode=",(butil:tobin(JournalMode))/binary,";",
			"$PRAGMA journal_size_limit=0;">>,
	case esqlite3:open(Path,Thread,Sql) of
		{ok,Db,Res} ->
			case Res of
				[[_,{rows,[{Size}]}],[{columns,_},{rows,[]}]] ->
					{ok,Db,false,Size};
				[[_,{rows,[{Size}]}],[{columns,_},{rows,[_|_]}]] ->
					{ok,Db,true,Size};
				{ok,[]} ->
					% Not sure yet why this happens, but opening again seems to work
					stop(Db),
					{ok,Db1,Res1} = esqlite3:open(Path,Thread,Sql),
					case Res1 of
						[[_,{rows,[{Size}]}],[{columns,_},{rows,[]}]] ->
							{ok,Db1,false,Size};
						[[_,{rows,[{Size}]}],[{columns,_},{rows,[_|_]}]] ->
							{ok,Db1,true,Size}
					end
			end;
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
	end.

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
stop(Db) ->
	exec(Db,<<"PRAGMA wal_checkpoint;">>),
	esqlite3:close(Db).


set_pragmas(Db,JournalMode) ->
	set_pragmas(Db,JournalMode,actordb_conf:sync()).
set_pragmas(Db,JournalMode,Sync) ->
	_Setpragma = exec(Db,<<%"PRAGMA locking_mode=EXCLUSIVE;",
						 "PRAGMA synchronous=",(butil:tobin(Sync))/binary,";",
						"PRAGMA journal_mode=",(butil:tobin(JournalMode))/binary,";",
						"PRAGMA journal_size_limit=0;">>),
	ok.

exec(undefined,_) ->
	ok;
exec(Db,Sql) ->
	Res = esqlite3:exec_script(Sql,Db),
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



