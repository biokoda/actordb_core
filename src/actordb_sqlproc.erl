% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sqlproc).
-behaviour(gen_server).
-define(LAGERDBG,true).
-export([start/1,start_copylock/2, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([print_info/1]).
-export([read/4,write/4,call/4,call/5,diepls/2]).
-export([call_slave/4,call_slave/5,call_master/4,dbcopy_send/5,okornot/1]).
-include_lib("actordb.hrl").
-include_lib("kernel/include/file.hrl").
% since sqlproc gets called so much, logging from here often makes it more difficult to find a bug.
% -define(NOLOG,1).
-define(EVNUM,<<"1">>).
-define(EVCRC,<<"2">>).
-define(SCHEMA_VERS,<<"3">>).
-define(ATYPE,<<"4">>).
-define(COPYFROM,<<"5">>).
-define(MOVEDTO,<<"6">>).
-define(ANUM,<<"7">>).
-define(EVNUMI,1).
-define(EVCRCI,2).
-define(SCHEMA_VERSI,3).
-define(ATYPEI,4).
-define(COPYFROMI,5).
-define(MOVEDTOI,6).
-define(ANUMI,7).
-define(WLOG_LIMIT,1024*4).
-define(FLAG_CREATE,1).
-define(FLAG_ACTORNUM,2).
-define(FLAG_EXISTS,4).
-define(FLAG_NOVERIFY,8).
-define(FLAG_TEST,16).
-define(FLAG_STARTLOCK,32).
-define(FLAG_NOHIBERNATE,64).
% Log events to the actual sqlite db file. For debugging.
% When shards are being moved across nodes it often may not be clear what exactly has been happening
% to an actor.
% -define(DODBLOG,1).
% -compile(export_all).

% For every actor, sqlproc is running on every node in cluster (1 master, other slaves). 
% For writes, master starts 2 phase commit. 
% 1. Master encapsulates call in savepoint without release. If statement fails, it will be rolled back 
%    and error returned. If succeeds, move on to 2.
% 2. Master sends sql statement to all other nodes (they keep it in state not db)
% 3. If a majority of nodes replied ok, tell them to commit.
% 4. If a majority of nodes replied ok, commit locally.
%  - if a crash happens in steps 3 or 4, db can be corrupted on some nodes. In this case entire sqlite db 
% 		for actor will be copied over from other valid nodes.
%  - if a node was offline and connected to the cluster while sqlproc is running for an actor, it will be started
% 		on next write call. If database is stale (it missed write events), the sqlite db
% 		file will be copied (just like if it was corrupted) from a valid node.
% If a node has a stale or corrupted database, it will restore it from another node.
% Node from which sqlite is being restored from will be switched to journal_mode=wal so that the db 
% does not get changed during stransfer. After the db has been copied, it will copy wal 
% journal file in chunks. Wal file is append only. So it calls sqlproc for current maximum file point
% and reads file up to that point. 


read(Name,Flags,[{copy,CopyFrom}],Start) ->
	case distreg:whereis(Name) of
		undefined ->
			case call(Name,Flags,{read,<<"select * from __adb limit 1;">>},Start) of
				{ok,_} ->
					{ok,[{columns,{<<"status">>}},{row,{<<"ok">>}}]};
				_E ->
					?AERR("Unable to copy actor ~p to ~p",[CopyFrom,Name]),
					{ok,[{columns,{<<"status">>}},{row,{<<"failed">>}}]}
			end;
		Pid ->
			diepls(Pid,overwrite),
			Ref = erlang:monitor(process,Pid),
			receive
				{'DOWN',Ref,_,_Pid,_} ->
					read(Name,Flags,[{copy,CopyFrom}],Start)
				after 2000 ->
					{ok,[{columns,{<<"status">>}},{row,{<<"failed_running">>}}]}
			end
	end;
read(Name,Flags,[delete],Start) ->
	call(Name,Flags,{write,{undefined,0,delete,undefined}},Start);
read(Name,Flags,Sql,Start) ->
	call(Name,Flags,{read,Sql},Start).

write(Name,Flags,{{_,_,_} = TransactionId,Sql},Start) ->
	write(Name,Flags,{undefined,TransactionId,Sql},Start);
write(Name,Flags,{MFA,TransactionId,Sql},Start) ->
	case TransactionId of
		{_,_,_} ->
			case Sql of
				commit ->
					call(Name,Flags,{commit,true,TransactionId},Start);
				abort ->
					call(Name,Flags,{commit,false,TransactionId},Start);
				[delete] ->
					call(Name,Flags,{write,{MFA,0,delete,TransactionId}},Start);
				_ ->
					call(Name,Flags,{write,{MFA,erlang:crc32(Sql),iolist_to_binary(Sql),TransactionId}},Start)
			end;
		_ when Sql == undefined ->
			call(Name,Flags,{write,{MFA,0,undefined,undefined}},Start);
		_ ->
			call(Name,Flags,{write,{MFA,erlang:crc32(Sql),iolist_to_binary(Sql),undefined}},Start)
	end;
write(Name,Flags,[delete],Start) ->
	call(Name,Flags,{write,{undefined,0,delete,undefined}},Start);
write(Name,Flags,Sql,Start) ->
	call(Name,Flags,{write,{undefined,erlang:crc32(Sql),iolist_to_binary(Sql),undefined}},Start).


call(Name,Flags,Msg,Start) ->
	call(Name,Flags,Msg,Start,false).
call(Name,Flags,Msg,Start,IsRedirect) ->
	case distreg:whereis(Name) of
		undefined ->
			case startactor(Name,Start,[{startreason,Msg}|Flags]) of %
				{ok,Pid} when is_pid(Pid) ->
					call(Name,Flags,Msg,Start,IsRedirect,Pid);
				{error,nocreate} ->
					{error,nocreate};
				Res ->
					Res
			end;
		Pid ->
			call(Name,Flags,Msg,Start,IsRedirect,Pid)

	end.
call(Name,Flags,Msg,Start,IsRedirect,Pid) ->
	% If call returns redirect, this is slave node not master node.
	case catch gen_server:call(Pid,Msg,infinity) of
		{redirect,Node} when is_binary(Node) ->
			case lists:member(Node,bkdcore:cluster_nodes()) of
				true ->
					case IsRedirect of
						true ->
							double_redirect;
						false ->
							?ADBG("Redirect call ~p ~p ~p",[Node,Name,Msg]),
							case actordb:rpc(Node,Name,{?MODULE,call,[Name,Flags,Msg,Start,true]}) of
								double_redirect ->
									diepls(Pid,nomaster),
									call(Name,Flags,Msg,Start);
								Res ->
									Res
							end
					end;
				false ->
					actordb:rpc(Node,Name,{?MODULE,call,[Name,Flags,Msg,Start,false]})
			end;
		{'EXIT',{noproc,_}} = _X  ->
			?ADBG("noproc call again ~p",[_X]),
			call(Name,Flags,Msg,Start);
		{'EXIT',{normal,_}} ->
			?ADBG("died normal"),
			call(Name,Flags,Msg,Start);
		Res ->
			Res
	end.
startactor(Name,Start,Flags) ->
	case Start of
		{Mod,Func,Args} ->
			apply(Mod,Func,[Name|Args]);
		undefined ->
			{ok,undefined};
		_ ->
			apply(Start,start,[Name,Flags])
	end.

call_master(Cb,Actor,Type,Msg) ->
	case apply(Cb,start,[Actor,Type,[{startreason,Msg}]]) of %
		{ok,Pid} ->
			ok;
		Pid when is_pid(Pid) ->
			ok
	end,
	case catch gen_server:call(Pid,Msg,infinity) of
		{'EXIT',{noproc,_}} ->
			call_master(Cb,Actor,Type,Msg);
		Res ->
			Res
	end.

call_slave(Cb,Actor,Type,Msg) ->
	call_slave(Cb,Actor,Type,Msg,[]).
call_slave(Cb,Actor,Type,Msg,Flags) ->
	case apply(Cb,cb_slave_pid,[Actor,Type,[{startreason,Msg}|Flags]]) of %
		{ok,Pid} ->
			ok;
		Pid when is_pid(Pid) ->
			ok
	end,
	case catch gen_server:call(Pid,Msg,infinity) of
		{'EXIT',{noproc,_}} ->
			call_slave(Cb,Actor,Type,Msg);
		{'EXIT',{normal,_}} ->
			call_slave(Cb,Actor,Type,Msg);
		Res ->
			Res
	end.

diepls(Pid,Reason) ->
	gen_server:cast(Pid,{diepls,Reason}).


% Opts:
% [{actor,Name},{type,Type},{mod,CallbackModule},{state,CallbackState},
%  {inactivity_timeout,SecondsOrInfinity},{slave,true/false},{copyfrom,NodeName},{copyreset,{Mod,Func,Args}}]
start(Opts) ->
	?ADBG("Starting ~p ~p",[butil:ds_vals([actor,type],Opts),butil:ds_val(slave,Opts)]),
	Ref = make_ref(),
	case gen_server:start(?MODULE, [{start_from,{self(),Ref}}|Opts], []) of
		{ok,Pid} ->
			{ok,Pid};
		{error,normal} ->
			% Init failed gracefully. It should have sent an explanation. 
			receive
				{Ref,nocreate} ->
					{error,nocreate};
				{Ref,{registered,Pid}} ->
					{ok,Pid};
				{Ref,{actornum,Path,Num}} ->
					{ok,Path,Num};
				{Ref,{ok,[{columns,_},_]} = Res} ->
					Res
				after 0 ->
					{error,cantstart}
			end;
		Err ->
			?AERR("start sqlproc error ~p",[Err]),
			Err
	end.

stop(Pid) ->
	gen_server:call(Pid, stop).

print_info(Pid) ->
	gen_server:cast(Pid,print_info).



-record(dp,{db, actorname,actortype, evnum = 0,evcrc = 0, activity = 0, timerref, start_time,
			page_size = 1024, activity_now,write_bytes = 0,schemanum,schemavers,flags = 0,
			% locked is a list of pids or markers that needs to be empty for actor to be unlocked.
			locked = [],
	% Multiupdate id, set to {Multiupdateid,TransactionNum} if in the middle of a distributed transaction
	transactionid, transactioncheckref,
  % actordb_sqlproc is not used directly, it always has a callback module that sits in front of it,
  %  providing an external interface
  %  to a sqlite backed process.
  cbmod, cbstate,
  % callfrom and commiter are either both set or none of them are. callfrom is who is calling, 
  % commiter is PID of process doing the processing.
  % Set when a write call is going through a 2 phase commit.
  callfrom, commiter,callres,
  % queue which holds gen_server:calls that can not be processed immediately because db has not 
  %  been verified, is in the middle of a 2phase commit
  %  or is being restored from another node.
  callqueue,
  % (short for masterorslave): slave/master
  mors, 
  % Sql statement received from master in first step of 2 phase commit. Only kept in memory.
  replicate_sql = <<>>,
  % In case of short-term disconnects, writelog keeps a binary of max size X. 
  %   From oldest sql statement to youngest.
  % <<EvNum:64/unsigned,Crc:32/unsigned,(byte_size(Sql)):32/unsigned,Sql/binary,
  %   ....>>
  writelog = <<>>,
  % Local copy of db needs to be verified with all nodes. It might be stale or in a conflicted state.
  % If local db is being restored, verified will be on false.
  % Possible values: true, false, failed (there is no majority of nodes with the same db state)
  verified = false,
  % Verification of db is done asynchronously in a monitored process. This holds pid.
  verifypid,
  % Current journal mode. It can switch to wal if another sqlproc is 
  %   restoring it's sqlite file from current one.
  journal_mode, 
  % Configured default journal mode.
  def_journal_mode,
  % Path to sqlite file.
  dbpath,
  % Which nodes current process is sending dbfile to.
  % [{Node,Pid,Ref,IsMove},..]
  dbcopy_to = [],
  % If node is sending us a complete copy of db, this identifies the operation
  dbcopyref,
  % Where is master sqlproc.
  masternode, masternodedist,
  % If db has been moved completely over to a new node. All calls will be redirected to that node.
  % Once this has been set, db files will be deleted on process timeout.
  movedtonode,
  % Will cause actor not to do any DB initialization, but will take the db from another node
  copyfrom,copyreset = false,copyproc}). 
-define(R2P(Record), butil:rec2prop(Record#dp{writelog = byte_size(P#dp.writelog)}, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	

-ifndef(NOLOG).
-define(DBG(F),lager:debug([$~,$p,$\s|F],[P#dp.actorname])).
-define(DBG(F,A),lager:debug([$~,$p,$\s|F],[P#dp.actorname|A])).
-define(INF(F),lager:info([$~,$p,$\s|F],[P#dp.actorname])).
-define(INF(F,A),lager:info([$~,$p,$\s|F],[P#dp.actorname|A])).
-define(ERR(F),lager:error([$~,$p,$\s|F],[P#dp.actorname])).
-define(ERR(F,A),lager:error([$~,$p,$\s|F],[P#dp.actorname|A])).
-else.
-define(DBG(F),ok).
-define(DBG(F,A),ok).
-define(INF(F),ok).
-define(INF(F,A),ok).
-define(ERR(F),ok).
-define(ERR(F,A),ok).
-endif.

-ifdef(DODBLOG).
-define(DBLOG(Db,LogFormat,LogArgs),actordb_sqlite:exec(Db,<<"INSERT INTO __evlog (line,pid,node,actor,type,txt) ",
											  "VALUES (",(butil:tobin(?LINE))/binary,",",
											  	"'",(list_to_binary(pid_to_list(self())))/binary,"',",
											  	"'",(bkdcore:node_name())/binary,"',",
											  	"'",(butil:tobin(P#dp.actorname))/binary,"',",
											  	"'",(butil:tobin(P#dp.actortype))/binary,"',",
											  "'",(butil:tobin(io_lib:fwrite(LogFormat,LogArgs)))/binary,"');">>)).
-define(LOGTABLE,<<"CREATE TABLE __evlog (id INTEGER PRIMARY KEY AUTOINCREMENT,line INTEGER,pid TEXT, node TEXT,",
						" actor TEXT, type TEXT, txt TEXT);">>).
-define(COPYTRASH,actordb_sqlite:copy_to_trash(P#dp.dbpath),actordb_sqlite:copy_to_trash(P#dp.dbpath++"-wal")).
-else.
-define(LOGTABLE,<<>>).
-define(DBLOG(_a,_b,_c),ok).
-define(COPYTRASH,ok).
-endif.

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

handle_call(_Msg,_,#dp{movedtonode = <<_/binary>>} = P) ->
	?ADBG("REDIRECT BECAUSE MOVED TO NODE ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.movedtonode,_Msg]),
	{reply,{redirect,P#dp.movedtonode},P#dp{activity = P#dp.activity + 1}};
handle_call({dbcopy_op,From1,What,Data} = Msg,CallFrom,P) ->
	case What of
		send_db ->
			% Send database to another node.
			% This gets called from that node.
			% If we have sql in wlog, send missing wlog. If not send entire db.
			% File is sent in 1MB chunks. If db is =< 1MB, send the file directly.
			% If file > 1MB, switch to journal mode wal. This will caue writes to go to a WAL file not into the db.
			%  Which means the sqlite file can be read from safely.
			{Node,Ref,IsMove,RemoteEvNum,RemoteEvCrc,ActornameToCopyto} = Data,
			case P#dp.verified of
				true ->
					case get_from_wlog(RemoteEvNum,RemoteEvCrc,P#dp.writelog) of
						undefined ->
							case file:read_file_info(P#dp.dbpath) of
								{ok,I} when I#file_info.size > 1024*1024 orelse P#dp.dbcopy_to /= [] orelse 
												IsMove == true orelse P#dp.journal_mode == wal ->
									?ADBG("senddb from ~p, myname ~p, remotename ~p info ~p, copyto already ~p",[bkdcore:node_name(),P#dp.actorname,ActornameToCopyto,
																								{Node,Ref,IsMove,I#file_info.size},P#dp.dbcopy_to]),
									?DBLOG(P#dp.db,"senddb to ~p ~p",[ActornameToCopyto,Node]),
									case P#dp.journal_mode of
										wal ->
											ok;
										_ ->
											actordb_sqlite:set_pragmas(P#dp.db,wal)
									end,
									Me = self(),
									case lists:keyfind(Node,1,P#dp.dbcopy_to) of
										false ->
											% actordb_sqlite:stop(P#dp.db),
											% {ok,Db,true,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
											?COPYTRASH,
											Db = P#dp.db,
											{Pid,_} = spawn_monitor(fun() -> 
													dbcopy(P#dp{dbcopy_to = Node, dbcopyref = Ref},Me,ActornameToCopyto) end),
											{reply,{ok,Ref},check_timer(P#dp{db = Db,journal_mode = wal, 
																				dbcopy_to = [{Node,Pid,Ref,IsMove}|P#dp.dbcopy_to], 
																				activity = P#dp.activity + 1})};
										{Node,_Pid,Ref,IsMove} ->
											?ERR("senddb already exists with same ref!!"),
											{reply,{ok,Ref},P};
										{Node,Pid,_SomeRef,IsMove} ->
											?ERR("senddb already exists diff ref!!"),
											exit(Pid,stop),
											handle_call(Msg,CallFrom,
														 P#dp{dbcopy_to = lists:keydelete(Node,1,P#dp.dbcopy_to)})
									end;
								{ok,_I} ->
									{ok,Bin} = file:read_file(P#dp.dbpath),
									{reply,{ok,Bin},P};
								{error,enoent} ->
									?AINF("enoent during senddb to ~p ~p",[Node,?R2P(P)]),
									gen_server:reply(CallFrom,{error,enoent}),
									{stop,normal,P};
								Err ->
									{reply,Err,P}
							end;
						{LastEv,LastCrc,Sql} ->
							{reply,{wlog,LastEv,LastCrc,Sql},P}
					end;
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
		wal_read ->
			{FromPid,Ref} = From1,
			Size = filelib:file_size([P#dp.dbpath,"-wal"]),
			case Size =< Data of
				true when P#dp.transactionid == undefined ->
					{reply,{[P#dp.dbpath,"-wal"],Size},P#dp{locked = butil:lists_add({FromPid,Ref},P#dp.locked)}};
				true ->
					{noreply,P#dp{callqueue = queue:in_r({CallFrom,{dbcopy_op,From1,What,Data}},P#dp.callqueue)}};
				false ->
					?DBG("wal_size ~p",[{From1,What,Data}]), 
					{reply,{[P#dp.dbpath,"-wal"],Size},P}
			end;
		unlock ->
			% For unlock data = copyref
			case lists:keyfind(Data,2,P#dp.locked) of
				false ->
					?AERR("Unlock attempt on non existing lock ~p ~p, locks ~p",[{P#dp.actorname,P#dp.actortype},Data,P#dp.locked]),
					{reply,false,P};
				{wait_copy,Data,IsMove,Node,_TimeOfLock} ->
					?ADBG("Actor unlocked ~p ~p",[{P#dp.actorname,P#dp.actortype},Data]),
					case IsMove of
						true ->
							write_call({undefined,0,{moved,Node},undefined},CallFrom,P#dp{locked = lists:keydelete(Data,2,P#dp.locked)});
						false ->
							self() ! doqueue,
							case [ok || Tuple <- P#dp.locked,element(2,Tuple) == Data] of
								% A bit silly but it avoids a race condition. Copy process has not died yet
								%  but we already received unlock request.
								[_,_] ->
									?ADBG("Race condition, wait for die ~p",[P#dp.actorname]),
									{reply,ok,P};
								[_] ->
									WithoutLock = lists:keydelete(Data,2,P#dp.locked),
									case WithoutLock of
										[] when P#dp.dbcopy_to == [] ->
											actordb_sqlite:set_pragmas(P#dp.db,P#dp.def_journal_mode),
											Md = P#dp.def_journal_mode;
										_ ->
											Md = P#dp.journal_mode
									end,
									{reply,ok,P#dp{locked = WithoutLock, journal_mode = Md}}
							end
					end;
				{_FromPid,Data} ->
					?ADBG("Early unlock attempt ~p ~p ~p",[P#dp.actorname,Data,P#dp.locked]),
					% Check if race condition
					case lists:keyfind(Data,3,P#dp.dbcopy_to) of
						{Node,_PID,Data,IsMove} ->
							handle_call(Msg,CallFrom,P#dp{locked = [{wait_copy,Data,IsMove,Node,os:timestamp()}|P#dp.locked]});
						false ->
							?AERR("dbcopy_to does not contain ref ~p, ~p",[Data,P#dp.dbcopy_to]),
							{reply,false,P}
					end
			end
	end;
handle_call({getinfo,What},_,P) ->
	case What of
		verifyinfo ->
			{reply,{ok,bkdcore:node_name(),P#dp.evcrc,P#dp.evnum,{P#dp.mors,P#dp.verified}},P};
		actornum ->
			{reply,{ok,P#dp.dbpath,read_num(P)},P};
		donothing ->
			{reply,ok,P};
		conflicted ->
			{stop,conflicted,P}
	end;
handle_call({commit,Doit,Id},From, P) ->
	?ADBG("Commit ~p ~p ~p",[Doit,Id,P#dp.transactionid]),
	case P#dp.transactionid == Id of
		true ->
			case P#dp.transactioncheckref of
				undefined ->
					ok;
				_ ->
					erlang:demonitor(P#dp.transactioncheckref)
			end,
			?ADBG("Commit write ~p ~p",[bkdcore:cluster_nodes_connected(),P#dp.replicate_sql]),
			{ConnectedNodes,LenCluster,_LenConnected} = nodes_for_replication(P),
			{Sql,EvNum,Crc,NewVers} = P#dp.replicate_sql,
			case Doit of
				true when LenCluster == 0 ->
					case Sql of
						delete ->
							delete_actor(P),
							reply(From,ok),
							{stop,normal,P};
						_ ->
							ok = okornot(actordb_sqlite:exec(P#dp.db,<<"RELEASE SAVEPOINT 'adb';">>)),
							{reply,ok,P#dp{transactionid = undefined,transactioncheckref = undefined,
									 replicate_sql = undefined, activity = P#dp.activity + 1}}
					end;
				true ->
					Commiter = commit_write(P,LenCluster,ConnectedNodes,EvNum,Sql,Crc,NewVers),
					{noreply,P#dp{callfrom = From, commiter = Commiter, activity = P#dp.activity + 1,
								  callres = ok,
								 transactionid = undefined,transactioncheckref = undefined}};
				false when LenCluster == 0 ->
					self() ! doqueue,
					actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
					{reply,ok,P#dp{transactionid = undefined, replicate_sql = undefined,
									transactioncheckref = undefined}};
				false ->
					self() ! doqueue,
					{Tid,Updaterid,_} = P#dp.transactionid,
					case Sql of
						<<"delete">> ->
							ok;
						_ ->
							actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)
					end,
					NewSql = <<"DELETE FROM __transactions WHERE tid=",(butil:tobin(Tid))/binary," AND updater=",
										(butil:tobin(Updaterid))/binary,";">>,
					handle_call({write,{undefined,erlang:crc32(NewSql),NewSql,undefined}},From,P#dp{callfrom = undefined,
										transactionid = undefined,replicate_sql = undefined,transactioncheckref = undefined})
			end;
		_ ->
			{reply,ok,P}
	end;
handle_call({delete,Moved},From,P) ->
	?ADBG("deleting actor from node ~p ~p",[P#dp.actorname,P#dp.actortype]),
	actordb_sqlite:stop(P#dp.db),
	delactorfile(P#dp{movedtonode = Moved}),
	distreg:unreg(self()),
	reply(From,ok),
	{stop,normal,P};
% If we are not ready to process calls atm (in the middle of a write or db not verified yet). Queue requests.
handle_call(Msg,From,P) when P#dp.callfrom /= undefined; P#dp.verified /= true; P#dp.transactionid /= undefined; P#dp.locked /= [] ->
	case Msg of
		{write,{_,_,_,TransactionId} = Msg1} when P#dp.transactionid == TransactionId, P#dp.transactionid /= undefined ->
			write_call(Msg1,From,P);
		_ ->
			?DBG("Queueing msg ~p ~p, because ~p",[Msg,P#dp.mors,{P#dp.callfrom,P#dp.verified,P#dp.transactionid}]),
			{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue), activity = P#dp.activity+1}}
	end;
handle_call({read,Msg},From,P) ->
	case P#dp.mors of
		master when Msg == [exists] ->
			{reply,{ok,[{columns,{<<"exists">>}},{rows,[{<<"true">>}]}]},P};
		master ->
			case check_schema(P,[]) of
				ok ->
					case Msg of
						{Mod,Func,Args} ->
							case apply(Mod,Func,[P#dp.cbstate|Args]) of
								{reply,What,Sql,NS} ->
									{reply,{What,actordb_sqlite:exec(P#dp.db,Sql)},P#dp{cbstate = NS}};
								{reply,What,NS} ->
									{reply,What,P#dp{cbstate = NS}};
								{reply,What} ->
									{reply,What,P};
								{Sql,State} ->
									{reply,actordb_sqlite:exec(P#dp.db,Sql),check_timer(P#dp{activity = P#dp.activity+1, 
																							  cbstate = State})};
								Sql ->
									{reply,actordb_sqlite:exec(P#dp.db,Sql),check_timer(P#dp{activity = P#dp.activity+1})}
							end;
						Sql ->
							{reply,actordb_sqlite:exec(P#dp.db,Sql),check_timer(P#dp{activity = P#dp.activity+1})}
					end;
				% Schema has changed. Execute write on schema update.
				% Place this read in callqueue for later execution.
				{NewVers,Sql1} ->
					case write_call(erlang:crc32(Sql1),Sql1,undefined,undefined,NewVers,P) of
						{reply,Reply,NP} ->
							case okornot(Reply) of
								ok ->
									handle_call({read,Msg},From,NP);
								Err ->
									{reply,Err,NP}
							end;
						{noreply,NP} ->
							{noreply,NP#dp{callqueue = queue:in_r({From,Msg},NP#dp.callqueue)}}
					end
			end;
		_ ->
			?DBG("redirect read ~p",[P#dp.masternode]),
			redirect_master(P)
	end;
handle_call({write,Msg},From, #dp{mors = master} = P) ->
	write_call(Msg,From,P);
handle_call({write,_},_,#dp{mors = slave} = P) ->
	?DBG("Redirect not master ~p",[P#dp.masternode]),
	redirect_master(P);
% Called from master
handle_call({replicate_start,_Ref,Node,PrevEvnum,PrevCrc,Sql,EvNum,Crc,NewVers},From,P) ->
	?ADBG("Replicate start ~p ~p ~p ~p ~p ~p ~p",[P#dp.actorname,P#dp.actortype,P#dp.evnum, PrevEvnum, P#dp.evcrc, PrevCrc,Sql]),
	?DBLOG(P#dp.db,"replicatestart ~p ~p ~p ~p",[_Ref,butil:encode_percent(_Node),EvNum,Crc]),
	case Sql of
		<<"delete">> ->
			Trump = true;
		<<"moved,",_/binary>> ->
			Trump = true;
		_ ->
			Trump = false
	end,
	case ok of
		_ when Trump; P#dp.mors == slave, Node == P#dp.masternodedist, P#dp.evnum == PrevEvnum, P#dp.evcrc == PrevCrc ->
			{reply,ok,check_timer(P#dp{replicate_sql = {Sql,EvNum,Crc,NewVers}, activity = P#dp.activity + 1})};
		_ ->
			?DBLOG(P#dp.db,"replicate conflict!!! ~p ~p in ~p ~p, cur ~p ~p",[_Ref,_Node,EvNum,Crc,P#dp.evnum,P#dp.evcrc]),
			?AERR("Replicate conflict!!!!! ~p ~p ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.evnum, PrevEvnum, P#dp.evcrc, PrevCrc]),
			actordb_sqlite:stop(P#dp.db),
			reply(From,desynced),
			% Doing init again will mean calling verify and restore db.
			{ok,NP} = init(P,replicate_conflict),
	 		{reply,ok,NP#dp{callqueue = P#dp.callqueue}}
	end;
% Called from master
handle_call(replicate_commit,From,P) ->
	?ADBG("Replicate commit! ~p ~p ~p",[P#dp.actorname,P#dp.actortype,P#dp.replicate_sql]),
	case P#dp.replicate_sql of
		<<>> ->
			?DBLOG(P#dp.db,"replicatecommit empty",[]),
			{reply,false,check_timer(P#dp{activity = P#dp.activity+1})};
		_ ->
			{Sql,EvNum,Crc,NewVers} = P#dp.replicate_sql,
			?DBLOG(P#dp.db,"replicatecommit ok ~p ~p",[EvNum,Crc]),
			case Sql of
				<<"delete">> ->
					actordb_sqlite:stop(P#dp.db),
					delactorfile(P),
					reply(From,ok),
					{stop,normal,P};
				<<"moved,",MovedTo/binary>> ->
					actordb_sqlite:stop(P#dp.db),
					?DBG("Stopping because moved ~p ~p",[P#dp.actorname,MovedTo]),
					delactorfile(P#dp{movedtonode = MovedTo}),
					reply(From,ok),
					{stop,normal,P};
				_ ->
					Res = actordb_sqlite:exec(P#dp.db,[
						 <<"$SAVEPOINT 'adb';">>,
						 semicolon(Sql),
						 <<"$UPDATE __adb SET val='">>,butil:tolist(EvNum),<<"' WHERE id=",?EVNUM/binary,";">>,
						 <<"$UPDATE __adb SET val='">>,butil:tolist(Crc),<<"' WHERE id=",?EVCRC/binary,";">>,
						 <<"$RELEASE SAVEPOINT 'adb';">>
						 ]),
					{reply,okornot(Res),check_timer(P#dp{replicate_sql = <<>>,evnum = EvNum, 
									 evcrc = Crc, activity = P#dp.activity + 1, schemavers = NewVers})}
			end
	end;
handle_call(replicate_rollback,_,P) ->
	?ERR("replicate_rollback"),
	{reply,ok,P#dp{replicate_sql = <<>>}};
handle_call({replicate_bad_commit,EvNum,Crc},_,P) ->
	?ERR("replicate_bad_commit in ~p, my ~p",[{EvNum,Crc},{P#dp.evnum,P#dp.evcrc}]),
	case P#dp.evnum == EvNum andalso P#dp.evcrc == Crc of
		true ->
			actordb_sqlite:stop(P#dp.db),
	 		delactorfile(P),
	 		{ok,NP} = init(P,replicate_bad_commit),
	 		{reply,ok,NP#dp{callqueue = P#dp.callqueue}};
		_ ->
			{reply,ok,P#dp{}}
	end;
handle_call(stop, _, P) ->
	actordb_sqlite:stop(P#dp.db),
	distreg:unreg(self()),
	{stop, shutdown, stopped, P};
handle_call(Msg,From,#dp{mors = master, verified = true} = P) ->
	?DBG("cb_call ~p",[{P#dp.cbmod,Msg}]),
	case apply(P#dp.cbmod,cb_call,[Msg,From,P#dp.cbstate]) of
		{reply,Resp,S} ->
			{reply,Resp,P#dp{cbstate = S}};
		{reply,Resp} ->
			{reply,Resp,P}
	end;
handle_call(_Msg,_From,#dp{mors = slave, verified = true} = P) ->
	?DBG("Redirect not master ~p ~p",[P#dp.masternode,_Msg]),
	% {reply,{redirect,P#dp.masternodedist},P};
	redirect_master(P);
handle_call(_Msg,_,P) ->
	?AINF("sqlproc ~p unhandled call ~p mors ~p verified ~p",[P#dp.cbmod,_Msg,P#dp.mors,P#dp.verified]),
	{reply,{error,unhandled_call},P}.

check_timer(P) ->
	case P#dp.timerref of
		undefined ->
			Ref = make_ref(),
			erlang:send_after(1000,self(),{inactivity_timer,Ref,P#dp.activity}),
			P#dp{timerref = Ref};
		_ ->
			P
	end.

delete_actor(P) ->
	?ADBG("deleting actor ~p ~p ~p",[P#dp.actorname,P#dp.dbcopy_to,P#dp.dbcopyref]),
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

semicolon(<<_/binary>> = Sql) ->
	case binary:last(Sql) of
		$; ->
			Sql;
		_ ->
			<<Sql/binary,$;>>
	end;
semicolon(S) ->
	S.
% Set on firt write and not changed after. This is used to prevent a case of an actor
% getting deleted, but later created new. A server that was offline during delete missed
% the delete call and relies on actordb_events.
actornum(#dp{evnum = 0} = P) ->
	ActorNum = butil:md5(term_to_binary({P#dp.actorname,P#dp.actortype,os:timestamp(),make_ref()})),
	<<"$INSERT OR REPLACE INTO __adb VALUES (",?ANUM/binary,",'",(butil:tobin(ActorNum))/binary,"');">>;
actornum(_) ->
	<<>>.

nodes_for_replication(P) ->
	ReplicatingTo = [Nd || {Nd,_,_,_} <- P#dp.dbcopy_to],
	% Nodes we are replicating DB to will eventually get the data. So do not send the write now since it will be sent
	%  over with db copy.
	ClusterNodes = lists:subtract(bkdcore:cluster_nodes(),ReplicatingTo),
	LenCluster = length(ClusterNodes),
	ConnectedNodes = lists:subtract(bkdcore:cluster_nodes_connected(), ReplicatingTo),
	LenConnected = length(ConnectedNodes),
	{ConnectedNodes,LenCluster,LenConnected}.

check_schema(P,Sql) ->
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

write_call({MFA,Crc,Sql,Transaction},From,P) ->
	?ADBG("writecall ~p ~p ~p",[MFA,Sql,Transaction]),	
	case MFA of
		undefined ->
			case check_schema(P,Sql) of
				ok ->
					write_call(Crc,Sql,Transaction,From,P#dp.schemavers,P);
				{NewVers,Sql1} ->
					write_call(Crc,Sql1,Transaction,From,NewVers,P)
			end;
		{Mod,Func,Args} ->
			case check_schema(P,[]) of
				ok ->
					NewVers = P#dp.schemavers,
					SqlUpdate = [];
				{NewVers,SqlUpdate} ->
					ok
			end,
			?DBLOG(P#dp.db,"writecall mfa ~p",[MFA]),
			case apply(Mod,Func,[P#dp.cbstate|Args]) of
				{reply,What,OutSql1,NS} ->
					gen_server:reply(From,What),
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(erlang:crc32(OutSql),OutSql,Transaction,undefined,NewVers,P#dp{cbstate = NS});
				{reply,What,NS} ->
					{reply,What,P#dp{cbstate = NS}};
				{reply,What} ->
					{reply,What,P};
				{OutSql1,State} ->
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(erlang:crc32(OutSql),OutSql,Transaction,From,NewVers,P#dp{cbstate = State});
				OutSql1 ->
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(erlang:crc32(OutSql),OutSql,Transaction,From,NewVers,P)
			end
	end.
write_call(Crc,Sql,undefined,From,NewVers,P) ->
	EvNum = P#dp.evnum+1,
	?DBLOG(P#dp.db,"writecall ~p ~p ~p ~p ~p",[EvNum,Crc,P#dp.dbcopy_to,From,Sql]),
	{ConnectedNodes,LenCluster,LenConnected} = nodes_for_replication(P),
	case LenCluster of
		0 -> 
			Tail = <<"$RELEASE SAVEPOINT 'adb';">>;
		_ ->
			Tail = []
	end,
	case Sql of
		delete ->
			ReplSql = ComplSql = <<"delete">>,
			Res = ok;
		{moved,Node} ->
			ReplSql = ComplSql = <<"moved,",Node/binary>>,
			Res = ok;
		_ ->
			case actornum(P) of
				<<>> ->
					ReplSql = semicolon(Sql);
				NumSql ->
					ReplSql = <<(semicolon(Sql))/binary,NumSql/binary>>
			end,
			ComplSql = 
					[<<"$SAVEPOINT 'adb';">>,
					 ReplSql,
					 <<"$UPDATE __adb SET val='">>,butil:tolist(EvNum),<<"' WHERE id=",?EVNUM/binary,";">>,
					 <<"$UPDATE __adb SET val='">>,butil:tolist(Crc),<<"' WHERE id=",?EVCRC/binary,";">>,
					 Tail
					 ],
			Res = actordb_sqlite:exec(P#dp.db,ComplSql)
	end,
	% ConnectedNodes = bkdcore:cluster_nodes_connected(),
	?DBG("Replicating write ~p    connected ~p",[Sql,ConnectedNodes]),
	case okornot(Res) of
		ok ->
			?DBG("Write result ~p",[Res]),
			case ok of
				_ when LenCluster == 0 ->
					case Sql of
						delete ->
							delete_actor(P);
						{moved,MovedTo} ->
							actordb_sqlite:stop(P#dp.db),
							delactorfile(P#dp{movedtonode = MovedTo});
						_ ->
							ok
					end,
					{reply,Res,P#dp{activity = P#dp.activity+1, evnum = EvNum, evcrc = Crc, schemavers = NewVers}};
				_ when (LenConnected+1)*2 > (LenCluster+1) ->
					Commiter = commit_write(P,LenCluster,ConnectedNodes,EvNum,ReplSql,Crc,NewVers),
					{noreply,P#dp{callfrom = From,callres = Res, commiter = Commiter, activity = P#dp.activity + 1,
									replicate_sql = {ComplSql,EvNum,Crc,NewVers}}};
				_ ->
					actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
					{reply,{error,{replication_failed_1,LenConnected,LenCluster}},P}
			end;
		Resp ->
			actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
			{reply,Resp,P#dp{activity = P#dp.activity+1, write_bytes = P#dp.write_bytes + iolist_size(Sql)}}
	end;
write_call(Crc,Sql1,{Tid,Updaterid,Node} = TransactionId,From,NewVers,P) ->
	?DBLOG(P#dp.db,"writetransaction ~p ~p ~p ~p",[P#dp.evnum,Crc,{Tid,Updaterid,Node},P#dp.dbcopy_to]),
	{_CheckPid,CheckRef} = start_transaction_checker(Tid,Updaterid,Node),
	{ConnectedNodes,LenCluster,LenConnected} = nodes_for_replication(P),
	?ADBG("Starting transaction write ~p, id ~p, curtr ~p, sql ~p",[LenCluster,TransactionId,P#dp.transactionid,Sql1]),
	case LenCluster of
		0 ->
			% If single node cluster, no need to store sql first.
			case P#dp.transactionid of
				TransactionId ->
					% Transaction can write to single actor more than once (especially for KV stores)
					% if we are already in this transaction, just update sql.
					{_OldSql,EvNum,EvCrc,_} = P#dp.replicate_sql,
					ComplSql = Sql1,
					Res = actordb_sqlite:exec(P#dp.db,ComplSql);
				undefined ->
					EvNum = P#dp.evnum+1,
					EvCrc = Crc,
					case Sql1 of
						delete ->
							Res = ok,
							ComplSql = delete;
						_ ->
							ComplSql = 
								[<<"$SAVEPOINT 'adb';">>,
								 semicolon(Sql1),actornum(P),
								 <<"$UPDATE __adb SET val='">>,butil:tolist(EvNum),<<"' WHERE id=",?EVNUM/binary,";">>,
								 <<"$UPDATE __adb SET val='">>,butil:tolist(Crc),<<"' WHERE id=",?EVCRC/binary,";">>
								 ],
							Res = actordb_sqlite:exec(P#dp.db,ComplSql)
					end
			end,
			case okornot(Res) of
				ok ->
					?ADBG("Transaction ok"),
					{reply, Res, P#dp{activity = P#dp.activity+1, evnum = EvNum, evcrc = EvCrc,
								 transactionid = TransactionId, schemavers = NewVers,
								transactioncheckref = CheckRef,replicate_sql = {ComplSql,EvNum,EvCrc,NewVers}}};
				_Err ->
					ok = okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)),
					erlang:demonitor(CheckRef),
					?ADBG("Transaction not ok ~p",[_Err]),
					{reply,Res,P#dp{actortype = P#dp.activity + 1, transactionid = undefined}}
			end;
		_ ->
			EvNum = P#dp.evnum+1,
			case P#dp.transactionid of
				TransactionId ->
					% Rollback prev version of sql.
					ok = okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)),
					{OldSql,_EvNum,_EvCrc,NewVers} = P#dp.replicate_sql,
					% Combine prev sql with new one.
					Sql = <<OldSql/binary,Sql1/binary>>,
					TransactionInfo = <<"$INSERT OR REPLACE INTO __transactions (id,tid,updater,node,schemavers,sql) VALUES (1,",
											(butil:tobin(Tid))/binary,",",(butil:tobin(Updaterid))/binary,",'",Node/binary,"',",
								 				(butil:tobin(NewVers))/binary,",",
								 				"'",(base64:encode(Sql))/binary,"');">>;
				_ ->
					case Sql1 of
						delete ->
							Sql = <<"delete">>;
						_ ->
							Sql = Sql1
					end,
					% First store transaction info 
					% Once that is stored (on all nodes), execute the sql to see if there are errors (but only on this master node).
					TransactionInfo = <<"$INSERT INTO __transactions (id,tid,updater,node,schemavers,sql) VALUES (1,",
											(butil:tobin(Tid))/binary,",",(butil:tobin(Updaterid))/binary,",'",Node/binary,"',",
											(butil:tobin(NewVers))/binary,",",
								 				"'",(base64:encode(Sql))/binary,"');">>
			end,
			CrcTransaction = erlang:crc32(TransactionInfo),
			ComplSql = 
					[<<"$SAVEPOINT 'adb';">>,
					 TransactionInfo,
					 <<"$UPDATE __adb SET val='">>,butil:tolist(EvNum),<<"' WHERE id=",?EVNUM/binary,";">>,
					 <<"$UPDATE __adb SET val='">>,butil:tolist(CrcTransaction),<<"' WHERE id=",?EVCRC/binary,";">>
					 ],
			ok = okornot(actordb_sqlite:exec(P#dp.db,ComplSql)),
			?DBG("Replicating transaction write, connected ~p",[ConnectedNodes]),
			case ok of
				_ when (LenConnected+1)*2 > (LenCluster+1) ->
					Commiter = commit_write(P,LenCluster,ConnectedNodes,EvNum,TransactionInfo,CrcTransaction,P#dp.schemavers),
					{noreply,P#dp{callfrom = From,callres = undefined, commiter = Commiter, 
								  activity = P#dp.activity + 1,replicate_sql = {Sql,EvNum+1,Crc,NewVers},
								  % evnum = EvNum,evcrc = CrcTransaction,
								  transactioncheckref = CheckRef,
								  transactionid = TransactionId,
								  write_bytes = P#dp.write_bytes + iolist_size(Sql)}};
				_ ->
					actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
					erlang:demonitor(CheckRef),
					{reply,{error,{replication_failed_2,LenConnected,LenCluster}},P}
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
			case rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,<<>>,done,original]}) of
				ok ->
					exit(ok);
				false ->
					exit(false);
				Err ->
					exit(Err)
			end;
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

start_copylock(Fullname,O) ->
	start_copylock(Fullname,O,0).
start_copylock(Fullname,Opt,N) when N < 2 ->
	case distreg:whereis(Fullname) of
		undefined ->
			start(Opt);
		_ ->
			timer:sleep(1000),
			start_copylock(Fullname,Opt,N+1)
	end;
start_copylock(Fullname,_,_) ->
	Pid = distreg:whereis(Fullname),
	print_info(Pid),
	{error,{slave_proc_running,Pid,Fullname}}.


start_copyrec(P) ->
	StartRef = make_ref(),
	Home = self(),
	true = is_reference(P#dp.dbcopyref),
	spawn(fun() ->
		case distreg:reg(self(),{copyproc,P#dp.dbcopyref}) of
			ok ->
				?ADBG("Started copyrec ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.dbcopyref,P#dp.copyfrom]),
				Home ! {StartRef,self()},
				file:delete(P#dp.dbpath++"-wal"),
				file:delete(P#dp.dbpath++"-shm"),
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
						[{ok,_} = rpc(Nd,{?MODULE,start_copylock,[{P#dp.actorname,P#dp.actortype},StartOpt]}) || Nd <- ConnectedNodes];
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
					{ok,F1} = file:open(P#dp.dbpath,[write,raw]),
					ok = file:write(F1,Bin);
				false when Status == wal ->
					ok = file:close(F),
					{ok,F1} = file:open(P#dp.dbpath++"-wal",[write,raw]),
					ok = file:write(F1,Bin);
				false when Status == done ->
					file:close(F),
					F1 = undefined,
					{ok,Db,HaveSchema,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
					case HaveSchema of
						false ->
							?AERR("DB open after move without schema? ~p ~p",[P#dp.actorname,P#dp.actortype]),
							actordb_sqlite:stop(Db),
							actordb_sqlite:move_to_trash(P#dp.dbpath),
							exit(copynoschema);
						true ->
							?ADBG("Copyreceive done ~p",[{P#dp.actorname,P#dp.actortype,Origin,P#dp.copyfrom}]),
							ok = okornot(actordb_sqlite:exec(Db,<<"INSERT OR REPLACE INTO __adb (id,val) VALUES (",?COPYFROM/binary,",
											'",(base64:encode(term_to_binary({P#dp.copyfrom,P#dp.copyreset,
																				P#dp.cbstate})))/binary,"');">>)),
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
		{redirect,Somenode} ->
			case lists:member(Somenode,bkdcore:all_cluster_nodes()) of
				true ->
					exit(ok);
				false ->
					exit({unlock_invalid_redirect,Somenode})
			end;
		Err ->
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
					ok = rpc(P#dp.dbcopy_to,{?MODULE,call_slave,[P#dp.cbmod,ActorTo,P#dp.actortype,{db_chunk,P#dp.dbcopyref,<<>>,delete}]})
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

checkfail(_,[]) ->
	ok;
checkfail(N,L) ->
	?AERR("commit failed on ~p  ~p",[N,L]).

commit_write(P,LenCluster,ConnectedNodes,EvNum,Sql,Crc,SchemaVers) ->
	Ref = make_ref(),
	{Commiter,_} = spawn_monitor(fun() ->
			{ResultsStart,StartFailed} = rpc:multicall(ConnectedNodes,?MODULE,call_slave,
						[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{replicate_start,Ref,node(),P#dp.evnum,
																	P#dp.evcrc,Sql,EvNum,Crc,SchemaVers},[{flags,P#dp.flags}]]),
			checkfail(1,StartFailed),
			% Only count ok responses
			LenStarted = lists:foldl(fun(X,NRes) -> case X == ok of true -> NRes+1; false -> NRes end end,0,ResultsStart),
			case (LenStarted+1)*2 > LenCluster+1 of
				true ->
					NodesToCommit = lists:subtract(ConnectedNodes,StartFailed),
					{ResultsCommit,CommitFailedOn} = rpc:multicall(NodesToCommit,?MODULE,call_slave,
										[P#dp.cbmod,P#dp.actorname,P#dp.actortype,replicate_commit,[{flags,P#dp.flags}]]),
					checkfail(2,CommitFailedOn),
					LenCommited = length(ResultsCommit),
					case (LenCommited+1)*2 > LenCluster+1 of
						true ->
							WLog = <<(trim_wlog(P#dp.writelog))/binary,
												EvNum:64/unsigned,Crc:32/unsigned,(iolist_size(Sql)):32/unsigned,
													(iolist_to_binary(Sql))/binary>>,
							exit({ok, EvNum, Crc, WLog});
						false ->
							CommitOkOn = lists:subtract(NodesToCommit,CommitFailedOn),
							[rpc:async_call(Nd,?MODULE,call_slave,
										[P#dp.cbmod,P#dp.actorname,P#dp.actortype,{replicate_bad_commit,EvNum,Crc},[{flags,P#dp.flags}]]) || 
													Nd <- CommitOkOn],
							exit({replication_failed_3,LenCommited,LenCluster})
					end;
				false ->
					?AERR("replicate failed ~p ~p ~p",[Sql,?R2P(P),{ResultsStart,StartFailed}]),
					rpc:multicall(ConnectedNodes,?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,
																P#dp.actortype,replicate_rollback,[{flags,P#dp.flags}]]),
					exit({replication_failed_4,LenStarted,LenCluster})
			end
	end),
	Commiter.

get_from_wlog(FEv,FCrc,Bin) ->
	get_from_wlog(FEv,FCrc,Bin,undefined).
get_from_wlog(FEv,_,<<Evn:64,_/binary>>,undefined) when FEv < Evn ->
	% FEv too far behind
	undefined;
get_from_wlog(FEv,_,<<Evn:64,Evcrc:64,Size:32/unsigned,Sql:Size/binary>>,Out) when FEv < Evn ->
	% Reached the end of wlog and have something to return.
	{Evn,Evcrc,<<Out/binary,Sql/binary>>};
get_from_wlog(FEv,FCrc,<<Evn:64,_:64,Size:32/unsigned,Sql:Size/binary,Rem/binary>>,Out) when FEv < Evn ->
	% We went past FEv == Evn and are building the sql statement
	get_from_wlog(FEv,FCrc,Rem,<<Out/binary,Sql/binary>>);
get_from_wlog(FEv,FCrc,<<Evn:64,_:64,Size:32/unsigned,_:Size/binary,Rem/binary>>,L) when FEv > Evn ->
	% wlog is bigger than missing events, move upto FEv == Evn
	get_from_wlog(FEv,FCrc,Rem,L);
get_from_wlog(FEv,FCrc,<<FEv:64,Crc:64,Size:32/unsigned,_:Size/binary,Rem/binary>>,undefined) ->
	% FEv == Evn, if crcs match, then we can start extracting sql statements
	case FCrc == Crc of
		true ->
			get_from_wlog(FEv,FCrc,Rem,<<>>);
		% something went seriously wrong. Crcs don't match for evnums.
		false ->
			undefined
	end;
get_from_wlog(_,_,<<>>,undefined) ->
	undefined.


trim_wlog(B) when byte_size(B) < ?WLOG_LIMIT ->
	B;
trim_wlog(<<_Evnum:64,_Crc:32,Size:32/unsigned,_Sql:Size/binary,Rem/binary>>) ->
	trim_wlog(Rem).


handle_cast({diepls,Reason},P) ->
	case Reason of
		nomaster ->
			?AERR("Die because nomaster"),
			actordb_sqlite:stop(P#dp.db),
			distreg:unreg(self()),
			{stop,normal,P};
		_ ->
			case handle_info(check_inactivity,P) of
				{noreply,_,hibernate} ->
					% case apply(P#dp.cbmod,cb_candie,[P#dp.mors,P#dp.actorname,P#dp.actortype,P#dp.cbstate]) of
					% 	true ->
							?DBLOG(P#dp.db,"reqdie",[]),
							actordb_sqlite:stop(P#dp.db),
							distreg:unreg(self()),
							?ADBG("req die ~p ~p ~p",[P#dp.actorname,P#dp.actortype,Reason]),
							{stop,normal,P};
					% 	false ->
					% 		?AINF("NOPE ~p",[P#dp.actorname]),
					% 		{noreply,P}
					% end;
				R ->
					R
			end
	end;
handle_cast(print_info,P) ->
	io:format("~p~n",[?R2P(P#dp{writelog = byte_size(P#dp.writelog)})]),
	{noreply,P};
handle_cast(Msg,#dp{mors = master, verified = true} = P) ->
	case apply(P#dp.cbmod,cb_cast,[Msg,P#dp.cbstate]) of
		{noreply,S} ->
			{noreply,P#dp{cbstate = S}};
		noreply ->
			{noreply,P}
	end;
handle_cast(_Msg,P) ->
	?AINF("sqlproc ~p unhandled cast ~p~n",[P#dp.cbmod,_Msg]),
	{noreply,P}.


reply(undefined,_Msg) ->
	ok;
reply(From,Msg) ->
	gen_server:reply(From,Msg).

handle_info(doqueue, P) when P#dp.callfrom == undefined, P#dp.verified /= false, P#dp.transactionid == undefined ->
	case queue:is_empty(P#dp.callqueue) of
		true ->
			?DBG("doqueue empty"),
			{noreply,P};
		false ->
			{{value,Call},CQ} = queue:out_r(P#dp.callqueue),
			{From,Msg} = Call,
			?DBG("doqueue ~p",[Call]),
			case P#dp.verified of
				failed ->
					?AINF("stop cant verify"),
					reply(From,cant_verify_db),
					distreg:unreg(self()),
					{stop,cant_verify_db,P};
				_ ->
					case handle_call(Msg,From,P#dp{callqueue = CQ}) of
						{reply,Res,NP} ->
							reply(From,Res),
							handle_info(doqueue,NP);
						% If call returns noreply, it will continue processing later.
						{noreply,NP} ->
							{noreply,NP}
					end
			end
	end;
handle_info(doqueue,P) ->
	?DBG("doqueue ~p",[{P#dp.verified,P#dp.callfrom}]),
	case P#dp.verified of
		failed ->
			?AERR("Verify failed, actor stop ~p ~p",[P#dp.actorname,P#dp.actortype]),
			distreg:unreg(self()),
			?DBLOG(P#dp.db,"verify failed",[]),
			{stop,normal,P};
		_ ->
			{noreply,P}
	end;
% handle_info({'DOWN',_Monitor,Ref,PID,_Result},#dp{commiter = PID, callfrom = dbcopy} = P) ->
% 	handle_info({'DOWN',_Monitor,Ref,PID,_Result},P#dp{callfrom = undefined,commiter = undefined});
handle_info({'DOWN',_Monitor,_,PID,Result},#dp{commiter = PID} = P) ->
	case Result of
		{ok,EvNum,Crc,WLog} ->
			?DBLOG(P#dp.db,"commiterdown ok ~p ~p",[EvNum,Crc]),
			?DBG("Commiter down ok ~p callres ~p ~p",[EvNum,P#dp.callres,P#dp.callqueue]),
			{Sql,EvNumNew,CrcSql,NewVers} = P#dp.replicate_sql,
			case Sql of 
				<<"delete">> ->
					case P#dp.transactionid == undefined of
						true ->
							Die = true,
							delete_actor(P);
						false ->
							Die = false
					end;
				<<"moved,",MovedTo/binary>> ->
					?DBG("Stopping because moved ~p ~p",[P#dp.actorname,MovedTo]),
					Die = true,
					actordb_sqlite:stop(P#dp.db),
					delactorfile(P#dp{movedtonode = MovedTo});
				_ ->
					Die = false,
					actordb_sqlite:exec(P#dp.db,<<"RELEASE SAVEPOINT 'adb';">>)
			end,
			case P#dp.transactionid of
				undefined ->
					ReplicateSql = undefined,
					reply(P#dp.callfrom,P#dp.callres);
				_ ->
					{Tid,Updaterid,_} = P#dp.transactionid,
					case Sql of
						<<"delete">> ->
							ReplicateSql = {<<"delete">>,EvNumNew,CrcSql,NewVers},
							reply(P#dp.callfrom,ok);
						_ ->
							NewSql = <<Sql/binary,"$DELETE FROM __transactions WHERE tid=",(butil:tobin(Tid))/binary,
												" AND updater=",(butil:tobin(Updaterid))/binary,";">>,
							% Execute transaction sql and at the same time delete transaction sql from table.
							ComplSql = 
									[<<"$SAVEPOINT 'adb';">>,
									 NewSql,
									 <<"$UPDATE __adb SET val='">>,butil:tolist(EvNumNew),<<"' WHERE id=",?EVNUM/binary,";">>,
									 <<"$UPDATE __adb SET val='">>,butil:tolist(CrcSql),<<"' WHERE id=",?EVCRC/binary,";">>
									 ],
							Res = actordb_sqlite:exec(P#dp.db,ComplSql),
							reply(P#dp.callfrom,Res),
							% Store sql for later execution on slave nodes.
							ReplicateSql = {NewSql,EvNumNew,CrcSql,NewVers},
							case okornot(Res) of
								ok ->
									ok;
								_ ->
									Me = self(),
									spawn(fun() -> gen_server:call(Me,{commit,false,P#dp.transactionid}) end)
							end
					end
			end,
			case ok of
				_ when Die ->
					{stop,normal,P};
				_ ->
					handle_info(doqueue,check_timer(P#dp{commiter = undefined,callres = undefined, 
												callfrom = undefined,activity = P#dp.activity+1, 
												evnum = EvNum, evcrc = Crc,writelog = WLog, 
												schemavers = NewVers,
												replicate_sql = ReplicateSql}))
			end;
		% Should always be: {replication_failed,HasNodes,NeedsNodes}
		Err ->
			?DBLOG(P#dp.db,"commiterdown error ~p",[Err]),
			{Sql,_EvNumNew,_CrcSql,_NewVers} = P#dp.replicate_sql,
			case Sql of
				<<"delete">> ->
					ok;
				_ ->
					ok = okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>))
			end,
			reply(P#dp.callfrom,{error,Err}),
			handle_info(doqueue,P#dp{callfrom = undefined,commiter = undefined, transactionid = undefined, replicate_sql = undefined})
	end;
handle_info({'DOWN',_Monitor,_,PID,Reason},#dp{verifypid = PID} = P) ->
	case Reason of
		{verified,Mors,MasterNode} when P#dp.transactionid == undefined; Mors == slave ->
			actordb_local:actor_mors(Mors,MasterNode),
			?ADBG("Verify down ~p ~p ~p ~p ~p ~p",[P#dp.actorname, P#dp.actortype, P#dp.evnum,
						Reason, P#dp.mors, queue:is_empty(P#dp.callqueue)]),
			?DBLOG(P#dp.db,"verified ~p ~p",[Mors,MasterNode]),
			case Mors of
				master ->
					% If any uncommited transactions, check if they are abandonded or to be executed
					NS = do_cb_init(P);
				_ ->
					NS = P#dp.cbstate
			end,
			handle_info(doqueue,P#dp{verified = true, verifypid = undefined, mors = Mors, masternode = MasterNode, 
									masternodedist = bkdcore:dist_name(MasterNode),
									cbstate = NS});
		{verified,Mors,MasterNode} ->
			?ADBG("Verify down ~p ~p ~p ~p ~p ~p",[P#dp.actorname, P#dp.actortype, P#dp.evnum,
						Reason, P#dp.mors, queue:is_empty(P#dp.callqueue)]),
			?DBLOG(P#dp.db,"verified ~p ~p",[Mors,MasterNode]),
			actordb_local:actor_mors(Mors,MasterNode),
			{Tid,Updid,Node} = P#dp.transactionid,
			{Sql,Evnum,Crc,_NewVers} = P#dp.replicate_sql,
			NP = P#dp{verified = true,verifypid = undefined, mors = Mors, 
					 masternode = MasterNode,masternodedist = bkdcore:dist_name(MasterNode), cbstate = do_cb_init(P)},
			case actordb:rpc(Node,Updid,{actordb_multiupdate,transaction_state,[Updid,Tid]}) of
				{ok,State} when State == 0; State == 1 ->
					ComplSql = 
						[<<"$SAVEPOINT 'adb';">>,
						 semicolon(Sql),actornum(P),
						 <<"$DELETE FROM __transactions WHERE tid=",(butil:tobin(Tid))/binary,
						 		" AND updater=",(butil:tobin(Updid))/binary,";">>,
						 <<"$UPDATE __adb SET val='">>,butil:tolist(Evnum),<<"' WHERE id=",?EVNUM/binary,";">>,
						 <<"$UPDATE __adb SET val='">>,butil:tolist(Crc),<<"' WHERE id=",?EVCRC/binary,";">>
						 ],
					actordb_sqlite:exec(P#dp.db,ComplSql),
					% 0 - transaction still running, wait for done.
					% 1 - finished, do commit straight away.
					case State of
						0 ->
							{_CheckPid,CheckRef} = start_transaction_checker(Tid,Updid,Node),
							{noreply,P#dp{transactioncheckref = CheckRef}};
						1 ->
							CQ = queue:in({undefined,{commit,true,{Tid,Updid,Node}}},NP#dp.callqueue),
							handle_info(doqueue,NP#dp{callqueue = CQ})
					end;
				% Lets forget this ever happened.
				{ok,-1} ->
					Sql = <<"DELETE FROM __transactions id=",(butil:tobin(Tid))/binary,
								" AND updater=",(butil:tobin(Updid))/binary,";">>,
					CQ = queue:in({undefined,{write,{erlang:crc32(Sql),Sql,undefined}}},NP#dp.callqueue),
					handle_info(doqueue,NP#dp{callqueue = CQ});
				% In case of error, process should crash, because it can not process sql if it can not verify last transaction
				_Err ->
					exit({error,{unable_to_verify_transaction,_Err}})
			end;
		{update_from,Node,Mors,MasterNode,Ref} ->
			?ADBG("Verify down ~p ~p ~p master ~p, update from ~p",[P#dp.actorname,PID,Reason,MasterNode,Node]),
			% handle_info(doqueue,P#dp{verified = false, verifypid = undefined, mors = Mors});
			case P#dp.copyfrom of
				undefined ->
					Copyfrom = Node;
				Copyfrom ->
					ok
			end,
			actordb_sqlite:stop(P#dp.db),
			{ok,RecvPid} = start_copyrec(P#dp{copyfrom = Copyfrom, mors = master, dbcopyref = Ref}),
			erlang:monitor(process,RecvPid),
			{noreply,P#dp{db = undefined,
							verifypid = undefined, verified = false, mors = Mors, masternode = MasterNode,
							masternodedist = bkdcore:dist_name(MasterNode),dbcopyref = Ref, copyfrom = Copyfrom, copyproc = RecvPid}};
		{update_direct,Mors,Bin} ->
			?ADBG("Verify down update direct ~p ~p ~p",[Mors,P#dp.actorname,P#dp.actortype]),
			actordb_sqlite:stop(P#dp.db),
			delactorfile(P),
			ok = file:write_file(P#dp.dbpath,Bin),
			{ok,NP} = init(P#dp{mors = Mors},update_direct),
			{noreply,NP};
		{redirect,Nd} ->
			?AINF("verify redirect ~p ~p",[P#dp.actorname,Nd]),
			% case lists:member(Nd,bkdcore:)
			{stop,normal,P};
		{wlog,Crc,EvNum,Sql} ->
			?ADBG("Verify wlog sql ~p ~p",[P#dp.actorname,P#dp.actortype]),
			ok = okornot(actordb_sqlite:exec(P#dp.db,[
					 <<"$SAVEPOINT 'adb';">>,
					 Sql,
					 <<"$UPDATE __adb SET val='">>,butil:tolist(EvNum),<<"' WHERE id=",?EVNUM/binary,";">>,
					 <<"$UPDATE __adb SET val='">>,butil:tolist(Crc),<<"' WHERE id=",?EVCRC/binary,";">>,
					 <<"$RELEASE SAVEPOINT 'adb';">>
					 ])),
			{ok,NP} = init(P#dp{evnum = EvNum, evcrc = Crc},update_wlog),
			{noreply,NP};
		{nomajority,Groups} ->
			?AERR("Verify nomajority ~p",[Groups]),
			handle_info(doqueue,P#dp{verified = failed, verifypid = undefined});
		{nomajority,Groups,Failed} ->
			?AERR("Verify nomajority ~p ~p",[Groups,Failed]),
			handle_info(doqueue,P#dp{verified = failed, verifypid = undefined});
		{error,enoent} ->
			?AERR("error enoent result of verify ~p ~p",[P#dp.actorname,P#dp.actortype]),
			distreg:unreg(self()),
			{stop,normal,P};
		nomaster ->
			?AERR("No master found for verify ~p",[?R2P(P)]),
			{stop,nomaster,P};
		_ ->
			case queue:is_empty(P#dp.callqueue) of
				true ->
					?AERR("Verify down for ~p error ~p",[P#dp.actorname,Reason]);
				false ->
					?AERR("Verify down for ~p error ~p ~p",[P#dp.actorname,Reason,queue:out_r(P#dp.callqueue)])
			end,
			{Verifypid,_} = spawn_monitor(fun() -> timer:sleep(500), 
													verifydb(P#dp.actorname,P#dp.actortype,P#dp.evcrc,
																P#dp.evnum,P#dp.mors,P#dp.cbmod,P#dp.flags) 
												end),
			{noreply,P#dp{verified = false, verifypid = Verifypid}}
	end;
handle_info({'DOWN',Ref,_,_PID,Reason},#dp{transactioncheckref = Ref} = P) ->
	?ADBG("Transactioncheck died ~p myid ~p",[Reason,P#dp.transactionid]),
	case P#dp.transactionid of
		{Tid,Updaterid,Node} ->
			case Reason of
				noproc ->
					{_CheckPid,CheckRef} = start_transaction_checker(Tid,Updaterid,Node),
					{noreply,P#dp{transactioncheckref = CheckRef}};
				abandonded ->
					case handle_call({commit,false,P#dp.transactionid},undefined,P#dp{transactioncheckref = undefined}) of
						{_,_,NP} ->
							{noreply,NP};
						{noreply,NP} ->
							{noreply,NP}
					end;
				done ->
					case handle_call({commit,true,P#dp.transactionid},undefined,P#dp{transactioncheckref = undefined}) of
						{_,_,NP} ->
							{noreply,NP};
						{noreply,NP} ->
							{noreply,NP}
					end
			end;
		_ ->
			{noreply,P#dp{transactioncheckref = undefined}}
	end;
handle_info({'DOWN',_Monitor,_,PID,Reason}, #dp{copyproc = PID} = P) ->
	?DBG("copyproc died ~p ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},Reason,P#dp.mors,P#dp.copyfrom]),
	case Reason of
		ok when P#dp.mors == master; is_binary(P#dp.copyfrom) ->
			{ok,NP} = init(P,copyproc_done),
			{noreply,NP};
		ok when P#dp.mors == slave ->
			{stop,normal,P};
		_ ->
			?AINF("Coproc died ~p~n",[?R2P(P)]),
			{stop,Reason,P}
	end;
handle_info({'DOWN',_Monitor,_,PID,Reason} = Msg,P) ->
	case lists:keyfind(PID,2,P#dp.dbcopy_to) of
		{Node,PID,Ref,IsMove} ->
			?ADBG("Down copyto proc ~p ~p ~p ~p ~p",[P#dp.actorname,Reason,Ref,P#dp.locked,P#dp.dbcopy_to]),
			case IsMove of
				true when Reason == ok ->
					Moved = Node;
				_ ->
					Moved = undefined
			end,
			case Reason of
				ok ->
					ok;
				_ ->
					?AERR("Copyto process invalid exit ~p",[Reason])
			end,
			WithoutCopy = lists:keydelete(PID,1,P#dp.locked),
			NewCopyto = lists:keydelete(PID,2,P#dp.dbcopy_to),
			case lists:keyfind(Ref,2,WithoutCopy) of
				false ->
					% wait_copy not in list add it (2nd stage of lock)
					WithoutCopy1 =  [{wait_copy,Ref,IsMove,Node,os:timestamp()}|WithoutCopy],
					Md = P#dp.journal_mode,
					Movedtonode = undefined;
				{wait_copy,Ref,_,_,_} ->
					% wait_copy already in list (race condition). Remove it. We already received confirmation.
					WithoutCopy1 = lists:keydelete(Ref,2,WithoutCopy),
					case ok of
						_ when WithoutCopy1 == [], NewCopyto == [] ->
							actordb_sqlite:set_pragmas(P#dp.db,P#dp.def_journal_mode),
							Md = P#dp.def_journal_mode;
						_ ->
							Md = P#dp.journal_mode
					end,
					Movedtonode = Moved
			end,
			NP = P#dp{dbcopy_to = NewCopyto, 
						locked = WithoutCopy1,journal_mode = Md,
						activity = P#dp.activity + 1, movedtonode = Movedtonode},
			case queue:is_empty(P#dp.callqueue) of
				true ->
					{noreply,NP};
				false ->
					handle_info(doqueue,NP)
			end;
		false ->
			?ADBG("downmsg, verify maybe? ~p",[P#dp.verifypid]),
			case apply(P#dp.cbmod,cb_info,[Msg,P#dp.cbstate]) of
				{noreply,S} ->
					{noreply,P#dp{cbstate = S}};
				noreply ->
					{noreply,P}
			end
	end;
handle_info({inactivity_timer,Ref,N},P) ->
	case Ref == P#dp.timerref of
		true ->
			handle_info({check_inactivity,N},P#dp{timerref = undefined});
		false ->
			handle_info({check_inactivity,N},P)
	end;
handle_info({check_inactivity,N}, P) ->
	% ?AINF("check_inactivity ~p ~p ~p~n",[{N,P#dp.activity},{P#dp.actorname,P#dp.callfrom},{P#dp.dbcopyref,P#dp.dbcopy_to,P#dp.locked,P#dp.copyproc,P#dp.verified,P#dp.transactionid}]),
	Empty = queue:is_empty(P#dp.callqueue),
	case P of
		% If true, process is inactive and can die (or go to sleep)
		#dp{activity = N, callfrom = undefined, verified = true, transactionid = undefined,
			dbcopyref = undefined, dbcopy_to = [], locked = [], copyproc = undefined} when Empty ->
			case P#dp.movedtonode of
				undefined ->
					case apply(P#dp.cbmod,cb_candie,[P#dp.mors,P#dp.actorname,P#dp.actortype,P#dp.cbstate]) of
						true ->
							?DBG("Die because temporary ~p ~p",[P#dp.actorname,P#dp.actortype]),
							distreg:unreg(self()),
							?DBLOG(P#dp.db,"die temporary ",[]),
							{stop,normal,P};
						_ when P#dp.activity == 0 ->
							case timer:now_diff(os:timestamp(),P#dp.start_time) > 10*1000000 of
								true ->
									?ADBG("die after 10sec inactive"),
									?DBLOG(P#dp.db,"die 0 after 10sec",[]),
									actordb_sqlite:stop(P#dp.db),
									distreg:unreg(self()),
									{stop,normal,P};
								false ->
									Now = actordb_local:actor_activity(P#dp.activity_now),
									{noreply,check_timer(P#dp{activity_now = Now})}
							end;
						_ when (P#dp.flags band ?FLAG_NOHIBERNATE) > 0 ->
							actordb_sqlite:stop(P#dp.db),
							distreg:unreg(self()),
							{stop,normal,P};
						_ ->
							?DBG("Process hibernate ~p",[P#dp.actorname]),
							case P#dp.timerref /= undefined of
								true ->
									erlang:cancel_timer(P#dp.timerref),
									Timer = undefined;
								false ->
									Timer = P#dp.timerref
							end,
							{noreply,P#dp{timerref = Timer},hibernate}
					end;
				_ ->
					case P#dp.db of
						undefined ->
							ok;
						_ ->
							actordb_sqlite:stop(P#dp.db),
							delactorfile(P),
							[rpc:async_call(Nd,?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
																	{delete,P#dp.movedtonode},[{flags,P#dp.flags}]]) 
									|| Nd <- bkdcore:cluster_nodes_connected()]
					end,
					case timer:now_diff(os:timestamp(),P#dp.start_time) > 10*1000000 of
						true ->
							?ADBG("Die because moved"),
							?DBLOG(P#dp.db,"die moved ",[]),
							distreg:unreg(self()),
							{stop,normal,P};
						false ->
							Now = actordb_local:actor_activity(P#dp.activity_now),
							{noreply,check_timer(P#dp{activity_now = Now, db = undefined})}
					end
			end;
		_ when P#dp.verifypid == undefined, P#dp.verified /= true ->
			case P#dp.verified of
				failed ->
					?AERR("verify fail ~p",[?R2P(P)]);
				_ ->
					ok
			end,
			case timer:now_diff(os:timestamp(),P#dp.start_time) > 20*1000000 of
				true ->
					{stop,verifyfail,P};
				false ->
					{noreply,P}
			end;
		_ ->
			Now = actordb_local:actor_activity(P#dp.activity_now),
			case P#dp.write_bytes > 1024*32 of
				true when P#dp.dbcopyref == undefined, P#dp.dbcopy_to == [] ->
					actordb_sqlite:checkpoint(P#dp.db),
					WB = 0;
				_ ->
					WB = P#dp.write_bytes
			end,
			{noreply,check_timer(P#dp{activity_now = Now, write_bytes = WB, locked = abandon_locks(P,P#dp.locked,[])})}
	end;
handle_info(check_inactivity,P) ->
	handle_info({check_inactivity,P#dp.activity+1},P#dp{activity = P#dp.activity + 1});
handle_info(stop,P) ->
	handle_info({stop,normal},P);
handle_info({stop,Reason},P) ->
	distreg:unreg(self()),
	?ADBG("Actor stop with reason ~p",[Reason]),
	{stop, normal, P};
handle_info(print_info,P) ->
	handle_cast(print_info,P);
handle_info(Msg,#dp{mors = master, verified = true} = P) ->
	case apply(P#dp.cbmod,cb_info,[Msg,P#dp.cbstate]) of
		{noreply,S} ->
			{noreply,P#dp{cbstate = S}};
		noreply ->
			{noreply,P}
	end;
handle_info({check_redirect,Db,IsMove},P) ->
	case check_redirect(P,P#dp.copyfrom) of
		false ->
			file:delete(P#dp.dbpath),
			file:delete(P#dp.dbpath++"-wal"),
			file:delete(P#dp.dbpath++"-shm"),
			{stop,{error,failed}};
		Red  ->
			?ADBG("Returned redirect ~p ~p ~p",[P#dp.actorname,Red,P#dp.copyreset]),
			case Red of
				{true,NewShard} when NewShard /= undefined ->
					ok = actordb_shard:reg_actor(NewShard,P#dp.actorname,P#dp.actortype);
				_ ->
					ok
			end,
			ResetSql = do_copy_reset(IsMove,P#dp.copyreset,P#dp.cbstate),
			case actordb_sqlite:exec(Db,<<"BEGIN;DELETE FROM __adb WHERE id=",(?COPYFROM)/binary,";",
												  ResetSql/binary,"COMMIT;">>) of
				{ok,_} ->
					ok;
				ok ->
					ok
			end,
			actordb_sqlite:stop(Db),
			{ok,NP} = init(P#dp{db = undefined,copyfrom = undefined, copyreset = false, mors = master},cleanup_copymove),
			{noreply,NP}
	end;
handle_info(_Msg,P) ->
	?DBG("sqlproc ~p unhandled info ~p~n",[P#dp.cbmod,_Msg]),
	{noreply,P}.

abandon_locks(P,[{wait_copy,_CpRef,_IsMove,_Node,TimeOfLock} = H|T],L) ->
	case timer:now_diff(os:timestamp(),TimeOfLock) > 3000000 of
		true ->
			?AERR("Abandoned lock ~p ~p ~p",[P#dp.actorname,_Node,_CpRef]),
			abandon_locks(P,T,L);
		false ->
			abandon_locks(P,T,[H|L])
	end;
abandon_locks(P,[H|T],L) ->
	abandon_locks(P,T,[H|L]);
abandon_locks(_,[],L) ->
	L.


terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init(#dp{} = P,_Why) ->
	?ADBG("Reinit because ~p, ~p",[_Why,?R2P(P)]),
	init([{actor,P#dp.actorname},{type,P#dp.actortype},{mod,P#dp.cbmod},{flags,P#dp.flags},
		  {state,P#dp.cbstate},{slave,P#dp.mors == slave},{queue,P#dp.callqueue},{startreason,{reinit,_Why}}]).
init([_|_] = Opts) ->
	case parse_opts(check_timer(#dp{mors = master, callqueue = queue:new(), start_time = os:timestamp(), 
									schemanum = actordb_schema:num()}),Opts) of
		{registered,Pid} ->
			?ADBG("die already registered"),
			explain({registered,Pid},Opts),
			{stop,normal};
		P when (P#dp.flags band ?FLAG_ACTORNUM) > 0 ->
			explain({actornum,P#dp.dbpath,read_num(P)},Opts),
			{stop,normal};
		P when (P#dp.flags band ?FLAG_EXISTS) > 0 ->
			{ok,Db,HaveSchema,_PageSize} = actordb_sqlite:init(P#dp.dbpath,actordb_conf:journal_mode()),
			actordb_sqlite:stop(Db),
			explain({ok,[{columns,{<<"exists">>}},{rows,[{butil:tobin(HaveSchema)}]}]},Opts),
			{stop,normal};
		P when (P#dp.flags band ?FLAG_STARTLOCK) > 0 ->
			case lists:keyfind(lockinfo,1,Opts) of
				{lockinfo,dbcopy,{Ref,CbState,CpFrom,CpReset}} ->
					?ADBG("Starting actor slave lock for copy on ref ~p",[Ref]),
					{ok,Pid} = start_copyrec(P#dp{mors = slave, cbstate = CbState, 
													dbcopyref = Ref,  copyfrom = CpFrom, copyreset = CpReset}),
					erlang:monitor(process,Pid),
					{ok,P#dp{copyproc = Pid, verified = false,mors = slave, copyfrom = P#dp.copyfrom}}
					% receive
					% 	{'DOWN',_Monitor,_,Pid,_Reason} ->
					% 		?AINF("Copy process for slave died ~p",[_Reason]),
					% 		{stop,normal}
					% end
			end;
		P ->
			ClusterNodes = bkdcore:cluster_nodes(),
			?ADBG("Actor start ~p ~p ~p ~p ~p ~p, startreason ~p",[P#dp.actorname,P#dp.actortype,P#dp.copyfrom,
													queue:is_empty(P#dp.callqueue),ClusterNodes,
					bkdcore:node_name(),butil:ds_val(startreason,Opts)]),
			case P#dp.mors of
				master when ClusterNodes == [] ->
					JournalMode = actordb_conf:journal_mode();
				master ->
					JournalMode = actordb_conf:journal_mode();
				slave ->
					JournalMode = off
			end,
			case P#dp.copyfrom of
				undefined ->
					MovedToNode = apply(P#dp.cbmod,cb_checkmoved,[P#dp.actorname,P#dp.actortype]),
					RightCluster = lists:member(MovedToNode,bkdcore:all_cluster_nodes()),
					case ok of
						_ when MovedToNode == undefined; RightCluster ->
							{ok,Db,HaveSchema,PageSize} = actordb_sqlite:init(P#dp.dbpath,JournalMode),
							NP = P#dp{db = Db, journal_mode = JournalMode, def_journal_mode = JournalMode, 
										page_size = PageSize},

							case HaveSchema of
								true ->
									?ADBG("Opening HAVE schema ~p",[{P#dp.actorname,P#dp.actortype}]),
									?DBLOG(Db,"init normal have schema",[]),
									{ok,[[{columns,_},{rows,Transaction}],
										[{columns,_},{rows,Rows}]]} = actordb_sqlite:exec(Db,
											<<"SELECT * FROM __adb;",
											  "SELECT * FROM __transactions;">>),
									Evnum = butil:toint(butil:ds_val(?EVNUMI,Rows)),
									Evcrc = butil:toint(butil:ds_val(?EVCRCI,Rows)),
									Vers = butil:toint(butil:ds_val(?SCHEMA_VERSI,Rows)),
									MovedToNode1 = butil:ds_val(?MOVEDTOI,Rows),
									CopyFrom = butil:ds_val(?COPYFROMI,Rows),
									case Transaction of
										[] when CopyFrom /= undefined ->
											CPFrom = binary_to_term(base64:decode(CopyFrom)),
											case CPFrom of
												{{move,_NewShard,_Node},CopyReset,CopyState} ->
													IsMove = true;
												{_,CopyReset,CopyState} ->
													IsMove = false
											end,
											self() ! {check_redirect,Db,IsMove},
											{ok,P#dp{copyreset = CopyReset,copyfrom = CPFrom,cbstate = CopyState}};
										[] ->
											case apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,Vers]) of
												{_,[]} ->
													SchemaVers = Vers;
												{SchemaVers,Schema} ->
													ok = okornot(actordb_sqlite:exec(Db,
															<<"BEGIN;",(iolist_to_binary(Schema))/binary,
																"UPDATE __adb SET val='",(butil:tobin(SchemaVers))/binary,
																		"' WHERE id=",?SCHEMA_VERS/binary,";",
																"COMMIT;">>))
											end,
											{ok,start_verify(NP#dp{evnum = Evnum, evcrc = Evcrc, schemavers = SchemaVers,movedtonode = MovedToNode1})};
										[{1,Tid,Updid,Node,SchemaVers,MSql1}] ->
											case base64:decode(MSql1) of
												<<"delete">> ->
													CrcSql = 0,
													MSql = delete;
												MSql ->
													CrcSql = erlang:crc32(MSql)
											end,
											ReplSql = {MSql,Evnum+1,CrcSql,SchemaVers},
											Transid = {Tid,Updid,Node},
											{ok,start_verify(NP#dp{evnum = Evnum, evcrc = Evcrc, replicate_sql = ReplSql, 
															transactionid = Transid, 
															movedtonode = MovedToNode1,
															schemavers = SchemaVers})}
									end;
								false when (P#dp.flags band ?FLAG_CREATE) > 0 ->
									?ADBG("Opening NO schema create ~p",[{P#dp.actorname,P#dp.actortype}]),
									{SchemaVers,Schema} = apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,0]),
									CreateDb = [base_schema(SchemaVers,P#dp.actortype),
												 Schema,
												 <<"COMMIT;">>],
									ok = okornot(actordb_sqlite:exec(Db,CreateDb)),
									?DBLOG(Db,"init normal created schema",[]),
									{ok,start_verify(NP#dp{schemavers = SchemaVers})};
								false ->
									actordb_sqlite:stop(NP#dp.db),
									?ADBG("Opening NO schema nocreate ~p",[{P#dp.actorname,P#dp.actortype}]),
									explain(nocreate,Opts),
									{stop,normal}
							end;
						_ ->
							?ADBG("Actor moved ~p ~p ~p",[P#dp.actorname,P#dp.actortype,MovedToNode]),
							{ok, P#dp{verified = true, movedtonode = MovedToNode,
										journal_mode = JournalMode, activity_now = actor_start(P)}}
					end;
				% Either create a copy of an actor or move an actor from one cluster to another.
				_ ->
					?ADBG("start copyfrom ~p ~p ~p",[P#dp.actorname,P#dp.actortype,P#dp.copyfrom]),
					IsMove = element(1,P#dp.copyfrom) == move,
					% First check if movement is already done.
					case actordb_sqlite:init(P#dp.dbpath,JournalMode) of
						{ok,Db,true,_PageSize} ->
							?DBLOG(P#dp.db,"init copyfrom ~p",[P#dp.copyfrom]),
							case actordb_sqlite:exec(Db,[<<"select * from __adb where id=">>,?COPYFROM,";"]) of
								{ok,[{columns,_},{rows,[]}]} ->
									case IsMove of
										true ->
											Doit = check;
										false ->
											Doit = true
									end;
								{ok,[{columns,_},{rows,[{_,_Copyf}]}]} ->
									Doit = false
							end;
						{ok,Db,false,_PageSize} ->
							actordb_sqlite:stop(Db),
							Doit = true;
						_ ->
							Db = undefined,
							Doit = true
					end,
					case Doit of
						true  ->
							{Verifypid,_} = spawn_monitor(fun() -> 
														verify_getdb(P#dp.actorname,P#dp.actortype,P#dp.copyfrom,
															undefined,master,P#dp.cbmod,P#dp.evnum,P#dp.evcrc) 
													end),
							{ok,P#dp{verified = false, verifypid = Verifypid, mors = master,
								journal_mode = JournalMode, activity_now = actor_start(P)}};
						_ ->
							?AINF("Started for copy but copy already done or need check (~p) ~p ~p",[Doit,P#dp.actorname,P#dp.actortype]),
							self() ! {check_redirect,Db,IsMove},
							{ok,P}
					end
			end
	end;
init(#dp{} = P) ->
	init(P,noreason).


check_redirect(P,Copyfrom) ->
	case Copyfrom of
		{move,NewShard,Node} ->
			case bkdcore:rpc(Node,{?MODULE,call_master,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
											{getinfo,donothing}]}) of
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
				ok ->
					false
			end;
		{Copyf1,_,_} when is_tuple(Copyf1) ->
			check_redirect(P,Copyf1);
		_ ->
			copy
	end.


do_copy_reset(IsMove,Copyreset,State) ->
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
	case IsMove of
		true ->
			<<>>;
		_ ->
			ResetSql
	end.

start_verify(P) ->
	ClusterNodes = bkdcore:cluster_nodes(),
	case ok of
		_ when P#dp.movedtonode /= undefined; ClusterNodes == [] ->
			P#dp{verified = true,cbstate = do_cb_init(P), activity_now = actor_start(P)};
		_ ->
			{Verifypid,_} = spawn_monitor(fun() -> 
							verifydb(P#dp.actorname,P#dp.actortype,P#dp.evcrc,P#dp.evnum,P#dp.mors,P#dp.cbmod,P#dp.flags) 
								end),
			P#dp{verifypid = Verifypid, verified = false, activity_now = actor_start(P)}
	end.

actor_start(P) ->
	actordb_local:actor_started(P#dp.actorname,P#dp.actortype,P#dp.page_size*?DEF_CACHE_PAGES+?WLOG_LIMIT).

read_num(P) ->
	case P#dp.db of
		undefined ->
			{ok,Db,HaveSchema,_PageSize} = actordb_sqlite:init(P#dp.dbpath,actordb_conf:journal_mode());
		Db ->
			HaveSchema = true
	end,
	case HaveSchema of
		false ->
			<<>>;
		_ ->
			Res = actordb_sqlite:exec(Db,
						<<"SELECT * FROM __adb WHERE id=",?ANUM/binary,";">>),
			case Res of
				{ok,[{columns,_},{rows,[]}]} ->
					<<>>;
				{ok,[{columns,_},{rows,[{_,Num}]}]} ->
					Num;
				{sql_error,{"exec_script",sqlite_error,"no such table: __adb"}} ->
					<<>>
			end
	end.


explain(What,Opts) ->
	case lists:keyfind(start_from,1,Opts) of
		{_,{FromPid,FromRef}} ->
			FromPid ! {FromRef,What};
		_ ->
			ok
	end.

base_schema(SchemaVers,Type) ->
	base_schema(SchemaVers,Type,undefined).
base_schema(SchemaVers,Type,MovedTo) ->
	case MovedTo of
		undefined ->
			Moved = <<>>;
		_ ->
			Moved = <<"INSERT INTO __adb (id,val) VALUES (",?MOVEDTO/binary,",'",MovedTo/binary,"');">>
	end,
	<<"BEGIN;",(?LOGTABLE)/binary,
	 "CREATE TABLE __transactions (id INTEGER PRIMARY KEY, tid INTEGER,",
	 	" updater INTEGER, node TEXT,schemavers INTEGER, sql TEXT);",
	 "CREATE TABLE __adb (id INTEGER PRIMARY KEY, val TEXT);",
	 "INSERT INTO __adb (id,val) VALUES (",?EVNUM/binary,",'0');",
	 "INSERT INTO __adb (id,val) VALUES (",?EVCRC/binary,",'0');",
	 "INSERT INTO __adb (id,val) VALUES (",?SCHEMA_VERS/binary,",'",
	 						(butil:tobin(SchemaVers))/binary, "');",
	Moved/binary,
	 "INSERT INTO __adb (id,val) VALUES (",?ATYPE/binary,",'",
	 		(butil:tobin(Type))/binary, "');">>.

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
			case apply(P#dp.cbmod,cb_init,[P#dp.cbstate,P#dp.evnum,actordb_sqlite:exec(P#dp.db,Sql)]) of
				{ok,NS} ->
					NS;
				ok ->
					P#dp.cbstate
			end;
		ok ->
			P#dp.cbstate
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


delactorfile(P) ->
	[Pid ! delete || {_,Pid,_,_} <- P#dp.dbcopy_to],
	?ADBG("delfile ~p ~p ~p",[P#dp.actorname,P#dp.actortype,P#dp.mors]),
	case P#dp.movedtonode of
		undefined ->
			file:delete(P#dp.dbpath),
			case P#dp.journal_mode of
				wal ->
					file:delete(P#dp.dbpath++"-wal"),
					file:delete(P#dp.dbpath++"-shm");
				_ ->
					ok
			end;
		_ ->
			% Leave behind redirect marker.
			% Create a file with "1" attached to end
			{ok,Db,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath++"1",off),
			ok = okornot(actordb_sqlite:exec(Db,[base_schema(0,P#dp.actortype,P#dp.movedtonode),<<"COMMIT;">>])),
			actordb_sqlite:stop(Db),
			% Rename into the actual dbfile (should be atomic op)
			ok = file:rename(P#dp.dbpath++"1",P#dp.dbpath),
			file:delete(P#dp.dbpath++"-wal"),
			file:delete(P#dp.dbpath++"-shm")
	end.

verified_response(MeMors,MasterNode) ->
	?ADBG("verified_response ~p ~p",[MeMors,MasterNode]),
	Me = bkdcore:node_name(),
	case ok of
		_ when MeMors == master, MasterNode == undefined ->
			exit({verified,master,Me});
		_ when MeMors == master, MasterNode /= Me, MasterNode /= undefined ->
			exit({verified,slave,MasterNode});
		_ when MasterNode /= undefined ->
			exit({verified,MeMors,MasterNode});
		_ when MeMors == master ->
			exit({verified,master,Me});
		_ ->
			exit(nomaster)
	end.
verifydb(Actor,Type,Evcrc,Evnum,MeMors,Cb,Flags) ->
	?ADBG("Verifydb ~p ~p ~p ~p ~p ~p",[Actor,Type,Evcrc,Evnum,MeMors,Cb]),
	ClusterNodes = bkdcore:cluster_nodes(),
	LenCluster = length(ClusterNodes),
	ConnectedNodes = bkdcore:cluster_nodes_connected(),
	{Results,GetFailed} = rpc:multicall(ConnectedNodes,?MODULE,call_slave,[Cb,Actor,Type,{getinfo,verifyinfo},[{flags,Flags}]]),
	?ADBG("verify from others ~p",[Results]),
	checkfail(3,GetFailed),
	Me = bkdcore:node_name(),
	% Count how many nodes have db with same last evnum and evcrc and gather nodes that are different.
	{Yes,Masters} = lists:foldl(
			fun({redirect,Nd},_) -> 
				exit({redirect,Nd});
			 ({ok,Node,NodeCrc,NodeEvnum,NodeMors},{YesVotes,Masters}) -> 
			 	case NodeMors of
			 		{master,true} ->
			 			Masters1 = [{Node,true}|Masters];
			 		{master,false} ->
			 			Masters1 = [{Node,false}|Masters];
			 		{slave,_} ->
			 			Masters1 = Masters;
			 		master ->
			 			Masters1 = [{Node,true}|Masters];
			 		slave ->
			 			Masters1 = Masters
			 	end,
				case Evcrc == NodeCrc andalso Evnum == NodeEvnum of
					true ->
						{YesVotes+1,Masters1};
					false ->
						{YesVotes,Masters1}
				end;
			(_,{YesVotes,Masters}) ->
				{YesVotes,Masters}
			end,
			{0,[]},Results),
	case Masters of
		[] when MeMors == master ->
			MasterNode = bkdcore:node_name();
		[] ->
			?AERR("No master node set ~p ~p ~p",[{Actor,Type},MeMors,Results]),
			MasterNode = undefined,
			exit(nomaster);
		[{MasterNode,true}] ->
			ok;
		[{MasterNode1,_}] when MeMors == master, Me < MasterNode1 ->
			MasterNode = Me;
		[{MasterNode,_}] ->
			ok;
		[_,_|_] ->
			case [MN || {MN,true} <- Masters] of
				[MasterNode] ->
					ok;
				% This should not be possible, kill all actors on all nodes
				[_,_|_] ->
					?AERR("Received multiple confirmed masters?? ~p ~p",[{Actor,Type},Results]),
					rpc:multicall(ConnectedNodes,?MODULE,call_slave,[Cb,Actor,Type,{getinfo,conflicted},[{flags,Flags}]]),
					MasterNode = undefined,
					exit(nomaster);
				[] ->
					case MeMors of
						master ->
							[MasterNode|_] = lists:sort([Me|[MN || {MN,_} <- Masters]]);
						_ ->
							[MasterNode|_] = lists:sort([MN || {MN,_} <- Masters])
					end
			end
	end,
	% This node is in majority group.
	case (Yes+1)*2 > (LenCluster+1) of
		true ->
			case Evnum == 0 of
				true ->
					case butil:findtrue(fun({ok,_,_,NodeEvnum,_}) -> NodeEvnum > 0 end,Results) of
						false ->
							verified_response(MeMors,MasterNode);
						_ ->
							?ADBG("Node does not have db some other node does! ~p",[Actor]),
							% Majority has evnum 0, but there is more than one group.
							% This is an exception. In this case highest evnum db is the right one.
							[{ok,Oknode,_,_,_}|_] = lists:reverse(lists:keysort(4,Results)),
							verify_getdb(Actor,Type,Oknode,MasterNode,MeMors,Cb,Evnum,Evcrc)
					end;
				_ ->
					verified_response(MeMors,MasterNode)
			end;
		false ->
			Grouped = butil:group(fun({ok,_Node,NodeCrc,NodeEvnum,_NodeMors}) -> {NodeEvnum,NodeCrc} end,
										[{ok,bkdcore:node_name(),Evcrc,Evnum,MeMors}|Results]),
			case butil:find(fun({Key,Group}) -> 
					case length(Group)*2 > (LenCluster+1) of
						true ->
							{Key,Group};
						false ->
							undefined
					end
				 end,Grouped) of
				% Group with a majority of nodes and evnum > 0. This is winner.
				{MajorityKey,MajorityGroup} when element(1,MajorityKey) > 0 ->
					% There is a group with a majority of nodes, if it has a node running set as master, 
					% 		then local node must be slave
					% If it does not have master and no master found, master for local node is unchanged.
					case butil:findtrue(fun({ok,Node,_,_,_}) -> Node == MasterNode end,MajorityGroup) of
						false ->
							[{ok,Oknode,_,_,_}|_]  = MajorityGroup,
							?ADBG("Restoring db from another node ~p",[Actor]),
							verify_getdb(Actor,Type,Oknode,MasterNode,slave,Cb,Evnum,Evcrc);
						{ok,MasterNode,_,_,_} ->
							verify_getdb(Actor,Type,MasterNode,MasterNode,slave,Cb,Evnum,Evcrc)
					end;
				% No clear majority or majority has no writes.
				_ ->
					% If only two types of actors and one has no events, the other type must
					%   have some events and consider it correct.
					% If local node part of that group db is verified. If not it needs to restore from that node.
					case Grouped of
						[_,_] ->
							case lists:keyfind({0,0},1,Grouped) of
								false ->
									exit({nomajority,Grouped});
								{_,_ZeroGroup} when Evnum == 0 ->
									?ADBG("Node does not have db some other node does! ~p",[Actor]),
									[{_,OtherGroup}] = lists:keydelete({0,0},1,Grouped),
									[{ok,Oknode,_,_,_}|_]  = OtherGroup,
									verify_getdb(Actor,Type,Oknode,MasterNode,MeMors,Cb,Evnum,Evcrc);
								{_,_ZeroGroup} ->
									verified_response(MeMors,MasterNode)
							end;
						_ ->
							exit({nomajority,Grouped,GetFailed})
					end
			end
	end.
verify_getdb(Actor,Type,Node1,MasterNode,MeMors,Cb,Evnum,Evcrc) ->
	Ref = make_ref(),
	case MasterNode of
		undefined ->
			CallFunc = call_master;
		_ ->
			CallFunc = call_slave
	end,
	case Node1 of
		{Node,ActorFrom} ->
			RpcRes = bkdcore:rpc(Node,{?MODULE,CallFunc,[Cb,ActorFrom,Type,
											{dbcopy_op,undefined, send_db,{bkdcore:node_name(),Ref,false,Evnum,Evcrc,Actor}}]});
		{move,_NewShard,Node} ->
			RpcRes = bkdcore:rpc(Node,{?MODULE,CallFunc,[Cb,Actor,Type,
											{dbcopy_op,undefined, send_db,{bkdcore:node_name(),Ref,true,Evnum,Evcrc,Actor}}]});
		Node when is_binary(Node) ->
			RpcRes = bkdcore:rpc(Node,{?MODULE,CallFunc,[Cb,Actor,Type,
											{dbcopy_op,undefined, send_db,{bkdcore:node_name(),Ref,false,Evnum,Evcrc,Actor}}]})
	end,
	?ADBG("Verify getdb ~p ~p ~p ~p ~p",[Actor,Type,Node1,MasterNode,{element(1,RpcRes),butil:type(element(2,RpcRes))}]),
	case RpcRes of
		{ok,Ref} ->
			% Remote node will start sending db file.
			exit({update_from,Node,MeMors,MasterNode,Ref});
		{ok,Bin} ->
			% db file small enough to be sent directly
			exit({update_direct,MeMors,Bin});
		{wlog,Evnum,Evcrc,Sql} ->
			exit({wlog,Evnum,Evcrc,Sql});
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
			{sql_error,Err};
		{sql_error,Err} ->
			{sql_error,Err}
	end.



