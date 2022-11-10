% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-include("actordb.hrl").
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
-define(WLOG_STATUS,<<"8">>).
-define(EVTERM,<<"9">>).
-define(BASE_SCHEMA_VERS,<<"10">>).
-define(EVNUMI,1).
-define(EVCRCI,2).
-define(SCHEMA_VERSI,3).
-define(ATYPEI,4).
-define(COPYFROMI,5).
-define(MOVEDTOI,6).
-define(ANUMI,7).
-define(WLOG_STATUSI,8).
-define(EVTERMI,9).
-define(BASE_SCHEMA_VERSI,10).

-define(WLOG_NONE,0).
-define(WLOG_ABANDONDED,-1).
-define(WLOG_ACTIVE,1).

-define(FLAG_CREATE,1).
-define(FLAG_ACTORNUM,2).
-define(FLAG_EXISTS,4).
-define(FLAG_NOVERIFY,8).
-define(FLAG_TEST,16).
-define(FLAG_STARTLOCK,32).
-define(FLAG_NOHIBERNATE,64).
% internal flags
-define(FLAG_WAIT_ELECTION,128).
-define(TIMEOUT_PENDING,256).
% -define(FLAG_NO_ELECTION_TIMEOUT,512).
-define(FLAG_REPORT_SYNC,1024).


% records: for bulk inserts to single actor. List of rows (tuples).
%          First element of tuple is table name. Sql must contain _insert; statement.
% sql and flags must always be first and second position in #read and #write records.
-record(write,{sql, flags = [], mfa, transaction, records = [], newvers}).
-record(read,{sql, flags = []}).
-record(flw,{node, distname, match_index = 0, match_term = 0, next_index = 0,
	file, wait_for_response_since, last_seen = 0, pagebuf = <<>>, 
	inrecovery = false, 
	election_rpc_ref, election_result}).

-record(cpto,{node,pid,ref,ismove,actorname}).
-record(lck,{ref,pid,ismove,node,time,actorname}).

% async info for reads/writes
-record(ai,{
% holds sql statements that will be batched together and executed at the same time
buffer = [],
buffer_recs = [],
buffer_cf = [],
buffer_nv,
buffer_moved,
buffer_fsync = false,
safe_read = false,
% for writes, we count every time a write was successful. This is necessary
% so we don't start processing reads too soon.
nreplies = 0,
% reference from actordb_driver. Match against it to read response.
wait,
% current #read/#write
info,
% who is waiting to get response.
callfrom,
% info used when performing writes. Once write is done, #dp values will be overwritten with these.
evnum, evterm, newvers, moved, fsync}).

-record(dp,{
	% queue which holds misc gen_server:calls that can not be processed immediately.
	callqueue,
	db, actorname,actortype, evnum = 0,evterm = 0,
	activity, schemanum,schemavers,flags = 0, netchanges = 0,
	% Raft parameters  (lastApplied = evnum)
	% follower_indexes: [#flw,..]
	current_term = 0,voted_for, followers = [],
	% evnum of last checkpoint
	last_checkpoint = 0, force_sync = false,
	% locked is a list of pids or markers that needs to be empty for actor to be unlocked.
	locked = [],inrecovery = false, recovery_age = 0, inrecovery_replies = [],
	% Multiupdate id, set to {Multiupdateid,TransactionNum,OriginNode} if in the middle of a distributed transaction
	transactioninfo,transactionid, transactioncheckref,
	% actordb_sqlproc is not used directly, it always has a callback module that sits in front of it,
	%  providing an external interface
	%  to a sqlite backed process.
	cbmod, cbstate,cbinit = false,
	% callfrom is who is calling,
	% callres result of sqlite call (need to replicate before replying)
	% callat time when call was executed and sent over to nodes but before any replies from nodes
	%  {Time,NumberOfRetries}
	callfrom,callres,callat = {0,0},
	% Writes/reads are processed asynchronously, this stores info while call is executing in driver.
	% If any writes come in during exec, they are batched together into a larger read or write
	wasync = #ai{}, rasync = #ai{},
	last_write_at = 0,
	% While write executing, state calls must be queued. After it is done, they can be processed.
	statequeue,
	% (short for masterorslave): slave/master
	% mors = slave                     -> follower
	% mors = master, verified == false -> candidate
	% mors == master, verified == true -> leader
	mors,
	% Local copy of db needs to be verified with all nodes. It might be stale or in a conflicted state.
	% If local db is being restored, verified will be on false.
	% Possible values: true, false
	verified = false,
	% undefined | {election, TimerRef, ElectionRef} | {timer,Ref}
	election_timer,
	% Last productive vote event (vote response or vote yes for a candidate)
	last_vote_event = 0,
	% Path to sqlite file.
	dbpath,
	% Which nodes current process is sending dbfile to.
	% [#cpto{},..]
	dbcopy_to = [],
	% If copy/move is unable to execute. Place data here and try later
	% {TimeOfLastTry,Copy/Move data}
	copylater,
	% If node is sending us a complete copy of db, this identifies the operation
	dbcopyref,
	% Where is master sqlproc.
	masternode, masternodedist, without_master_since, masternode_since = 0,
	% If db has been moved completely over to a new node. All calls will be redirected to that node.
	% Once this has been set, db files will be deleted on process timeout.
	movedtonode,
	% Used when receiving complete actor state from another node.
	copyfrom,copyreset = false,copyproc}).

-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).

-ifndef(NOLOG).
-define(DBG(F),lager:debug([$~,$p,$.,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype])).
-define(DBG(F,A),lager:debug([$~,$p,$.,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype|A])).
-define(INF(F),lager:info([$~,$p,$.,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype])).
-define(INF(F,A),lager:info([$~,$p,$.,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype|A])).
-define(ERR(F),lager:error([$~,$p,$.,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype])).
-define(ERR(F,A),lager:error([$~,$p,$.,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype|A])).
-define(WARN(F,A),lager:warning([$~,$p,$.,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype|A])).
-define(WARN(F),lager:warning([$~,$p,$.,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype])).
% -define(DBG(F),lager:debug([$~,$p,$-,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype]),put(lines,[{?MODULE,?LINE}|get(lines)])).
% -define(DBG(F,A),lager:debug([$~,$p,$-,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype|A]),put(lines,[{?MODULE,?LINE}|get(lines)])).
% -define(INF(F),lager:info([$~,$p,$-,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype]),put(lines,[{?MODULE,?LINE}|get(lines)])).
% -define(INF(F,A),lager:info([$~,$p,$-,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype|A]),put(lines,[{?MODULE,?LINE}|get(lines)])).
% -define(ERR(F),lager:error([$~,$p,$-,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype]),put(lines,[{?MODULE,?LINE}|get(lines)])).
% -define(ERR(F,A),lager:error([$~,$p,$-,$~,$p,$\s|F],[P#dp.actorname,P#dp.actortype|A]),put(lines,[{?MODULE,?LINE}|get(lines)])).
-else.
-define(DBG(F),ok).
-define(DBG(F,A),ok).
-define(INF(F),ok).
-define(INF(F,A),ok).
-define(ERR(F),ok).
-define(ERR(F,A),ok).
-define(WARN(F),ok).
-define(WARN(F,A),ok).
-endif.
