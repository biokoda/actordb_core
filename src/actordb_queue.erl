% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_queue).
-compile(export_all).
-include_lib("actordb_core/include/actordb.hrl").
-define(TYPE_EVENT,1).
-define(TYPE_STATE,2).

-define(HEAD_SIZE,23).
-define(EV_HEAD_SIZE,27).

-define(DATA_BINARY,  0).
-define(DATA_MSGPACK, 1).
-define(DATA_TEXT,    2).
-define(DATA_JSON,    3).
%
% Implements a queue on top of actordb_sqlproc. Instead of using the sql engine, it uses
% and append only log file, which is replicated exactly the same as sqlproc. 
%

% 
% File format
% 

% Event Header:
% <<0x184D2A50,Size:32/unsigned-little, 1, QActorNameSize, QActorName/binary, 
%   NEvents:32, MapAndDataSize:32, term:64, evnum:64, time:64>>
% Event table is compressed (lz4 format). Single event in the table is:
% <<EntireLen, SizeName, Name:SizeName/binary, DataType, Size:32/unsigned,UncompressedOffset:32/unsigned>>
% Event data section is <<Item1:SizeInTable/binary, Item2:SizeInTable:/binary>>.

% Term store:
% <<0x184D2A51, Size:32/unsigned-little, 2, QActorNameSize, QActorName/binary,
%   VotedForNameSize, VotedForName/binary, EvTerm:64>>

-record(st,{db, name, indexname, cursize = 0, 
	curterm, evnum, staged_events = [], written_events = [],
	nevents = 0,
	voted_for = <<>>, voted_for_term, replbin = <<>>}).

start({Name,queue}) ->
	start(Name,queue);
start(Name) ->
	start(Name,queue).
start({Name,queue},Flags) ->
	case lists:keyfind(slave,1,Flags) of
		false ->
			start(Name,queue,[{slave,false}|Flags]);
		true ->
			start(Name,queue,Flags)
	end;
start(Name,Type) ->
	start(Name,Type,[{slave,false}]).
start(Name,Type1,Opt) ->
	Type = actordb_util:typeatom(Type1),
	actordb_util:wait_for_startup(Type,Name,0),
	case distreg:whereis({Name,Type}) of
		undefined ->
			actordb_sqlproc:start([{actor,Name},{type,Type},{mod,?MODULE},
				{state,#st{name = Name, indexname = <<0,Name/binary>>, db = aqdrv:open(Name,false)}}|Opt]);
		Pid ->
			{ok,Pid}
	end.

start_steal(Name,queue,Node,ShardName) ->
	{ok,Pid} = actordb_sqlproc:start([{actor,Name},{type,queue},{mod,?MODULE},{state,#st{name = Name}},
		{copyfrom,{move,ShardName,Node}},{startreason,{steal,Node}}]),
	{ok,Pid}.


read(_Shard, #{actor := _Actor, flags := _Flags, statements := _Sql} = _Call) ->
	ok.

write(_Shard, #{actor := Actor, flags := Flags, statements := Sql} = _Call) ->
	% TODO: determine wactor based on actual actor. There should be a shard tree.
	WActor = _Shard rem 10,
	Type = ?DATA_TEXT,
	actordb_sqlproc:write({WActor,queue},[create|Flags],{{?MODULE,cb_write,[Actor,Sql,Type]},undefined,undefined},?MODULE).

%
% Callbacks from actordb_sqlproc
%

% Every scheduler has it's own event index for current file. This is why every connection (queue process)
% must be fixed to it. This way we can do lookups directly on a scheduler for as long as index exists.
% Finished queue files have a read-only LMDB index and the scheduler bound one is not used.
cb_spawnopts(Name) ->
	Hash = actordb_util:hash(butil:tobin(Name)),
	Sch = 1 + (Hash rem erlang:system_info(schedulers)),
	[{spawn_opt,[{scheduler,Sch}]}].
% Buffer write
cb_write(S,A,{EvName, Data},Type) ->
	ok = aqdrv:stage_map(S#st.db, EvName, Type, byte_size(Data)),
	ok = aqdrv:stage_data(S#st.db, Data),
	{[],S#st{staged_events = [EvName|S#st.staged_events], nevents = S#st.nevents + 1}}.
% Write to disk
cb_write_exec(S, Items, Term, Evnum, VarHeader) ->
	{MapSize,DataSize} = aqdrv:stage_flush(S#st.db),

	ReplHdr = [<<(iolist_size(S#st.replbin)):16/unsigned>>, S#st.replbin, 
		<<(iolist_size(VarHeader)):16/unsigned>>, VarHeader,
		<<24>>,<<Term:64/unsigned-big,Evnum:64/unsigned-big,0:32,1:32>>],

	Header = [<<(?TYPE_EVENT),(byte_size(S#st.name))>>, 
		S#st.name, 
		<<(S#st.nevents):32/unsigned,
		(MapSize+DataSize):32/unsigned,
		Term:64/unsigned,
		Evnum:64/unsigned>>],

	{WPos,Size,_Time} = aqdrv:write(S#st.db, ReplHdr, Header),
	{ok, S#st{written_events = S#st.staged_events, staged_events = [], nevents = 0}}.
% Write replicated, same as cb_replication_done but meant for callback modules
% instead of engine callback. 
cb_write_done(S,Term,Evnum) ->
	ok = aqdrv:index_events(S#st.db,S#st.written_events,S#st.indexname, Term,Evnum),
	{ok,S#st{written_events = []}}.

cb_schema(_,queue,_) ->
	{1,[]}.

cb_path(_,_Name,queue) ->
	queue.

cb_slave_pid(Name,Type) ->
	cb_slave_pid(Name,Type,[]).
cb_slave_pid(Name,Type,Opts) ->
	Actor = {Name,Type},
	case distreg:whereis(Actor) of
		undefined ->
			{ok,Pid} = actordb_sqlproc:start([{actor,Name},{state,#st{name = Name}},
				{type,Type},{mod,?MODULE},{slave,true},create|Opts]),
			{ok,Pid};
		Pid ->
			{ok,Pid}
	end.

cb_candie(_Mors,_Name,queue,_S) ->
	never.
cb_checkmoved(_Name,queue) ->
	undefined.

cb_startstate(Name,queue) ->
	#st{name = Name}.

cb_idle(_S) ->
	ok.

cb_nodelist(S,_HasSchema) ->
	{ok,S,bkdcore:cluster_nodes()}.

cb_replicate_type(_S) ->
	1.

cb_redirected_call(_S,_MovedTo,_Call,_RedType) ->
	ok.

cb_unverified_call(_S,_Msg)  ->
	queue.

% These only get called on leader
cb_call(_Msg,_From,_S) ->
	{reply,{error,uncrecognized_call}}.
cb_cast(_Msg,_S) ->
	noreply.
cb_info(_Msg,_S) ->
	noreply.
cb_init(_S,_EvNum) ->
	ok.

%  Always called on actor start before leader/follower established.
cb_init_engine(S) ->
	S.

% 
% Storage engine callbacks
% 
cb_inject_page(#st{written_events = []} = S,Bin,_ReplHeader) ->
	{ok,WE} = aqdrv:inject(S#st.db, Bin),
	{ok,S#st{written_events = WE}};
cb_inject_page(S, Bin, <<Term:64/unsigned-big,Evnum:64/unsigned-big,0:32,1:32>>) ->
	ok = aqdrv:index_events(S#st.db,S#st.written_events, S#st.indexname, Term, Evnum),
	cb_inject_page(S#st{written_events = []}, Bin, <<>>).

% Called on open queue. Regardless if leader/follower (before that is established).
cb_actor_info(#st{evnum = undefined} = _S) ->
	undefined;
cb_actor_info(S) ->
	{{0,0},{S#st.curterm,S#st.evnum},{0,0},0,0,S#st.voted_for_term,S#st.voted_for}.

cb_term_store(S, CurrentTerm, VotedFor) ->
	{ok,S}.

cb_wal_rewind(S,Evnum) ->
	ok = aqdrv:index_events(S#st.db,false,S#st.indexname, 0, Evnum),
	{ok,S#st{written_events = []},todo,todo}.
cb_wal_rewind(S,_Evnum,_ReplaceSql) ->
	cb_wal_rewind(S, _Evnum).

cb_replicate_opts(S, Bin, _Type) ->
	S#st{replbin = Bin}.

cb_replicate_opts(S, Bin) ->
	S#st{replbin = Bin}.

% Write successfully replicated to all nodes.
% This is an engine callback. We don't need it in aqdrv.
cb_replication_done(_S) ->
	ok.

cb_fsync(S) ->
	aqdrv:fsync(S#st.db).

