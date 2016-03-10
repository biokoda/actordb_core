% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_queue).
-compile(export_all).
-include_lib("actordb_core/include/actordb.hrl").
-define(TYPE_EVENT,0).
-define(TYPE_STATE,1).

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

% Page size: 4096 bytes
% Every replication event is page aligned. First byte of every page determines the data on that page.
% Items:
% - Replication event start:
%   <<1, QActor, PgnoPrevEvent:32, NEvents:32, NDataPages:32, term:Varint, evnum:Varint, time:Varint,
%     Table:NTablePages*4096/binary, CrcBlock:NDataPages*4/binary, Data:NDataPages*4096/binary>>
%   TableEntry:
%   <<SizeName, Name:SizeName/binary, DataType, DataSize(varint)>>



-record(st,{db, name, cursize = 0, 
	curterm, evnum, prev_event = {0,0}, staged_events = [], written_events = [],
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
				{state,#st{name = Name, db = aqdrv:open(Name,false)}}|Opt]);
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
	% Sz = aqdrv:stage_write(S#st.db, Data),
	ok = aqdrv:stage_map(S#st.db, EvName, Type, byte_size(Data)),
	ok = aqdrv:stage_data(S#st.db, Data),
	{[],S#st{staged_events = [EvName|S#st.staged_events]}}.
% Write to disk
cb_write_exec(#st{prev_event = {PrevFile,PrevOffset}} = S, Items, Term, Evnum, VarHeader) ->
	{_,_} = aqdrv:stage_flush(S#st.db),
	{ok, S#st{written_events = S#st.staged_events, staged_events = []}}.
	% Map = term_to_binary((S#st.wmap)#{vi => {S#st.voted_for_term, S#st.voted_for}}),
	% Map = lists:sort(S#st.wmap),
	% EvHeader = <<Term:64/unsigned-little, Evnum:64/unsigned-little,
	% 	(erlang:system_time(micro_seconds)):64/unsigned-little, 
	% 	0,(byte_size(S#st.mapsize)):24/unsigned-little>>,
	
	% HeadWithoutCrc = [<<?TYPE_EVENT,(S#st.name),PrevFile:64/unsigned-little,PrevOffset:32/unsigned-little,
	% 	(S#st.cursize+byte_size(EvHeader)+S#st.mapsize*8):32/unsigned-little>>],
	% TAIL = <<(byte_size(HeadWithoutCrc)+S#st.cursize+byte_size(EvHeader)+S#st.mapsize*8+4*2):32/unsigned-little>>,
	
	% % Combine to single binary since its going to be done in driver anyway.
	% Event = iolist_to_binary([EvHeader,Map,Items,TAIL]),
	% Header = [<<(erlang:crc32([HeadWithoutCrc,Event])):32/unsigned-little>>,HeadWithoutCrc],

	% RHdr = [<<(iolist_size(S#st.replbin)):16/unsigned>>, S#st.replbin, 
	% 	<<(iolist_size(VarHeader)):16/unsigned>>, VarHeader,
	% 	24,<<Term:64/unsigned-big,Evnum:64/unsigned-big,0:32,1:32>>],
	% actordb_driver:all_tunnel_call(RHdr,Event),
	
	% write_to_log(S#st{curterm = Term, evnum = Evnum, cursize = 0, mapsize = 0, wmap = []}, 
	% 	iolist_size(Header)+byte_size(Event), 
	% 	[Header, Event]).
% Write replicated
cb_write_done(S,_Evnum) ->
	ok = aqdrv:index_events(S#st.db,[S#st.written_events]),
	{ok,S#st{written_events = []}}.

% write_to_log(S,Size,Data) ->
% 	{FileIndex,Offset} = actordb_queue_srv:get_chunk(Size),
% 	case S#st.fd_index of
% 		FileIndex ->
% 			Fd = S#st.fd;
% 		_ ->
% 			file:close(S#st.fd),
% 			actordb_queue_srv:moved_to(S#st.name,FileIndex),
% 			{ok,Fd} = file:open(actordb_conf:db_path()++"/q."++butil:tolist(FileIndex),[write,read,binary,raw])
% 	end,
% 	ok = prim_file:pwrite(Fd,Offset,Data),
% 	{ok,S#st{fd = Fd, fd_index = FileIndex, prev_event = {FileIndex,Offset}}}.


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
	case actordb_queue_srv:list_files() of
		[] ->
			S;
		L1 ->
			InitFrom = actordb_queue_srv:init_from(S#st.name),
			case find_event(S,InitFrom,lists:reverse(L1)) of
				false ->
					S;
				{ok,NS} ->
					NS
			end
	end.


% 
% Storage engine callbacks
% 
cb_inject_page(#st{prev_event = {PrevFile,PrevOffset}} = S,<<OriginalSize:32/unsigned,Bin1/binary>>,ReplHeader) ->
	{ok,S}.
	% <<Evterm:64/unsigned-big,Evnum:64/unsigned-big,_/binary>> = ReplHeader,
	
	% Bin = actordb_driver:lz4_decompress(Bin1,OriginalSize),
	
	% HeadWithoutCrc = <<?TYPE_STATE,(S#st.name):16/unsigned-little,PrevFile:64/unsigned-little,PrevOffset:32/unsigned-little,
	% 	% -4 because Bin also contains TAIL
	% 	(byte_size(Bin)-4):32/unsigned-little>>,
	% Header = [<<(erlang:crc32([HeadWithoutCrc,Bin])):32/unsigned-little>>,HeadWithoutCrc],
	% write_to_log(S#st{curterm = Evterm, evnum = Evnum},byte_size(Bin)+iolist_size(Header),[Header,Bin]).

% Called on open queue. Regardless if leader/follower (before that is established).
cb_actor_info(#st{evnum = undefined} = _S) ->
	undefined;
cb_actor_info(S) ->
	{{0,0},{S#st.curterm,S#st.evnum},{0,0},0,0,S#st.voted_for_term,S#st.voted_for}.

cb_term_store(#st{prev_event = {PrevFile,PrevOffset}} = S, CurrentTerm, VotedFor) ->
	{ok,S}.
	% Bin = term_to_binary({CurrentTerm,VotedFor,S#st.curterm,S#st.evnum}),
	% HeaderWithoutCrc = <<?TYPE_STATE,(S#st.name):16/unsigned-little,PrevFile:64/unsigned-little,PrevOffset:32/unsigned-little,
	% 	(byte_size(Bin)):32/unsigned-little>>,
	% TAIL = <<(byte_size(HeaderWithoutCrc)+byte_size(Bin)+4*2):32/unsigned-little>>,

	% ToWrite = [<<(erlang:crc32([HeaderWithoutCrc,Bin,TAIL])):32/unsigned-little>>,HeaderWithoutCrc,Bin,TAIL],
	% write_to_log(S#st{voted_for_term = CurrentTerm, voted_for = VotedFor}, iolist_size(ToWrite), ToWrite).

cb_wal_rewind(S,Evnum) ->
	ok.

cb_replicate_opts(S, Bin, _Type) ->
	S#st{replbin = Bin}.

cb_replicate_opts(S, Bin) ->
	S#st{replbin = Bin}.

% Write successfully replicated to all nodes.
cb_replication_done(_S) ->
	ok.

cb_fsync(_S) ->
	ok.

% Make sure we are not bootstrapping from log files generated this session. This queue actor
% can not be in there.
find_event(S,InitFrom,[{Index,Nm}|T]) when Index =< InitFrom ->
	{ok,F} = file:open(Nm,[read,raw,binary]),
	{ok,FSize} = file:position(F,eof),
	R = find_event1(S,F,FSize),
	file:close(F),
	case R of
		{ok,#st{prev_event = {Position,_}} = NS} ->
			{ok,NS#st{prev_event = {Position, Index}}};
		_ ->
			find_event(S,InitFrom,T)
	end;
find_event(S,InitFrom,[_|T]) ->
	find_event(S,InitFrom,T);
find_event(_S,_,[]) ->
	false.

% Move from eof to begin, find first event for this queue index.
% Any kind of event has sufficient data. 
find_event1(S,F,Position) ->
	case file:pread(F,Position-4,4) of
		{ok,<<Size:32/unsigned-little>>} ->
			{ok,<<_Crc:32/unsigned-little, EvType, QIndex:16/unsigned-little, PrevFile:64/unsigned-little,
				PrevOffset:32/unsigned-little,BodySize:32/unsigned-little>>} = file:pread(F,Position-Size,?HEAD_SIZE),
			case QIndex == S#st.name of
				true when EvType == ?TYPE_EVENT  ->
					{ok, <<Term:64/unsigned-little, Evnum:64/unsigned-little, _Time:64/unsigned-little,
						MapSize:24/unsigned-little>>} = file:pread(F,Position-Size+?HEAD_SIZE, ?EV_HEAD_SIZE),
					{ok, MapBin} = file:pread(F,Position-Size+?HEAD_SIZE+?EV_HEAD_SIZE, MapSize),
					#{vi := {VotedForTerm,VotedFor}} = binary_to_term(MapBin),
					?ADBG("ev=~p, qindex=~p, pf=~p, poff=~p, term=~p, evnum=~p, time=~p",
						[event,QIndex,PrevFile,PrevOffset,Term,Evnum,_Time]),
					S#st{prev_event = {Position,0}, voted_for = VotedFor, 
						voted_for_term = VotedForTerm, curterm = Term, evnum = Evnum};
				true when EvType == ?TYPE_STATE ->
					{ok,MapBin} = file:pread(F,Position-Size+?HEAD_SIZE, BodySize),
					{VotedForTerm,VotedFor,CurTerm,Evnum} = binary_to_term(MapBin),
					S#st{prev_event = {Position,0}, voted_for = VotedFor, voted_for_term = VotedForTerm,
						curterm = CurTerm, evnum = Evnum};
				false ->
					?ADBG("Skipping index, me=~p, it=~p, position=~p",[S#st.name, QIndex,Position]),
					find_event1(S,F,Position-Size)
			end;
		_ ->
			undefined
	end.

print([_|_] = File) ->
	{ok,F} = file:open(File,[read,binary,raw]),
	print(F);
print(F) ->
	case file:read(F,?HEAD_SIZE) of
		{ok,<<_Crc:32/unsigned-little, 0, QIndex:16/unsigned-little, PrevFile:64/unsigned-little, PrevOffset:32/unsigned-little,
			BodySize:32/unsigned-little>>} ->
			{ok,<<Term:64/unsigned-little,Evnum:64/unsigned-little, Time:64/unsigned-little, MapSize:24/unsigned-little,Map:MapSize/binary,Body/binary>>}
				= file:read(F,BodySize),
			io:format(lager_format:format("ev=~p, qindex=~p, pf=~p, poff=~p, term=~p, evnum=~p, time=~p, map=~p, body=~p~n",
			[event,QIndex,PrevFile,PrevOffset,Term,Evnum,Time,binary_to_term(Map),Body],4096)),
			file:read(F,4),
			print(F);
		{ok,<<_Crc:32/unsigned-little, 1, QIndex:16/unsigned-little, PrevFile:64/unsigned-little, PrevOffset:32/unsigned-little,
			BodySize:32/unsigned-little>>} ->
			{ok,Map} = file:read(F,BodySize),
			io:format(lager_format:format("ev=~p, qindex=~p, pf=~p, poff=~p, data=~p~n",
				[state,QIndex,PrevFile,PrevOffset,binary_to_term(Map)],4096)),
			file:read(F,4),
			print(F);
		_R ->
			io:format("~p~n",[_R]),
			file:close(F)
	end.
