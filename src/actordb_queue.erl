% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_queue).
-compile(export_all).
-include_lib("actordb_core/include/actordb.hrl").
-define(TYPE_EVENT,0).
-define(TYPE_STATE,1).

%
% Implements a queue on top of actordb_sqlproc. Instead of using the sql engine, it uses
% and append only log file, which is replicated exactly the same as sqlproc. 
%

% 
% File format (integers are unsigned-little)
% 
% 
% HEAD = <<crc:32, 
%          type:8, 
%          prev_file:64,        % file of previous event, either current index or prev
%          prev_file_offset:32, % file offset of previous event
%          size:32, 
%          queue_actor_id:16>>
% 
% TAIL = entire_ev_size:32
% 
% types:
% snaphost    -> [HEAD, term_to_binary({VotedForTerm,VotedFor, CurTerm, CurEvnum}),TAIL] 
% replevent   -> [HEAD, term:64, evnum:64, time:64, map_size:24, event_map, [ev1data,ev2data], TAIL]
%                event_map = [Size:32,term_to_binary(#{actorname => [{pos1,size1},{pos2,size2}]})]



% wmap is a map of event data for every actor in replication event: #{ActorName => [{DataSectionOffset,DataSize}]}
% It also has last valid election info: #{vi => {S#st.voted_for_term,S#st.voted_for}}
-record(st,{name, fd, fd_index, wmap = #{}, cursize = 0, 
	curterm, evnum, prev_event = {0,0}, voted_for = <<>>, voted_for_term}).

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
				{state,#st{name = Name}}|Opt]);
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
	WActor = 1,
	actordb_sqlproc:write({WActor,queue},[create|Flags],{?MODULE,cb_write,[Actor,Sql]},?MODULE).

%
% Callbacks from actordb_sqlproc
%

% Buffer write
cb_write(#st{wmap = Map} = S,A,Data) ->
	case Map of
		#{A := Positions} ->
			ok;
		_ ->
			Positions = []
	end,
	Size = iolist_size(Data),
	{Data,S#st{wmap = Map#{A => [{S#st.cursize,Size}|Positions]}, cursize = S#st.cursize + Size}}.
% Write to disk
cb_write_exec(#st{prev_event = {PrevFile,PrevOffset}} = S,Items,Term,Evnum) ->
	Map = term_to_binary((S#st.wmap)#{vi => {S#st.voted_for_term, S#st.voted_for}}),
	EvHeader = <<Term:64/unsigned-little, Evnum:64/unsigned-little,
		(erlang:system_time(micro_seconds)):64/unsigned-little, 
		(byte_size(Map)):24/unsigned-little, Map/binary>>,
	HeadWithoutCrc = <<?TYPE_EVENT,PrevFile:64/unsigned-little,PrevOffset:32/unsigned-little,
		(S#st.cursize+byte_size(EvHeader)):32/unsigned-little,(S#st.name):16/unsigned-little>>,
	EncSize = <<(byte_size(HeadWithoutCrc)+S#st.cursize+byte_size(EvHeader)+4*2):32/unsigned-little>>,
	Header = [<<(erlang:crc32([HeadWithoutCrc,EvHeader,Items,EncSize])):32/unsigned-little>>,HeadWithoutCrc],
	% 
	% TODO: send to followers
	% 
	write_to_log(S#st{curterm = Term, evnum = Evnum, cursize = 0, wmap = #{}}, 
		S#st.cursize+byte_size(EvHeader)+iolist_size(Header)+byte_size(EncSize), 
		[Header, EvHeader,Items,EncSize]).
% Write replicated
cb_write_done(S,_Evnum) ->
	{ok,S}.

write_to_log(S,Size,Data) ->
	{FileIndex,Offset} = actordb_queue_srv:get_chunk(Size),
	case S#st.fd_index of
		FileIndex ->
			Fd = S#st.fd;
		_ ->
			file:close(S#st.fd),
			{ok,Fd} = file:open(actordb_conf:db_path()++"/q."++butil:tolist(FileIndex),[write,read,binary,raw])
	end,
	ok = prim_file:pwrite(Fd,Offset,Data),
	{ok,S#st{fd = Fd, fd_index = FileIndex, prev_event = {FileIndex,Offset}}}.


cb_schema(_,queue,_) ->
	"".

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
	false.
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

cb_redirected_call(_S,_MovedTo,_Call,queue) ->
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
cb_init(S,_EvNum) ->
	S.


% 
% Storage engine callbacks
% 

% Called on open queue. Regardless if leader/follower (before that is established).
cb_actor_info(#st{evnum = undefined} = S) ->
	% Establish state.
	case actordb_queue_srv:list_files() of
		[] ->
			undefined;
		L1 ->
			L = lists:reverse(L1)
	end;
cb_actor_info(S) ->
	{{0,0},{S#st.curterm,S#st.evnum},{0,0},0,0,S#st.voted_for_term,S#st.voted_for}.

cb_term_store(#st{prev_event = {PrevFile,PrevOffset}} = S, CurrentTerm, VotedFor) ->
	Bin = term_to_binary({CurrentTerm,VotedFor,S#st.curterm,S#st.evnum}),
	HeaderWithoutCrc = <<?TYPE_STATE,PrevFile:64/unsigned-little,PrevOffset:32/unsigned-little,
		(byte_size(Bin)):32/unsigned-little,(S#st.name):16/unsigned-little>>,
	EncSize = <<(byte_size(HeaderWithoutCrc)+byte_size(Bin)+4*2):32/unsigned-little>>,

	ToWrite = [<<(erlang:crc32([HeaderWithoutCrc,Bin,EncSize])):32/unsigned-little>>,HeaderWithoutCrc,Bin,EncSize],
	write_to_log(S#st{curterm = CurrentTerm, voted_for = VotedFor}, iolist_size(ToWrite), ToWrite).

cb_wal_rewind(S,Evnum) ->
	ok.

cb_replicate_opts(S, Bin, Type) ->
	ok.

cb_replicate_opts(S, Bin) ->
	ok.

cb_replication_done(S) ->
	ok.

cb_fsync(_S) ->
	ok.

cb_inject_page(S,Bin,Header) ->
	ok.



