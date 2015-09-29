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

-record(st,{name, fd, wmap = #{}, cursize = 0, curterm, evnum, prev_event = {0,0}}).

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
	actordb_sqlproc:write({WActor,queue},[create],{?MODULE,cb_write,[Actor,Sql]},?MODULE).

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
	% [?TYPE_EVENT,],
	Body = [<<Term:64/unsigned-little, Evnum:64/unsigned-little,
	PrevFile:64/unsigned-little,
	PrevOffset:32/unsigned-little,
	(erlang:system_time()):64/unsigned-little>>],
	S#st{curterm = Term, evnum = Evnum};
% Write replicated
cb_write_done(S,_Evnum) ->
	{ok,S#st{wmap = #{}, cursize = 0}}.



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
cb_actor_info(S) ->
	% {{_FCT,LastCheck},{VoteEvTerm,VoteEvnum},_InProg,0,0,VotedCurrentTerm,<<>>} ->
	ok.

cb_term_store(S, CurrentTerm, VotedFor) ->
	ok.

cb_wal_rewind(S,Evnum) ->
	ok.

cb_replicate_opts(S, Bin, Type) ->
	ok.

cb_replicate_opts(S, Bin) ->
	ok.

cb_replication_done(S) ->
	ok.

cb_fsync(S) ->
	ok.

cb_inject_page(S,Bin,Header) ->
	ok.

