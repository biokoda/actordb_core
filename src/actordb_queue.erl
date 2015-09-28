% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_queue).
-compile(export_all).
-include_lib("actordb_core/include/actordb.hrl").
%
% Implements a queue on top of actordb_sqlproc. Instead of using the sql engine, it uses
% and append only log file, which is replicated exactly the same as sqlproc. 
%

-record(st,{name}).

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
	% BV = maps:get(bindingvals, Call, []),
	% case actordb_schema:iskv(Type) of
	% 	true ->
	% 		actordb_shard:kvread(Shard,Actor,Type,{Sql,BV});
	% 	_ ->
	% 		read(Call)
	% end.
% read(#{actor:= Actor, type:= Type, flags := Flags, statements := Sql} = Call) ->
% 	BV = maps:get(bindingvals, Call, []),
% 	actordb_sqlproc:read({Actor, Type},Flags,{Sql,BV},?MODULE);
% read(#{actor:= Actor, flags := Flags, statements := Sql} = Call) ->
% 	BV = maps:get(bindingvals, Call, []),
% 	actordb_sqlproc:read(Actor,Flags,{Sql,BV},?MODULE).

write(_Shard, #{actor := _Actor, flags := _Flags, statements := _Sql} = _Call) ->
	ok.
	% BV = maps:get(bindingvals, Call, []),
	% case actordb_schema:iskv(Type) of
	% 	true ->
	% 		actordb_shard:kvwrite(Shard,Actor,Type,{Sql,BV});
	% 	_ ->
	% 		write(Call)
	% end.
% write(#{actor:= Actor, type:= Type, flags := Flags, statements := Sql} = Call) ->
% 	BV = maps:get(bindingvals, Call, []),
% 	actordb_sqlproc:write({Actor, Type},Flags,{Sql,BV},?MODULE);
% write(#{actor:= Actor, flags := Flags, statements := Sql} = Call) ->
% 	BV = maps:get(bindingvals, Call, []),
% 	actordb_sqlproc:write(Actor,Flags,{Sql,BV},?MODULE).


%
% Callbacks from actordb_sqlproc
%


cb_schema(_,queue,_) ->
	"".

cb_path(_,_Name,queue) ->
	queue.

% Start or get pid of slave process for actor (executed on slave nodes in cluster)
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

cb_write_done(_S,_Evnum) ->
	ok.

% These only get called on master
cb_call(_Msg,_From,_S) ->
	{reply,{error,uncrecognized_call}}.
cb_cast(_Msg,_S) ->
	noreply.
cb_info(_Msg,_S) ->
	noreply.
cb_init(S,_EvNum) ->
	S.


% queue specific callbacks
cb_term_info(S) ->
	{1,1}.


