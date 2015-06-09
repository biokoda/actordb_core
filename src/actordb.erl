% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb).
% API
-export([exec/1,exec/2,types/0,tables/1,columns/2,prepare_statement/1]).
% API backpressure
-export([start_bp/0,exec_bp/2,exec_bp/3,exec_bp/5,exec_bp/6,check_bp/0,sleep_bp/1,stop_bp/1]).
% start/stop
-export([start/0,stop/0,stop_complete/0,is_ready/0]).
% start/stop internal
-export([schema_changed/0]).

% Internal. Generally not to be called from outside actordb
-export([direct_call/1,actor_id_type/1,configfiles/0,exec1/1,
		 exec_bp1/3,rpc/3,hash_pick/2,hash_pick/1]).
-include("actordb.hrl").


is_ready() ->
	case application:get_env(actordb_core,isready) of
		{ok,true} ->
			true;
		_ ->
			false
	end.

start() ->
	actordb_core:start().
stop() ->
	actordb_core:stop().
stop_complete() ->
	actordb_core:stop_complete().


types() ->
	case catch actordb_schema:types() of
		L when is_list(L) ->
			L;
		_Err ->
			schema_not_loaded
	end.
tables(Type) when is_atom(Type) ->
	case catch apply(actordb_schema,Type,[tables]) of
		L when is_list(L) ->
			L;
		_Err ->
			schema_not_loaded
	end;
tables(Type) ->
	case Type of
		[_|_] ->
			case catch list_to_existing_atom(Type) of
				A ->
					ok
			end;
		<<_/binary>> ->
			case catch binary_to_existing_atom(Type,utf8) of
				A ->
					ok
			end
	end,
	case is_atom(A) of
		true ->
			tables(A);
		_ ->
			schema_not_loaded
	end.

columns(Type,Table) when is_atom(Type) ->
	case apply(actordb_schema,Type,[butil:tobin(Table)]) of
		L when is_list(L) ->
			L;
		_Err ->
			schema_not_loaded
	end;
columns(Type,Table) ->
	case Type of
		[_|_] ->
			case catch list_to_existing_atom(Type) of
				A ->
					ok
			end;
		<<_/binary>> ->
			case catch binary_to_existing_atom(Type,utf8) of
				A ->
					ok
			end
	end,
	case is_atom(A) of
		true ->
			columns(A,Table);
		_ ->
			schema_not_loaded
	end.

% Create prepared statement from a full sql query: "actor user(*); insert into tab values (?1,?2);"
% Returns ID (of the form #[r|w]XXXX) that can be used as a marker inside exec.
% Prepared statement parameters are a list of statements, containing list of rows, containing list of columns.
% Examples:
% Inserting 2 rows: actordb:exec("actor user(1);#w0102;",[[[1,'text1'],[2,'text2']]]).
% Inserting with 2 statements: actordb:exec("actor user(1);#w0102;#w0100;",[[[1,'text1'],[2,'text2']],[['asdf'],['defg']]]).
prepare_statement(Sql) ->
	{[{{Type,_,_},IsWrite,Statements}],_} = actordb_sqlparse:parse_statements(butil:tobin(Sql)),
	actordb_sharedstate:save_prepared(actordb_util:typeatom(Type),IsWrite,Statements).


% Calls with backpressure.
% Generally all calls to actordb should get executed with these functions.
% Otherwise the node might get overloaded and crash.

% For external connectors:
% 1. Accept connection (use ranch, limit number of concurrent connections).
% 2. actordb:start_bp/0
% 3. actordb:check_bp/0 -> sleep | ok
% 4 If ok call actordb:exec_bp(Sql)
% 4 If sleep call actordb:sleep_bp(P), when it returns start receiving and call exec_bp
% 5 If result of exec {ok,Result}, wait for new data
% 5 If result of exec {sleep,Result}
%   Send response to client
%   call actordb:sleep_bp(P)
%   start receiving again when it returns
start_bp() ->
	actordb_backpressure:start_caller().

% Exec with backpressure keeps track of number of unanswered SQL calls and combined size of SQL statements.
exec_bp(P,[_|_] = Sql) ->
	exec_bp(P,butil:tobin(Sql));
exec_bp(P,Sql) ->
	exec_bp(P,Sql,[]).
%prep statements
exec_bp(P,Sql,BindingValues)->
	Size = byte_size(Sql),
	Parsed = actordb_sqlparse:parse_statements(P,Sql),
	exec_bp1(P,Size,Parsed,BindingValues).

exec_bp(_P,<<>>,_Type,_Flags,_Sql) ->
	exec_bp(_P,<<>>,_Type,_Flags,_Sql,[]);
exec_bp(P,Actor,Type,Flags,Sql) when is_binary(Actor) ->
	exec_bp(P,[Actor],Type,Flags,Sql,[]);
exec_bp(P,Actors,Type,Flags,Sql) ->
	exec_bp(P,Actors,Type,Flags,Sql,[]).

exec_bp(_P,<<>>,_Type,_Flags,_Sql,_BindingValues) ->
	throw({error,empty_actor_name});
exec_bp(P,Actor,Type,Flags,Sql,BindingValues) when is_binary(Actor) ->
	exec_bp(P,[Actor],Type,Flags,Sql,BindingValues);
exec_bp(P,Actors,Type,Flags,Sql,BindingValues)->
	Size = byte_size(Sql),
	Parsed = actordb_sqlparse:parse_statements(P,Sql,{Type,Actors,Flags}),
	exec_bp1(P,Size,Parsed,BindingValues).

exec_bp1(_,Size,_) when Size > 1024*1024*16 ->
	{error,sql_too_large};
exec_bp1(P,Size,Sql) ->
	exec_bp1(P,Size,Sql,[]).

exec_bp1(_,Size,_,_BindingValues) when Size > 1024*1024*16 ->
	{error,sql_too_large};
exec_bp1(P,Size,Sql,BindingValues) ->
	% Global inc
	actordb_backpressure:inc_callcount(),
	% Local inc
	actordb_backpressure:inc_callcount(P),
	actordb_backpressure:inc_callsize(Size),
	actordb_backpressure:inc_callsize(P,Size),
	Res = exec1(Sql,BindingValues),
	GCount = actordb_backpressure:dec_callcount(),
	actordb_backpressure:dec_callcount(P),
	GSize = actordb_backpressure:dec_callsize(Size),
	actordb_backpressure:dec_callsize(P,Size),

	case actordb_backpressure:is_enabled(GSize,GCount) of
		true ->
			{sleep,Res};
		false ->
			{ok,Res}
	end.

check_bp() ->
	case actordb_backpressure:is_enabled() of
		true ->
			sleep;
		false ->
			ok
	end.

sleep_bp(P) ->
	actordb_backpressure:sleep_caller(P).

stop_bp(P) ->
	actordb_backpressure:stop_caller(P).

exec([_|_] = Sql) ->
	exec(butil:tobin(Sql),[]);
exec(Sql) ->
	exec1(actordb_sqlparse:parse_statements(Sql),[]).

exec([_|_] = Sql, BindingValues) ->
	exec(butil:tobin(Sql), BindingValues);
exec(Sql, BindingValues) ->
	exec1(actordb_sqlparse:parse_statements(Sql),BindingValues).

exec1(St) ->
	exec1(St,[]).
exec1(St,BindingValues)->
	case {St,BindingValues} of
		{{[{{Type,[Actor],Flags},IsWrite,Statements}],_}, []} when is_binary(Type) ->
			Call = #{type => Type, actor => Actor, flags => Flags, iswrite => IsWrite, statements => Statements,
			bindingvals => [], dorpc => true},
			direct_call(Call);
		{{[{{Type,[Actor],Flags},IsWrite,Statements}],_}, _} when is_binary(Type) ->
			Call = #{type => Type, actor => Actor, flags => Flags, iswrite => IsWrite,
			statements => Statements, bindingvals => BindingValues, dorpc => true},
			direct_call(Call);
		% Single block, writes to more than one actor
		{{[{{Type,[_,_|_] = Actors,Flags},true,Statements}],_}, []} ->
			Call = [#{type => Type, actor => Actors, flags => Flags, iswrite => true,
			statements => Statements, bindingvals => [], var => undefined, column => undefined, blockvar => undefined}],
			actordb_multiupdate:exec(Call);
		{{[{{Type,[_,_|_] = Actors,Flags},true,Statements}],_}, _} ->
			Call = #{type => Type, actor => Actors, flags => Flags, iswrite => true,
			statements => Statements, bindingvals => BindingValues, var => undefined, column => undefined, blockvar => undefined},
			actordb_multiupdate:exec(Call);



		% Single block, lookup across multiple actors
		%<<"actor user(denis,ino); SELECT * FROM todos;">>
		% {{[{{Type,[_,_|_] = Actors,Flags},false,Statements}],_}, []} ->
		% 	Call = #{type => Type, actor => Actors, flags => Flags, iswrite => failsafe,
		% 	statements => Statements, bindingvals => []},
		% 	actordb_multiupdate:multiread(Call);
		{{[{{Type, [_,_|_] = Actors, Flags},false, Statements}] = Multiread,_}, []} ->
			Call = #{type => Type, actor => Actors, flags => Flags, iswrite => false,
			statements => Statements, bindingvals => [], var => undefined, column => undefined, blockvar => undefined},
			actordb_multiupdate:multiread([Call]);
		{{[{{Type, [_,_|_] = Actors, Flags}, false, Statements}],_}, _} ->
			Call = #{type => Type, actor => Actors, flags => Flags, iswrite => false,
			statements => Statements, bindingvals => BindingValues, var => undefined, column => undefined, blockvar => undefined},
			actordb_multiupdate:multiread([Call]);
		% Single block, lookup across all actors of certain type





		% Single block, lookup across all actors of type
		%<<"actor user(*); SELECT * FROM todos;">>
		{{[{{ Type, $*, Flags}, false, Statements}],_}, []} ->
			Call = #{type => Type, actor => $*, flags => Flags, iswrite => false,
			statements => Statements, bindingvals => [], var => undefined, column => undefined, blockvar => undefined},
			actordb_multiupdate:multiread([Call]);
		{{[{{Type, $*, Flags}, false, Statements}],_}, _} ->
			Call = #{type => Type, actor => $*, flags => Flags, iswrite => false,
			statements => Statements, bindingvals => BindingValues, var => undefined, column => undefined, blockvar => undefined},
			actordb_multiupdate:multiread([Call]);


		% Single block, write across all actors of certain type
		{{[{{_Type,$*,_Flags},true,_Statements}] = Multiblock,_}, []} ->
			actordb_multiupdate:exec(Multiblock);
		{{[{{_Type,$*,_Flags},true,_Statements}],_}, _} ->
			Multiblock = [{{_Type,$*,_Flags},true,{_Statements, BindingValues}}],
			actordb_multiupdate:exec(Multiblock);

		% Multiple blocks, that change db
		{{[_,_|_] = Multiblock,true}, [] } ->
			actordb_multiupdate:exec(Multiblock);
			% Multiple blocks, that change db
		{{[_,_|_] = Multiblock,true}, BindingValues } ->
			actordb_multiupdate:exec({Multiblock,BindingValues});
		% Multiple blocks but only reads



		%actordb_sqlparse:parse_statements(<<"actor user(denis); SELECT * FROM todos; actor user(ino); SELECT * FROM todos;">>).
		{{[_,_|_] = Multiblock,false}, [] } ->
			actordb_multiupdate:multiread(Multiblock);
		{{[_,_|_] = Multiblock,false},_} ->
			actordb_multiupdate:multiread(Multiblock,BindingValues);
		{undefined, _} ->
			[];
		{[],_} ->
			[];
		{[[{columns,_},_]|_],_} ->
			St
 	end.

direct_call(#{actor := undefined}) ->
	[];
direct_call(#{actor := Actor, type := Type1, flags := Flags, iswrite := IsWrite, statements := Statements,
				bindingvals := BindingVals, dorpc := DoRpc} = Call)->
  Type = actordb_util:typeatom(Type1),
	Where = actordb_shardmngr:find_local_shard(Actor,Type),
	% ?AINF("direct_call ~p ~p ~p ~p",[Actor,Statements,Where,DoRpc]),
	% ?ADBG("call for actor local ~p global ~p ~p~n",[{Where,bkdcore:node_name()},actordb_shardmngr:find_global_shard(Actor),{IsWrite,Statements}]),
	Res = case Where of
		undefined when DoRpc ->
			{_,_,Node} = actordb_shardmngr:find_global_shard(Actor),
			rpc(Node,Actor,{?MODULE,direct_call,[Call#{dorpc := false}]});
		{redirect,_,Node} when DoRpc ->
			rpc(Node,Actor,{?MODULE,direct_call,[Call#{dorpc := false}]});
		_ ->
			case Where of
				undefined ->
					{Shard,_,_Node} = actordb_shardmngr:find_global_shard(Actor);
				{redirect,Shard,_} ->
					ok;
				Shard ->
					ok
			end,
			case IsWrite of
				true ->
					actordb_actor:write(Shard,Call#{type := Type});
				false ->
					actordb_actor:read(Shard,Call#{type := Type})
			end
	end,
	Res.

% First call node that should handle request.
% If node not working, check cluster nodes and pick one by consistent hashing.
rpc(undefined,Actor,MFA) ->
	?AERR("Call to undefined node! ~p ~p",[Actor,MFA]),
	{error,badnode};
rpc(Node,Actor,MFA) ->
	case actordb_conf:node_name() == Node of
		true ->
			{Mod,Func,Arg} = MFA,
			apply(Mod,Func,Arg);
		_ ->
			case bkdcore:rpc(Node,MFA) of
				{error,econnrefused} ->
					case lists:delete(Node,bkdcore:nodelist(bkdcore:cluster_group(Node))) of
						[] ->
							{error,econnrefused};
						Nodes ->
							call_loop(Actor,Nodes,MFA)
					end;
				Res ->
					Res
			end
	end.

call_loop(_,[],_) ->
	{error,econnrefused};
call_loop(Actor,L,MFA) ->
	Nd = hash_pick(Actor,L),
	case bkdcore:rpc(Nd,MFA) of
		{error,econnrefused} ->
			call_loop(Actor,lists:delete(Nd,L),MFA);
		Res ->
			Res
	end.


actor_id_type(Type) ->
	case lists:keyfind(actordb_util:typeatom(Type),1,actordb_schema:ids()) of
		{_,string} ->
			string;
		{_,integer} ->
			integer;
		_ ->
			undefined
	end.

hash_pick(Val) ->
	hash_pick(Val,bkdcore:nodelist(bkdcore:cluster_group())).

hash_pick(_,[]) ->
	undefined;
% Nodes are taken from bkdcore:nodelist, nodes from there are always sorted by name.
hash_pick({Val,_},L) ->
	hash_pick(Val,L);
hash_pick(Val,L) ->
	ValInt = actordb_util:hash([butil:tobin(Val),"asdf"]),
	Len = length(L),
	Step = ?NAMESPACE_MAX div Len,
	Index = ValInt div Step,
	lists:nth(Index+1,L).


schema_changed() ->
	actordb_shardmngr:schema_changed(),
	% [actordb_shard:kv_schema_check(Type) || Type <- actordb_schema:types(), actordb_schema:iskv(Type)],
	ok.

configfiles() ->
	[
		{cfg,"schema.yaml",[{autoload,true},
							{mod,actordb_schema},
							{preload,{actordb_util,parse_cfg_schema,[]}},
							{onload,{actordb,schema_changed,[]}}]}
	].
