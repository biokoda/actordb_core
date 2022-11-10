% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_dummy).
-behaviour(gen_server).
-export([start/0,stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
-include("actordb.hrl").
-define(DBS,dummydbs).
-export([prepare/2]).
% Maintains a memory only actor of every type. 
% We use it parse prepare statement call over mysql protocol.

prepare(Type,Sql) ->
	case butil:ds_val(actordb_util:typeatom(Type),?DBS) of
		undefined ->
			undefined;
		{_,Db} ->
			actordb_driver:stmt_info(Db,Sql)
	end.

start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).


-record(dp,{}).

handle_call(print_info,_,P) ->
	?AINF("~p",[P]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast(_, P) ->
	{noreply, P}.

handle_info({actordb,sharedstate_change},P) ->
	case catch actordb_schema:types() of
		[_|_] = L ->
			[begin
				case butil:ds_val(T,?DBS) of
					undefined ->
						Db = undefined,
						Vers = 0,
						{NV,Sql} = actordb_util:type_schema(T,0);
					{Db,Vers} ->
						{NV,Sql} = actordb_util:type_schema(T,Vers)
				end,
				case ok of
					_ when Vers == NV ->
						ok;
					_ when Db == undefined ->
						{ok,Db1,_,_} = actordb_sqlite:init(":memory:"),
						actordb_sqlite:exec(Db1,Sql,write),
						butil:ds_add(T,{NV,Db1},?DBS);
					_ ->
						actordb_sqlite:exec(Db,Sql,write),
						butil:ds_add(T,{NV,Db},?DBS)
				end
			end || T <- L];
		_ ->
			ok
	end,
	{noreply, P};
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info(_, P) ->
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init(_) ->
	case ets:info(?DBS,size) of
		undefined ->
			ets:new(?DBS, [named_table,public,set,{read_concurrency,true}]);
		_ ->
			ok
	end,
	actordb_sharedstate:subscribe_changes(?MODULE),
	{ok,#dp{}}.

