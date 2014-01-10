% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sup).
-behavior(supervisor).
-export([start_link/0, init/1]).
-include("actordb.hrl").
-compile(export_all).
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
	case length(actordb_conf:paths())*2 >= erlang:system_info(logical_processors) of
		true ->
			NProcs = length(actordb_conf:paths())*2;
		false ->
			NProcs = length(actordb_conf:paths())
	end,
	esqlite3:init(NProcs),

	spawn(fun() -> check_rowid() end),

	{ok, {{one_for_one, 10, 1},
		 [
		{actordb_shardmngr,
			{actordb_shardmngr, start, []},
			 permanent,
			 100,
			 worker,
			[actordb_shardmngr]},
		{actordb_shardmvr,
			{actordb_shardmvr, start, []},
			 permanent,
			 100,
			 worker,
			[actordb_shardmvr]},
		{actordb_local,
			{actordb_local, start, []},
			 permanent,
			 100,
			 worker,
			[actordb_local]},
		{actordb_backpressure,
			{actordb_backpressure, start, []},
			 permanent,
			 100,
			 worker,
			[actordb_backpressure]}
			]
	}}.

check_rowid() ->
	{ok,Db,_,_} = actordb_sqlite:init(":memory:"),
	case actordb_sqlite:exec(Db,"CREATE TABLE t (id TEXT PRIMARY KEY) WITHOUT ROWID;") of
		ok ->
			application:set_env(actordb,withoutrowid,<<"WITHOUT ROWID">>);
		_ ->
			application:set_env(actordb,withoutrowid,<<>>)
	end.