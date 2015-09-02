% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_catchup).
-behaviour(gen_server).
-export([print_info/0, start/0, stop/0, init/1, handle_call/3, 
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([report/2,synced/2]).
-export([synced_local/2]). % internal
-include_lib("actordb_core/include/actordb.hrl").
-define(ETS,catchup_actors).
-compile(export_all).

% Report which actor is unable to sync because node is offline.
% This is stored locally to lmdb and not replicated. 
% Once network conditions change, actors will be notified to resync.
report(Actor,Type) ->
	K = key(Actor,Type),
	case butil:ds_val(K,?ETS) of
		undefined ->
			gen_server:call(?MODULE,{report,K});
		_ ->
			ok
	end.
synced(A,T) ->
	% We don't actually know which node wanted us to report when synced. So call on all.
	[rpc:cast(Nd,?MODULE,synced_local,[A,T]) || Nd <- [bkdcore:cluster_nodes()|node()]],
	ok.

synced_local(A,T) ->
	K = key(A,T),
	case butil:ds_val(K,?ETS) of
		undefined ->
			ok;
		_ ->
			gen_server:call(?MODULE,{synced,K})
	end.

key(A,T) ->
	{undefined,butil:tobin(A),butil:tobin(T)}.

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop() ->
	gen_server:call(?MODULE, stop).
print_info() ->
	gen_server:call(?MODULE, print_info).



-record(dp,{evnum = 0, db}).

handle_call({report,K},_,P) ->
	ok = actordb_sqlite:exec(P#dp.db,<<"#s05;">>,[[K]],1,P#dp.evnum+1,<<>>),
	case P#dp.evnum+1 rem 10 of
		0 ->
			actordb_driver:checkpoint(P#dp.db,P#dp.evnum-2);
		_ ->
			ok
	end,
	ets:insert(?ETS,{K}),
	{reply,ok,P#dp{evnum = P#dp.evnum+1}};
handle_call({synced,K},_,P) ->
	{ok,_} = actordb_sqlite:exec(P#dp.db,<<"DELETE FROM actors WHERE actor=? AND type=?;">>,[[K]],1,P#dp.evnum+1,<<>>),
	case P#dp.evnum+1 rem 10 of
		0 ->
			actordb_driver:checkpoint(P#dp.db,P#dp.evnum-2);
		_ ->
			ok
	end,
	ets:delete(?ETS,K),
	{reply,ok,P#dp{evnum = P#dp.evnum+1}};
handle_call(print_info, _, P) ->
	io:format("~p~n~p~n", [P,get()]),
	{reply, ok, P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call(_,_,P) ->
	{reply, ok, P}.

handle_cast(_, P) ->
	{noreply, P}.

do_sync_actors(0,_) ->
	ok;
do_sync_actors(_,'$end_of_table') ->
	ok;
do_sync_actors(N,K) ->
	case ets:lookup(?ETS,K) of
		[{{_,A,T}}] ->
			case actordb_sqlproc:call({A,actordb_util:typeatom(T)},[],report_synced,actordb_actor) of
				{error,nocreate} ->
					synced(A,T);
				{error,_} ->
					[exit(normal) || is_normal() == false];
				_ ->
					ok
			end;
		_ ->
			ok
	end,
	do_sync_actors(N-1,ets:next(?ETS,K)).

is_normal() ->
	Nodes = bkdcore:cluster_nodes(),
	[Nd || Nd <- Nodes, lists:member(Nd,nodes()) == false] == [].

do_sync_actors() ->
	case is_normal() andalso whereis(sync_actor_proc) == undefined of
		true ->
			spawn(fun() -> register(sync_actor_proc,self()), do_sync_actors(100,ets:first(?ETS)) end);
		_ ->
			ok
	end.

handle_info(check,P) ->
	case ets:info(?ETS,size) of
		N when is_integer(N), N > 0 ->
			do_sync_actors();
		_ ->
			ok
	end,
	erlang:send_after(1000,self(),check),
	{noreply,P};
handle_info(_, P) -> 
	{noreply, P}.


terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	Pth = "catchup",
	case actordb_driver:actor_info(Pth,actordb_util:hash(Pth)) of
		{_FC,{_,Evnum},_InProg,_MxPage,_AllPages,_,<<>>} ->
			ok;
		_ ->
			Evnum = 0
	end,
	{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(Pth,wal),
	case SchemaTables of
		[_|_] ->
			ok;
		_ ->
			{ok,_} = actordb_sqlite:exec(Db,"CREATE TABLE actors (actor TEXT, type TEXT,PRIMARY KEY (actor,type)) WITHOUT ROWID;",write)
	end,
	case ets:info(?ETS,size) of
		undefined ->
			ets:new(?ETS, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{read_concurrency,true}]),
			{ok,[{columns,_},{rows,Actors}]} = actordb_sqlite:exec(Db,"select * from actors;"),
			?AINF("Catchup: ~p",[Actors]),
			% No values, {Actor,Type} (as binaries both) are our keys and we only care about keys.
			% So we must place it in a double tuple.
			ets:insert(?ETS,[{key(A,B)} || {A,B} <- Actors]);
		_ ->
			ok
	end,
	erlang:send_after(1000,self(),check),
	{ok, #dp{evnum = Evnum, db = Db}}.



