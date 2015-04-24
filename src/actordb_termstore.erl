% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_termstore).
-behaviour(gen_server).
-export([start/0,stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
-export([store_term_info/6,read_term_info/2]).
-include_lib("actordb.hrl").

store_term_info(A,T,VF,CurTerm,EvNum,EvTerm) ->
	gen_server:call(?MODULE,{write,A,T,VF,CurTerm,EvNum,EvTerm},infinity).

read_term_info(A,T) ->
	butil:ds_val({A,T},termstore).

start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).


stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).



-record(dp,{db, writes = [], write_count = 0}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).

handle_call({write,Actor,Type,VotedFor,CurTerm,EvNum,EvTerm},CallFrom,P) ->
	% Do not write directly. Use timeout which will mean draining message queue.
	% This way if many write calls they will get combined.
	% Add CallFrom as first element of tuple, it will get ignored in write.
	case P#dp.writes of
		[_|_] ->
			ok;
		[] ->
			self() ! timeout
	end,
	ets:insert(termstore,{{butil:tobin(Actor),Type},VotedFor,CurTerm,EvNum,EvTerm}),
	{noreply,P#dp{writes = [{CallFrom,butil:tobin(Actor),Type,VotedFor,CurTerm,EvNum,EvTerm}|P#dp.writes],
				write_count = P#dp.write_count + 1}};
handle_call({read,Actor,Type},_,P) ->
	case actordb_sqlite:exec(P#dp.db,<<"#d10;">>,[[[butil:tobin(Actor),Type]]]) of
		{ok,[{columns,_},{rows,[{_Actor,_Type,VotedFor,CurTerm,EvNum,EvTerm}]}]} ->
			{reply,{ok,VotedFor,CurTerm,EvNum,EvTerm},P};
		_ ->
			{reply,undefined,P}
	end;
handle_call(readall,_,P) ->
	{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(P#dp.db,"SELECT * FROM terms;",read),
	{reply,Rows,P};
handle_call(print_info,_,P) ->
	io:format("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast(_, P) ->
	{noreply, P}.

handle_info(timeout,P) ->
	ok = actordb_sqlite:exec(P#dp.db,<<"#s09;">>,[P#dp.writes]),
	[gen_server:reply(element(1,W),ok) || W <- P#dp.writes],
	% case P#dp.write_count > 1000 of
	% 	true ->
	% 		NW = 0,
	% 		actordb_sqlite:checkpoint(P#dp.db);
	% 	false ->
	% 		NW = P#dp.write_count
	% end,
	NW = P#dp.write_count,
	{noreply,P#dp{writes = [], write_count = NW}};
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
	case ets:info(termstore,size) of
		undefined ->
			ets:new(termstore, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{read_concurrency,true}]);
		_ ->
			ok
	end,
	{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init("termstore",wal,0),
	case SchemaTables of
		[] ->
			{ok,_} = actordb_sqlite:exec(Db,schema(),write);
		_ ->
			ok
	end,
	{ok,[{columns,_},{rows,R}]} = actordb_sqlite:exec(Db,<<"select * from terms;">>,read),

	ets:insert(termstore,[{{A,T},VF,CT,EN,ET} || {A,T,VF,CT,EN,ET} <- R]),
	{ok,#dp{db = Db}}.


schema() ->
	<<"CREATE TABLE terms (actor TEXT, type TEXT, votedfor TEXT, curterm INTEGER,",
		" evnum INTEGER, evterm INTEGER, PRIMARY KEY(actor,type)) WITHOUT ROWID;">>.
