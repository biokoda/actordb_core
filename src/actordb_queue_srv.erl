% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_queue_srv).
-behaviour(gen_server).
-export([print_info/0, start/0, stop/0, init/1, handle_call/3, 
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([list_files/0]).
-include_lib("actordb_core/include/actordb.hrl").
% -compile(export_all).
-define(GSTATE,queueets).

% Sorted list of files. From oldest to newest. [{Index,Filename},..]
list_files() ->
	L = filelib:wildcard(actordb_conf:db_path()++"/q.*"),
	lists:sort(butil:sparsemap(fun(Nm) ->
		case catch list_to_integer(tl(filename:extension(Nm))) of
			N when is_integer(N) ->
				{N,Nm};
			_ ->
				undefined
		end
	end,L)).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop() ->
	gen_server:call(?MODULE, stop).
print_info() ->
	gen_server:call(?MODULE, print_info).



-record(dp,{}).

handle_call(print_info, _, P) ->
	io:format("~p~n~p~n", [P,get()]),
	{reply, ok, P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call(_,_,P) ->
	{reply, ok, P}.

handle_cast(_, P) ->
	{noreply, P}.

handle_info(_, P) -> 
	{noreply, P}.


terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	case ets:info(?GSTATE,size) of
		undefined ->
			ets:new(?GSTATE, [named_table,public,set,{heir,whereis(actordb_sup),<<>>}]),
			butil:ds_add([{index,0},{offset,0}],?GSTATE);
		_ ->
			ok
	end,
	case lists:reverse(list_files()) of
		[{Latest,Pth}|_] ->
			case filelib:file_size(Pth) > 0 of
				true ->
					butil:ds_add({index,Latest+1},?GSTATE);
				_ ->
					butil:ds_add({index,Latest},?GSTATE)
			end;
		_ ->
			ok
	end,
	{ok, #dp{}}.

