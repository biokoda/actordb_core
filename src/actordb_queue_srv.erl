% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_queue_srv).
-behaviour(gen_server).
-export([print_info/0, start/0, stop/0, init/1, handle_call/3, 
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([list_files/0, get_chunk/1, init_from/1, moved_to/2]).
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

init_from(Q) ->
	gen_server:call(?MODULE,{init_from,Q}).

moved_to(Queue,FileIndex) ->
	gen_server:cast(?MODULE,{moved_to,Queue,FileIndex}).

get_chunk(Size) ->
	L = ets:tab2list(?GSTATE),
	Offset = ets:update_counter(?GSTATE,offset,{2,Size}),
	{butil:ds_val(index,L),Offset - Size}.


start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop() ->
	gen_server:call(?MODULE, stop).
print_info() ->
	gen_server:call(?MODULE, print_info).


% Queues: [{QueueIndex,FileIndex}]
-record(dp,{init_from = 0, queues = []}).

handle_call({init_from,Q},_,P) ->
	case lists:keyfind(Q,1,P#dp.queues) of
		false ->
			{reply, P#dp.init_from, P};
		{_,Index} ->
			{reply, Index, P}
	end;
handle_call(print_info, _, P) ->
	io:format("~p~n~p~n", [P,get()]),
	{reply, ok, P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call(_,_,P) ->
	{reply, ok, P}.

handle_cast({moved_to,Q,FI},P) ->
	{noreply,P#dp{queues = lists:keystore(Q,1,P#dp.queues,{Q,FI})}};
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
					InitFrom = Latest,
					butil:ds_add({index,Latest+1},?GSTATE);
				_ ->
					InitFrom = Latest-1,
					butil:ds_add({index,Latest},?GSTATE)
			end;
		_ ->
			InitFrom = 0
	end,
	{ok, #dp{init_from = InitFrom}}.

