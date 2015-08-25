% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_idgen).
-behaviour(gen_server).
-export([print_info/0, start/0, stop/0, init/1, handle_call/3, 
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([getid/0]).
-include_lib("actordb_core/include/actordb.hrl").
% -compile(export_all).
-define(CHUNKSIZE,1000).


% Public ets table stores idcounter:
%  {idcounter,CurVal,MaxVal}
% If CurVal exceeds MaxVal after increment, call genserver and wait for new value.

% Return: 
% - integer 
% - nostate - global state has not been established yet
getid() ->
	gen_server:call(?MODULE,getid).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop() ->
	gen_server:call(?MODULE, stop).
print_info() ->
	gen_server:call(?MODULE, print_info).


progression(N) when N =< 80000 ->
	N*2;
progression(N) ->
	N.


-record(dp,{curfrom, curto, ranges = [], gmax, storage, chunk_size = 0}).

handle_call(getid,_,P) when P#dp.curto > P#dp.curfrom, is_integer(P#dp.curto) ->
	{reply, {ok,P#dp.curfrom},P#dp{curfrom = P#dp.curfrom+1}};
% There are still intervals in the ranges buffer. Move to next.
handle_call(getid,_,#dp{ranges = [{From,To}|Rem]} = P) ->
	% butil:savetermfile([bkdcore:statepath(),"/ranges"],Rem),
	{ok,{[]}} = actordb_driver:exec_script({1},{term_to_binary({actordb_conf:node_name(),P#dp.chunk_size,Rem})},P#dp.storage),
	case Rem of
		[_,_,_,_|_] ->
			ok;
		% Less than 4 ranges remaining, get new ones
		_ ->
			spawn(fun() -> gen_server:call(?MODULE,getstorerange) end)
	end,
	{reply,{ok,From},P#dp{curfrom = From+1, curto = To, ranges = Rem}};
% We are completely empty. No ranges available. Get a new range.
handle_call(getid,MFrom,P) ->
	case handle_call(getstorerange,MFrom,P) of
		{reply,ok,NP} ->
			handle_call(getid,MFrom,NP);
		Err ->
			Err
	end;
handle_call(getstorerange,_MFrom,P) ->
	case P#dp.ranges of
		[_,_,_,_|_] ->
			{reply,ok,P};
		_ ->
			case actordb_sharedstate:get_id_chunk(P#dp.chunk_size) of
				{From,To} when is_integer(From), is_integer(To) ->
					% Master gives a big range, split into smaller chunks of 100 ids.
					Ranges = P#dp.ranges ++ [{Num-?CHUNKSIZE,Num} || Num <- lists:seq(From+?CHUNKSIZE,To,?CHUNKSIZE)],
					NextChunk = progression(P#dp.chunk_size),
					{ok,{[]}} = actordb_driver:exec_script({1},{term_to_binary({actordb_conf:node_name(),NextChunk,Ranges})},P#dp.storage),
					case P#dp.curfrom of
						undefined ->
							[{RFrom,RTo}|RemRanges] = Ranges,
							{reply,ok,P#dp{curfrom = RFrom+1, curto = RTo,ranges = RemRanges, chunk_size = NextChunk}};
						_ ->
							{reply,ok,P#dp{ranges = Ranges, chunk_size = NextChunk}}
					end;
				Err ->
					{reply, Err,P}
			end
	end;
handle_call(print_info, _, P) ->
	io:format("~p~n~p~n", [P,get()]),
	{reply, ok, P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call(_,_,P) ->
	{reply, ok, P}.

handle_cast(_, P) ->
	{noreply, P}.


handle_info({actordb,sharedstate_change},P) ->
	case P#dp.ranges == [] andalso P#dp.storage == undefined of
		true ->
			{ok,Blob} = actordb_driver:open("ranges",0,blob),
			case actordb_driver:exec_script({1},Blob) of
				{ok,{[]}} ->
					case butil:readtermfile([bkdcore:statepath(),"/ranges"]) of
						Ranges when is_list(Ranges) ->
							{noreply,P#dp{ranges = Ranges, storage = Blob}};
						_ ->
							{noreply,P#dp{storage = Blob, chunk_size = 5000}}
					end;
				{ok,{[Bin]}} ->
					Me = actordb_conf:node_name(),
					case binary_to_term(Bin) of
						{Me,ChunkSize,Ranges} when is_list(Ranges) ->
							{noreply,P#dp{ranges = Ranges, storage = Blob, chunk_size = ChunkSize}};
						_ ->
							{noreply,P#dp{storage = Blob, chunk_size = 5000}}
					end
			end;
		false ->
			{noreply,P}
	end;
handle_info(_, P) -> 
	{noreply, P}.


terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	actordb_sharedstate:subscribe_changes(?MODULE),
	{ok, #dp{}}.



