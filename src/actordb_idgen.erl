% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_idgen).
-behaviour(gen_server).
-export([print_info/0, start/0, stop/0, init/1, handle_call/3, 
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([getid/0]).
-export([test/0]).
-define(RANGE_SIZE,5000).
-define(RANGE_FROM,1000).
-include_lib("actordb_core/include/actordb.hrl").
% -compile(export_all).



% Public ets table stores idcounter:
%  {idcounter,CurVal,MaxVal}
% If CurVal exceeds MaxVal after increment, call genserver and wait for new value.

% Return: 
% - integer 
% - nostate - global state has not been established yet
getid() ->
	case catch ets:lookup(?MODULE,idcounter) of
		[{idcounter,Val,Max}] when Val < Max ->
			case ets:update_counter(?MODULE,idcounter,{2,1,Max-1,Max}) of
				NVal when is_integer(NVal), NVal < Max ->
					{ok,NVal};
				_ ->
					getid1(Max)
			end;
		[{idcounter,_Val,Max}] ->
			getid1(Max);
		[] ->
			getid1(0);
		{'EXIT',{badarg,_}} ->
			nostate
	end.

getid1(Val) ->
	case gen_server:call(?MODULE,{getid,Val},infinity) of
		again ->
			getid();
		N when is_integer(N) ->
			{ok,N};
		nomajority ->
			nostate;
		Err ->
			Err
	end.

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop() ->
	gen_server:call(?MODULE, stop).
print_info() ->
	gen_server:call(?MODULE, print_info).


-record(dp,{curfrom, curto, ranges = [], gmax, storage}).

% If multiple calls to getid are in the message queue at the same time, after first updates ets, the rest of getid calls
%  need to be dismissed.
handle_call({getid,Val},_,P) when P#dp.curto > Val, is_integer(P#dp.curto) ->
	{reply, again,P};
% There are still intervals in the ranges buffer. Move to next.
handle_call({getid,_Val},_,#dp{ranges = [{From,To}|Rem]} = P) ->
	butil:ds_add({idcounter,From,To},?MODULE),
	% butil:savetermfile([bkdcore:statepath(),"/ranges"],Rem),
	{ok,{[]}} = actordb_driver:exec_script({1},{term_to_binary(Rem)},P#dp.storage),
	case Rem of
		[_,_,_|_] ->
			ok;
		% Less than 3 ranges remaining, get new ones
		_ ->
			spawn(fun() -> gen_server:call(?MODULE,getstorerange) end)
	end,
	{reply,From,P#dp{curfrom = From, curto = To, ranges = Rem}};
% We are completely empty. No ranges available. Get a new range.
handle_call({getid,Val},MFrom,P) ->
	case handle_call(getstorerange,MFrom,P) of
		{reply,ok,NP} ->
			handle_call({getid,Val},MFrom,NP);
		Err ->
			Err
	end;
handle_call(getstorerange,MFrom,P) ->
	case handle_call(getrange,MFrom,P) of
		{reply,{From,To},NP} when is_integer(From), is_integer(To) ->
			% Master gives a big range, split into smaller chunks of 100 ids.
			Ranges = P#dp.ranges ++ [{Num-100,Num} || Num <- lists:seq(From+100,To,100)],
			% io:format("getid getranges ~p   ~p ~n",[{Val,From,To},Ranges]),
			% butil:savetermfile([bkdcore:statepath(),"/ranges"],Ranges),
			{ok,{[]}} = actordb_driver:exec_script({1},{term_to_binary(Ranges)},P#dp.storage),
			{reply,ok,NP#dp{ranges = Ranges}};
		Err ->
			Err
	end;
handle_call(getrange,Client,P) ->
	case actordb_sharedstate:whois_global_master() of
		Err when Err == undefined; Err == nostate ->
			{reply,nomajority,P};
		Master ->
			case actordb_conf:node_name() == Master of
				true ->
					case P#dp.gmax of
						undefined ->
							case actordb_sharedstate:read_global(idmax) of
								nostate ->
									{reply,nomajority,P};
								undefined ->
									handle_call(getrange,Client,P#dp{gmax = ?RANGE_FROM});
								Gmax when is_integer(Gmax) ->
									handle_call(getrange,Client,P#dp{gmax = Gmax})
							end;
						_ ->
							From = P#dp.gmax,
							To = P#dp.gmax + ?RANGE_SIZE,
							case actordb_sharedstate:write_global_on(actordb_conf:node_name(),idmax,To) of
								{master_is,Othernode} ->
									case bkdcore:rpc(Othernode,gen_server,call,[?MODULE,getrange]) of
										{From,To} when is_integer(From), is_integer(To) ->
											{reply,{From,To},P};
										Err ->
											{reply,Err,P}
									end;
								ok ->
									{reply,{From,To},P#dp{gmax = To}};
								_ ->
									{reply,nomajority,P}
							end
					end;
				false ->
					case bkdcore:rpc(Master,gen_server,call,[?MODULE,getrange]) of
						{From,To} when is_integer(From), is_integer(To) ->
							{reply,{From,To},P};
						Err ->
							{reply,Err,P}
					end
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
	case P#dp.ranges == [] of
		true ->
			{ok,Blob} = actordb_driver:open("ranges",0,blob),
			case actordb_driver:exec_script({1},Blob) of
				{ok,{[]}} ->
					case butil:readtermfile([bkdcore:statepath(),"/ranges"]) of
						Ranges when is_list(Ranges) ->
							{noreply,P#dp{ranges = Ranges, storage = Blob}};
						_ ->
							{noreply,P#dp{storage = Blob}}
					end;
				{ok,{[Bin]}} ->
					case binary_to_term(Bin) of
						Ranges when is_list(Ranges) ->
							{noreply,P#dp{ranges = Ranges, storage = Blob}};
						_ ->
							{noreply,P#dp{storage = Blob}}
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
	case ets:info(?MODULE,size) of
		undefined ->
			ets:new(?MODULE, [named_table,public,set]);
		_ ->
			ok
	end,
	actordb_sharedstate:subscribe_changes(?MODULE),
	{ok, #dp{}}.




test() ->
	test(0,10000).
test(_,N) when N =< 0 ->
	ok;
test(PrevVal,N) ->
	NV = getid(),
	% io:format("~p~n",[NV]),
	case PrevVal < NV of
		true ->
			ok;
		false ->
			throw({error,NV,PrevVal})
	end,
	test(NV,N-1).
