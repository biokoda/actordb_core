% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_backpressure).
-behaviour(gen_server).
-export([start/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
-export([start_caller/0,start_caller/2,start_caller/3,stop_caller/1,sleep_caller/1, is_enabled/0,is_enabled/2,
			inc_callcount/0,inc_callcount/1,dec_callcount/0,dec_callcount/1,call_count/0,call_size/0, nclients/0,
			inc_callsize/1,dec_callsize/1,inc_callsize/2,dec_callsize/2,
			save/3,getval/2,delval/2,has_authentication/3]).
-define(LAGERDBG,true).
-include("actordb.hrl").

-record(caller,{ets, login, auth = []}).
start_caller() ->
	% E = ets:new(callerets,[set,private,{heir,whereis(?MODULE),self()}]),
	% butil:ds_add(curcount,0,E),
	% butil:ds_add(cursize,0,E),
	% #caller{ets = E}.
	start_caller(<<>>,<<>>,<<>>).

start_caller(Username, Password) ->
	start_caller(Username, Password, <<>>).
start_caller(Username, Password, Salt1) ->
	E = ets:new(callerets,[set,protected,{heir,whereis(?MODULE),self()}]),
	% ets:update_counter(bpcons,clients,),
	butil:ds_add({client,E},self(),bpcons),
	Users = actordb_sharedstate:read_global_users(),
	case Users of
		nostate ->
			handle_login(E);
		[] ->
			handle_login(E);
		[_|_] ->
			handle_login(E, Username, Password, Salt1, Users)
	end.

stop_caller(P) ->
	stop_caller(P,self()).
stop_caller(P,_Pid) ->
	Tid = P#caller.ets,
	butil:ds_rem({client,Tid},bpcons),
	Size = butil:ds_val(cursize,Tid),
	Count = butil:ds_val(curcount,Tid),
	dec_callcount(Count),
	dec_callsize(Size),
	ets:delete(Tid),
	% ets:update_counter(bpcounters,clients,-1),
	ok.

handle_login(E)->
	% Only if no state or no users
	Authentication = [],
	butil:ds_add(auth,Authentication,E),
	butil:ds_add(curcount,0,E),
	butil:ds_add(cursize,0,E),
	#caller{ets = E, login = <<>>, auth = [{'*',read},{'*',write},{{config},read},{{config},write}]}.

handle_login(E, Username, Password, Salt1, Users)->
	case lists:keyfind(butil:tobin(Username),1,Users) of
		false when Users == [] ->
			Pw = undefined,
			Rights = [{'*',read},{'*',write},{{config},read},{{config},write}];
		false = Rights ->
			?AERR("User not found ~p",[Username]),
			Pw = undefined,
			throw(invalid_login);
		{_,Pw,Rights} ->
			ok
	end,
	case Salt1 of
		<<Salt:20/binary,_/binary>> when is_binary(Pw) ->
			<<Num1:160>> = HashBin = crypto:hash(sha, Pw),
			<<Num2:160>> = crypto:hash(sha, <<Salt/binary, (crypto:hash(sha, HashBin))/binary>>),
			case <<(Num1 bxor Num2):160>> of
				Password ->
					ok;
				_ ->
					% ?AERR("Login hash comparison failed"),
					throw(invalid_login)
			end;
		_ when Pw == Password ->
			ok;
		_ when Pw == undefined ->
			ok;
		_ ->
			% ?AERR("Received salt is invalid ~p",[Salt1]),
			throw(invalid_login)
	end,
	butil:ds_add(curcount,0,E),
	butil:ds_add(cursize,0,E),
	#caller{ets = E, auth = Rights}.

% Store data inside connection ets (for instance prepared statement info)
save(#caller{ets = Ets},K,V) ->
	butil:ds_add(K,V,Ets);
save(L,K,V) when is_list(L) ->
	[{K,V}|L];
save(undefined,_,_) ->
	undefined.
getval(#caller{ets = Ets},K) ->
	butil:ds_val(K,Ets);
getval(L,K) when is_list(L) ->
	butil:ds_val(K,L);
getval(undefined,_) ->
	undefined.
delval(#caller{ets = Ets},K) ->
	butil:ds_rem(K,Ets);
delval(L,K) when is_list(L) ->
	lists:delete(K,L);
delval(undefined,_) ->
	ok.

% High watermark. If more than 2000 calls are waiting for result or
%  more than 32MB of queries is waiting to be processed
%  turn on bp.
is_enabled() ->
	% L = ets:tab2list(bpcounters),
	% is_enabled(butil:ds_val(global_size,L),butil:ds_val(global_count,L)).
	is_enabled(actordb_driver:get_counter(?COUNTER_SQLSIZE),actordb_driver:get_counter(?COUNTER_REQSNOW)).
is_enabled(GSize,GCount) ->
	GSize > 1024*1024*32 orelse GCount > 2000.

sleep_caller(_P) ->
	case whereis(backpressure_proc) of
		undefined ->
			% Spawn backpressure_proc and wait for it to die
			{Pid,_} = spawn_monitor(fun() ->
				case catch register(backpressure_proc,self()) of
					true ->
						?AINF("Backpressure applied."),
						backpressure_proc();
					_ ->
						% Proc lost the race. Simply link to the real proc so that they die together.
						link(whereis(backpressure_proc)),
						receive
							ok ->
								ok
						end
				end
				end);
		Pid ->
			erlang:monitor(process,Pid)
	end,
	receive
		{'DOWN',_Monitor,_,PID,_Reason} when PID == Pid ->
			ok
	end.

backpressure_proc() ->
	% low watermark 1MB, 300 calls
	case ets:info(bpcons,size) == undefined orelse call_size() > 1024*1024 orelse call_count() > 300 of
		true ->
			timer:sleep(30),
			backpressure_proc();
		false ->
			?AINF("Backpressure released."),
			exit(continue)
	end.

inc_callcount() ->
	inc_callcount(1).
% Global increment
inc_callcount(N) when is_integer(N) ->
	actordb_driver:counter_inc(?COUNTER_REQSNOW,N);
% Local increment
inc_callcount(P) ->
	ets:update_counter(P#caller.ets,curcount,1).
dec_callcount() ->
	dec_callcount(1).
% Global decrement
dec_callcount(N) when is_integer(N) ->
	actordb_driver:counter_inc(?COUNTER_REQSNOW,-N);
% Local decrement
dec_callcount(undefined) ->
	ok;
dec_callcount(P) ->
	ets:update_counter(P#caller.ets,curcount,-1).

inc_callsize(N) ->
	actordb_driver:counter_inc(?COUNTER_SQLSIZE,N).
inc_callsize(P,N) ->
	ets:update_counter(P#caller.ets,cursize,N).
dec_callsize(undefined) ->
	ok;
dec_callsize(N) ->
	actordb_driver:counter_inc(?COUNTER_SQLSIZE,-N).
dec_callsize(P,N) ->
	ets:update_counter(P#caller.ets,cursize,-N).

call_count() ->
	actordb_driver:get_counter(?COUNTER_REQSNOW).
call_size() ->
	actordb_driver:get_counter(?COUNTER_SQLSIZE).
nclients() ->
	% butil:ds_val(clients,bpcounters).
	ets:info(bpcons,size).

start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

% No stop because this process must never die.
% stop() ->
% 	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).



-record(dp,{}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).


handle_call(print_info,_,P) ->
	io:format("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast(_, P) ->
	{noreply, P}.

handle_info({'ETS-TRANSFER',Tid,FromPid,_HeirData},P) ->
	case catch stop_caller(#caller{ets = Tid},FromPid) of
		ok ->
			ok;
		Err ->
			?AERR("Cleanup caller error ~p",[Err])
	end,
	{noreply,P};
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
	process_flag(trap_exit,true),
	case ets:info(bpcons,size) of
		undefined ->
			ets:new(bpcons, [{write_concurrency,true},named_table,public,set,{heir,whereis(actordb_sup),<<>>}]);
			% butil:ds_add(global_count,0,bpcounters),
			% butil:ds_add(global_size,0,bpcounters),
			% butil:ds_add(clients,0,bpcounters);
		_ ->
			ok
	end,
	{ok,#dp{}}.


has_authentication(#caller{auth = Auth} = P,ActorType1,Action)->
	Auth = P#caller.auth,
	case ActorType1 of
		ActorType when is_tuple(ActorType) ->
			ok;
		_ ->
			ActorType = actordb_util:typeatom(ActorType1)
	end,
	has_authentication(Auth,ActorType,Action);

% check for atom because of config. User must have explicit {config} type access.
% For all other types, '*' is ok.
has_authentication([{'*',Action}|_],Type,Action) when is_atom(Type) ->
	true;
has_authentication([{Type,Action}|_],Type,Action) ->
	true;
has_authentication([_|T],Type,A) ->
	has_authentication(T,Type,A);
has_authentication([],_,_) ->
	false.
