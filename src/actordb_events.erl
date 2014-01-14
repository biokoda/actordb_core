-module(actordb_events).
-include_lib("actordb.hrl").
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, 
		terminate/2, code_change/3,print_info/0]).
-export([get_schema/1,start_ready/0,newevent/1]).
-export([actor_deleted/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			API
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
actor_deleted(Name,Type,Num) ->
	gen_server:call(?MODULE,{newevent,[{what,delete},{actor,{Name,Type,Num}}]}).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			Callbacks
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

newevent(Info) ->
	<<"INSERT INTO events (data) values ('",(base64:encode(term_to_binary(Info,[compressed])))/binary,"');">>.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% 
% 			gen_server
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
start_ready() ->
	% gen_server:call(?MODULE,start_ready).
	ok.


start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).


-record(dp,{started = false, evproc}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	

sqlname() ->
	{<<"events">>,?CLUSTEREVENTS_TYPE}.

handle_call({newevent,Info},_,P) ->
	case actordb_sqlproc:write(sqlname(),[create],{{?MODULE,newevent,[Info]},undefined,undefined},actordb_actor) of
		{ok,_} ->
			{reply,ok,P};
		_ ->
			{reply,false,P}
	end;
handle_call(start_ready,_,P) ->
	{Pid,_} = spawn_monitor(fun() -> process_ev() end),
	{reply,ok,P#dp{started = true,evproc = Pid}};
handle_call(print_info,_,P) ->
	io:format("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.



handle_cast(_, P) ->
	{noreply, P}.


handle_info(dostuff,P) ->
	erlang:send_after(1000,self(),dostuff),
	case P#dp.evproc of
		undefined when P#dp.started == true ->
			{Pid,_} = spawn_monitor(fun() -> process_ev() end),
			{noreply,P#dp{evproc = Pid}};
		_ ->
			{noreply,P}
	end;
handle_info({'DOWN',_Monitor,_,PID,Result},#dp{evproc = PID} = P) ->
	case Result of
		ok ->
			ok;
		_ ->
			ok
	end,
	{noreply,P#dp{evproc = undefined}};
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
	erlang:send_after(1000,self(),dostuff),
	{ok,#dp{}}.


process_ev() ->
	case bkdcore_sharedstate:get_cluster_state(?MODULE,{last_evnum,bkdcore:node_name()}) of
		undefined ->
			Evnum = 0;
		Evnum when is_integer(Evnum) ->
			ok;
		nostate ->
			Evnum = 0,
			exit(nostate)
	end,
	case actordb_sqlproc:read(sqlname(),[create],<<"SELECT FROM events WHERE id > ",(butil:tobin(Evnum))/binary," LIMIT 100;">>,actordb_actor) of
		{ok,[{columns,_},{rows,[]}]} ->
			exit(ok);
		{ok,[{columns,_},{rows,Rows}]} ->
			process_ev(Evnum,lists:reverse(Rows));
		_ ->
			exit(ok)
	end.
process_ev(Evnum,[{Id,Data}|T]) ->
	case binary_to_term(base64:decode(Data)) of
		[_|_] = Info ->
			case butil:ds_val(what,Info) of
				delete ->
					{Name,Type,Num} = butil:ds_val(actor,Info)
			end;
		_ ->
			ok
	end,
	process_ev(Evnum,T);
process_ev(Evnum,[]) ->
	process_ev().


get_schema(0) ->
	{schema_version(), [schema(N) || N <- lists:seq(1,schema_version())]};
get_schema(Version) ->
	case schema_version() > Version of
		true ->
			{schema_version(),[schema(N) || N <- lists:seq(Version+1,schema_version())]};
		false ->
			{Version,[]}
	end.

schema_version() ->
	1.

schema(1) ->
	<<"CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT);">>.

