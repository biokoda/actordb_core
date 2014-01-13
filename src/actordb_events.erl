-module(actordb_events).
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, 
		terminate/2, code_change/3,print_info/0]).
-export([get_schema/1,start_ready/0]).


start_ready() ->
	ok.

start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE,print_info).

-record(dp,{started = false}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	


handle_call(start_ready,_,P) ->
	{reply,ok,P#dp{started = true}};
handle_call(print_info,_,P) ->
	io:format("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.



handle_cast(_, P) ->
	{noreply, P}.


handle_info(dostuff,P) ->
	erlang:send_after(1000,self(),dostuff),
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
	erlang:send_after(1000,self(),dostuff),
	{ok,#dp{}}.


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

