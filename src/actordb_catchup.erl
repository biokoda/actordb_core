% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

% not used...
-module(actordb_catchup).
-behaviour(gen_server).
-export([start/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,print_info/0]).
-export([ping/2,node_num/1]).
-export([report_limbo/4, get_limbo_data/2, del_limbo_data/2]).
-define(LAGERDBG,true).
-include_lib("actordb.hrl").

% Public ETS tables
% pingtable - [{Node,N}] -> last ping number for every node
% limbocache - [{{ActorName,Type}}] -> list of actors in limbo at the moment

node_num(Node) ->
	butil:ds_val(Node,pingtable,0).

% LimboType: 
%  - unconfirmed - slave commited write, but master was gone before receiving replicate_confirm.
%  - uncommited - slave received data for write, but was disconnected next. Does not know if write was confirmed or not. 
report_limbo(Actor,Type,LimboType,LimboInfo) ->
	Db = butil:ds_val(limbodb,limbocache),
	{ok,_} = actordb_sqlite:exec(Db,[<<"INSERT OR REPLACE INTO actors VALUES ('">>,Actor," ",butil:tobin(Type),"','",
			butil:tobin(LimboType),"','",base64:encode(term_to_binary(LimboInfo)),"');"]),
	butil:ds_add({{Actor,Type}},limbocache),
	ok.

get_limbo_data(Actor,Type) ->
	case butil:ds_val({Actor,Type},limbocache) of
		undefined ->
			undefined;
		_ ->
			Db = butil:ds_val(limbodb,limbocache),
			{ok,[{columns,_},{rows,{_Id,_LType,Data}}]} = actordb_sqlite:exec(Db,[<<"SELECT * FROM actors WHERE id='">>,Actor," ",butil:tobin(Type),"';"]),
			{ok,binary_to_term(base64:decode(Data))}
	end.
del_limbo_data(Actor,Type) ->
	Db = butil:ds_val(limbodb,limbocache),
	{ok,_} = actordb_sqlite:exec(Db,[<<"DELETE FROM actors WHERE id='">>,Actor," ",butil:tobin(Type),"';"]),
	butil:ds_rem({{Actor,Type}},limbocache),
	ok.


ping(FromNode,N) ->
	% gen_server:cast(?MODULE,{ping,FromNode,N}).
	butil:ds_add(FromNode,N,pingtable),
	ok.


start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

print_info() ->
	gen_server:call(?MODULE,print_info).


-record(dp,{nodelist_ping = [], pingnum = 0}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	


handle_call(print_info,_,P) ->
	io:format("~p~n",[?R2P(P)]),
	{reply,ok,P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

% handle_cast({ping,Node,N},P) ->
% 	{noreply,P};
handle_cast(_, P) ->
	{noreply, P}.

handle_info(doping,P) ->
	erlang:send_after(200,self(),doping),
	case P#dp.nodelist_ping of
		[] ->
			ok;
		_ ->
			[rpc:async_call(Nd,?MODULE,ping,[bkdcore:node_name(),P#dp.pingnum]) || Nd <- P#dp.nodelist_ping]
	end,
	{noreply, P#dp{pingnum = P#dp.pingnum + 1}};
handle_info({actordb,sharedstate_change},P) ->
	{noreply,P#dp{nodelist_ping = [bkdcore:dist_name(Nd) || Nd <- bkdcore:cluster_nodes()]}};
handle_info(stop,P) ->
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
	erlang:send_after(200,self(),doping),
	actordb_sharedstate:subscribe_changes(?MODULE),
	self() ! {actordb,sharedstate_change},
	case ets:info(pingtable,size) of
		undefined ->
			ets:new(pingtable, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{read_concurrency,true}]);
		_ ->
			ok
	end,
	case ets:info(limbocache,size) of
		undefined ->
			ets:new(limbocache, [named_table,public,set,{heir,whereis(actordb_sup),<<>>},{read_concurrency,true},{write_concurrency,true}]);
		_ ->
			ok
	end,
	{ok,LimboDb,SchemaTables,_PageSize} = actordb_sqlite:init(hd(actordb_conf:paths())++"/limbodb",wal),
	case SchemaTables of
		[] ->
			actordb_sqlite:exec(LimboDb,"CREATE TABLE actors (id TEXT, limbotype TEXT, limbodata TEXT) WITHOUT ROWID;");
		_ ->
			{ok,[{columns,_},{rows,Rows1}]} = actordb_sqlite:exec(LimboDb,"SELECT id FROM actors;"),
			Rows = [begin
				[Actor,Type] = binary:split(Row,<<" ">>),
				{{Actor,binary_to_atom(Type,utf8)}}
			end || {Row} <- Rows1],
			butil:ds_add(Rows,limbocache)
	end,
	butil:ds_add(limbodb,LimboDb,limbocache),
	{ok,#dp{}}.

