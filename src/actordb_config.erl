% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_config).
-include_lib("actordb_core/include/actordb.hrl").
-export([exec/1, exec/2]).

% Replacement for actordb_cmd
% Query/change actordb config.

exec(Sql) ->
	exec(undefined,Sql).

exec(BP,Sql) ->
	% If DB uninitialized we do not have any users created.
	% If in embedded mode and no BP is being used execute normally
	case actordb:types() of
		schema_not_loaded ->
			Init = false;
		_ ->
			Init = true
	end,
	case BP of
		undefined ->
			ok;
		_ when Init ->
			% TODO: Check if user has rights to change config
			ok;
		_ ->
			ok
	end,
	exec1(Init,cmd([],butil:tobin(Sql))).

exec1(true,_Cmds) ->
	_Cmds;
exec1(false,Cmds) ->
	% To initialize we need:
	% Insert to group
	% Insert to nodes
	% Create user
	Grp1 = lists:flatten([simple_values(I#insert.values,[]) || I <- Cmds, I#insert.table == <<"groups">>]),
	Grp2 = [case G of {Nm} -> {Nm,<<"cluster">>}; _ -> G end || G <- Grp1],
	Nodes = lists:flatten([simple_values(I#insert.values,[]) || I <- Cmds, I#insert.table == <<"nodes">>]),
	Usrs = [I || I <- Cmds, element(1,I) == management],

	Nodes1 = [butil:tolist(Nd) || {Nd,_} <- Nodes],

	Me = bkdcore_changecheck:read_node(butil:tolist(node())),
	case lists:member(Me,[bkdcore_changecheck:read_node(Nd) || Nd <- Nodes1]) of
		false ->
			throw(local_node_missing);
		true ->
			ok
	end,

	Grp3 = [
		{butil:toatom(GName),
		 [element(1,bkdcore_changecheck:read_node(butil:tolist(Nd))) || {Nd,Name} <- Nodes, Name == GName],
		 butil:toatom(Type),[]} 
	|| {GName,Type} <- Grp2],

	check_el(Grp3,missing_group_insert),
	check_el(Nodes1,missing_nodes_insert),
	check_el(Usrs,missing_root_user),

	case actordb_sharedstate:init_state(Nodes1,Grp3,[]) of
		ok ->
			ok;
		E ->
			E
	end.


simple_values([[{value,_,_}|_] = H|T],L) ->
	simple_values(T,[list_to_tuple([V || {value,_,V} <- H])|L]);
simple_values([],L) ->
	L.


check_el([],E) ->
	throw({error,E});
check_el(_,_) ->
	ok.

cmd(P,<<";",Rem/binary>>) ->
	cmd(P,Rem);
cmd(P,<<>>) ->
	P;
cmd(P,Bin) when is_binary(Bin) ->
	cmd(P,Bin,actordb_sql:parse(Bin)).
cmd(P,Bin,Tuple) ->
	case Tuple of
		{fail,_} ->
			{error,"bad_query"};
		% #show{} = R ->
		% 	cmd_show(P,R);
		create_table ->
			cmd_create(P,Bin);
		#management{} ->
			[Tuple|P];
		#select{} = R ->
			cmd_select(P,R,Bin);
		#insert{} = R ->
			cmd_insert(P,R,Bin);
		#update{} = R ->
			cmd_update(P,R,Bin);
		#delete{} = R ->
			cmd_delete(P,R,Bin);
		_ when is_tuple(Tuple), is_tuple(element(1,Tuple)), is_binary(element(2,Tuple)) ->
			cmd(cmd(P,Bin,element(1,Tuple)), element(2,Tuple));
		_ ->
			{error,"bad_query"}
	end.

cmd_create(_P,_Bin) ->
	% Only in change schema...
	ok.

cmd_select(P,R,_Bin) ->
	[R|P].

cmd_insert(P,#insert{table = #table{name = Table}, values = V},_Bin) ->
	[#insert{table = Table, values = V}|P].

cmd_update(P,R,_Bin) ->
	[R|P].

cmd_delete(P,R,_Bin) ->
	[R|P].

