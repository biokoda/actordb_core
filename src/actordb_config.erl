% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_config).
-include_lib("actordb_core/include/actordb.hrl").
-export([exec/1, exec/2]).

% Replacement for actordb_cmd
% Query/change actordb config.


% Initialize:
% actordb_config:exec("insert into groups values ('grp1','cluster');insert into nodes values ('testnd','grp1');CREATE USER 'monty' IDENTIFIED BY 'some_pass';").
% actordb_config:exec("insert into groups values ('grp1','cluster');insert into nodes values (localnode(),'grp1');CREATE USER 'monty' IDENTIFIED BY 'some_pass';").

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
			case actordb_backpressure:has_authentication(BP,{config},[write]) of
				true ->
					ok;
				false ->
					throw({error,no_permission})
			end;
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
	% Create user, this user will have all privileges
	Grp1 = lists:flatten([simple_values(I#insert.values,[]) || I <- Cmds, I#insert.table == <<"groups">>]),
	Grp2 = [case G of {Nm} -> {Nm,<<"cluster">>}; _ -> G end || G <- Grp1],
	Nodes = lists:flatten([simple_values(I#insert.values,[]) || I <- Cmds, I#insert.table == <<"nodes">>]),
	Usrs1 = [I || I <- Cmds, element(1,I) == management],

	Nodes1 = [node_name(Nd) || {Nd,_} <- Nodes],

	Me = bkdcore_changecheck:read_node(butil:tolist(node())),
	case lists:member(Me,[bkdcore_changecheck:read_node(Nd) || Nd <- Nodes1]) of
		false ->
			throw(local_node_missing);
		true ->
			ok
	end,

	Grp3 = [
		{butil:toatom(GName),
		 [element(1,bkdcore_changecheck:read_node(node_name(Nd))) || {Nd,Name} <- Nodes, Name == GName],
		 butil:toatom(Type),[]} 
	|| {GName,Type} <- Grp2],

	Usrs2 = [{Username,Password,Host} || #management{action = create, data = #account{access =
	[#value{name = <<"password">>, value = Password},
	#value{name = <<"username">>, value = Username},
	#value{name = <<"host">>, value = Host}]}} <- Usrs1],

	check_el(Grp3,missing_group_insert),
	check_el(Nodes1,missing_nodes_insert),
	check_el(Usrs2,missing_root_user),

	[_,Users,Auths] = lists:foldl(fun({U,P,H},[Seq,GUsrs,GAuth]) ->
		Sha = butil:sha256(<<U/binary,";",P/binary>>),
		% Set {config} so that it can not be created from outside.
		% {config} user is only created here
		Auth1 = {{config},Seq,Sha,[read,write]},
		Auth2 = {'*',Seq,Sha,[read,write]},
		Usr = {Seq,U,H,Sha},
		[Seq+1,[Usr|GUsrs],[Auth1,Auth2|GAuth]]
	end, [1,[],[]], Usrs2),

	case actordb_sharedstate:init_state(Nodes1,Grp3,[{auth,Auths},{users,Users}],[{'schema.yaml',[]}]) of
		ok ->
			ok;
		E ->
			E
	end.


simple_values([[VX|_] = H|T],L) when element(1,VX) == value; element(1,VX) == function ->
	simple_values(T,[list_to_tuple([just_value(V) || V <- H])|L]);
simple_values([],L) ->
	L.

% We can insert with localnode() function. 
node_name({<<"localnode">>,[]}) ->
	butil:tolist(node());
node_name(V) ->
	butil:tolist(V).

just_value({value,_,V}) ->
	V;
just_value({function,Nm,Params,_}) ->
	{Nm,Params}.

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

