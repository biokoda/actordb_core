% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_config).
-include_lib("actordb_core/include/actordb.hrl").
-export([exec/1, exec/2, exec_schema/1, exec_schema/2]).
-export([lte/2, gte/2,lt/2,gt/2,eq/2,neq/2]).
-export([cmd/2]).
-export([test/0]).
% Replacement for actordb_cmd
% Query/change actordb config.


% Initialize:
% actordb_config:exec("insert into groups values ('grp1','cluster');insert into nodes values ('testnd','grp1');CREATE USER 'monty' IDENTIFIED BY 'some_pass';").
% actordb_config:exec("insert into groups values ('grp1','cluster');insert into nodes values (localnode(),'grp1');CREATE USER 'monty' IDENTIFIED BY 'some_pass';").

% Schema:
% actordb_config:exec_schema("actor type1; CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT); ALTER TABLE tab ADD i INTEGER; actor counters kv; CREATE TABLE actors (id TEXT UNIQUE, hash INTEGER, val INTEGER);").

exec_schema(Sql) ->
	exec_schema([{canempty,true}],Sql).

exec_schema(BP,Sql) ->
	auth(BP,false),
	case actordb_schema:types() of
		Types when is_list(Types) ->
			ok;
		Types ->
			throw({error,db_not_initialized})
	end,

	% actordb_sqlparse cares about actor statements and leaves sql statements intact.
	case actordb_sqlparse:parse_statements(BP,butil:tobin(Sql)) of
		{[{{_Type,_,_},_,_Statements}|_] = L1,_} ->
			L = [case Sub of
			[<<"kv">>] ->
				{butil:tolist(Type),[{"type","kv"},{"schema",tol(Statements)}]};
			[_|_] ->
				{butil:tolist(Type),tol(Statements)}
			end || {{Type,Sub,_},_,Statements} <- L1],
			
			Existing = actordb_sharedstate:read_global('schema.yaml'),
			Merged = merge_schema(L,Existing),
			Parsed = actordb_util:parse_cfg_schema(Merged),

			case catch actordb_cmd:compare_schema(Types,Parsed) of
				{ok,_} ->
					case actordb_sharedstate:write_global([{'schema.yaml',Merged}]) of
						ok ->
							{ok,{changes,1,1}};
						Err ->
							Err
					end;
				{error,E} ->
					throw({error,E});
				E ->
					?AERR("Schema error: ~p",[E]),
					throw({error,schema_unparsable})
			end;
		[[{columns,_},_]|_] = St ->
			{ok,St}
	end.

merge_schema([{Type,Schema}|T],Ex) ->
	case lists:keyfind(Type,1,Ex) of
		false ->
			merge_schema(T,[{Type,Schema}|Ex]);
		{Type,Old} -> %[{_,_}|_] = 
			% ExSch = butil:ds_val("schema",Old),
			case Old of
				[{_,_}|_] ->
					SubType = butil:ds_val("type",Old);
				_ ->
					SubType = "actor"
			end,
			ExSch = get_schema(Old),
			NewSch = get_schema(Schema),
			Merged = {Type,[{"type",SubType},{"schema",ExSch++NewSch}]},
			merge_schema(T,lists:keystore(Type,1,Ex,Merged))
	end;
merge_schema([],Ex) ->
	Ex.

get_schema(Schema) ->
	case Schema of
		[{_,_}|_] ->
			butil:ds_val("schema",Schema);
		NewSch ->
			NewSch
	end.

tol(Statements) ->
	[butil:tolist(S) || S <- Statements].

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
	auth(BP,Init),
	exec1(Init,cmd([],butil:tobin(Sql))).

auth(BP,Init) ->
	case BP of
		undefined ->
			ok;
		[_|_] ->
			ok;
		_ when Init ->
			case actordb_backpressure:has_authentication(BP,{config},write) of
				true ->
					ok;
				false ->
					throw({error,no_permission})
			end;
		_ ->
			ok
	end.

exec1(true,Cmds) ->
	Reads  = [S || S <- Cmds, element(1,S) == select],
	Writes = [S || S <- Cmds, element(1,S) /= select],
	Show = [show || show <- Cmds],
	case Writes of
		[] when Show /= [] ->
			G = "CREATE TABLE groups (name TEXT, type TEXT DEFAULT 'cluster');",
			N = "CREATE TABLE nodes (name TEXT, group_name TEXT);",
			{ok,[{columns,{<<"sql">>}},{rows,[{G},{N}]}]};
		[] ->
			do_reads(Reads);
		_ ->
			Out = interpret_writes(Writes),
			case actordb_sharedstate:write_global(Out) of
				ok ->
					{ok,{changes,1,1}};
				Err ->
					Err
			end
	end;
exec1(false,Cmds) ->
	% To initialize we need:
	% Insert to group
	% Insert to nodes
	% Create user, this user will have all privileges
	Usrs1 = [I || I <- Cmds, element(1,I) == management],
	Usrs2 = [{Username,Password,Host} || #management{action = create, data = #account{access =
		[#value{name = <<"password">>, value = Password},
		#value{name = <<"username">>, value = Username},
		#value{name = <<"host">>, value = Host}]}} <- Usrs1],

	[_,Users,Auths] = lists:foldl(fun({U,P,H},[Seq,GUsrs,GAuth]) ->
		Sha = butil:sha256(<<U/binary,";",P/binary>>),
		% Set {config} so that it can not be created from outside.
		% {config} user is only created here
		Auth1 = {{config},Seq,Sha,[read,write]},
		Auth2 = {'*',Seq,Sha,[read,write]},
		Usr = {Seq,U,H,Sha},
		[Seq+1,[Usr|GUsrs],[Auth1,Auth2|GAuth]]
	end, [1,[],[]], Usrs2),

	{Nodes1,Grp3} = insert_to_grpnd(Cmds),
	
	check_el(Grp3,missing_group_insert),
	check_el(Nodes1,missing_nodes_insert),
	check_el(Usrs2,missing_root_user),

	Me = element(1,bkdcore_changecheck:read_node(butil:tolist(node()))),
	case lists:member(Me,[element(1,bkdcore_changecheck:read_node(Nd)) || Nd <- Nodes1]) of
		false ->
			throw({error,local_node_missing});
		true ->
			ok
	end,

	case actordb_sharedstate:init_state(Nodes1,Grp3,[{auth,Auths},{users,Users}],[{'schema.yaml',[]}]) of
		ok ->
			case get(adbt) of
				true ->
					timer:sleep(1000),
					% After initialization we need a session for further requests.
					{Username,Password,_} = hd(Usrs2),
					put(bp,actordb_backpressure:start_caller(Username, Password));
				_ ->
					ok
			end,
			{ok,{changes,1,1}};
		E ->
			E
	end.

% If we get more than one read, we will only process the first. 
% Getting more than one is a bug.
% Nodes/groups always return entire node/group list for now.
do_reads([S|_]) ->
	[#table{name = Table}] = S#select.tables,
	Nodes = actordb_sharedstate:read_global(nodes),
	Groups = actordb_sharedstate:read_global(groups),
	case Table of
		<<"nodes">> ->
			% For every node, get group list, create a list of [{NodeName,GroupName}]
			NL = lists:flatten([[{butil:tobin(Nd),butil:tobin(Grp)} || 
				Grp <- bkdcore:node_membership(element(1,bkdcore_changecheck:read_node(Nd)))] 
				|| Nd <- Nodes]),
			{ok,[{columns,{<<"name">>,<<"group_name">>}},{rows,NL}]};
		<<"groups">> ->
			NG = [{butil:tobin(Nm),butil:tobin(Typ)} || {Nm,_Nds,Typ,_Opt} <- Groups],
			{ok,[{columns,{<<"name">>,<<"type">>}},{rows,NG}]};
		<<"users">> ->
			ML = mngmnt_execute0({users,actordb_sharedstate:read_global_users()}, 
				{auth,actordb_sharedstate:read_global_auth()},
				S),
			KL = [<<"id">>,<<"username">>,<<"host">>],
			{ok,[{columns,list_to_tuple(KL)},{rows,map_rows(butil:maplistsort(<<"id">>,ML),KL)}]}
	end.

map_rows([Map|MT],KL) ->
	[list_to_tuple([maps:get(K,Map) || K <- KL, K /= <<"sha">>])|map_rows(MT,KL)];
map_rows([],_) ->
	[].

insert_to_grpnd(Cmds) ->
	Grp1 = lists:flatten([simple_values(I#insert.values,[]) || I <- Cmds, I#insert.table == <<"groups">>]),
	Grp2 = [case G of {Nm} -> {Nm,<<"cluster">>}; _ -> G end || G <- Grp1],
	Nodes = lists:flatten([simple_values(I#insert.values,[]) || I <- Cmds, I#insert.table == <<"nodes">>]),
	Nodes1 = [node_name(Nd) || {Nd,_} <- Nodes],

	Grp3 = [
		{butil:toatom(GName),
		 [element(1,bkdcore_changecheck:read_node(node_name(Nd))) || {Nd,Name} <- Nodes, Name == GName],
		 butil:toatom(Type),[]} 
	|| {GName,Type} <- Grp2],

	{Nodes1,Grp3}.

interpret_writes(Cmds) ->
	% 1.Take existing
	% 2.Check inserts don't overwrite
	% 3.Combine inserts and existing
	% 4.Process updates/deletes
	ExistingNodes = actordb_sharedstate:read_global(nodes),
	ExistingGroups = actordb_sharedstate:read_global(groups),
	interpret_writes(Cmds,ExistingNodes,ExistingGroups).
interpret_writes(Cmds,ExistingNodes,ExistingGroups) ->
	Users = update_users({users,actordb_sharedstate:read_global_users()}, 
		{auth,actordb_sharedstate:read_global_auth()},
		[I || I <- Cmds, element(1,I) == management]),
	% {InsertNodes,InsertGroups} = insert_to_grpnd(Cmds),
	NewNodes = lists:flatten([simple_values(I#insert.values,[]) || I <- Cmds, I#insert.table == <<"nodes">>]),
	NewGroups = lists:flatten([simple_values(I#insert.values,[]) || I <- Cmds, I#insert.table == <<"groups">>]),
	InsertGroups = [{butil:toatom(GName),[],butil:toatom(GType),[]} || {GName,GType} <- NewGroups],
	InsertNodes = [node_name(Nd) || {Nd,_} <- NewNodes],
	NodeUpdates = [U || U <- Cmds, U#update.table == <<"nodes">>],
	case InsertNodes -- ExistingNodes of
		InsertNodes ->
			ok;
		_ ->
			throw({error,"insert_on_existing_node"})
	end,
	case InsertGroups -- ExistingGroups of
		InsertGroups ->
			ok;
		_ ->
			throw({error,"insert_on_existing_group"})
	end,
	Nodes = InsertNodes++ExistingNodes,
	% New groups have no nodes and new nodes are not a part of any groups yet.
	Groups = add_nodes_if_missing(NewNodes,InsertGroups++ExistingGroups),
	% Now check for updates
	case catch node_update(Nodes,Groups,NodeUpdates) of
		{'EXIT',_} ->
			GroupsFinal = NodeFinal = [],
			throw({error,unsupported_update});
		{NodeFinal,GroupsFinal} ->
			ok
	end,
	interpret_writes1(lists:flatten([{nodes,NodeFinal},{groups,GroupsFinal},Users]),[]).
interpret_writes1([{_,[]}|T],L) ->
	interpret_writes1(T,L);
interpret_writes1([{K,V}|T],L) ->
	interpret_writes1(T,[{K,V}|L]);
interpret_writes1([],L) ->
	L.

update_users(Users,Auth,[H|T]) ->
	?AINF("UPDATE ~p",[H]),
	{NU,NA} = mngmnt_execute0(Users,Auth,H),
	update_users(NU,NA,T);
update_users(U,A,[]) ->
	[U,A].

% New nodes, list of all groups (including just added ones)
add_nodes_if_missing([{Nd1,Grp1}|T],Grps) ->
	Grp = butil:toatom(Grp1),
	Nd = element(1,bkdcore_changecheck:read_node(node_name(Nd1))),
	case lists:keyfind(Grp,1,Grps) of
		false ->
			throw({error,node_to_unknown_group});
		{Grp,Nodes,Type,Opt} ->
			case lists:member(Nd,Nodes) of
				true ->
					add_nodes_if_missing(T,Grps);
				false ->
					NG = {Grp,[Nd|Nodes],Type,Opt},
					NGL = lists:keystore(Grp,1,Grps,NG),
					add_nodes_if_missing(T,NGL)
			end
	end;
add_nodes_if_missing([],G) ->
	G.

node_update(Nodes,Groups,[U|T]) ->
	#condition{nexo = Op, op1 = FromKey, op2 = FromVal} = U#update.conditions,
	case U#update.set of
		[{set,<<"name">>,To}] when FromKey#key.name == <<"name">> ->
			ok;
		_ ->
			To = undefined,	
			throw({error,only_name_updatable})
	end,
	From = FromVal#value.value,
	case Op of
		eq ->
			Node = butil:tolist(From),
			case lists:member(Node,Nodes) of
				true ->
					ok;
				false ->
					throw({error,update_nomatch})
			end;
		like ->
			case like_match_list(From,Nodes) of
				[] = Node ->
					throw({error,update_nomatch});
				[Node] ->
					ok;
				[_,_|_] = Node ->
					throw({error,update_match_multiple})
			end
	end,
	BNew = element(1,bkdcore_changecheck:read_node(butil:tolist(To))),
	BOld = element(1,bkdcore_changecheck:read_node(butil:tolist(Node))),
	NewGroups = replace_nd_in_grp(BOld,BNew,Groups),
	node_update([butil:tolist(To)|Nodes--[Node]],NewGroups,T);
node_update(Nodes,Groups,[]) ->
	{Nodes,Groups}.

replace_nd_in_grp(Old,New,[{GrpNm,Nodes,GrpTyp,GrpParam}|T]) ->
	case lists:member(Old,Nodes) of
		true ->
			[{GrpNm,[New|Nodes--[Old]],GrpTyp,GrpParam}|replace_nd_in_grp(Old,New,T)];
		false ->
			[{GrpNm,Nodes,GrpTyp,GrpParam}|replace_nd_in_grp(Old,New,T)]
	end;
replace_nd_in_grp(_,_,[]) ->
	[].

rematch(match) ->
	true;
rematch({match,_}) ->
	true;
rematch(_) ->
	false.

like_match_list(Pattern,Nodes) ->
	Regex = like_to_regex(Pattern),
	{ok,R} = re:compile(Regex),
	[Nd || Nd <- Nodes, rematch(re:run(Nd,R))].

like_to_regex(Bin) ->
	case binary:split(Bin,<<"%">>,[global]) of
		[<<>>,Str] when byte_size(Str) > 0 ->
			["^.*?",Str,"$"];
		[<<>>,Str,<<>>] when byte_size(Str) > 0 ->
			["^.*?",Str,".*?$"];
		[Str,<<>>] when byte_size(Str) > 0 ->
			["^",Str,".*?$"];
		[Bin] ->
			["^",Bin,"$"]
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
	lists:reverse(P);
cmd(P,Bin) when is_binary(Bin) ->
	cmd(P,Bin,actordb_sql:parse(Bin)).
cmd(P,Bin,Tuple) ->
	case Tuple of
		{fail,_} ->
			{error,bad_query};
		show ->
			[show|P];
		% #show{} = R ->
		% 	cmd_show(P,R);
		% create_table ->
		% 	cmd_create(P,Bin);
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
			RemBin = element(2,Tuple),
			ThisSize = byte_size(Bin) - byte_size(RemBin),
			NextSize = byte_size(RemBin),
			<<This:ThisSize/binary,Next:NextSize/binary>> = Bin,
			cmd(cmd(P,This,element(1,Tuple)), Next);
		_ ->
			{error,bad_query}
	end.

% cmd_create(_P,_Bin) ->
% 	% Only in change schema...
% 	ok.

cmd_select(P,R,_Bin) ->
	[R|P].

cmd_insert(P,#insert{table = #table{name = Table}, values = V},_Bin) ->
	[#insert{table = Table, values = V}|P].

cmd_update(P,#update{table = #table{name = Table}, set = Setlist} = R,_Bin) ->
	Set1 = [S#set{value = just_value(S#set.value)} || S <- Setlist],
	[R#update{table = Table, set = Set1}|P].

cmd_delete(P,R,_Bin) ->
	[R|P].



mngmnt_execute0(_,_,{fail,{expected,_,_}})->
	check_sql;
mngmnt_execute0({users,Users},A,#management{action = create, data = #account{access =
	[#value{name = <<"password">>, value = Password},
	#value{name = <<"username">>, value = Username},
	#value{name = <<"host">>, value = Host}]}}) ->
		Index = increment_index([Index||{Index,_,_,_} <- Users]),
		case [ok || {_,U,_Host,_} <- Users, U == Username] of
			[_|_] ->
				throw({error,user_exists});
			_ ->
				write_user({users,Users},A,Index,Username,Host,Password)
		end;

%should grant append?
mngmnt_execute0({users,Users},{auth,Auth},#management{action = grant, data = #permission{
	on = On,
	conditions = Conditions,
	account = [#value{name = <<"username">>,value = Username},
		#value{name = <<"host">>,value = _Host}]}})->
	
	case On of
		#table{name = ActorType,alias = ActorType} ->
			ok;
		{all,_} ->
			ActorType = '*'
	end,
	case {lists:keyfind(value,1,Conditions), 
		Conditions -- [read,write], 
		ActorType == '*' orelse lists:member(butil:toatom(ActorType),actordb:types())} of
		{false,[],true} ->
			% case actordb_sharedstate:read_global_users(Username,Host) of
			case [X || {_,U,_,_} = X <- Users, U == Username] of
				[{UserIndex,_,_,Sha}] ->
					merge_replace_or_insert({users,Users},{auth,Auth},ActorType,UserIndex,Sha,Conditions);
				_ ->
					throw({error,user_not_found})
			end;
		{_,_,false} ->
			throw({error,invalid_actor_type});
		_ ->
			throw({error,query_not_supported})
	end;
mngmnt_execute0(_,_,#management{action = grant, data = _})->
	throw({error,not_supported});

mngmnt_execute0({users,AllUsers},{auth,Authentication},#management{action = drop, 
	data = #account{access =[#value{name = <<"username">>,value = Username},
	#value{name = <<"host">>,value = _Host}]}}) ->
	% User = actordb_sharedstate:read_global_users(Username, Host),
	User = [X || {_,U,_,_} = X <- AllUsers, U == Username],
	case User of
		[]-> throw({error,user_not_found});
		[{UserIndex,_,_,_}] ->
			RemUser = AllUsers -- User,
			% Authentication = actordb_sharedstate:read_global_auth(),
			UserAuthentication = [UI || {_,UI,_,_} <- Authentication, UI == UserIndex],
			% UserAuthentication = actordb_sharedstate:read_global_auth(UserIndex),
			{{users,RemUser},{auth,Authentication -- UserAuthentication}}
	end;
mngmnt_execute0(_,_,#management{action = drop, data = _}) ->
	throw({error,not_supported});
mngmnt_execute0({users,AllUsers},{auth,Auth},#management{action = rename, 
	data = [#account{access = [#value{name = <<"username">>,value = Username},
	#value{name = <<"host">>,value = Host}]},
	#value{name = <<"username">>,value = ToUsername},
	#value{name = <<"host">>,value = ToHost}]}) ->
	% User = actordb_sharedstate:read_global_users(Username, Host),
	User = [X || {_,U,_,_} = X <- AllUsers, U == Username],
	% AllUsers = actordb_sharedstate:read_global_users(),
	% FutureUser = actordb_sharedstate:read_global_users(ToUsername, ToHost),
	FutureUser = [X || {_,U,_,_} = X <- AllUsers, U == ToUsername],
	case FutureUser of
		[]->
			case User of
				[]-> throw({error,user_not_found});
				[{Index,Username,Host,Sha}] ->
					RemUser = AllUsers -- User,
					{{users,[{Index,ToUsername,ToHost,Sha}|RemUser]},{auth,Auth}}
			end;
		_ -> throw({error,user_exists})
	end;
mngmnt_execute0(_,_,#management{action = rename, data = _ }) ->
	throw({error,not_supported});
mngmnt_execute0({users,Users},{auth,Authentication},#management{action = revoke,
	data = #permission{on = #table{name = ActorType,alias = ActorType},
	account = [#value{name = <<"username">>, value = Username},#value{name = <<"host">>,value = Host}],
	conditions = Conditions}}) ->
	% Authentication = actordb_sharedstate:read_global_auth(),
	% case actordb_sharedstate:read_global_users(Username, Host) of
	case [X || {_,U,_,_} = X <- Users, U == Username] of
		[] -> user_not_found;
		[{UserIndex,Username,Host,Sha}] ->
			[{ActorType,UserIndex,Sha,OldConditions}] = lists:filter(fun(X)-> case X of
				{ActorType,UserIndex,Sha,_} -> true;
				_ -> false end
				end, Authentication),
			NewConditions = OldConditions -- Conditions,
			{{users,Users},{auth,(Authentication -- [{ActorType,UserIndex,Sha,OldConditions}])
			++ [{ActorType,UserIndex,Sha,NewConditions}]}}
	end;
mngmnt_execute0(_,_,#management{action = revoke,data = _})->
	throw({error,not_supported});
mngmnt_execute0({users,Users},{auth,Auth},#management{action = setpasswd,
	data = #account{access = [#value{name = <<"password">>,value = Password},
	#value{name = <<"username">>,value = Username},
	#value{name = <<"host">>,value = Host}]}})->
	% Users = actordb_sharedstate:read_global_users(),
	% case actordb_sharedstate:read_global_users(Username, Host) of
	case [X || {_,U,_,_} = X <- Users, U == Username] of
		[] -> throw({error,user_not_found});
		[{UserIndex,Username,Host,_Sha}] = User ->
			RemUser = Users -- User,
			{{users,[{UserIndex,Username,Host,butil:sha256(<<Username/binary,";",Password/binary>>)}|RemUser]},
			{auth,Auth}}
	end;

mngmnt_execute0(_,_,#management{action = setpasswd, data = _})->
	throw({error,not_supported});
mngmnt_execute0({users,Users},{auth,_Auth},#select{params = Params, tables = [#table{name = <<"users">>,alias = <<"users">>}],
		conditions = Conditions, group = undefined,order = Order, limit = Limit,offset = Offset})->
	% Users = actordb_sharedstate:read_global_users(),%id,username,host,sha
	NumberOfUsers = length(Users),
	Con = fun(UsersLO)->
		case Conditions of
			undefined -> UsersLO;
			_ -> conditions(UsersLO,Conditions)
		end
	end,
	FilterdUsers =
	case {Limit, Offset} of
		{undefined, undefined} -> Con(Users);
		{Limit, undefined} -> Con(lists:sublist(Users, 1, Limit));
		{undefined, Offset} -> Con(lists:sublist(Users, case Offset of 0 -> 1; _ -> Offset end, NumberOfUsers));
		{Limit, Offset} -> Con(lists:sublist(Users, case Offset of 0 -> 1; _ -> Offset end, Limit))
	end,
	Ordered = case Order of
		undefined ->
			[#{<<"id">> => Id, <<"username">> => Username, <<"host">> => Host, <<"sha">> => Sha}|| 
				{Id,Username,Host,Sha} <- FilterdUsers];
		_ ->
			MapUsers = [#{<<"id">> => Id, <<"username">> => Username, <<"host">> => Host, <<"sha">> => Sha}|| 
				{Id,Username,Host,Sha} <- FilterdUsers],
			lists:sort(fun(U1,U2)->
				sorting_fun(tuple_g(U1,Order), tuple_g(U2,Order), Order)
			end, MapUsers)
	end,
	filter_by_keys_param(Params,Ordered);

mngmnt_execute0(_,_,#select{params = _, tables = _, conditions = _,group = _,order = _, limit = _,offset = _})->
	throw({error,not_supported}).

filter_by_keys_param(Params,Users)->
	case Params of
		[#all{table = _}] -> Users;
		_ ->
			[lists:foldl(fun(#key{alias = _,name = Name,table = _}, MapOut) ->
					maps:put(Name,maps:get(Name,UO),MapOut)
				end, #{}, Params)
			||UO <- Users]
	end.

tuple_g(User,Orders)->
	list_to_tuple([maps:get(Order#order.key, User)||Order <- Orders]).

%this probably needs an explanation
%since erlang sort function can compare tuples
%and we can order lists by ASC and DESC
%what we do is, in case we are ordering by id DESC, username ASC
%we switch ids between two comparing tuples
sorting_fun(X, Y, Orders)->
	{XX,YY} = lists:foldl(fun(#order{key = Name,sort = Sort},{X0, Y0}) ->
		case Sort of
			asc -> {X0, Y0};
			desc ->
				Index = user_element(Name),
				Xelement = element(Index, X0),
				Yelement = element(Index, Y0),
				XX = setelement(Index,X0,Yelement),
				YY = setelement(Index,Y0,Xelement),
				{XX,YY}
			end
		end, {X, Y}, Orders),
	XX < YY.

increment_index(Indexes)->
	case lists:sort(Indexes) of
		[] -> 1;
		IndexesNum -> lists:last(lists:sort(IndexesNum)) + 1
	end.

write_user({users,U},A,Index,Username,Host,Password) ->
	case U of
		[] ->
			{{users,[{Index,Username,Host,butil:sha256(<<Username/binary,";",Password/binary>>)}]},A};
		OtherUsers ->
			{{users,[{Index,Username,Host,butil:sha256(<<Username/binary,";",Password/binary>>)}|OtherUsers]},A}
	end.

merge_replace_or_insert(U,{auth,A},ActorType,UserIndex,Sha,Conditions)->
	case lists:filter(fun(X)-> case X of {ActorType,UserIndex,Sha,_} -> true; _ -> false end end, A) of
	[]-> 
		{U,{auth,[{ActorType,UserIndex,Sha,Conditions}|A]}};
	Remove ->
		{U,{auth,(A -- Remove) ++ [{ActorType,UserIndex,Sha,Conditions}]}}
	end.

%NexoCondition is between op1 and op2Tail
%NexoCondition is either AND or OR
%Users 1 ID, 2 username, 3 Host, 4 SHA
conditions(Users,Condition)->
	conditions(Users,Condition,[]).

conditions(Users,#condition{nexo = nexo_and,
	op1 = #condition{nexo = _, op1 = _, op2 = _} = Op,
	op2 = Tail},Part) ->
	conditions(Users,Tail,[Op|Part]);
conditions(Users,#condition{nexo = nexo_or,
	op1 = #condition{nexo = _, op1 = _, op2 = _} = Op,op2 = Tail}, Part) ->
	Conditions = [Op|Part],
	FilterdUsers = lists:filter(fun(User)->
		condition(Conditions,User)
	end, Users),
	conditions(FilterdUsers, Tail, []);
conditions(Users,#condition{nexo = _, op1 = _, op2 = _} = Op,Part) ->
	Conditions = [Op|Part],
	lists:filter(fun(User)->
		condition(Conditions,User)
	end, Users).

lte(A,B)->
	A =< B.
gte(A,B)->
	A >= B.
lt(A,B)->
	A < B.
gt(A,B)->
	A > B.
eq(A,B)->
	A =:= B.
neq(A,B)->
	A =/= B.

user_element(<<"id">>)->
	1;
user_element(<<"username">>)->
	2;
user_element(<<"host">>)->
	3;
user_element(<<"sha">>)->
	4.

condition(Conditions,User)->
	condition(Conditions,User,true).
condition([C|T],User,true) ->
	UserValue = element(user_element(C#condition.op1#key.name),User),
	ComparingTo = C#condition.op2#value.value,
	Result = apply(?MODULE,C#condition.nexo,[UserValue,ComparingTo]),
	condition(T,User,Result);
condition(_, _, false) ->
	false;
condition([],_,true) ->
	true.



test() ->
	Nodes = ["alfa","beta","omega"],
	["omega"] = like_match_list(<<"%ga">>,Nodes),
	["beta"] = like_match_list(<<"%et%">>,Nodes),
	["alfa"] = like_match_list(<<"a%">>,Nodes),

	From = bkdcore:node_name(),
	Tob = butil:tobin([From,"_test_update"]),
	To = butil:tolist(Tob),
	ExistingNodes = actordb_sharedstate:read_global(nodes),
	[{GrpName,[From],cluster,[]}] = ExistingGroups = actordb_sharedstate:read_global(groups),
	
	UpdSql = ["update nodes set name='",To,"' where name like '",binary:first(From),"%';"],
	Cmd = cmd([],butil:tobin([UpdSql])),
	{NewNodesRaw,[{GrpName,NewNodesB,cluster,[]}]} = node_update(ExistingNodes,ExistingGroups,Cmd),
	[] = NewNodesRaw -- [To],
	[] = NewNodesB -- [Tob],

	Newb = <<"newnode@127.0.0.1:43801">>,
	New = butil:tolist(Newb),
	NewSql = ["insert into nodes values ('",New,"','",butil:tobin(GrpName),"');"],
	Cmd1 = cmd([],butil:tobin([NewSql,UpdSql])),
	[{groups,[OutG]},{nodes,OutN}] = interpret_writes(Cmd1),
	GrpNodes = element(2,OutG),
	[] = GrpNodes -- [element(1,bkdcore_changecheck:read_node(New)),Tob],
	[] = OutN -- [New,To],
	ok.

	% ok.






