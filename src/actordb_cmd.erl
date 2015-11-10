% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_cmd).
-compile(export_all).
-define(DELIMITER,"~n-------------------------~n~p~n-------------------------~n").
-include_lib("actordb_core/include/actordb.hrl").
-define(ERR(F,P),lists:flatten(io_lib:fwrite(F,P))).

getschema(Etc) ->
	case application:get_env(actordb,schema) of
		undefined ->
			case yamerl_constr:file(Etc++"/schema.yaml") of
				[Schema] ->
					Schema;
				[] ->
					[]
			end;
		{ok,Mod} ->
			apply(Mod,schema,[])
	end.

cmd(init,parse,Etc) ->
	try case readnodes(Etc++"/nodes.yaml") of
		{Nodes,Groups} ->
			Schema = getschema(Etc),
			NewCfg = parse_schema(Schema),
			case catch compare_schema([],NewCfg) of
				{ok,L} ->
					ok;
				{error,Err} ->
					{error,Err};
				Err ->
					{error,?ERR("~p",[Err])}
			end
	end of
		ok ->
			{ok,"Start new cluster?"};
		{error,E} ->
			{error,?ERR("~p",[E])}
	catch
		throw:Str when is_list(Str) ->
			{error,?ERR("~s~n",[Str])};
		throw:S ->
			{error,?ERR("~p~n",[S])};
		_:{badmatch,{error,enoent}} ->
			{error,?ERR("File(s) missing: ~n~p~n~p~n~p~n",[Etc++"/nodes.yaml",Etc++"/groups.yaml",Etc++"/schema.yaml"])};
		_:Err1 ->
			{error,?ERR("Error parsing nodes.yaml or schema.yaml ~p~n",[Err1])}
	end;
cmd(init,commit,Etc) ->
	try {Nodes,Groups1} = readnodes(Etc++"/nodes.yaml"),
		Groups = bkdcore_changecheck:parse_yaml_groups(Groups1),
		Schema = getschema(Etc),
		init_state(Nodes,Groups,Schema)
	catch
		_:{badmatch,{error,enoent}} ->
			?ERR("File(s) missing: ~n~p~n~p~n",[Etc++"/nodes.yaml",Etc++"/schema.yaml"]);
		_:Err ->
			?ERR("Parsing configs ~p~n",[Err])
	end;
cmd(updatenodes,parse,Etc) ->
	try case readnodes(Etc++"/nodes.yaml") of
		{Nodes,Groups1} ->
			Groups = bkdcore_changecheck:parse_yaml_groups(Groups1),
			compare_groups(nodes_to_names(Nodes),Groups,compare_nodes(Nodes,[]));
		X ->
			throw(?ERR("Error parsing nodes.yaml ~p",[X]))
	end of
		[_|_] = Out ->
			{ok,Out};
		[] ->
			{ok,nochange}
	catch
		throw:Str ->
			{error,?ERR("~s~n",[Str])};
		_:{badmatch,{error,enoent}} ->
			{error,?ERR("File(s) missing: ~n~p~n~p~n",[Etc++"/nodes.yaml",Etc++"/groups.yaml"])};
		_:Err ->
			{error,?ERR("~p~n",[Err])}
	end;
cmd(updatenodes,commit,Etc) ->
	try {Nodes,Groups1} = readnodes(Etc++"/nodes.yaml"),
		Groups = bkdcore_changecheck:parse_yaml_groups(Groups1),
		[_|_] = compare_groups(nodes_to_names(Nodes),Groups,compare_nodes(Nodes,[])),
		actordb_sharedstate:write_global([{nodes,Nodes},{groups,Groups}]) of
		ok ->
			"done";
		Err ->
			?ERR("~p~n",[Err])
	catch
		_:{badmatch,{error,enoent}} ->
			?ERR("File(s) missing: ~n~p~n~p~n",[Etc++"/nodes.yaml",Etc++"/groups.yaml"]);
		_:Err ->
			?ERR("Error ~p~n",[Err])
	end;
cmd(updateschema,parse,Etc) ->
	case catch actordb_schema:types() of
		Types when is_list(Types) ->
			try getschema(Etc) of
				Schema ->
					NewCfg = parse_schema(Schema),
					case catch compare_schema(Types,NewCfg) of
						{ok,L} ->
							{ok,L};
						{error,Err} ->
							{error,Err};
						Err ->
							{error,?ERR("~p",[Err])}
					end
			catch
				_:{badmatch,{error,enoent}} ->
					{error,?ERR("File missing ~p~n",[Etc++"/schema.yaml"])};
				_:Err ->
					{error,?ERR("Unable to parse schema:~n~p.",[Err])}
			end;
		_ ->
			{error,?ERR("No existing schema, run init?",[])}
	end;
cmd(updateschema,commit,Etc) ->
	try Schema = getschema(Etc),
		actordb_sharedstate:write_global([{'schema.yaml',Schema}]) of
		ok ->
			"done";
		Err ->
			?ERR("~p~n",[Err])
	catch
		_:{badmatch,{error,enoent}} ->
			?ERR("File missing ~p~n",[Etc++"/schema.yaml"]);
		_:Err ->
			?ERR("~p~n",[Err])
	end;
cmd(dummy,_,Etc) ->
	Etc;
cmd(stats,describe,ok) ->
	{ok,{"allreads", "readsnow", "allwrites", "writesnow", "nactors", "nactive","tmngrs"}};
cmd(stats,stats,{Node,Pid,Ref}) ->
	spawn(fun() -> actordb_local:subscribe_stat(),
					send_stats(Node,Pid,Ref) end),
	ok;
cmd(_,_,_) ->
	{error,?ERR("uncrecognized command.~nSupported commands: ~p, ~p, ~p~n",[init,updateschema,updatenodes])}.

%Account Management
cmd(Statement)->
	actordb:exec_mngmnt(Statement).

send_stats(Node,Pid,Ref) ->
	case lists:member(Node,nodes(connected)) of
		true ->
			receive
				{doread,Reads,Writes,PrevReads,PrevWrites,NActive} ->
					Mngrs = actordb_local:get_mupdaters_state(),
					Busy = [ok || {_,true} <- actordb_local:get_mupdaters_state()],
					Pid ! {Ref,{Reads,PrevReads,Writes,PrevWrites,actordb_local:get_nactors(),NActive,butil:tolist(length(Busy))++"/"++butil:tolist(length(Mngrs))}},
					send_stats(Node,Pid,Ref)
				after 5000 ->
					ok
			end;
		false ->
			ok
	end.

readnodes(Pth) ->
	{ok,_} = file:read_file_info(Pth),
	case catch yamerl_constr:file(Pth) of
		[[A1,A2]] ->
			case A1 of
				{"nodes",Nodes} ->
					{"groups",Groups} = A2,
					{Nodes,Groups};
				{"groups",Groups} ->
					{"nodes",Nodes} = A2,
					{Nodes,Groups};
				_ ->
					throw("Invalid nodes.yaml. First object is neither nodes nor groups")
			end;
		Err ->
			throw(?ERR("Invalid ~s:~p~n",[Pth,Err]))
	end.

nodes_to_names(Nodes) ->
	[butil:tolist(element(1,bkdcore_changecheck:read_node(Nd))) || Nd <- Nodes].

compare_groups(Nodes,[GroupInfo|T],Out) ->
	case GroupInfo of
		{Name1,Nodes1} ->
			Type = undefined,
			GP = [];
		{Name1,Nodes1,Type} ->
			GP = [];
		{Name1,Nodes1,Type,GP} ->
			ok
	end,
	GNodes = lists:sort([butil:tobin(N) || N <- Nodes1]),
	Name = butil:toatom(Name1),
	case [GNode || GNode <- Nodes1, lists:member(GNode,Nodes) == false] of
		[] ->
			case bkdcore:nodelist(Name) of
				[] ->
					compare_groups(Nodes,T,?ERR("New group:"++?DELIMITER,[GroupInfo])++Out);
				ExistingNodes ->
					case lists:subtract(ExistingNodes,GNodes) == [] andalso
								GP == bkdcore:group_param(Name) andalso
								Type == bkdcore:group_type(Name) of
						true ->
							compare_groups(Nodes,T,Out);
						false ->
							compare_groups(Nodes,T,?ERR("Changed group:"++?DELIMITER,[GroupInfo])++Out)
					end
			end;
		Unknown ->
			throw(?ERR("Nodes ~p in group ~p not listed in nodes.yaml",[Unknown,Name]))
	end;
compare_groups(_,[],Out) ->
	Out.

compare_nodes([NewInfo|T],Out) ->
	{Name,_AddrReal,_Port,_Pub,_Dist} = All = bkdcore_changecheck:read_node(NewInfo),
	case bkdcore:node_address(Name) of
		undefined ->
			compare_nodes(T,?ERR("New node:"++?DELIMITER,[NewInfo])++Out);
		{IPCur,PortCur} ->
			case All == {Name,IPCur,PortCur,bkdcore:public_address(Name),bkdcore:dist_name(Name)} of
				true ->
					compare_nodes(T,Out);
				false ->
					compare_nodes(T,?ERR("Changed node:"++?DELIMITER,[NewInfo])++Out)
			end
	end;
compare_nodes([],Out) ->
	Out.

parse_schema(Schema) ->
	actordb_util:parse_cfg_schema(Schema).
	% [Tuple || Tuple <- L1, tuple_size(Tuple) == 2].

compare_schema(Types,New) ->
	N1 = [Data || Data <- New, (tuple_size(Data) == 2 orelse element(1,Data) == iskv)],
	compare_schema(Types,N1,[]).
% Move over existing tyes of actors.
% For every type check if it exists in new schema and if any sql statements added.
compare_schema([Type|T],New,Out) when Type == ids; Type == types; Type == iskv; Type == num ->
	compare_schema(T,New,Out);
compare_schema([Type|T],New,Out) ->
	case lists:keyfind(Type,1,New) of
		false ->
			{error,?ERR("Missing type ~p in new config. Config invalid.~n",[Type])};
		{_,_,_} ->
			compare_schema(T,New,Out);
		{_,SqlNew} ->
			case apply(actordb_schema,Type,[]) of
				SqlNew ->
					compare_schema(T,lists:keydelete(Type,1,New),Out);
				SqlCur ->
					SizeCur = tuple_size(SqlCur),
					SizeNew = tuple_size(SqlNew),
					check_sql(Type,SizeCur,SqlNew,actordb_schema:iskv(Type)),
					case ok of
						_ when SizeCur < SizeNew ->
							Lines = [binary_to_list(iolist_to_binary(element(N,SqlNew))) || N <- lists:seq(SizeCur+1,SizeNew)],
							Out1 = Out ++ ?ERR("Update type ~p:"++?DELIMITER,[Type,Lines]);
						_ ->
							Out1 = Out
					end,
					compare_schema(T,lists:keydelete(Type,1,New),Out1)
			end
	end;
compare_schema([],New,O) ->
	NewTypes = lists:keydelete(ids,1,lists:keydelete(types,1,lists:keydelete(iskv,1,lists:keydelete(num,1,New)))),
	case NewTypes of
		[] ->
			{ok,O};
		_ ->
			{iskv,multihead,MultiheadList1} = lists:keyfind(iskv,1,New),
			MultiheadList = [Name || {Name,true} <- lists:keydelete(any,1,MultiheadList1)],
			[check_sql(Type,0,SqlNew,lists:member(Type,MultiheadList)) || {Type,SqlNew} <- NewTypes],

			case [Tuple || Tuple <- NewTypes, tuple_size(Tuple) == 2] of
				[] ->
					{ok,O};
				NewTypes1 ->
					{ok,O ++ ?ERR("New actors:"++?DELIMITER,[NewTypes1])}
			end
	end.

check_sql(Type,SizeCur,SqlNew,IsKv) ->
	SizeNew = tuple_size(SqlNew),
	{ok,Db,_,_} = actordb_sqlite:init(":memory:"),
	% io:format("~p~n",[Type]),
	[begin
		% io:format("Check sql running: ~s\n",[element(N,SqlNew)]),
		case actordb_sqlite:exec(Db,[element(N,SqlNew)]) of
			ok ->
				ok;
			{ok,_} ->
				ok;
			{sql_error,E,_E1} ->
				actordb_sqlite:stop(Db),
				throw({error,?ERR("SQL Error for type \"~p\"~n~p~n~p~n",[Type,E,
									binary_to_list(iolist_to_binary(element(N,SqlNew)))])});
			{error,E} ->
				actordb_sqlite:stop(Db),
				throw({error,?ERR("SQL Error for type ~p, ~p~n",[Type,E])})
		end,
		case N == SizeCur of
			true ->
				compare_tables(Type,Db);
			false ->
				ok
		end
	end || N <- lists:seq(1,SizeNew)],
	case IsKv of
		true ->
			case actordb_sqlite:exec(Db,"select name from sqlite_master where type='table';",read) of
				{ok,[{columns,{<<"name">>}},{rows,Tables}]} ->
					case lists:member({<<"actors">>},Tables) of
						true ->
							check_actor_table(Db,Type);
						_ ->
							throw({error,?ERR("KV requires an \"actors\" table."++
												"~nType ~p has tables: ~p",
								[Type,[X || {X} <- Tables]])})
					end
			end;
		false ->
			ok
	end.

compare_tables(Type,NewDb) ->
	{ok,Db,_,_} = actordb_sqlite:init(":memory:"),
	% io:format("Running: ~s~n",[tuple_to_list(apply(actordb_schema,Type,[]))]),
	ok = actordb_sqlite:exec(Db,tuple_to_list(apply(actordb_schema,Type,[])),write),
	compare_tables1(Type,actordb:tables(Type),Db,NewDb).
compare_tables1(Type,[Table|T],Db,NewDb) ->
	OldColumns = actordb_sqlite:exec(Db,["pragma table_info(",Table,");"],read),
	NewColumns = actordb_sqlite:exec(NewDb,["pragma table_info(",Table,");"],read),
	case OldColumns == NewColumns of
		true ->
			compare_tables1(Type,T,Db,NewDb);
		false ->
			throw({error,?ERR("For type=~p, schema was not modified correctly, add lines to the end of the list."++
				" Do not modify existing SQL statements.~nMismatch was found for table: ~s~n",[Type,Table])})
	end;
compare_tables1(_,[],_,_) ->
	ok.

check_actor_table(Db,Type) ->
	{ok,[{columns,Columns},{rows,Rows1}]} = actordb_sqlite:exec(Db,"pragma table_info(actors);",read),
	Rows = [lists:zip(tuple_to_list(Columns),tuple_to_list(Row)) || Row <- Rows1],
	case butil:findobj(<<"name">>,<<"id">>,Rows) of
		false ->
			throw({error,?ERR("KV data type ~p does not contain \"id\" column of type TEXT.",[Type])});
		IdCol ->
			case butil:ds_val(<<"type">>,IdCol) of
				<<"TEXT">> ->
					ok;
				IdColType ->
					throw({error,?ERR("KV data type ~p \"id\" column should be TEXT, but is ~p.",
							[Type,butil:tolist(IdColType)])})
			end
	end,
	case butil:findobj(<<"name">>,<<"hash">>,Rows) of
		false ->
			throw({error,?ERR("KV data type ~p does not contain \"hash\" column of type INTEGER.",
					[Type])});
		HashCol ->
			case butil:ds_val(<<"type">>,HashCol) of
				<<"INTEGER">> ->
					ok;
				ColType ->
					throw({error,?ERR("KV data type ~p \"hash\" column should be INTEGER, but is ~p.",
							[Type,butil:tolist(ColType)])})
			end
	end.

init_state(Nodes, Groups, Schema0) ->
	case actordb_sharedstate:init_state(Nodes,Groups,[],[{'schema.yaml',Schema0}]) of
		ok ->
			"ok";
		Err ->
			throw(Err)
	end.
