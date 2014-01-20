% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_cmd).
-compile(export_all).
-define(DELIMITER,"~n-------------------------~n~p~n-------------------------~n").

cmd(init,parse,Etc) ->
	try case readnodes(Etc++"/nodes.yaml") of
		{Nodes,Groups} ->
			case yamerl_constr:file(Etc++"/schema.yaml") of
				[_Schema] ->
					case bkdcore:nodelist() of
						[] ->
							[_|_] = compare_groups(nodes_to_names(Nodes),
											bkdcore_changecheck:parse_yaml_groups(Groups),
											compare_nodes(Nodes,[])),
							ok;
						_ ->
							throw(io_lib:fwrite("ActorDB already initialized."))
					end;
				X ->
					throw(io_lib:fwrite("Error parsing schema.yaml ~p",[X]))
			end;
		X ->
			throw(io_lib:fwrite("Error parsing nodes.yaml: ~p",[X]))
	end of
		ok ->
			{ok,"Start new cluster?"}
	catch
		throw:Str when is_list(Str) ->
			{error,io_lib:fwrite("~s~n",[Str])};
		throw:S ->
			{error,io_lib:fwrite("~p~n",[S])};
		_:{badmatch,{error,enoent}} ->
			{error,io_lib:fwrite("File(s) missing: ~n~p~n~p~n~p~n",[Etc++"/nodes.yaml",Etc++"/groups.yaml",Etc++"/schema.yaml"])};
		_:Err ->
			{error,io_lib:fwrite("Error parsing configs ~p~n",[Err])}
	end;
cmd(init,commit,Etc) ->
	try {Nodes,Groups1} = readnodes(Etc++"/nodes.yaml"),
		Groups = bkdcore_changecheck:parse_yaml_groups(Groups1),
		[Schema] = yamerl_constr:file(Etc++"/schema.yaml"),
		ok = bkdcore_sharedstate:set_global_state([{bkdcore,nodes,Nodes},{bkdcore,groups,Groups},
						{actordb,'schema.yaml',Schema}]) of
		ok ->
			"ok"
	catch
		_:{badmatch,{error,enoent}} ->
			io_lib:fwrite("File(s) missing: ~n~p~n~p~n",[Etc++"/nodes.yaml",Etc++"/schema.yaml"]);
		_:Err ->
			io_lib:fwrite("Parsing configs ~p~n",[Err])
	end;
cmd(updatenodes,parse,Etc) ->
	try case readnodes(Etc++"/nodes.yaml") of
		{Nodes,Groups1} ->
			Groups = bkdcore_changecheck:parse_yaml_groups(Groups1),
			compare_groups(nodes_to_names(Nodes),Groups,compare_nodes(Nodes,[]));
		X ->
			throw(io_lib:fwrite("Error parsing nodes.yaml ~p",[X]))
	end of
		[_|_] = Out ->
			{ok,Out};
		[] ->
			{ok,nochange}
	catch
		throw:Str ->
			{error,io_lib:fwrite("~s~n",[Str])};
		_:{badmatch,{error,enoent}} ->
			{error,io_lib:fwrite("File(s) missing: ~n~p~n~p~n",[Etc++"/nodes.yaml",Etc++"/groups.yaml"])};
		_:Err ->
			{error,io_lib:fwrite("~p~n",[Err])}
	end;
cmd(updatenodes,commit,Etc) ->
	try {Nodes,Groups1} = readnodes(Etc++"/nodes.yaml"),
		Groups = bkdcore_changecheck:parse_yaml_groups(Groups1),
		[_|_] = compare_groups(nodes_to_names(Nodes),Groups,compare_nodes(Nodes,[])),
		bkdcore_sharedstate:set_global_state([{bkdcore,nodes,Nodes},{bkdcore,groups,Groups}]) of
		ok ->
			"done";
		Err ->
			io_lib:fwrite("~p~n",[Err])
	catch
		_:{badmatch,{error,enoent}} ->
			io_lib:fwrite("File(s) missing: ~n~p~n~p~n",[Etc++"/nodes.yaml",Etc++"/groups.yaml"]);
		_:Err ->
			io_lib:fwrite("Error ~p~n",[Err])
	end;
cmd(updateschema,parse,Etc) ->
	case catch actordb_schema:types() of
		Types when is_list(Types) ->
			try yamerl_constr:file(Etc++"/schema.yaml") of
				[Schema] ->
					NewCfg = actordb_util:parse_cfg_schema(Schema),
					case catch compare_schema(Types,NewCfg,[]) of
						{ok,L} ->
							{ok,L};
						{error,Err} ->
							{error,Err};
						Err ->
							{error,Err}
					end
			catch
				_:{badmatch,{error,enoent}} ->
					{error,io_lib:fwrite("File missing ~p~n",[Etc++"/schema.yaml"])};
				_:Err ->
					{error,io_lib:fwrite("Unable to parse schema:~n~p.",[Err])}
			end;
		_ ->
			{error,io_lib:fwrite("No existing schema, run init?",[])}
	end;
cmd(updateschema,commit,Etc) ->
	try [Schema] = yamerl_constr:file(Etc++"/schema.yaml"),
		bkdcore_sharedstate:set_global_state([{actordb,'schema.yaml',Schema}]) of
		ok ->
			"done";
		Err ->
			io_lib:fwrite("~p~n",[Err])
	catch
		_:{badmatch,{error,enoent}} ->
			io_lib:fwrite("File missing ~p~n",[Etc++"/schema.yaml"]);
		_:Err ->
			io_lib:fwrite("~p~n",[Err])
	end;
cmd(dummy,_,Etc) ->
	Etc;
cmd(_,_,_) ->
	{error,io_lib:fwrite("uncrecognized command.~nSupported commands: ~p, ~p, ~p~n",[init,updateschema,updatenodes])}.

readnodes(Pth) ->
	{ok,_} = file:read_file_info(Pth),
	[[A1,A2]] = yamerl_constr:file(Pth),
	case A1 of
		{"nodes",Nodes} ->
			{"groups",Groups} = A2,
			{Nodes,Groups};
		{"groups",Groups} ->
			{"nodes",Nodes} = A2,
			{Nodes,Groups};
		_ ->
			throw("Invalid nodes.yaml. First object is neither nodes nor groups")
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
					compare_groups(Nodes,T,io_lib:fwrite("New group:"++?DELIMITER,[GroupInfo])++Out);
				ExistingNodes ->
					case lists:subtract(ExistingNodes,GNodes) == [] andalso
								GP == bkdcore:group_param(Name) andalso
								Type == bkdcore:group_type(Name) of
						true ->
							compare_groups(Nodes,T,Out);
						false ->
							compare_groups(Nodes,T,io_lib:fwrite("Changed group:"++?DELIMITER,[GroupInfo])++Out)
					end
			end;
		Unknown ->
			throw(io_lib:fwrite("Nodes ~p in group ~p not listed in nodes.yaml",[Unknown,Name]))
	end;
compare_groups(_,[],Out) ->
	Out.

compare_nodes([NewInfo|T],Out) ->
	{Name,_AddrReal,_Port,_Pub,_Dist} = All = bkdcore_changecheck:read_node(NewInfo),
	case bkdcore:node_address(Name) of
		undefined ->
			compare_nodes(T,io_lib:fwrite("New node:"++?DELIMITER,[NewInfo])++Out);
		{IPCur,PortCur} ->
			case All == {Name,IPCur,PortCur,bkdcore:public_address(Name),bkdcore:dist_name(Name)} of
				true ->
					compare_nodes(T,Out);
				false ->
					compare_nodes(T,io_lib:fwrite("Changed node:"++?DELIMITER,[NewInfo])++Out)
			end
	end;
compare_nodes([],Out) ->
	Out.
 
% Move over existing tyes of actors. 
% For every type check if it exists in new schema and if any sql statements added.
compare_schema([Type|T],New,Out) when Type == ids; Type == types; Type == iskv; Type == num ->
	compare_schema(T,New,Out);
compare_schema([Type|T],New,Out) ->
	case lists:keyfind(Type,1,New) of
		false ->
			{error,io_lib:fwrite("Missing type ~p in new config. Config invalid.~n",[Type])};
		{_,SqlNew} ->
			case apply(actordb_schema,Type,[]) of
				SqlNew ->
					compare_schema(T,lists:keydelete(Type,1,New),Out);
				SqlCur ->
					SizeCur = tuple_size(SqlCur),
					SizeNew = tuple_size(SqlNew),
					{ok,Db,_,_} = actordb_sqlite:init(":memory:",off),
					[begin
						case actordb_sqlite:exec(Db,[element(N,SqlNew)]) of
							ok ->
								ok;
							{ok,_} ->
								ok;
							{sql_error,E,_E1} ->
								actordb_sqlite:stop(Db),
								throw({error,io_lib:fwrite("SQL Error for type \"~p\"~n~p~n~p~n",[Type,E,binary_to_list(iolist_to_binary(element(N,SqlNew)))])});
							{error,E} ->
								actordb_sqlite:stop(Db),
								throw({error,io_lib:fwrite("SQL Error for type ~p, ~p~n",[Type,E])})
						end
					end || N <- lists:seq(1,SizeNew)],
					case ok of
						_ when SizeCur < SizeNew ->
							Lines = [binary_to_list(iolist_to_binary(element(N,SqlNew))) || N <- lists:seq(SizeCur+1,SizeNew)],
							Out1 = Out ++ io_lib:fwrite("Update type ~p:"++?DELIMITER,
																			[Type,Lines]);
						_ ->
							Out1 = Out
					end,
					compare_schema(T,lists:keydelete(Type,1,New),Out1)
			end
	end;
compare_schema([],New,O) ->
	case lists:keydelete(ids,1,lists:keydelete(types,1,lists:keydelete(iskv,1,lists:keydelete(num,1,New)))) of
		[] ->
			{ok,O};
		NewTypes ->
			{ok,O ++ io_lib:fwrite("New actors:"++?DELIMITER,[NewTypes])}
	end.


