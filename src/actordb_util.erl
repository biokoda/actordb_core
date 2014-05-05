% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_util).
-include("actordb.hrl").
-compile(export_all).


hash(V) ->
	% erlang:phash2([V,{1234982,32402942}]).
	emurmur3:hash_x86_32(V,1283346540).

actor_types() ->
	actordb_schema:types().

typeatom(<<_/binary>> = Type) ->
	case catch binary_to_existing_atom(Type,utf8) of
		TypeAtom when is_atom(TypeAtom) ->
			case actordb:actor_id_type(TypeAtom) of
				undefined ->
					throw({unknown_actor_type,Type});
				_ ->
					TypeAtom
			end;
		_ ->
			throw({unknown_actor_type,Type})
	end;
typeatom(T) when is_atom(T) ->
	T.


tunnel_bin(<<LenPrefix:16/unsigned,FixedPrefix:LenPrefix/binary,
             LenVarPrefix:16/unsigned,VarPrefix:LenVarPrefix/binary,
             LenHeader,Header:LenHeader/binary,
             LenPage:16,Page:LenPage/binary>>) ->
	{Cb,Actor,Type,Term} = binary_to_term(FixedPrefix),
	case VarPrefix of
		<<>> ->
			ok;
		_ ->
			{Term,Leader,PrevEvnum,PrevTerm,LeaderCommit} = binary_to_term(VarPrefix),
			Res = actordb_sqlproc:call_slave(Cb,Actor,Type,
					{state_rw,{appendentries_start,Term,Leader,PrevEvnum,PrevTerm,LeaderCommit,false}}),
			put(proceed,Res)
	end,
	% When header arrives, we check parameters if all ok.
	% If not, ignore wal pages untill next header.
	case get(proceed) of
		ok ->
			actordb_sqlproc:call_slave(Cb,Actor,Type,{appendentries_wal,Term,Header,Page});
		_ ->
			ok
	end,
	ok.

shard_path(Name) ->
	[drive(Name), "/shards/"].

actorpath(Actor) ->
	Path = drive(Actor),
	case actordb_conf:level_size() of
		0 ->
			[Path, "/actors/"];
		Max ->
			[Path,"/actors/", butil:tolist(hash(["db_level",butil:tobin(Actor)]) rem Max), 
					"/"]
	end.

drive(Actor) ->
	case actordb_conf:paths() of
		[Path] ->
			Path;
		Paths ->
			actordb:hash_pick(Actor,Paths)
	end.

split_point(From,To) ->
	From + ((To-From) div 2).

type_schema(?MULTIUPDATE_TYPE,Vers) ->
	actordb_multiupdate:get_schema(Vers);
type_schema(?CLUSTEREVENTS_TYPE,Vers) ->
	actordb_events:get_schema(Vers);
type_schema(Type,0) ->
	{tuple_size(apply(actordb_schema,Type,[])),tuple_to_list(apply(actordb_schema,Type,[]))};
type_schema(Type,Version) ->
	Schema = apply(actordb_schema,Type,[]),
	case tuple_size(Schema) > Version of
		true ->
			{tuple_size(Schema),[element(N,Schema) || N <- lists:seq(Version+1,tuple_size(Schema))]};
		false ->
			{Version,[]}
	end.

createcfg(Main,Extra,Level,Journal,Sync,QueryTimeout) ->
	bkdcore:mkmodule(actordb_conf,[{db_path,Main},{paths,[Main|Extra]},{level_size,butil:toint(Level)},
								   {journal_mode,Journal},{sync,Sync},{query_timeout,QueryTimeout}]).

change_journal(Journal,Sync) ->
	bkdcore:mkmodule(actordb_conf,[{db_path,actordb_conf:db_path()},{paths,actordb_conf:paths()},
								   {level_size,actordb_conf:level_size()},{journal_mode,Journal},{sync,butil:tobin(Sync)},{query_timeout,actordb_conf:query_timeout()}]).

% Out of schema.cfg create module with functions:
% types() -> [actortype1,actortype2,...]
% iskv(actortype) -> true/false.
% ids() -> [{actortype1,integer},{actortype2,text}]
% actortype1() -> {SqlVersion1,SqlUpdate1,SqlUpdate2,..}
% actortypeN() -> ...
parse_cfg_schema(G1) ->
	G = [begin
			case V of
				[{_,_}|_] = VObj ->
					TypeType = butil:ds_val("type",VObj),
					Sql = butil:ds_val("schema",VObj),
					case TypeType of
						"kv" ->
							{butil:toatom(Typ),kv,Sql};
						_ ->
							{butil:toatom(Typ),actor,Sql}
					end;
				_ ->
					{butil:toatom(Typ),actor,V}
			end
	end || {Typ,V} <- G1, Typ /= "ids"],
	Types = [element(1,Group) || Group <- G, element(1,Group) /= "ids"],
	case lists:keyfind("ids",1,G) of
		{"ids",Ids1} ->
			case [{AType,IdType} || {AType,IdType} <- Ids1, IdType /= "integer" andalso IdType /= "string"] of
				[] ->
					ok;
				Invalid ->
					exit({invalid_idtypes,Invalid})
			end,
			Ids = [case lists:keyfind(Type,1,Ids1) of
							false ->
								{Type,string};
							AType ->
								AType
						 end || Type <- Types];
		_ ->
			Ids = [{Type,string} || Type <- Types]
	end,
	TypeSqls = [{Type,list_to_tuple([check_for_end(S) || S <- check_str(Sqls)])} || {Type,_,Sqls} <- G],
	TypeColumns = [begin
		EntireSchema = tuple_to_list(Sqls),
		{ok,Db,_,_} = actordb_sqlite:init(":memory:",off),
		actordb_sqlite:exec(Db,EntireSchema),
		{ok,[{columns,{<<"name">>}},{rows,Tables}]} = actordb_sqlite:exec(Db,"select name from sqlite_master where type='table';"),
		Val = [begin
			{ok,[{columns,Columns},{rows,Rows1}]} = actordb_sqlite:exec(Db,["pragma table_info(",Table,");"]),
			Rows = [lists:zip(tuple_to_list(Columns),tuple_to_list(Row)) || Row <- Rows1],
			{Table,[{butil:ds_val(<<"name">>,Row),butil:ds_val(<<"type">>,Row)} || Row <- Rows]}
		 end || {Table} <- Tables],
		 actordb_sqlite:stop(Db),
		 {Type,multihead,[{tables,[Table || {Table} <- Tables]}|Val]}
	end || {Type,Sqls} <- TypeSqls],

	Out = [{types,Types}, {num,erlang:phash2(G1)}] ++ 
	[{iskv,multihead,[{Type,true} || {Type,kv,_Sqls} <- G] ++ [{any,false}]}] ++
	 [{ids,Ids}] ++ TypeColumns ++
	 TypeSqls,

	Out.

check_str(S) ->
	case S of
		[[_|_]|_] ->
			S;
		[X|_] when is_integer(X) ->
			[S]
	end.
check_for_end(L) ->
	case lists:reverse(butil:tolist(L)) of
		";" ++ _ ->
			L;
		_ ->
			[L,$;]
	end.
			
