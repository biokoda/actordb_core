% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_util).
-include_lib("actordb_core/include/actordb.hrl").
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

wait_for_startup(?STATE_TYPE,_,_) ->
	ok;
wait_for_startup(Type,Who,N) ->
	case catch actordb_schema:num() of
		X when is_integer(X) ->
			ok;
		{'EXIT',_} ->
			% ?ADBG("WAIT FOR STARTUP ~p",[Who]),
			case N > 100 of
				true ->
					throw(startup_timeout);
				_ ->
					timer:sleep(100),
					wait_for_startup(Type,Who,N+1)
			end
	end.

tunnel_bin(Pid,<<>>) ->
	Pid;
tunnel_bin(Pid,<<LenBin:16/unsigned,Bin:LenBin/binary>>) ->
	131 = binary:first(Bin),
	{Mod,Param} = binary_to_term(Bin),
	apply(Mod,tunnel_callback,Param),
	Pid;
tunnel_bin(Pid,<<LenBin:24/unsigned,Bin:LenBin/binary>>) ->
	Pid ! {continue,Bin},
	Pid;
tunnel_bin(Pid,<<LenPrefix:16/unsigned,FixedPrefix:LenPrefix/binary,
		LenVarPrefix:16/unsigned,VarPrefix:LenVarPrefix/binary,
		LenHeader,Header:LenHeader/binary,
		LenPage:16/unsigned,Rem/binary>>) ->
	{Cb,Actor,Type} = binary_to_term(FixedPrefix),
	case VarPrefix of
		<<>> ->
			PidOut = Pid;
		_ ->
			Home = self(),
			Ref = make_ref(),
			case distreg:whereis({actor_ae_stream,Actor,Type}) of
				undefined ->
					PidOut1 = spawn(fun() ->
						% ?ADBG("Started ae stream proc ~p.~p",[Actor,Type]),
						case distreg:reg({actor_ae_stream,Actor,Type}) of
							ok ->
								Home ! {ok,Ref};
							name_exists ->
								Home ! {exists,Ref},
								exit(normal)
						end,
						case apply(Cb,cb_slave_pid,[Actor,Type,[]]) of
							{ok,ActorPid} ->
								ok;
							ActorPid when is_pid(Pid) ->
								ok
						end,
						erlang:monitor(process,ActorPid),
						% ?ADBG("Have pid for ae ~p.~p",[Actor,Type]),
						actor_ae_stream(ActorPid,undefined,Actor,Type)
					end),
					receive
						{ok,Ref} ->
							PidOut = PidOut1;
						{exists,Ref} ->
							PidOut = distreg:whereis({actor_ae_stream,Actor,Type})
					end;
				PidOut ->
					ok
			end,
			PidOut ! {start,Cb,Actor,Type,VarPrefix}
	end,
	case Rem of
		<<Page:LenPage/binary>> ->
			PidOut ! {call_slave,Cb,Actor,Type,Header,Page};
		<<Len:16/unsigned,PagePart/binary>> ->
			EntireLen = (LenPage bsl 16) + Len,
			?ADBG("Entire len ~p, ~p",[EntireLen,byte_size(PagePart)]),
			case EntireLen > byte_size(PagePart) of
				true ->
					PidOut ! {call_slave,Cb,Actor,Type,Header,EntireLen,PagePart};
				false ->
					PidOut ! {call_slave,Cb,Actor,Type,Header,PagePart}
			end
	end,
	PidOut;
tunnel_bin(Pid,Bin) ->
	?AERR("Tunnel invalid data ~p",[Bin]),
	Pid.

-record(astr,{apid, count, actor, type, entire_len = 0, received = 0, buffer = [], header, cb}).

actor_ae_stream(ActorPid,Count,AD,TD) ->
	actor_ae_stream(#astr{apid = ActorPid, count = Count, actor = AD, type = TD}).
actor_ae_stream(P) ->
	receive
		{start,Cb,Actor,Type,VarPrefix} ->
			check_actor(P#astr.actor,P#astr.type,Actor,Type),
			?ADBG("AE stream proc start ~p.~p",[Actor,Type]),
			case binary_to_term(VarPrefix) of
				{Term,Leader,PrevEvnum,PrevTerm,CallCount} ->
					DbFile = undefined;
				{Term,Leader,PrevEvnum,PrevTerm,CallCount,DbFile} ->
					ok;
				Invalid ->
					Term = DbFile = CallCount = Leader = PrevEvnum = PrevTerm = undefined,
					?AERR("Variable header invalid fixed=~p, var=~p",[{Cb,Actor,Type,Term},Invalid]),
					exit(error)
			end,
			case lists:keyfind(actordb_conf:node_name(),1,CallCount) of
				{_Me,Count1} ->
					ok;
				_ ->
					% Special case when nodes are added at run time.
					Count1 = {PrevEvnum,PrevTerm}
			end,
			?ADBG("AE stream proc call ~p.~p",[Actor,Type]),
			case actordb_sqlproc:call_slave(Cb,Actor,Type,
					{state_rw,{appendentries_start,Term,Leader,PrevEvnum,PrevTerm,head,Count1}}) of
				ok ->
					case is_binary(DbFile) of
						true ->
							ok = actordb_sqlproc:call_slave(Cb,Actor,Type,
								{state_rw,{set_dbfile,DbFile}});
						_ ->
							ok
					end,
					?ADBG("AE stream proc ok! ~p.~p",[Actor,Type]),
					actor_ae_stream(P#astr{count = Count1});
				_ ->
					?ADBG("AE die"),
					% Die off if we should not continue
					% All subsequent WAL messages are thus discarded and process is restarted
					%  on next ae start.
					ok
			end;
		{'DOWN',_,_,ActorPid,_} when P#astr.apid == ActorPid ->
			ok;
		{call_slave,Cb,Actor,Type,Header, EntireLen, PagePart} when EntireLen > PagePart ->
			check_actor(P#astr.actor,P#astr.type,Actor,Type),
			actor_ae_stream(P#astr{cb = Cb, entire_len = EntireLen, received = byte_size(PagePart), buffer = [PagePart], header = Header});
		{continue,Bin} when P#astr.received > 0 ->
			case byte_size(Bin) + P#astr.received >= P#astr.entire_len of
				true ->
					self() ! {call_slave, P#astr.cb, P#astr.actor, P#astr.type, P#astr.header, iolist_to_binary(lists:reverse([Bin|P#astr.buffer]))},
					actor_ae_stream(P#astr{entire_len = 0, received = 0, buffer = [], header = <<>>});
				false ->
					actor_ae_stream(P#astr{received = P#astr.received + byte_size(Bin), buffer = [Bin|P#astr.buffer]})
			end;
		{continue,_} ->
			?AERR("Received continue without initial packet!"),
			ok;
		{call_slave,Cb,Actor,Type,Header,Page} ->
			check_actor(P#astr.actor,P#astr.type,Actor,Type),
			?ADBG("Calling slave ~p",[Actor]),
			case actordb_sqlproc:call_slave(Cb,Actor,Type,{state_rw,{appendentries_wal,Header,Page,head,P#astr.count}},[nostart]) of
				ok ->
					actor_ae_stream(P);
				done ->
					actor_ae_stream(P);
				_Err ->
					% Same as start ae. Die off.
					ok
			end;
		stop ->
			ok
	after 30000 ->
		distreg:unreg(self()),
		erlang:send_after(3000,self(),stop)
	end.

% Safety precation. A pid must only belong to a specific actor.
check_actor(Actor,Type,Actor,Type) ->
	ok;
check_actor(AD,TD,Actor,Type) ->
	?AERR("Tunnel calling another actor, is={~p,~p}, input={~p,~p}",[AD,TD,Actor,Type]),
	throw(badcall).

shard_path(_Name) ->
	"shards/".

actorpath(_Actor) ->
	["actors/"].

drive(_Actor) ->
	[].

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

createcfg(Main,Extra,Sync,QueryTimeout,Repl) ->
	createcfg(Main,Extra,Sync,QueryTimeout,Repl,bkdcore:node_name()).
createcfg(Main,Extra,Sync,QueryTimeout,Repl,Name) ->
	bkdcore:mkmodule(actordb_conf,[{db_path,Main},{paths,[Main|Extra]},{cfgtime,os:timestamp()},
		{replication_space,Repl},{sync,Sync},{query_timeout,QueryTimeout},{node_name,Name}]).

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
		{ok,Db,_,_} = actordb_sqlite:init(":memory:"),
		actordb_sqlite:exec(Db,EntireSchema),
		{ok,[{columns,{<<"name">>}},{rows,Tables}]} = actordb_sqlite:exec(Db,"select name from sqlite_master where type='table';",read),
		Val = [begin
			{ok,[{columns,Columns},{rows,Rows1}]} = actordb_sqlite:exec(Db,["pragma table_info(",Table,");"],read),
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
check_for_end([$\s|L]) ->
	check_for_end(L);
check_for_end([$\$|L]) ->
	case lists:reverse(butil:tolist(L)) of
		";" ++ _ ->
			[$\$|L];
		_ ->
			[[$\$|L],$;]
	end;
check_for_end(L) ->
	check_for_end([$\$|L]).
