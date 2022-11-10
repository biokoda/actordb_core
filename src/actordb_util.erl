% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_util).
-include("actordb.hrl").
-export([hash/1, varint_enc/1, varint_dec/1,reg_actor/2,
	actor_types/0, typeatom/1, wait_for_startup/3, tunnel_bin/2,
	actor_path/1, shard_path/1, type_schema/2, createcfg/8,
	parse_cfg_schema/1, flatnow/0, split_point/2]).


hash(V) ->
	% erlang:phash2([V,{1234982,32402942}]).
	emurmur3:hash_x86_32(V,1283346540).

% Variable length encoded integer. Ported from sqlite4 source.
varint_enc(X) when X =< 240 ->
	<<X>>;
varint_enc(X) when X =< 2287 ->
	Y = X - 240,
	<<((Y div 256) + 241),(Y rem 256)>>;
varint_enc(X) when X =< 67823 ->
	Y = X - 2288,
	<<249,(Y div 256),(Y rem 256)>>;
varint_enc(X) ->
	<<W:32/unsigned,Y:32/unsigned>> = <<X:64/unsigned>>,
	case W of
		0 when Y =< 16777215 ->
			<<250, Y:24/unsigned>>;
		0 ->
			<<251,Y:32/unsigned>>;
		_ when W =< 255 ->
			<<252,W,Y:32/unsigned>>;
		_ when W =< 65535 ->
			<<253, W:16/unsigned, Y:32/unsigned>>;
		_ when W =< 16777215 ->
			<<254, W:24/unsigned, Y:32/unsigned>>;
		_ ->
			<<255, X:64/unsigned>>
	end.

varint_dec(<<X,_/binary>>) when X =< 240 ->
	{X,1};
varint_dec(<<A,B,_/binary>>) when A =< 248 ->
	{(A-241)*256+B+240, 2};
varint_dec(<<249,A,B,_/binary>>) ->
	{2288+256*A+B, 3};
varint_dec(<<250,A,B,C,_/binary>>) ->
	{(A bsl 16)+(B bsl 8)+C, 4};
varint_dec(<<251,X:32/unsigned,_/binary>>) ->
	{X, 5};
varint_dec(<<252,X:32/unsigned,A,_/binary>>) ->
	{(X bsl 8) + A, 6};
varint_dec(<<253,X:32/unsigned,A:16/unsigned,_/binary>>) ->
	{(X bsl 16)+A, 7};
varint_dec(<<254,X:32/unsigned,A:24/unsigned,_/binary>>) ->
	{(X bsl 24)+A, 8};
varint_dec(<<255,X:64/unsigned,_/binary>>) ->
	{X,9}.


actor_types() ->
	actordb_schema:types().

typeatom(<<_/binary>> = Type) ->
	case catch binary_to_existing_atom(Type,utf8) of
		?STATE_TYPE ->
			?STATE_TYPE;
		TypeAtom when is_atom(TypeAtom) ->
			case actordb:actor_id_type(TypeAtom) of
				undefined ->
					throw({error,"unknown_actor_type "++butil:tolist(Type)});
				_ ->
					TypeAtom
			end;
		_ ->
			throw({error,"unknown_actor_type: "++butil:tolist(Type)})
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
	% term_to_binary produced bin always starts with 131
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
					BufBin = iolist_to_binary(lists:reverse([Bin|P#astr.buffer])),
					self() ! {call_slave, P#astr.cb, P#astr.actor, P#astr.type, P#astr.header, BufBin},
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

reg_actor(Name,Type) ->
	LocalShardForReg = actordb_shardmngr:find_local_shard(Name,Type),
	?ADBG("shard for reg ~p",[LocalShardForReg]),
	case LocalShardForReg of
		{redirect,Shard,Node} ->
			actordb:rpc(Node,Shard,{actordb_shard,reg_actor,[Shard,Name,Type]});
		undefined ->
			{Shard,_,Node} = actordb_shardmngr:find_global_shard(Name),
			actordb:rpc(Node,Shard,{actordb_shard,reg_actor,[Shard,Name,Type]});
		Shard ->
			ok = actordb_shard:reg_actor(Shard,Name,Type)
	end.

% Safety precation. A pid must only belong to a specific actor.
check_actor(Actor,Type,Actor,Type) ->
	ok;
check_actor(AD,TD,Actor,Type) ->
	?AERR("Tunnel calling another actor, is={~p,~p}, input={~p,~p}",[AD,TD,Actor,Type]),
	throw(badcall).

shard_path(_Name) ->
	"shards/".

actor_path(_Actor) ->
	["actors/"].

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

createcfg(Main,Extra,Sync,QueryTimeout,Repl,WThreads, RThreads,Name) ->
	bkdcore:mkmodule(actordb_conf,[{db_path,Main},{paths,[Main|Extra]},{cfgtime,os:timestamp()},
		{replication_space,Repl},{sync,Sync},{query_timeout,QueryTimeout},{node_name,Name},
		{wthreads, WThreads}, {rthreads, RThreads}]).

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

% Time in microseconds.
% erlang:system_time(micro_seconds) is much slower on osx.
flatnow() ->
	case os:type() of
		{unix,darwin} ->
			{MS, S, MiS} = os:timestamp(),
			MS*1000000000000 + S*1000000 + MiS;
		_ ->
			erlang:system_time(micro_seconds)
	end.

check_str(S) ->
	case S of
		[] ->
			throw("can not have empty schema");
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
