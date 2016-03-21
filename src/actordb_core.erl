-module(actordb_core).
-export([start/0, start/2, stop/0, stop/1, stop_complete/0, start_ready/0, prestart/0]).
-include_lib("actordb_core/include/actordb.hrl").
-include_lib("kernel/include/file.hrl").

% manually closes all open db handles
stop() ->
	% Artificially increase callcount so it never reaches lower than high watermark
	% Wait untill call size is 0, which will mean all requests have been handled.
	actordb_backpressure:inc_callcount(1000000),
	% Max wait 30s
	wait_done_queries(30000),
	application:stop(actordb_core).
stop_complete()	 ->
	case ets:info(bpcounters,size) == undefined of
		true ->
			ok;
		_ ->
			stop()
	end,
	init:stop().


wait_done_queries(N) when N < 0 ->
	ok;
wait_done_queries(N) ->
	case actordb_backpressure:call_size() of
		0 ->
			case [ok || {_,true} <- actordb_local:get_mupdaters_state()] of
				[] ->
					[spawn(fun() -> gen_server:call(Pid,stop) end) || {_,Pid} <- distreg:processes()],
					timer:sleep(1000),
					?AINF("All requests done."),
					ok;
				_ ->
					timer:sleep(100),
					wait_done_queries(N-100)
			end;
		X ->
			?AINF("Waiting for requests to finish. ~p bytes to go.~n",[X]),
			timer:sleep(100),
			wait_done_queries(N-100)
	end.

% wait_distreg_procs() ->
% 	case distreg:processes() of
% 		[] ->
% 			ok;
% 		_ ->
% 			timer:sleep(1000),
% 			wait_distreg_procs()
% 	end.

start_ready() ->
	?AINF("Start ready."),
	application:set_env(actordb_core,isready,true),
	% actordb_termstore:start(),
	case application:get_env(actordb_core,mysql_protocol) of
		undefined ->
			ok;
		{ok, Port} ->
			case Port > 0 of
				true ->
					Ulimit = actordb_local:ulimit(),
					case ok of
						_ when Ulimit =< 256 ->
							MaxCon = 8;
						_ when Ulimit =< 1024 ->
							MaxCon = 64;
						_  when Ulimit =< 1024*4 ->
							MaxCon = 128;
						_ ->
							MaxCon = 1024
					end,
					TransOpts0 = [{port, Port},{max_connections,MaxCon}],
					% TransOpts = TransOpts0,
					case application:get_env(actordb_core,api_network_interface) of
						{ok,Interface1} ->
							TransOpts = [{ip,butil:ip_to_tuple(Interface1)}|TransOpts0];
						_ ->
							TransOpts = TransOpts0
					end,
					% TransOpts = case get_network_interface() of
					% 	[] -> TransOpts0;
					% 	IP -> [{ip,IP}|TransOpts0]
					% end,
					case ranch:start_listener(myactor, 20, ranch_tcp, TransOpts, myactor_proto, []) of
						{ok, _} ->
							ok;
						{error,already_started} ->
							ok;
						Err ->
							?AERR("Unable to start ranch ~p",[Err])
					end,
					ok;
				false ->
					ok
			end
	end.

prestart() ->
	application:ensure_all_started(lager),
	application:ensure_all_started(sasl),
	application:ensure_all_started(os_mon),
	application:ensure_all_started(yamerl),
	application:ensure_all_started(folsom),
	application:ensure_all_started(thrift),
	application:set_env(bkdcore,usesharedstate,false),
	case catch actordb_conf:paths() of
		[_|_] ->
			ok;
		_ ->
			case init:get_argument(config) of
				{ok, Files} ->
					prestart1(Files);
				_ ->
					?AERR("No app.config file in parameters! ~p",[init:get_arguments()]),
					init:stop()
			end
	end.
prestart1(Files) ->
	% ?AINF("Starting actordb ~p ~p",[butil:ds_val(config,Args),file:get_cwd()]),
	% Read args file manually to get paths for state.
	[Name1|_] = string:tokens(butil:tolist(node()),"@"),
	Name = butil:tobin(Name1),
	L = lists:foldl(
			fun([File], Env) ->
					BFName = filename:basename(File,".config"),
					FName = filename:join(filename:dirname(File),
											BFName ++ ".config"),
					case file:consult(FName) of
						{ok, [L]} ->
							L++Env;
						_ ->
						?AERR("Error in config ~p",[FName]),
						init:stop()
					end
			end, [], Files),
	ActorParam = butil:ds_val(actordb_core,L),
	[Main,Extra,Sync1,NumMngrs,QueryTimeout1,Repl,MaxDbSize] =
		butil:ds_vals([main_db_folder,extra_db_folders,
						fsync,num_transaction_managers,
						query_timeout,max_replication_space,max_db_size],ActorParam,
						["db",[],undefined,12,60000,{5000,0.1},"1TB"]),
	case QueryTimeout1 of
		0 ->
			QueryTimeout = infinity;
		QueryTimeout ->
			ok
	end,
	Sync = case Sync1 of
		undefined ->
			case butil:ds_val(sync,ActorParam) of
				undefined ->
					true;
				true ->
					true;
				false ->
					false
			end;
		0 ->
			false;
		1 ->
			true;
		on ->
			true;
		off ->
			false;
		true ->
			true;
		false ->
			false;
		safe ->
			true;
		fast ->
			false;
		{interval,_} ->
			Sync1;
		_ when is_integer(Sync1) ->
			true
	end,
	application:set_env(actordb_core,num_transaction_managers,NumMngrs),
	Statep = butil:expand_path(butil:tolist(Main)),
	filelib:ensure_dir(Statep),
	% ?AINF("State path ~p, ~p",[Main,Statep]),
	% No etc folder. config files are provided manually.
	BkdcoreParam = butil:ds_val(bkdcore,L),
	case butil:ds_val(etc,BkdcoreParam) of
		undefined ->
			application:set_env(bkdcore,etc,none);
		_ ->
			ok
	end,
	case application:get_env(bkdcore,statepath) of
		{ok,_} ->
			ok;
		_ ->
			application:set_env(bkdcore,statepath,Statep)
	end,

	Sch = erlang:system_info(schedulers),
	SchOnline = erlang:system_info(schedulers_online),
	case ok of
		_ when SchOnline > 8 ->
			% Limit number of erlang schedulers to leave space for engine threads
			erlang:system_flag(schedulers_online,6),
			Paths = length(actordb_conf:paths()),
			WThreads = 2,
			% For every write thread, max 4 read threads, if enough CPU cores avail
			case min(round(abs(Sch-Paths-6) / Paths), 3) of
				0 ->
					RdThreads = 2;
				RdThreads ->
					ok
			end;
		_ when SchOnline == 8 ->
			erlang:system_flag(schedulers_online,4),
			WThreads = 2,
			RdThreads = 2;
		_ when SchOnline == 6 ->
			erlang:system_flag(schedulers_online,3),
			WThreads = 1,
			RdThreads = 2;
		_ when SchOnline == 4 ->
			erlang:system_flag(schedulers_online,2),
			WThreads = 1,
			RdThreads = 1;
		_ ->
			WThreads = RdThreads = 1
	end,

	actordb_util:createcfg(Main,Extra,Sync,QueryTimeout,Repl,WThreads, RdThreads,Name),
	ensure_folders(actordb_conf:paths()),

	case Sync of
		true ->
			% If every transaction is synced, batch more concurrent writes together.
			LMBatch = 500,
			LMSync = 1;
		_ ->
			% If we're not syncing every transaction to disk, smaller batches perform better
			LMBatch = 30,
			LMSync = 0
	end,

	DrvInfo = #{paths => list_to_tuple(actordb_conf:paths()),
	staticsqls => actordb_sqlprocutil:static_sqls(),
	dbsize => parse_size(MaxDbSize),
	wthreads => WThreads,
	rthreads => RdThreads,
	lmdbsync => LMSync,
	counters => 10,
	nbatch => LMBatch},
	ok = try_load_driver(DrvInfo),
	emurmur3:init().

% LMDB needs max size. We try with configured size, if fail, try lowering value.
% Lowest tried value is 1GB.
try_load_driver(#{dbsize := Size} = Info) ->
	case (catch actordb_driver:init(Info)) of
		ok ->
			ok;
		Err ->
			Mem = butil:ds_val(total_memory,memsup:get_system_memory_data()),
			case ok of
				_ when Size > Mem*10 ->
					try_load_driver(Info#{dbsize => Mem*10});
				_ when Size > Mem ->
					try_load_driver(Info#{dbsize => Mem});
				_ when Size > (Mem div 2) ->
					try_load_driver(Info#{dbsize => Mem div 2});
				_ when Size > 1024*1024*1024*2 ->
					try_load_driver(Info#{dbsize => 1024*1024*1024*2});
				_ when Size > 1024*1024*1024 ->
					try_load_driver(Info#{dbsize => 1024*1024*1024});
				_ ->
					Err
			end
	end.


parse_size(DbSize1) ->
	S = string:to_lower(DbSize1),
	fix_size(S,butil:toint(filter_num(S))).

find_size_char(L) ->
	[N || N <- L, N == $M orelse N == $G orelse N == $T orelse N == $m orelse N == $g orelse N == $t].
filter_num(L) ->
	[N || N <- L, N >= $0 andalso N =< $9].
fix_size(Max,Max1)->
	case find_size_char(Max) of
		[SS|_] when SS == $m; SS == $M ->
			1024*1024*Max1;
		[SS|_] when SS == $g; SS == $G ->
			1024*1024*1024*Max1;
		[SS|_] when SS == $t; SS == $T ->
			1024*1024*1024*1024*Max1
	end.


start() ->
	% ?AINF("Starting actordb"),
	application:ensure_all_started(actordb_core,permanent).

start(_Type, _Args) ->
	prestart(),
	bkdcore:start(actordb:configfiles()),
	butil:wait_for_app(bkdcore),
	case actordb_conf:sync() of
		{interval,Interval} ->
			bkdcore_task:add_task(Interval,actordbfsync, fun actordb_driver:fsync/0);
		_ ->
			ok
	end,

	Pth1 = [actordb_sharedstate:cb_path(undefined,undefined,undefined),
			butil:tolist(?STATE_NM_GLOBAL),".",butil:tolist(?STATE_TYPE)],
	case file:read_file_info(Pth1) of
		{ok,I} when I#file_info.size > 0 ->
			StateStart = normal;
		_I ->
			% case file:read_file_info([actordb_conf:db_path(),"/",Pth1]) of
			% 	{ok,I} when I#file_info.size > 0 ->
			% 		StateStart = normal;
			% 	_ ->
			% 		StateStart = wait
			% end
			StPth = actordb_sharedstate:cb_path(1,2,3)++butil:tolist(?STATE_NM_GLOBAL)++"."++butil:tolist(?STATE_TYPE),
			{ok,_Db,SchemaTables,_PageSize} = actordb_sqlite:init(StPth,wal),
			case SchemaTables of
				[] ->
					case file:list_dir(actordb_conf:db_path()++"/shards/") of
						{ok,[_|_]} ->
							GL = get(),
							import_legacy(actordb_conf:paths(),0),
							erase(),
							[put(K,V) || {K,V} <- GL],
							StateStart = normal;
						_ ->
							StateStart = wait
					end;
				[_|_] ->
					StateStart = normal
			end
	end,

	{ok,SupPid} = actordb_sup:start_link(),

	% ?AINF("Starting sharedstate type ~p",[StateStart]),
	case StateStart of
		normal ->
			actordb_sharedstate:start(?STATE_NM_GLOBAL,?STATE_TYPE,[{slave,false},create]);
		wait ->
			actordb_sharedstate:start_wait(?STATE_NM_GLOBAL,?STATE_TYPE)
	end,
	case application:get_env(actordb_core,api_network_interface) of
		{ok,Interface1} ->
			application:set_env(thrift,network_interface,Interface1);
		_ ->
			ok
	end,
	case application:get_env(actordb_core,thrift_port) of
		{ok,ThriftPort} when ThriftPort > 0 ->
			{ok,_} = adbt:start(ThriftPort);
		_ ->
			ok
	end,
	{ok,SupPid}.

stop(_State) ->
	ok.

ensure_folders([])->
	ok;
ensure_folders([H|T])->
	ok = filelib:ensure_dir(H++"/"),
	ensure_folders(T).

% get_network_interface()->
% 	case application:get_env(actordb_core, network_interface) of
% 	  {ok, Value} ->
% 		  case inet:parse_address(Value) of
% 			{ok, IPAddress} -> ok;
% 			_ ->
% 			  {ok, {hostent, _, [], inet, _, [IPAddress]}} = inet:gethostbyname(Value)
% 		  end;
% 		_ ->
% 		  case string:tokens(atom_to_list(node()), "@") of
% 			["nonode","nohost"] -> IPAddress = {127,0,0,1};
% 			[_Name, Value] ->
% 			  case inet:parse_address(Value) of
% 				{ok, IPAddress} -> ok;
% 				_ ->
% 				  {ok, Hostname} = inet:gethostname(),
% 				  {ok, {hostent, _, [], inet, _, [IPAddress]}} = inet:gethostbyname(Hostname)
% 			  end
% 		  end
% 	end,
% 	{ok, Addresses} = inet:getif(),
% 	case lists:keyfind(IPAddress, 1, Addresses) of
% 	  false ->
% 		[];
% 	  _ ->
% 		IPAddress
% 	end.

import_legacy([H|T],Thr) ->
	erase(),
	case file:read_file_info(H++"/termstore") of
		{ok,_} ->
			import_db(H++"/termstore","termstore",Thr),
			[big_wal(Pth) || Pth <- lists:sort(fun walsort/2, filelib:wildcard(H++"/wal.*"))];
		_ ->
			ok
	end,
	Actors = [A || A <- element(2,file:list_dir(H++"/actors")), is_dbfile(A)],
	Shards = [A || A <- element(2,file:list_dir(H++"/shards")), is_dbfile(A)],
	case [A || A <- element(2,file:list_dir(H++"/state")), is_dbfile(A)] of
		[] ->
			State = [A || A <- element(2,file:list_dir(H++"/"++butil:tolist(bkdcore:node_name())++"/state")), is_dbfile(A)];
		State ->
			ok
	end,
	% {ok,Actors} = file:list_dir(H++"/actors"),
	import_dbs(H,"actors",Thr,Actors),
	import_dbs(H,"shards",Thr,Shards),
	import_dbs(H,"state",Thr,State),
	[big_wal(Pth) || Pth <- lists:sort(fun walsort/2, filelib:wildcard(H++"/wal.*"))],
	case get(<<"termstore">>) of
		undefined ->
			ok;
		{TermDb,_,_} ->
			{ok,[[{columns,_},{rows,R}]]} = actordb_driver:exec_script(<<"select * from terms;">>,TermDb),
			[begin
				case VF of
					undefined ->
						VotedFor = <<>>;
					VotedFor ->
						ok
				end,
				Nm = butil:tobin(butil:encode_percent(butil:tolist(Actor))++"."++
				butil:encode_percent(butil:tolist(Type))),
				case get(<<"actors/",Nm/binary>>) of
					undefined ->
						ok;
					_ ->
						ok = actordb_driver:term_store(<<"actors/",Nm/binary>>, CT, VotedFor, Thr)
				end,
				case get(<<"shards/",Nm/binary>>) of
					undefined ->
						ok;
					_ ->
						ok = actordb_driver:term_store(<<"shards/",Nm/binary>>, CT, VotedFor, Thr)
				end,
				case get(<<"state/",Nm/binary>>) of
					undefined ->
						ok;
					_ ->
						ok = actordb_driver:term_store(<<"state/",Nm/binary>>, CT, VotedFor, Thr)
				end
			end || {Actor,Type,VF,CT,_EN,_ET} <- R]
	end,
	import_legacy(T,Thr+1);
import_legacy([],_) ->
	ok.

import_dbs(Root,Sub,Thr,[H|T]) ->
	?AINF("Import actor ~p",[H]),
	import_db(Root++"/"++Sub++"/"++H,Sub++"/"++H,Thr),
	import_dbs(Root,Sub,Thr,T);
import_dbs(_,_,_,[]) ->
	ok.

import_db(Pth,Name,Thr) ->
	{ok,F} = file:open(Pth,[read,binary,raw]),
	{ok,Db} = actordb_driver:open(Name,actordb_util:hash(Name),wal),
	Size = filelib:file_size(Pth),
	cp_dbfile(F,Db,1,Size),
	put(butil:tobin(Name),{Db,1,2}),
	file:close(F),
	case file:open(Pth++"-wal",[read,binary,raw]) of
		{ok,Wal} ->
			try begin {ok,_} = file:read(Wal,32),
				file:read(Wal,40+4096) end of
			{ok,Bin} ->
				small_wal(Db,Wal,Bin),
				case butil:readtermfile(Pth++"-term") of
					undefined ->
						ok;
					{VotedFor,CurTerm,_} ->
						?AINF("Votedfor ~p, curterm=~p",[VotedFor,CurTerm]),
						actordb_driver:term_store(butil:tobin(Name), CurTerm, VotedFor, Thr);
					{VotedFor,CurTerm,_,_} ->
						?AINF("Votedfor ~p, curterm=~p",[VotedFor,CurTerm]),
						actordb_driver:term_store(butil:tobin(Name), CurTerm, VotedFor, Thr)
				end
			catch
				_:_ ->
					ok
			end;
		_ ->
			ok
	end,
	% {ok,X} = actordb_driver:exec_script("select * from sqlite_master;",Db),
	% ?AINF("~p",[X]),
	ok.

big_wal(Name) ->
	{ok,F} = file:open(Name,[read,binary,raw]),
	{ok,_} = file:read(F,40),
	{ok,Bin} = file:read(F,144+4096),
	big_wal(F,Bin),
	file:close(F).
big_wal(F,<<Pgno:32/unsigned,DbSize:32/unsigned,WN:64,WTN:64,_:32,_:32,_:32,Name1:92/binary,_:16/binary,Page:4096/binary>>) ->
	[Name|_] = binary:split(Name1,<<0>>),
	case get(Name) of
		undefined ->
			ok;
		{Db,TN,TEN} ->
			case WN + WTN of
				0 when DbSize /= 0 ->
					Term = TN,
					Num = TEN,
					put(Name,{Db,TN,TEN+1});
				0 ->
					Term = TN,
					Num = TEN;
				_ ->
					% Term = WTN,
					Term = 1,
					Num = WN
			end,
			Head = <<Term:64,Num:64,Pgno:32/unsigned,DbSize:32/unsigned>>,
			?AINF("Wal inject for ~p, term=~p, evnum=~p pgno=~p size=~p",[Name,{WTN,Term},{WN,Num},Pgno,DbSize]),
			ok = actordb_driver:inject_page(Db,Page,Head)
	end,
	case file:read(F,144+4096) of
		{ok,Bin} ->
			big_wal(F,Bin);
		_ ->
			ok
	end.

small_wal(Db,F,<<Pgno:32/unsigned,DbSize:32/unsigned,WN:64,WTN:64,_:16/binary,Page:4096/binary>>) ->
	Head = <<WTN:64,WN:64,Pgno:32/unsigned,DbSize:32/unsigned>>,
	?AINF("Wal inject term=~p num=~p pgno=~p size=~p",[WTN,WN,Pgno,DbSize]),
	case WTN+WN > 0 of
		true ->
			ok = actordb_driver:inject_page(Db,Page,Head);
		_ ->
			ok
	end,
	case file:read(F,40+4096) of
		{ok,Bin} ->
			small_wal(Db,F,Bin);
		_ ->
			ok
	end.

cp_dbfile(F,Db,Pgno,Size) ->
	case file:read(F,4096) of
		{ok,Bin} ->
			case Size - Pgno*4096 > 0 of
				true ->
					Commit = 0;
				false ->
					Commit = Pgno
			end,
			Head = <<1:64,1:64,Pgno:32/unsigned,Commit:32/unsigned>>,
			ok = actordb_driver:inject_page(Db,Bin,Head),
			cp_dbfile(F,Db,Pgno+1,Size);
		_ ->
			ok
	end.

walsort(A,B) ->
	[_,AN] = string:tokens(A,"."),
	[_,BN] = string:tokens(B,"."),
	butil:toint(AN) < butil:toint(BN).
is_dbfile(Nm) ->
	case lists:reverse(Nm) of
		"law-"++_ ->
			false;
		"mret-"++_ ->
			false;
		"mhs-"++_ ->
			false;
		_ ->
			true
	end.
