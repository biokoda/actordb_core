-module(actordb_core).
-compile(export_all).
-include("actordb.hrl").
-include_lib("kernel/include/file.hrl").

% manually closes all open db handles
stop() ->
	% Artificially increase callcount so it never reaches lower than high watermark
	% Wait untill call size is 0, which will mean all requests have been handled.
	actordb_backpressure:inc_callcount(1000000),
	% Max wait 30s
	wait_done_queries(30000),
	application:stop(actordb_core),
	actordb_election:stop().
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

wait_distreg_procs() ->
	case distreg:processes() of
		[] ->
			ok;
		_ ->
			timer:sleep(1000),
			wait_distreg_procs()
	end.

start_ready() ->
	?AINF("Start ready."),
	application:set_env(actordb_core,isready,true),
	% actordb_termstore:start(),
	case application:get_env(actordb_core,thrift_port) of
		{ok,ThriftPort} when ThriftPort > 0 ->
			{ok,_} = adbt:start(ThriftPort);
		_ ->
			ok
	end,
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
					TransOpts = case get_network_interface() of
						[] -> TransOpts0;
						IP -> [{ip,IP}|TransOpts0]
					end,
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
	[Main,Extra,Sync1,NumMngrs,QueryTimeout1,Repl] =
		butil:ds_vals([main_db_folder,extra_db_folders,
						sync,num_transaction_managers,
						query_timeout,max_replication_space],ActorParam,
						["db",[],true,12,60000,{5000,0.1}]),
	case QueryTimeout1 of
		0 ->
			QueryTimeout = infinity;
		QueryTimeout ->
			ok
	end,
	Sync = case Sync1 of
		0 ->
			0;
		1 ->
			1;
		on ->
			1;
		off ->
			0;
		true ->
			1;
		false ->
			0;
		_ when is_integer(Sync1) ->
			1
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
	% case filelib:wildcard(Main++"/*.wal") of
	% 	[] ->
	% 		case filelib:wildcard(Main++"/shards/*-wal") of
	% 			[] ->
	% 				Driver = actordb_driver;
	% 			_ ->
	% 				Driver = esqlite3
	% 		end;
	% 	_ ->
	% 		Driver = actordb_driver
	% end,
	actordb_util:createcfg(Main,Extra,Sync,QueryTimeout,Repl,Name),
	ensure_folders(actordb_conf:paths()),
	ok = actordb_driver:init({list_to_tuple(actordb_conf:paths()),actordb_sqlprocutil:static_sqls(),Sync}),
	emurmur3:init().

start() ->
	% ?AINF("Starting actordb"),
	application:start(actordb_core).

start(_Type, _Args) ->
	prestart(),
	bkdcore:start(actordb:configfiles()),
	butil:wait_for_app(bkdcore),

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
							import_legacy(actordb_conf:paths()),
							StateStart = normal;
						_ ->
							StateStart = wait
					end;
				[_|_] ->
					StateStart = normal
			end
	end,

	Res = actordb_sup:start_link(),

	% ?AINF("Starting sharedstate type ~p",[StateStart]),
	case StateStart of
		normal ->
			actordb_sharedstate:start(?STATE_NM_GLOBAL,?STATE_TYPE,[{slave,false},create]);
		wait ->
			actordb_sharedstate:start_wait(?STATE_NM_GLOBAL,?STATE_TYPE)
	end,
	Res.

stop(_State) ->
	ok.

ensure_folders([])->
	ok;
ensure_folders([H|T])->
	ok = filelib:ensure_dir(H++"/"),
	ensure_folders(T).

get_network_interface()->
	case application:get_env(actordb_core, network_interface) of
	  {ok, Value} ->
		  case inet:parse_address(Value) of
			{ok, IPAddress} -> ok;
			_ ->
			  {ok, {hostent, _, [], inet, _, [IPAddress]}} = inet:gethostbyname(Value)
		  end;
		_ ->
		  case string:tokens(atom_to_list(node()), "@") of
			["nonode","nohost"] -> IPAddress = {127,0,0,1};
			[_Name, Value] ->
			  case inet:parse_address(Value) of
				{ok, IPAddress} -> ok;
				_ ->
				  {ok, Hostname} = inet:gethostname(),
				  {ok, {hostent, _, [], inet, _, [IPAddress]}} = inet:gethostbyname(Hostname)
			  end
		  end
	end,
	{ok, Addresses} = inet:getif(),
	case lists:keyfind(IPAddress, 1, Addresses) of
	  false ->
		[];
	  _ ->
		IPAddress
	end.

import_legacy([H|T]) ->
	case file:read_file_info(H++"/termstore") of
		{ok,_} ->
			import_db(H++"/termstore","termstore");
		_ ->
			ok
	end,
	Actors = [A || A <- element(2,file:list_dir(H++"/actors")), is_dbfile(A)],
	Shards = [A || A <- element(2,file:list_dir(H++"/shards")), is_dbfile(A)],
	State = [A || A <- element(2,file:list_dir(H++"/state")), is_dbfile(A)],
	% {ok,Actors} = file:list_dir(H++"/actors"),
	import_dbs(H,"actors",Actors),
	import_dbs(H,"shards",Shards),
	import_dbs(H,butil:tolist("state"),State),
	import_legacy(T);
import_legacy([]) ->
	ok.


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

import_dbs(Root,Sub,[H|T]) ->
	?AINF("~p",[H]),
	import_db(Root++"/"++Sub++"/"++H,Sub++"/"++H),
	import_dbs(Root,Sub,T);
import_dbs(_,_,[]) ->
	ok.

import_db(Pth,Name) ->
	{ok,F} = file:open(Pth,[read,binary,raw]),
	{ok,Db} = actordb_driver:open(Name,actordb_util:hash(Name),wal),
	Size = filelib:file_size(Pth),
	cp_dbfile(F,Db,1,Size),
	file:close(F),
	{ok,X} = actordb_driver:exec_script("select * from sqlite_master;",Db),
	?AINF("~p",[X]),
	ok.

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
