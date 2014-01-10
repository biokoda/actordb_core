-module(actordb_core).
-compile(export_all).
-include("actordb.hrl").

% manually closes all open db handles
stop() ->
	% Artificially increase callcount so it never reaches lower than high watermark
	% Wait untill call size is 0, which will mean all requests have been handled.
	actordb_backpressure:inc_callcount(1000000),
	% Max wait 30s
	wait_done_queries(30000),
	application:stop(actordb).
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
			case [ok || {_,false} <- actordb_local:get_mupdaters_state()] of
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

start() ->
	?AINF("Starting actordb"),
	application:start(actordb_core).
start(_Type, _Args) ->
	application:ensure_started(sasl),
	application:ensure_started(os_mon),
	application:ensure_started(yamerl),
	?AINF("Starting actordb ~p",[_Args]),
	case butil:is_app_running(bkdcore) of
		false ->
			Args = init:get_arguments(),
			% application:set_env(bkdcore,startapps,[actordb]),
			?AINF("Starting actordb ~p ~p",[butil:ds_val(config,Args),file:get_cwd()]),
			% Read args file manually to get paths for state.
			case butil:ds_val(config,Args) of
				undefined ->
					?AERR("No app.config file in parameters! ~p",[init:get_arguments()]),
					init:stop();
					% case [hd(tl(V)) || {bkdcore, V} <- Args, hd(V) == "main_db_folder"] of
					% 	[Path] ->
					% 		ok;
					% 	_ ->
					% 		ok
					% end;
				Cfgfile ->
					case catch file:consult(Cfgfile) of
						{ok,[L]} ->
							ActorParam = butil:ds_val(actordb,L),
							[Main,Extra,Level,Journal,Sync] = butil:ds_vals([main_db_folder,extra_db_folders,level_size,
																				journal_mode,sync],ActorParam,
																			["db",[],0,wal,0]),
							Statep = butil:expand_path(butil:tolist(Main)),
							?AINF("State path ~p, ~p",[Main,Statep]),
							% No etc folder. config files are provided manually.
							BkdcoreParam = butil:ds_val(bkdcore,L),
							case butil:ds_val(etc,BkdcoreParam) of
								undefined ->
									application:set_env(bkdcore,etc,none);
								_ ->
									ok
							end,
							application:set_env(bkdcore,statepath,Statep),
							% My proto config
							MyActorCfg = butil:ds_val(myactor,L),
							case butil:ds_val(enabled,MyActorCfg) of
								MpEnabled when MpEnabled == false; MpEnabled == undefined ->
									application:set_env(myactor,enabled,false);
								MpEnabled ->
									application:set_env(myactor,port,butil:ds_val(port,MyActorCfg,undefined)),
									application:set_env(myactor,enabled,MpEnabled)
							end,
							actordb_util:createcfg(Main,Extra,Level,Journal,butil:tobin(Sync));
						Err ->
							?AERR("Config invalid ~p~n~p ~p",[init:get_arguments(),Err,Cfgfile]),
							init:stop()
					end
			end,
			% Ensure folders exist.
			[begin
				case filelib:ensure_dir(F++"/actors/") of
					ok ->
						ok;
					Errx1 ->
						throw({path_invalid,F++"/actors/",Errx1})
				end,
				case  filelib:ensure_dir(F++"/shards/") of
					ok -> 
						ok;
					Errx2 ->
						throw({path_invalid,F++"/shards/",Errx2})
				end
			 end || F <- actordb_conf:paths()],

			% Start dependencies
			application:start(esqlite),
			application:start(lager),						
			bkdcore:start([
							{cfg,"schema.yaml",[{autoload,true},
												{mod,actordb_schema},
												{preload,{actordb_util,parse_cfg_schema,[]}},
												{onload,{actordb,schema_changed,[]}}]}
						]),
			butil:wait_for_app(bkdcore);
		true ->
			ok
	end,
	Res = actordb_sup:start_link(),
	case application:get_env(myactor,enabled) of
		{ok,true} ->
			?AINF("Starting myactor mysql server acceptor"),
			application:start(myactor);
		_ ->					
			ok		
	end,
	Res.

stop(_State) ->
	ok.

