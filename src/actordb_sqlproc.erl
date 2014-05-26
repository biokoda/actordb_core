% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_sqlproc).
-behaviour(gen_server).
-define(LAGERDBG,true).
-export([start/1, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([print_info/1]).
-export([read/4,write/4,call/4,call/5,diepls/2,try_actornum/3]).
-export([call_slave/4,call_slave/5,start_copylock/2]). %call_master/4,call_master/5
-export([write_call/3]).
-include_lib("actordb_sqlproc.hrl").

% Read actor number without creating actor.
try_actornum(Name,Type,CbMod) ->
	case call({Name,Type},[actornum],{state_rw,actornum},CbMod) of
		{error,nocreate} ->
			{"",undefined};
		{ok,Path,NumNow} ->
			{Path,NumNow}
	end.

read(Name,Flags,[{copy,CopyFrom}],Start) ->
	case distreg:whereis(Name) of
		undefined ->
			case call(Name,Flags,{read,<<"select * from __adb limit 1;">>},Start) of
				{ok,_} ->
					{ok,[{columns,{<<"status">>}},{row,{<<"ok">>}}]};
				_E ->
					?AERR("Unable to copy actor ~p to ~p",[CopyFrom,Name]),
					{ok,[{columns,{<<"status">>}},{row,{<<"failed">>}}]}
			end;
		Pid ->
			diepls(Pid,overwrite),
			Ref = erlang:monitor(process,Pid),
			receive
				{'DOWN',Ref,_,_Pid,_} ->
					read(Name,Flags,[{copy,CopyFrom}],Start)
				after 2000 ->
					{ok,[{columns,{<<"status">>}},{row,{<<"failed_running">>}}]}
			end
	end;
read(Name,Flags,[delete],Start) ->
	call(Name,Flags,{write,{undefined,0,delete,undefined}},Start);
read(Name,Flags,Sql,Start) ->
	call(Name,Flags,{read,Sql},Start).

write(Name,Flags,{{_,_,_} = TransactionId,Sql},Start) ->
	write(Name,Flags,{undefined,TransactionId,Sql},Start);
write(Name,Flags,{MFA,TransactionId,Sql},Start) ->
	case TransactionId of
		{_,_,_} ->
			case Sql of
				commit ->
					call(Name,Flags,{commit,true,TransactionId},Start);
				abort ->
					call(Name,Flags,{commit,false,TransactionId},Start);
				[delete] ->
					call(Name,Flags,{write,{MFA,delete,TransactionId}},Start);
				_ ->
					call(Name,Flags,{write,{MFA,iolist_to_binary(Sql),TransactionId}},Start)
			end;
		_ when Sql == undefined ->
			call(Name,Flags,{write,{MFA,undefined,undefined}},Start);
		_ ->
			call(Name,Flags,{write,{MFA,iolist_to_binary(Sql),undefined}},Start)
	end;
write(Name,Flags,[delete],Start) ->
	call(Name,Flags,{write,{undefined,delete,undefined}},Start);
write(Name,Flags,Sql,Start) ->
	call(Name,Flags,{write,{undefined,iolist_to_binary(Sql),undefined}},Start).


call(Name,Flags,Msg,Start) ->
	call(Name,Flags,Msg,Start,false).
call(Name,Flags,Msg,Start,IsRedirect) ->
	case distreg:whereis(Name) of
		undefined ->
			case startactor(Name,Start,[{startreason,Msg}|Flags]) of %
				{ok,Pid} when is_pid(Pid) ->
					call(Name,Flags,Msg,Start,IsRedirect,Pid);
				{error,nocreate} ->
					{error,nocreate};
				Res ->
					Res
			end;
		Pid ->
			% ?AINF("Call have pid ~p for name ~p, alive ~p",[Pid,Name,erlang:is_process_alive(Pid)]),
			call(Name,Flags,Msg,Start,IsRedirect,Pid)

	end.
call(Name,Flags,Msg,Start,IsRedirect,Pid) ->
	% If call returns redirect, this is slave node not master node.
	case catch gen_server:call(Pid,Msg,infinity) of
		{redirect,Node} when is_binary(Node) ->
			case lists:member(Node,bkdcore:cluster_nodes()) of
				true ->
					case IsRedirect of
						true ->
							double_redirect;
						false ->
							case actordb:rpc(Node,element(1,Name),{?MODULE,call,[Name,Flags,Msg,Start,true]}) of
								double_redirect ->
									diepls(Pid,nomaster),
									call(Name,Flags,Msg,Start);
								Res ->
									Res
							end
					end;
				false ->
					actordb:rpc(Node,element(1,Name),{?MODULE,call,[Name,Flags,Msg,Start,false]})
			end;
		{'EXIT',{noproc,_}} = _X  ->
			?ADBG("noproc call again ~p",[_X]),
			call(Name,Flags,Msg,Start);
		{'EXIT',{normal,_}} ->
			?ADBG("died normal"),
			call(Name,Flags,Msg,Start);
		{'EXIT',{nocreate,_}} ->
			{error,nocreate};
		Res ->
			Res
	end.
startactor(Name,Start,Flags) ->
	case Start of
		{Mod,Func,Args} ->
			apply(Mod,Func,[Name|Args]);
		undefined ->
			{ok,undefined};
		_ ->
			apply(Start,start,[Name,Flags])
	end.

% call_master(Cb,Actor,Type,Msg) ->
% 	call_master(Cb,Actor,Type,Msg,[]).
% call_master(Cb,Actor,Type,Msg,Flags) ->
% 	case apply(Cb,start,[Actor,Type,[{startreason,Msg}|Flags]]) of %
% 		{ok,Pid} ->
% 			ok;
% 		Pid when is_pid(Pid) ->
% 			ok
% 	end,
% 	% ?AINF("Callmaster ~p ~p",[Actor,Msg]),
% 	case catch gen_server:call(Pid,Msg,infinity) of
% 		{'EXIT',{noproc,_}} ->
% 			call_master(Cb,Actor,Type,Msg);
% 		{redirect,Nd} ->

% 		Res ->
% 			Res
% 	end.

call_slave(Cb,Actor,Type,Msg) ->
	call_slave(Cb,Actor,Type,Msg,[]).
call_slave(Cb,Actor,Type,Msg,Flags) ->
	case apply(Cb,cb_slave_pid,[Actor,Type,[{startreason,Msg}|Flags]]) of %
		{ok,Pid} ->
			ok;
		Pid when is_pid(Pid) ->
			ok
	end,
	case catch gen_server:call(Pid,Msg,infinity) of
		{'EXIT',{noproc,_}} ->
			call_slave(Cb,Actor,Type,Msg);
		{'EXIT',{normal,_}} ->
			call_slave(Cb,Actor,Type,Msg);
		Res ->
			Res
	end.

diepls(Pid,Reason) ->
	gen_server:cast(Pid,{diepls,Reason}).

start_copylock(Fullname,O) ->
	start_copylock(Fullname,O,0).
start_copylock(Fullname,Opt,N) when N < 2 ->
	case distreg:whereis(Fullname) of
		undefined ->
			start(Opt);
		_ ->
			timer:sleep(1000),
			start_copylock(Fullname,Opt,N+1)
	end;
start_copylock(Fullname,_,_) ->
	Pid = distreg:whereis(Fullname),
	print_info(Pid),
	{error,{slave_proc_running,Pid,Fullname}}.

% Opts:
% [{actor,Name},{type,Type},{mod,CallbackModule},{state,CallbackState},
%  {inactivity_timeout,SecondsOrInfinity},{slave,true/false},{copyfrom,NodeName},{copyreset,{Mod,Func,Args}}]
start(Opts) ->
	?ADBG("Starting ~p ~p",[butil:ds_vals([actor,type],Opts),butil:ds_val(slave,Opts)]),
	Ref = make_ref(),
	case gen_server:start(?MODULE, [{start_from,{self(),Ref}}|Opts], []) of
		{ok,Pid} ->
			{ok,Pid};
		{error,normal} ->
			% Init failed gracefully. It should have sent an explanation. 
			receive
				{Ref,nocreate} ->
					{error,nocreate};
				{Ref,{registered,Pid}} ->
					{ok,Pid};
				{Ref,{actornum,Path,Num}} ->
					{ok,Path,Num};
				{Ref,{ok,[{columns,_},_]} = Res} ->
					Res
				after 0 ->
					{error,cantstart}
			end;
		Err ->
			?AERR("start sqlproc error ~p",[Err]),
			Err
	end.

stop(Pid) when is_pid(Pid) ->
	Pid ! stop;
stop(Name) ->
	case distreg:whereis(Name) of
		undefined ->
			ok;
		Pid ->
			stop(Pid)
	end.

print_info(Pid) ->
	gen_server:cast(Pid,print_info).



handle_call(_Msg,_,#dp{movedtonode = <<_/binary>>} = P) ->
	?ADBG("REDIRECT BECAUSE MOVED TO NODE ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.movedtonode,_Msg]),
	{reply,{redirect,P#dp.movedtonode},P#dp{activity = make_ref()}};
handle_call({dbcopy,Msg},CallFrom,P) ->
	actordb_sqlprocutil:dbcopy_call(Msg,CallFrom,P);
handle_call({state_rw,What},From,P) ->
	state_rw_call(What,From,P#dp{activity = make_ref()});
handle_call({commit,Doit,Id},From, P) ->
	commit_call(Doit,Id,From,P);
handle_call(Msg,From,P) ->
	case Msg of
		_ when P#dp.mors == slave ->
			case P#dp.masternode of
				undefined ->
					{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue)}};
				_ ->
					actordb_sqlprocutil:redirect_master(P)
			end;
		_ when P#dp.verified == false ->
			{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue)}};
		{write,{_,_,TransactionId} = Msg1} when P#dp.transactionid == TransactionId, P#dp.transactionid /= undefined ->
			write_call(Msg1,From,check_timer(P#dp{activity = make_ref()}));
		_ when P#dp.callres /= undefined; P#dp.locked /= []; P#dp.transactionid /= undefined ->
			{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue)}};
		{write,Msg1} ->
			write_call(Msg1,From,check_timer(P#dp{activity = make_ref()}));
		{read,Msg1} ->
			read_call(Msg1,From,check_timer(P#dp{activity = make_ref()}));
		{move,NewShard,Node,CopyReset,CbState} ->
			% Call to move this actor to another cluster. 
			% First store the intent to move with all needed data. This way even if a node chrashes, the actor will attempt to move
			%  on next startup.
			% When write done, reply to caller and start with move process (in ..util:reply_maybe.
			Sql = <<"$INSERT INTO __adb (id,val) VALUES (",?COPYFROM/binary,",'",
						(base64:encode(term_to_binary({{move,NewShard,Node},CopyReset,CbState})))/binary,"');">>,
			write_call({undefined,Sql,undefined},{exec,From,{move,Node}},check_timer(P));
		{split,MFA,Node,OldActor,NewActor,CopyReset,CbState} ->
			% Similar to above. Both have just insert and not insert and replace because
			%  we can only do one move/split at a time. It makes no sense to do both at the same time.
			% So rely on DB to return error for these conflicting calls.
			Sql = <<"$INSERT INTO __adb (id,val) VALUES (",?COPYFROM/binary,",'",
						(base64:encode(term_to_binary({{split,MFA,Node,OldActor,NewActor},CopyReset,CbState})))/binary,"');">>,
			write_call({undefined,Sql,undefined},{exec,From,{split,MFA,Node,OldActor,NewActor}},check_timer(P));
		{copy,{Node,OldActor,NewActor}} ->
			Ref = make_ref(),
			case actordb:rpc(Node,NewActor,{?MODULE,call,[{NewActor,P#dp.actortype},[{lockinfo,wait}],
							{dbcopy,{start_receive,{actordb_conf:node_name(),OldActor},Ref}},P#dp.cbmod]}) of
				ok ->
					actordb_sqlprocutil:dbcopy_call({send_db,{Node,Ref,false,NewActor}},From,check_timer(P));
				Err ->
					{reply, Err,P}
			end;
		stop ->
			{stop, shutdown, stopped, P};
		Msg ->
			?DBG("cb_call ~p",[{P#dp.cbmod,Msg}]),
			case apply(P#dp.cbmod,cb_call,[Msg,From,P#dp.cbstate]) of
				{reply,Resp,S} ->
					{reply,Resp,P#dp{cbstate = S}};
				{reply,Resp} ->
					{reply,Resp,P}
			end
	end.

% If we are not ready to process calls atm (in the middle of a write or db not verified yet). Queue requests.
% handle_call(Msg,From,P) when P#dp.callfrom /= undefined; P#dp.verified /= true; 
% 								P#dp.transactionid /= undefined; P#dp.locked /= [] ->
% 	case Msg of
% 		{write,{_,_,_,TransactionId} = Msg1} when P#dp.transactionid == TransactionId, P#dp.transactionid /= undefined ->
% 			write_call(Msg1,From,P);
% 		% _ when element(1,Msg) == replicate_start, P#dp.mors == master, P#dp.verified ->
% 		% 	reply(From,reinit),
% 		% 	{ok,NP} = init(P,replicate_conflict),
% 		% 	{noreply,NP};
% 		% _ when element(1,Msg) == replicate_start, P#dp.copyproc /= undefined ->
% 		% 	{reply,notready,P};
% 		_ ->
% 			?DBG("Queueing msg ~p ~p, because ~p",[Msg,P#dp.mors,{P#dp.callfrom,P#dp.verified,P#dp.transactionid}]),
% 			{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue), activity = P#dp.activity+1}}
% 	end;
% handle_call({read,Msg},From,P) ->
% 	read_call(Msg,From,P#dp{activity = P#dp.activity + 1});
% handle_call({write,Msg},From, #dp{mors = master} = P) ->
% 	write_call(Msg,From,P#dp{activity = P#dp.activity + 1});
% handle_call({write,_},_,#dp{mors = slave} = P) ->
% 	?DBG("Redirect not master ~p",[P#dp.masternode]),
% 	actordb_sqlprocutil:redirect_master(P);


commit_call(Doit,Id,From,P) ->
	?ADBG("Commit ~p doit=~p, id=~p, from=~p, trans=~p",[{P#dp.actorname,P#dp.actortype},Doit,Id,From,P#dp.transactionid]),
	case P#dp.transactionid == Id of
		true ->
			case P#dp.transactioncheckref of
				undefined ->
					ok;
				_ ->
					erlang:demonitor(P#dp.transactioncheckref)
			end,
			?ADBG("Commit write ~p",[P#dp.transactioninfo]),
			{Sql,EvNum,_NewVers} = P#dp.transactioninfo,
			case Doit of
				true when P#dp.follower_indexes == [] ->
					case Sql of
						delete ->
							actordb_sqlprocutil:delete_actor(P),
							reply(From,ok),
							{stop,normal,P};
						_ ->
							ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"RELEASE SAVEPOINT 'adb';">>)),
							{reply,ok,P#dp{transactionid = undefined,transactioncheckref = undefined,
									 transactioninfo = undefined, activity = make_ref(),
									 evnum = EvNum, evterm = P#dp.current_term}}
					end;
				true ->
					case Sql of
						<<"delete">> ->
							actordb_sqlprocutil:delete_actor(P),
							reply(From,ok),
							?ADBG("Stopping ~p",[{P#dp.actorname,P#dp.actortype}]),
							{stop,normal,P};
						_ ->
							% We can safely release savepoint.
							% This will send the remaining WAL pages to followers that have commit flag set.
							% Followers will then rpc back appendentries_response.
							% We can also set #dp.evnum now.
							ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"RELEASE SAVEPOINT 'adb';">>,
														P#dp.evterm,EvNum,<<>>)),
							{noreply,P#dp{callfrom = From, activity = make_ref(),
										  callres = ok,evnum = EvNum,
										  follower_indexes = update_followers(P#dp.follower_indexes),
										 transactionid = undefined, transactioninfo = undefined,transactioncheckref = undefined}}
					end;
				false when P#dp.follower_indexes == [] ->
					actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
					{reply,ok,doqueue(P#dp{transactionid = undefined, transactioninfo = undefined,
									transactioncheckref = undefined,activity = make_ref()})};
				false ->
					% Transaction failed.
					% Delete it from __transactions.
					% EvNum will actually be the same as transactionsql that we have not finished.
					%  Thus this EvNum section of WAL contains pages from failed transaction and 
					%  cleanup of transaction from __transactions.
					{Tid,Updaterid,_} = P#dp.transactionid,
					case Sql of
						<<"delete">> ->
							ok;
						_ ->
							actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>,P#dp.evterm,P#dp.evnum,<<>>)
					end,
					NewSql = <<"DELETE FROM __transactions WHERE tid=",(butil:tobin(Tid))/binary," AND updater=",
										(butil:tobin(Updaterid))/binary,";">>,
					write_call({undefined,NewSql,undefined},From,P#dp{callfrom = undefined,
										transactionid = undefined,transactioninfo = undefined,transactioncheckref = undefined})
			end;
		_ ->
			{reply,ok,P}
	end.


state_rw_call(What,From,P) ->
	case What of
		% verifyinfo ->
		% 	{reply,{ok,bkdcore:node_name(),P#dp.evcrc,P#dp.evnum,{P#dp.mors,P#dp.verified}},P};
		actornum ->
			case P#dp.mors of
				master ->
					{reply,{ok,P#dp.dbpath,actordb_sqlprocutil:read_num(P)},P};
				slave when P#dp.masternode /= undefined ->
					actordb_sqlprocutil:redirect_master(P);
				slave ->
					{noreply, P#dp{callqueue = queue:in_r({From,{state_rw,What}},P#dp.callqueue)}}
			end;
		donothing ->
			{reply,ok,P};
		% Executed on follower.
		% AE is split into multiple calls (because wal is sent page by page as it is written)
		% Start sets parameters. There may not be any wal append calls after if empty write.
		% AEType = [head,empty,recover]
		{appendentries_start,Term,LeaderNode,PrevEvnum,PrevTerm,AEType} ->
			?ADBG("AE start ~p ~p ~p",[{P#dp.actorname,P#dp.actortype,AEType},{PrevEvnum,PrevTerm},LeaderNode]),
			case ok of
				_ when Term < P#dp.current_term ->
					?AERR("AE start, input term too old ~p ~p",[{P#dp.actorname,P#dp.actortype,AEType},{Term,P#dp.current_term}]),
					reply(From,false),
					actordb_sqlprocutil:ae_respond(P,LeaderNode,false,PrevEvnum,AEType),
					% Some node thinks its master and sent us appendentries start.
					% Because we are master with higher term, we turn it down.
					% But we also start a new election so that nodes get synchronized.
					case P#dp.mors of
						master ->
							{noreply, actordb_sqlprocutil:start_verify(P,false)};
						_ ->
							{noreply,P}
					end;
				_ when P#dp.mors == slave, P#dp.masternode == undefined ->
					?ADBG("AE start, slave now knows leader ~p ~p",[{P#dp.actorname,P#dp.actortype,AEType},LeaderNode]),
					case P#dp.callres /= undefined of
						true ->
							reply(P#dp.callfrom,{redirect,LeaderNode});
						false ->
							ok
					end,
					actordb_local:actor_mors(slave,LeaderNode),
					state_rw_call(What,From,doqueue(actordb_sqlprocutil:reopen_db(P#dp{masternode = LeaderNode, 
															masternodedist = bkdcore:dist_name(LeaderNode), 
															callfrom = undefined, callres = undefined, 
															verified = true, activity = make_ref()})));
				% This node is candidate or leader but someone with newer term is sending us log
				_ when P#dp.mors == master ->
					?AERR("AE start, stepping down as leader ~p ~p",
							[{P#dp.actorname,P#dp.actortype,AEType},{Term,P#dp.current_term}]),
					case P#dp.callres /= undefined of
						true ->
							reply(P#dp.callfrom,{redirect,LeaderNode});
						false ->
							ok
					end,
					state_rw_call(What,From,
									doqueue(actordb_sqlprocutil:save_term(actordb_sqlprocutil:reopen_db(
												P#dp{mors = slave, verified = true, 
													voted_for = undefined,callfrom = undefined, callres = undefined,
													masternode = LeaderNode,activity = make_ref(),
													masternodedist = bkdcore:dist_name(LeaderNode),
													current_term = Term}))));
				_ when P#dp.evnum /= PrevEvnum; P#dp.evterm /= PrevTerm ->
					?AERR("AE start, evnum evterm do not match ~p, {MyEvnum,MyTerm}=~p, {InNum,InTerm}=~p",
								[{P#dp.actorname,P#dp.actortype,AEType},{P#dp.evnum,P#dp.evterm},{PrevEvnum,PrevTerm}]),
					case P#dp.evnum > PrevEvnum andalso PrevEvnum > 0 of
						% Node is conflicted, delete last entry
						true when AEType /= empty ->
							NP = actordb_sqlprocutil:rewind_wal(P);
						% If false this node is behind. If empty this is just check call.
						% Wait for leader to send an earlier event.
						_ ->
							NP = P
					end,
					reply(From,false),
					actordb_sqlprocutil:ae_respond(NP,LeaderNode,false,PrevEvnum,AEType),
					{noreply,NP#dp{activity = make_ref()}};
				_ when Term > P#dp.current_term ->
					?AERR("AE start, my term out of date ~p ~p",[{P#dp.actorname,P#dp.actortype,AEType},{Term,P#dp.current_term}]),
					state_rw_call(What,From,actordb_sqlprocutil:save_term(
												P#dp{current_term = Term,voted_for = undefined,
												 masternode = LeaderNode,verified = true,activity = make_ref(),
												 masternodedist = bkdcore:dist_name(LeaderNode)}));
				_ when AEType == empty ->
					?ADBG("AE start, ok for empty ~p",[{P#dp.actorname,P#dp.actortype,AEType}]),
					reply(From,ok),
					actordb_sqlprocutil:ae_respond(P,LeaderNode,true,PrevEvnum,AEType),
					{noreply,P#dp{verified = true,activity = make_ref()}};
				% Ok, now it will start receiving wal pages
				_ ->
					{reply,ok,P#dp{verified = true,activity = make_ref()}}
			end;
		% Executed on follower.
		% sqlite wal, header tells you if done (it has db size in header)
		{appendentries_wal,Term,Header,Body,AEType} ->
			case ok of
				_ when Term == P#dp.current_term ->
					actordb_sqlprocutil:append_wal(P,Header,Body),
					case Header of
						% dbsize == 0, not last page
						<<_:32,0:32,_/binary>> ->
							{reply,ok,P#dp{activity = make_ref()}};
						% last page
						<<_:32,_:32,Evnum:64/unsigned-big,Evterm:64/unsigned-big,_/binary>> ->
							?ADBG("AE WAL done ~p ~p ~p ~p",
									[{P#dp.actorname,P#dp.actortype},Evnum,AEType,queue:is_empty(P#dp.callqueue)]),
							NP = P#dp{evnum = Evnum, evterm = Evterm,activity = make_ref()},
							reply(From,ok),
							actordb_sqlprocutil:ae_respond(NP,NP#dp.masternode,true,P#dp.evnum,AEType),
							{noreply,NP}
					end;
				_ ->
					?AERR("AE WAL received wrong term ~p ~p",[{P#dp.actorname,P#dp.actortype},{Term,P#dp.current_term}]),
					reply(From,false),
					actordb_sqlprocutil:ae_respond(P,P#dp.masternode,false,AEType),
					{noreply,P}
			end;
		% Executed on leader.
		{appendentries_response,Node,CurrentTerm,Success,EvNum,EvTerm,MatchEvnum,AEType} ->
			Follower = lists:keyfind(Node,#flw.node,P#dp.follower_indexes),
			case Follower of
				false ->
					state_rw_call(What,From,actordb_sqlprocutil:store_follower(P,#flw{node = Node}));
				_ ->
					?ADBG("AE response ~p, from=~p, success=~p, type=~p, {PrevEvnum,EvNum,Match}=~p, {From,Res}=~p",
							[{P#dp.actorname,P#dp.actortype},Node,Success,AEType,
							 {Follower#flw.match_index,EvNum,MatchEvnum},{P#dp.callfrom,P#dp.callres}]),
					NFlw = Follower#flw{match_index = EvNum, match_term = EvTerm,next_index = EvNum+1,
											wait_for_response_since = undefined}, 
					case Success of
						% An earlier response.
						_ when P#dp.mors == slave ->
							{reply,ok,P};
						true ->
							reply(From,ok),
							NP = actordb_sqlprocutil:reply_maybe(actordb_sqlprocutil:continue_maybe(P,NFlw,AEType)),
							?ADBG("AE response for node ~p, processed=~p followers=~p",
									[{P#dp.actorname,P#dp.actortype},Node,[{F#flw.node,F#flw.next_index} || F <- NP#dp.follower_indexes]]),
							{noreply,doqueue(NP)};
						% What we thought was follower is ahead of us and we need to step down
						false when P#dp.current_term < CurrentTerm ->
							{reply,ok,actordb_sqlprocutil:reopen_db(actordb_sqlprocutil:save_term(
								P#dp{mors = slave,current_term = CurrentTerm,voted_for = undefined, follower_indexes = []}))};
						% In case of overlapping responses for appendentries rpc. We do not care about responses
						%  for appendentries with a match index different than current match index.
						false when Follower#flw.match_index /= MatchEvnum, Follower#flw.match_index > 0 ->
							{reply,ok,P};
						false ->
							case lists:keymember(Follower#flw.node,1,P#dp.dbcopy_to) of
								true ->
									{reply,ok,P};
								false ->
									case actordb_sqlprocutil:try_wal_recover(P,NFlw) of
										{false,NP,NF} ->
											?ADBG("Can not recover from log, sending entire db ~p",[{P#dp.actorname,P#dp.actortype}]),
											% We can not recover from wal. Send entire db.
											Ref = make_ref(),
											case bkdcore:rpc(NF#flw.node,{?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
																{dbcopy,{start_receive,actordb_conf:node_name(),Ref}}]}) of
												ok ->
													actordb_sqlprocutil:dbcopy_call({send_db,{NF#flw.node,Ref,false,P#dp.actorname}},
																					From,NP);
												_ ->
													{reply,false,P}
											end;
										{true,NP,NF} ->
											% we can recover from wal
											?ADBG("Recovering from wal ~p, node=~p, {HisIndex,MyMaxIndex}=~p",
													[{P#dp.actorname,P#dp.actortype},NF#flw.node,{NF#flw.match_index,P#dp.evnum}]),
											reply(From,ok),
											{noreply,actordb_sqlprocutil:continue_maybe(NP,NF,recover)}
									end
							end
					end
			end;
		{request_vote,Candidate,NewTerm,LastTerm,LastEvnum} ->
			?ADBG("Request vote on=~p for=~p, {histerm,myterm}=~p",
					[{P#dp.actorname,P#dp.actortype},Candidate,{NewTerm,P#dp.current_term}]),
			Now = os:timestamp(),
			Uptodate = 
				case ok of
					_ when P#dp.evterm < LastTerm ->
						true;
					_ when P#dp.evterm > LastTerm ->
						false;
					_ when P#dp.evnum < LastEvnum ->
						true;
					_ when P#dp.evnum > LastEvnum ->
						false;
					_ ->
						true
				end,
			case ok of
				% Candidates term is lower than current_term, ignore.
				_ when NewTerm < P#dp.current_term ->
					DoElection = P#dp.mors == master,
					reply(From,{outofdate,actordb_conf:node_name(),P#dp.current_term,DoElection}),
					NP = P#dp{election = Now};
				% We've already seen this term, only vote yes if we have not voted
				%  or have voted for this candidate already.
				_ when NewTerm == P#dp.current_term ->
					case (P#dp.voted_for == undefined orelse P#dp.voted_for == Candidate) of
						true when Uptodate ->
							DoElection = false,
							reply(From,{true,actordb_conf:node_name(),NewTerm,DoElection}),
							NP = actordb_sqlprocutil:save_term(P#dp{voted_for = Candidate, current_term = NewTerm, election = Now,
																masternode = undefined, masternodedist = undefined});
						true ->
							DoElection = P#dp.mors == master,
							reply(From,{outofdate,actordb_conf:node_name(),NewTerm,DoElection}),
							NP = actordb_sqlprocutil:save_term(P#dp{voted_for = undefined, current_term = NewTerm, election = Now});
						false ->
							DoElection = P#dp.mors == master,
							reply(From,{alreadyvoted,actordb_conf:node_name(),P#dp.current_term,DoElection}),
							NP = P
					end;
				% New candidates term is higher than ours, is he as up to date?
				_ when Uptodate ->
					DoElection = false,
					reply(From,{true,actordb_conf:node_name(),NewTerm,DoElection}),
					NP = actordb_sqlprocutil:save_term(P#dp{voted_for = Candidate, current_term = NewTerm, election = Now,
																masternode = undefined, masternodedist = undefined});
				% Higher term, but not as up to date. We can not vote for him.
				% We do have to remember new term index though.
				_ ->
					DoElection = P#dp.mors == master,
					reply(From,{outofdate,actordb_conf:node_name(),NewTerm,DoElection}),
					NP = actordb_sqlprocutil:save_term(P#dp{voted_for = undefined, current_term = NewTerm,election = Now})
			end,
			% If voted no and we are leader, start a new term, which causes a new write and gets all nodes synchronized.
			% If the other node is actually more up to date, vote was yes and we do not do election.
			case DoElection of
				true ->
					?ADBG("Do election to sync nodes ~p",[{P#dp.actorname,P#dp.actortype}]),
					{noreply,actordb_sqlprocutil:start_verify(NP,false)};
				false ->
					{noreply,NP#dp{activity = make_ref()}}
			end;
		{set_dbfile,Bin} ->
			ok = file:write_file(P#dp.dbpath,esqlite3:lz4_decompress(Bin,?PAGESIZE)),
			{reply,ok,P#dp{activity = make_ref()}};
		% Hint from a candidate that this node should start new election, because
		%  it is more up to date.
		doelection ->
			?ADBG("Doelection ~p ~p",[{P#dp.actorname,P#dp.actortype},{P#dp.verified,P#dp.election}]),
			reply(From,ok),
			case is_pid(P#dp.election) of
				false ->
					{noreply,actordb_sqlprocutil:start_verify(P,false)};
				_ ->
					{noreply,P}
			end;
		{delete,MovedToNode} ->
			reply(From,ok),
			actordb_sqlite:stop(P#dp.db),
			actordb_sqlprocutil:delactorfile(P#dp{movedtonode = MovedToNode}),
			{stop,normal,P#dp{db = undefined}};
		checkpoint ->
			actordb_sqlprocutil:do_checkpoint(P),
			{reply,ok,P}
	end.

read_call(Msg,From,P) ->
	case P#dp.mors of
		master when Msg == [exists] ->
			{reply,{ok,[{columns,{<<"exists">>}},{rows,[{<<"true">>}]}]},P};
		master ->
			case actordb_sqlprocutil:has_schema_updated(P,[]) of
				ok ->
					case Msg of	
						{Mod,Func,Args} ->
							case apply(Mod,Func,[P#dp.cbstate|Args]) of
								{reply,What,Sql,NS} ->
									{reply,{What,actordb_sqlite:exec(P#dp.db,Sql,read)},P#dp{cbstate = NS}};
								{reply,What,NS} ->
									{reply,What,P#dp{cbstate = NS}};
								{reply,What} ->
									{reply,What,P};
								{Sql,State} ->
									{reply,actordb_sqlite:exec(P#dp.db,Sql,read),P#dp{cbstate = State}};
								Sql ->
									{reply,actordb_sqlite:exec(P#dp.db,Sql,read),P}
							end;
						{Sql,{Mod,Func,Args}} ->
							case apply(Mod,Func,[actordb_sqlite:exec(P#dp.db,Sql,read)|Args]) of
								{write,Write} ->
									case Write of
										_ when is_binary(Write); is_list(Write) ->
											write_call({undefined,iolist_to_binary(Sql),undefined},From,P);
										{_,_,_} ->
											write_call({Write,undefined,undefined},From,P)
									end;
								{write,Write,NS} ->
									case Write of
										_ when is_binary(Write); is_list(Write) ->
											write_call({undefined,iolist_to_binary(Sql),undefined},
													   From,P#dp{cbstate = NS});
										{_,_,_} ->
											write_call({Write,undefined,undefined,undefined},From,P#dp{cbstate = NS})
									end;
								{reply,What,NS} ->
									{reply,What,P#dp{cbstate = NS}};
								{reply,What} ->
									{reply,What,P}
							end;
						Sql ->
							{reply,actordb_sqlite:exec(P#dp.db,Sql,read),P}
					end;
				% Schema has changed. Execute write on schema update.
				% Place this read in callqueue for later execution.
				{NewVers,Sql1} ->
					case write_call(Sql1,undefined,undefined,NewVers,P) of
						{reply,Reply,NP} ->
							case actordb_sqlite:okornot(Reply) of
								ok ->
									read_call(Msg,From,NP);
								Err ->
									{reply,Err,NP}
							end;
						{noreply,NP} ->
							{noreply,NP#dp{callqueue = queue:in_r({From,Msg},NP#dp.callqueue)}}
					end
			end;
		_ ->
			?DBG("redirect read ~p",[P#dp.masternode]),
			actordb_sqlprocutil:redirect_master(P)
	end.


write_call({MFA,Sql,Transaction},From,P) ->
	?ADBG("writecall ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.evnum]),
	case MFA of
		undefined ->
			case actordb_sqlprocutil:has_schema_updated(P,Sql) of
				ok ->
					write_call(Sql,Transaction,From,P#dp.schemavers,P);
				{NewVers,Sql1} ->
					write_call(Sql1,Transaction,From,NewVers,P)
			end;
		{Mod,Func,Args} ->
			case actordb_sqlprocutil:has_schema_updated(P,[]) of
				ok ->
					NewVers = P#dp.schemavers,
					SqlUpdate = [];
				{NewVers,SqlUpdate} ->
					ok
			end,
			case apply(Mod,Func,[P#dp.cbstate|Args]) of
				{reply,What,OutSql1,NS} ->
					reply(From,What),
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(OutSql,Transaction,undefined,NewVers,P#dp{cbstate = NS});
				{reply,What,NS} ->
					{reply,What,P#dp{cbstate = NS}};
				{reply,What} ->
					{reply,What,P};
				{OutSql1,State} ->
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(OutSql,Transaction,From,NewVers,P#dp{cbstate = State});
				OutSql1 ->
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(OutSql,Transaction,From,NewVers,P)
			end
	end.
% Not a multiactor transaction write
write_call(Sql,undefined,From,NewVers,P) ->
	EvNum = P#dp.evnum+1,
	% {ConnectedNodes,LenCluster,LenConnected} = nodes_for_replication(P),
	case Sql of
		delete ->
			actordb_sqlprocutil:delete_actor(P),
			reply(From,ok),
			{stop,normal,P};
		{moved,MovedTo} ->
			actordb_sqlprocutil:delete_actor(P#dp{movedtonode = MovedTo}),
			reply(From,ok),
			{stop,normal,P};
		_ ->
			ComplSql = 
					[<<"$SAVEPOINT 'adb';">>,
					 actordb_sqlprocutil:semicolon(Sql),
					 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
					 <<"$UPDATE __adb SET val='">>,butil:tobin(P#dp.current_term),<<"' WHERE id=">>,?EVTERM,";",
					 <<"$RELEASE SAVEPOINT 'adb';">>
					 ],
			% We are sending prevevnum and prevevterm, #dp.evterm is less than #dp.current_term only 
			%  when election is won and this is first write after it.
			case EvNum of
				1 ->
					{ok,Dbfile} = file:read_file(P#dp.dbpath),
					{Compressed,CompressedSize} = esqlite3:lz4_compress(Dbfile),
					<<DbCompressed:CompressedSize/binary,_/binary>> = Compressed,
					VarHeader = term_to_binary({P#dp.current_term,actordb_conf:node_name(),P#dp.evnum,P#dp.evterm,DbCompressed});
				_ ->
					VarHeader = term_to_binary({P#dp.current_term,actordb_conf:node_name(),P#dp.evnum,P#dp.evterm})
			end,
			Res = actordb_sqlite:exec(P#dp.db,ComplSql,P#dp.current_term,EvNum,VarHeader),
			case actordb_sqlite:okornot(Res) of
				ok ->
					?DBG("Write result ~p",[Res]),
					case ok of
						_ when P#dp.follower_indexes == [] ->
							{noreply,doqueue(actordb_sqlprocutil:reply_maybe(P#dp{callfrom = From, callres = Res,evnum = EvNum, 
																			schemavers = NewVers,evterm = P#dp.current_term},1,[]))};
						_ ->
							% reply on appendentries response or later if nodes are behind.
							{noreply, P#dp{callfrom = From, callres = Res, 
											follower_indexes = update_followers(P#dp.follower_indexes),
										evterm = P#dp.current_term, evnum = EvNum}}
					end;
				Resp ->
					actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
					{reply,Resp,P}
			end
	end;
write_call(Sql1,{Tid,Updaterid,Node} = TransactionId,From,NewVers,P) ->
	{_CheckPid,CheckRef} = actordb_sqlprocutil:start_transaction_checker(Tid,Updaterid,Node),
	?ADBG("Starting transaction ~p write id ~p, curtr ~p, sql ~p",
				[{P#dp.actorname,P#dp.actortype},TransactionId,P#dp.transactionid,Sql1]),
	case P#dp.follower_indexes of
		[] ->
			% If single node cluster, no need to store sql first.
			case P#dp.transactionid of
				TransactionId ->
					% Transaction can write to single actor more than once (especially for KV stores)
					% if we are already in this transaction, just update sql.
					{_OldSql,EvNum,_} = P#dp.transactioninfo,
					ComplSql = Sql1,
					Res = actordb_sqlite:exec(P#dp.db,ComplSql,write);
				undefined ->
					EvNum = P#dp.evnum+1,
					case Sql1 of
						delete ->
							Res = ok,
							ComplSql = delete;
						_ ->
							ComplSql = 
								[<<"$SAVEPOINT 'adb';">>,
								 actordb_sqlprocutil:semicolon(Sql1),
								 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
								 <<"$UPDATE __adb SET val='">>,butil:tobin(P#dp.current_term),<<"' WHERE id=">>,?EVTERM,";"
								 ],
							Res = actordb_sqlite:exec(P#dp.db,ComplSql,write)
					end
			end,
			case actordb_sqlite:okornot(Res) of
				ok ->
					?ADBG("Transaction ok"),
					{noreply, actordb_sqlprocutil:reply_maybe(P#dp{transactionid = TransactionId, 
								schemavers = NewVers,evterm = P#dp.current_term,
								transactioncheckref = CheckRef,
								transactioninfo = {ComplSql,EvNum,NewVers}, callfrom = From, callres = Res},1,[])};
				_Err ->
					ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)),
					erlang:demonitor(CheckRef),
					?ADBG("Transaction not ok ~p",[_Err]),
					{reply,Res,P#dp{activity = make_ref(), transactionid = undefined, evterm = P#dp.current_term}}
			end;
		_ ->
			EvNum = P#dp.evnum+1,
			case P#dp.transactionid of
				TransactionId ->
					% Rollback prev version of sql.
					ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)),
					{OldSql,_EvNum,NewVers} = P#dp.transactioninfo,
					% Combine prev sql with new one.
					Sql = iolist_to_binary([OldSql,Sql1]),
					TransactionInfo = [<<"$INSERT OR REPLACE INTO __transactions (id,tid,updater,node,schemavers,sql) VALUES (1,">>,
											(butil:tobin(Tid)),",",(butil:tobin(Updaterid)),",'",Node,"',",
								 				(butil:tobin(NewVers)),",",
								 				"'",(base64:encode(Sql)),"');"];
				_ ->
					case Sql1 of
						delete ->
							Sql = <<"delete">>;
						_ ->
							Sql = iolist_to_binary(Sql1)
					end,
					% First store transaction info. Then run actual sql of transaction.
					TransactionInfo = [<<"$INSERT INTO __transactions (id,tid,updater,node,schemavers,sql) VALUES (1,">>,
											(butil:tobin(Tid)),",",(butil:tobin(Updaterid)),",'",Node,"',",
											(butil:tobin(NewVers)),",",
								 				"'",(base64:encode(Sql)),"');"]
			end,
			ComplSql = 
					[<<"$SAVEPOINT 'adb';">>,
					 TransactionInfo,
					 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
					 <<"$UPDATE __adb SET val='">>,butil:tobin(P#dp.current_term),<<"' WHERE id=">>,?EVTERM,";",
					 <<"$RELEASE SAVEPOINT 'adb';">>
					 ],
			VarHeader = term_to_binary({P#dp.current_term,actordb_conf:node_name(),P#dp.evnum,P#dp.evterm}),
			ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,ComplSql,P#dp.current_term,EvNum,VarHeader)),
			{noreply,P#dp{callfrom = From,callres = undefined, evterm = P#dp.current_term,evnum = EvNum,
						  transactioninfo = {Sql,EvNum+1,NewVers},
						  follower_indexes = update_followers(P#dp.follower_indexes),
						  transactioncheckref = CheckRef,
						  transactionid = TransactionId}}
	end.

update_followers(L) ->
	Ref = make_ref(),
	[begin
		F#flw{wait_for_response_since = Ref}
	end || F <- L].




handle_cast({diepls,Reason},P) ->
	?ADBG("diepls ~p",[{P#dp.actorname,P#dp.actortype}]),
	case Reason of
		nomaster ->
			?AERR("Die because nomaster"),
			{stop,normal,P};
		_ ->
			case handle_info(check_inactivity,P) of
				{noreply,_,hibernate} ->
					% case apply(P#dp.cbmod,cb_candie,[P#dp.mors,P#dp.actorname,P#dp.actortype,P#dp.cbstate]) of
					% 	true ->
							?ADBG("req die ~p ~p ~p",[P#dp.actorname,P#dp.actortype,Reason]),
							{stop,normal,P};
					% 	false ->
					% 		?AINF("NOPE ~p",[P#dp.actorname]),
					% 		{noreply,P}
					% end;
				R ->
					R
			end
	end;
handle_cast(print_info,P) ->
	?AINF("~p~n",[?R2P(P)]),
	{noreply,P};
handle_cast(Msg,#dp{mors = master, verified = true} = P) ->
	case apply(P#dp.cbmod,cb_cast,[Msg,P#dp.cbstate]) of
		{noreply,S} ->
			{noreply,P#dp{cbstate = S}};
		noreply ->
			{noreply,P}
	end;
handle_cast(_Msg,P) ->
	?AINF("sqlproc ~p unhandled cast ~p~n",[P#dp.cbmod,_Msg]),
	{noreply,P}.


handle_info(doqueue, P) ->
	{noreply,doqueue(P)};
handle_info({'DOWN',Monitor,_,PID,Reason},P) ->
	down_info(PID,Monitor,Reason,P);
handle_info({inactivity_timer,N},P) ->
	handle_info({check_inactivity,N},P#dp{timerref = {undefined,N}});
handle_info({check_inactivity,N}, P) ->
	case check_inactivity(N,P) of
		{noreply, NP} ->
			{noreply,doqueue(NP)};
		R ->
			R
	end;
handle_info(check_inactivity, P) ->
	handle_info({check_inactivity,10},P);
handle_info(stop,P) ->
	handle_info({stop,normal},P);
handle_info({stop,Reason},P) ->
	?ADBG("Actor stop with reason ~p",[Reason]),
	{stop, normal, P};
handle_info(print_info,P) ->
	handle_cast(print_info,P);
handle_info(Msg,#dp{mors = master, verified = true} = P) ->
	case apply(P#dp.cbmod,cb_info,[Msg,P#dp.cbstate]) of
		{noreply,S} ->
			{noreply,P#dp{cbstate = S}};
		noreply ->
			{noreply,P}
	end;
handle_info(start_copy,P) ->
	?ADBG("Start copy ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.copyfrom]),
	case P#dp.copyfrom of
		{move,NewShard,Node} ->
			OldActor = P#dp.actorname,
			Msg = {move,NewShard,actordb_conf:node_name(),P#dp.copyreset,P#dp.cbstate};
		{split,MFA,Node,OldActor,NewActor} ->
			% Change node to this node, so that other actor knows where to send db.
			Msg = {split,MFA,actordb_conf:node_name(),OldActor,NewActor,P#dp.copyreset,P#dp.cbstate};
		{Node,OldActor} ->
			Msg = {copy,{actordb_conf:node_name(),OldActor,P#dp.actorname}}
	end,
	Home = self(),
	spawn(fun() ->
		case actordb:rpc(Node,OldActor,{?MODULE,call,[{OldActor,P#dp.actortype},[],Msg,P#dp.cbmod]}) of
			ok ->
				ok;
			{ok,_} ->
				ok;
			Err ->
				?AERR("Unable to start copy for ~p from ~p, ~p",[{P#dp.actorname,P#dp.actortype},P#dp.copyfrom,Err]),
				Home ! {stop,Err}
		end
	end),
	{noreply,P};
handle_info(_Msg,P) ->
	?DBG("sqlproc ~p unhandled info ~p~n",[P#dp.cbmod,_Msg]),
	{noreply,P}.

doqueue(P) when P#dp.callfrom == undefined, P#dp.verified /= false, P#dp.transactionid == undefined ->
	case queue:is_empty(P#dp.callqueue) of
		true ->
			% ?AINF("Queue empty ~p",[{P#dp.actorname,P#dp.actortype}]),
			P;
		false ->
			{{value,Call},CQ} = queue:out_r(P#dp.callqueue),
			{From,Msg} = Call,
			case handle_call(Msg,From,P#dp{callqueue = CQ}) of
				{reply,Res,NP} ->
					reply(From,Res),
					doqueue(NP);
				{stop,_,NP} ->
					self() ! stop,
					NP;
				% If call returns noreply, it will continue processing later.
				{noreply,NP} ->
					% We may have just inserted the same call back in the queue. If we did, it
					%  is placed in the wrong position. It should be in rear not front. So that
					%  we continue with this call next time we try to execute queue.
					% If we were to leave it as is, process might execute calls in a different order
					%  than it received them.
					case queue:is_empty(NP#dp.callqueue) of
						false ->
							{{value,Call1},CQ1} = queue:out(NP#dp.callqueue),
							case Call1 == Call of
								true ->
									NP#dp{callqueue = queue:in(Call,CQ1)};
								false ->
									NP
							end;
						_ ->
							NP
					end
			end
	end;
doqueue(P) ->
	% ?AINF("Queue notyet ~p ~p",[{P#dp.actorname,P#dp.actortype},{P#dp.callfrom,P#dp.verified,P#dp.transactionid}]),
	P.


check_inactivity(NTimer,P) ->
	% ?AINF("check inactivity ~p",[{P#dp.actorname,P#dp.actortype}]),
	Empty = queue:is_empty(P#dp.callqueue),
	Age = actordb_local:min_ref_age(P#dp.activity),
	case P#dp.mors of
		master ->
			% If we have been waiting for response for an unreasonable amount of time (600ms),
			%  call appendentries_start on node. If received node will call back appendentries_response.
			NResponsesWaiting =
			lists:foldl(fun(F,Count) ->
				case F#flw.wait_for_response_since of
					undefined ->
						Count;
					_ ->
						case actordb_local:min_ref_age(F#flw.wait_for_response_since) > 600 of
							true ->
								DN = bkdcore:dist_name(F#flw.node),
								case lists:member(DN,nodes()) of
									true ->
										rpc:cast(DN,
											?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
											{state_rw,{appendentries_start,P#dp.current_term,actordb_conf:node_name(),
											F#flw.match_index,F#flw.match_term,empty}}]),
										Count+1;
									% Do not count nodes that are gone. If those would be counted then actors
									%  would never go to sleep.
									false ->
										Count
								end;
							false ->
								Count
						end
				end
			end,0,P#dp.follower_indexes);
		_ ->
			NResponsesWaiting = 0
	end,
	case P of
		#dp{callfrom = undefined, verified = true, transactionid = undefined,dbcopyref = undefined,
			 dbcopy_to = [], locked = [], copyproc = undefined, copylater = undefined} when Empty, Age >= 1000, 
			 																				NResponsesWaiting == 0 ->
		
			case P#dp.movedtonode of
				undefined ->
					case apply(P#dp.cbmod,cb_candie,[P#dp.mors,P#dp.actorname,P#dp.actortype,P#dp.cbstate]) of
						true ->
							?ADBG("Die because temporary ~p ~p master ~p",[P#dp.actorname,P#dp.actortype,P#dp.masternode]),
							{stop,normal,P};
						false ->
							case P#dp.timerref of
								{undefined,_} ->
									Timer = P#dp.timerref;
								{TimerRef,_} ->
									erlang:cancel_timer(TimerRef),
									Timer = {undefined,element(2,P#dp.timerref)}
							end,
							{noreply,P#dp{timerref = Timer},hibernate}
					end;
				_ when Age >= 5000 ->
					{stop,normal,P};
				_ ->
					Now = actordb_local:actor_activity(P#dp.activity_now),
					{noreply,check_timer(P#dp{activity_now = Now})}
			end;
		_ when Empty == false, P#dp.verified == false, NTimer > 1, is_tuple(P#dp.election) ->
			case timer:now_diff(os:timestamp(),P#dp.election) > 1000000 of
				true ->
					{noreply, check_timer(actordb_sqlprocutil:start_verify(P,false))};
				false ->
					{noreply, check_timer(P)}
			end;
		_ ->
			Now = actordb_local:actor_activity(P#dp.activity_now),
			case P#dp.mors of
				master when P#dp.db /= undefined ->
					{_,NPages} = actordb_sqlite:wal_pages(P#dp.db),
					DbSize = NPages*(?PAGESIZE+40),
					case DbSize > 1024*1024 andalso P#dp.dbcopyref == undefined andalso P#dp.dbcopy_to == [] of
						true ->
							NotSynced = lists:foldl(fun(F,Count) ->
								case F#flw.match_index /= P#dp.evnum of
									true ->
										Count+1;
									false ->
										Count
								end 
							end,0,P#dp.follower_indexes),
							case NotSynced of
								0 ->
									WalFrom = {0,0},
									actordb_sqlprocutil:do_checkpoint(P);
								% If nodes arent synced, tolerate 30MB of wal size.
								_ when DbSize >= 1024*1024*30 ->
									WalFrom = {0,0},
									actordb_sqlprocutil:do_checkpoint(P);
								_ ->
									WalFrom = P#dp.wal_from
							end;
						false ->
							WalFrom = P#dp.wal_from
					end;
				_ ->
					WalFrom = P#dp.wal_from
			end,
			{noreply,check_timer(retry_copy(P#dp{activity_now = Now, wal_from = WalFrom,
												locked = abandon_locks(P,P#dp.locked,[])}))}
	end.

abandon_locks(P,[{wait_copy,_CpRef,_IsMove,_Node,TimeOfLock} = H|T],L) ->
	case timer:now_diff(os:timestamp(),TimeOfLock) > 3000000 of
		true ->
			?AERR("Abandoned lock ~p ~p ~p",[P#dp.actorname,_Node,_CpRef]),
			abandon_locks(P,T,L);
		false ->
			abandon_locks(P,T,[H|L])
	end;
abandon_locks(P,[H|T],L) ->
	abandon_locks(P,T,[H|L]);
abandon_locks(_,[],L) ->
	L.

retry_copy(#dp{copylater = undefined} = P) ->
	P;
retry_copy(P) ->
	{LastTry,Copy} = P#dp.copylater,
	case timer:node_diff(os:timestamp(),LastTry) > 1000000*3 of
		true ->
			case Copy of
				{move,Node} ->
					NewActor = P#dp.actorname,
					IsMove = true,
					Msg = {move,actordb_conf:node_name()};
				{split,MFA,Node,OldActor,NewActor} ->
					IsMove = {split,MFA},
					Msg = {split,MFA,actordb_conf:node_name(),OldActor,NewActor}
			end,
			Ref = make_ref(),
			case actordb:rpc(Node,NewActor,{?MODULE,call,[{NewActor,P#dp.actortype},[{lockinfo,wait}],
														{dbcopy,{start_receive,Msg,Ref}},P#dp.cbmod]}) of
				ok ->
					{reply,_,NP1} = actordb_sqlprocutil:dbcopy_call({send_db,{Node,Ref,IsMove,NewActor}},undefined,P),
					NP1#dp{copylater = undefined};
				_ ->
					P#dp{copylater = {os:timestamp(),Msg}}
			end;
		false ->
			P
	end.




down_info(PID,_Ref,Reason,#dp{election = PID} = P1) ->
	case Reason of
		% We are leader, evnum == 0, which means no other node has any data.
		% If create flag not set stop.
		leader when (P1#dp.flags band ?FLAG_CREATE) == 0, P1#dp.evnum == 0 ->
			{stop,nocreate,P1};
		leader ->
			?ADBG("Elected leader ~p ~p",[{P1#dp.actorname,P1#dp.actortype},P1#dp.current_term]),
			actordb_local:actor_mors(master,actordb_conf:node_name()),
			FollowerIndexes = [#flw{node = Nd,match_index = 0,next_index = P1#dp.evnum+1} || Nd <- bkdcore:cluster_nodes()],
			P = actordb_sqlprocutil:reopen_db(P1#dp{mors = master, election = os:timestamp(), 
													follower_indexes = FollowerIndexes, verified = true}),
			ok = esqlite3:replicate_opts(P#dp.db,term_to_binary({P#dp.cbmod,P#dp.actorname,P#dp.actortype,P#dp.current_term})),

			case P#dp.schemavers of
				undefined ->
					Transaction = [],
					Rows = [];
				_ ->
					{ok,[[{columns,_},{rows,Transaction}],
						[{columns,_},{rows,Rows}]]} = actordb_sqlite:exec(P#dp.db,
							<<"SELECT * FROM __adb;",
							  "SELECT * FROM __transactions;">>,read)
			end,
			
			case butil:ds_val(?COPYFROMI,Rows) of
				CopyFrom1 when byte_size(CopyFrom1) > 0 ->
					{CopyFrom,CopyReset,CbState} = binary_to_term(base64:decode(CopyFrom1));
				_ ->
					CopyFrom = CopyReset = undefined,
					CbState = P#dp.cbstate
			end,
			% After election is won a write needs to be executed. What we will write depends on the situation:
			%  - If this actor has been moving, do a write to clean up after it (or restart it)
			%  - If transaction active continue with write.
			%  - If empty db or schema not up to date create/update it.
			%  - It can also happen that both transaction active and actor move is active. Sqls will be combined.
			%  - Otherwise just empty sql, which still means an increment for evnum and evterm in __adb.
			{NP,Sql,Callfrom} = actordb_sqlprocutil:post_election_sql(P#dp{copyreset = CopyReset, cbstate = CbState},
																		Transaction,CopyFrom,[],undefined),
			case P#dp.callres of
				undefined ->
					% it must always return noreply
					write_call({undefined,Sql,NP#dp.transactionid},Callfrom, NP);
				_ ->
					{noreply,NP#dp{callqueue = queue:in_r({Callfrom,{write,{undefined,Sql,NP#dp.transactionid}}},
															P#dp.callqueue)}}
			end;
		follower ->
			{noreply,actordb_sqlprocutil:reopen_db(P1#dp{election = os:timestamp(), mors = slave})}
	end;
down_info(_PID,Ref,Reason,#dp{transactioncheckref = Ref} = P) ->
	?ADBG("Transactioncheck died ~p myid ~p",[Reason,P#dp.transactionid]),
	case P#dp.transactionid of
		{Tid,Updaterid,Node} ->
			case Reason of
				noproc ->
					{_CheckPid,CheckRef} = actordb_sqlprocutil:start_transaction_checker(Tid,Updaterid,Node),
					{noreply,P#dp{transactioncheckref = CheckRef}};
				abandonded ->
					case handle_call({commit,false,P#dp.transactionid},undefined,P#dp{transactioncheckref = undefined}) of
						{stop,normal,NP} ->
							{stop,normal,NP};
						{reply,_,NP} ->
							{noreply,NP};
						{noreply,NP} ->
							{noreply,NP}
					end;
				done ->
					case handle_call({commit,true,P#dp.transactionid},undefined,P#dp{transactioncheckref = undefined}) of
						{stop,normal,NP} ->
							{stop,normal,NP};
						{reply,_,NP} ->
							{noreply,NP};
						{noreply,NP} ->
							{noreply,NP}
					end
			end;
		_ ->
			{noreply,P#dp{transactioncheckref = undefined}}
	end;
down_info(PID,_Ref,Reason,#dp{copyproc = PID} = P) ->
	?ADBG("copyproc died ~p ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},Reason,P#dp.mors,P#dp.copyfrom]),
	case Reason of
		ok when P#dp.mors == master; is_binary(P#dp.copyfrom) ->
			{ok,NP} = init(P,copyproc_done),
			{noreply,NP};
		ok when P#dp.mors == slave ->
			{stop,normal,P};
		nomajority ->
			{stop,{error,nomajority},P};
		% Error copying. 
		%  - There is a chance copy succeeded. If this node was able to send unlock msg
		%    but connection was interrupted before replying. If this is the case next read/write call will start
		%    actor on this node again and everything will be fine.
		%  - If copy failed before unlock, then it actually did fail. In that case move will restart 
		%    eventually.
		_ ->
			?AERR("Coproc died with error ~p ~p~n",[{P#dp.actorname,P#dp.actortype},Reason]),
			actordb_sqlprocutil:empty_queue(P#dp.callqueue,{error,copyfailed}),
			{stop,Reason,P}
	end;
down_info(PID,_Ref,Reason,P) ->
	case lists:keyfind(PID,2,P#dp.dbcopy_to) of
		{Node,PID,Ref,IsMove} ->
			?ADBG("Down copyto proc ~p ~p ~p ~p ~p",[P#dp.actorname,Reason,Ref,P#dp.locked,P#dp.dbcopy_to]),
			case Reason of
				ok ->
					ok;
				_ ->
					?AERR("Copyto process invalid exit ~p",[Reason])
			end,
			WithoutCopy = lists:keydelete(PID,1,P#dp.locked),
			NewCopyto = lists:keydelete(PID,2,P#dp.dbcopy_to),
			false = lists:keyfind(Ref,2,WithoutCopy),
			% wait_copy not in list add it (2nd stage of lock)
			WithoutCopy1 =  [{wait_copy,Ref,IsMove,Node,os:timestamp()}|WithoutCopy],
			NP = P#dp{dbcopy_to = NewCopyto, 
						locked = WithoutCopy1,
						activity = make_ref()},
			case queue:is_empty(P#dp.callqueue) of
				true ->
					{noreply,NP};
				false ->
					handle_info(doqueue,NP)
			end;
		false ->
			?ADBG("downmsg, verify maybe? ~p",[P#dp.election]),
			case apply(P#dp.cbmod,cb_info,[{'DOWN',_Ref,process,PID,Reason},P#dp.cbstate]) of
				{noreply,S} ->
					{noreply,P#dp{cbstate = S}};
				noreply ->
					{noreply,P}
			end
	end.


terminate(_, P) ->
	?ADBG("Terminating ~p",[{P#dp.actorname,P#dp.actortype}]),
	actordb_sqlite:stop(P#dp.db),
	distreg:unreg(self()),
	ok.
code_change(_, P, _) ->
	{ok, P}.
init(#dp{} = P,_Why) ->
	% ?ADBG("Reinit because ~p, ~p, ~p",[_Why,?R2P(P),get()]),
	?ADBG("Reinit because ~p ~p",[_Why,{P#dp.actorname,P#dp.actortype}]),
	init([{actor,P#dp.actorname},{type,P#dp.actortype},{mod,P#dp.cbmod},{flags,P#dp.flags},
		  {state,P#dp.cbstate},{slave,P#dp.mors == slave},{queue,P#dp.callqueue},{startreason,{reinit,_Why}}]).
% Never call other processes from init. It may cause deadlocks. Whoever
% started actor is blocking waiting for init to finish.
init([_|_] = Opts) ->
	% put(opt,Opts),
	case actordb_sqlprocutil:parse_opts(check_timer(#dp{mors = master, callqueue = queue:new(), 
									schemanum = actordb_schema:num()}),Opts) of
		{registered,Pid} ->
			?AINF("registered"),
			explain({registered,Pid},Opts),
			{stop,normal};
		P when (P#dp.flags band ?FLAG_ACTORNUM) > 0 ->
			explain({actornum,P#dp.dbpath,actordb_sqlprocutil:read_num(P)},Opts),
			{stop,normal};
		P when (P#dp.flags band ?FLAG_EXISTS) > 0 ->
			{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
			actordb_sqlite:stop(Db),
			explain({ok,[{columns,{<<"exists">>}},{rows,[{butil:tobin(SchemaTables /= [])}]}]},Opts),
			{stop,normal};
		P when (P#dp.flags band ?FLAG_STARTLOCK) > 0 ->
			case lists:keyfind(lockinfo,1,Opts) of
				{lockinfo,dbcopy,{Ref,CbState,CpFrom,CpReset}} ->
					?ADBG("Starting actor slave lock for copy on ref ~p",[Ref]),
					{ok,Pid} = actordb_sqlprocutil:start_copyrec(P#dp{mors = slave, cbstate = CbState, 
													dbcopyref = Ref,  copyfrom = CpFrom, copyreset = CpReset}),
					{ok,P#dp{copyproc = Pid, verified = false,mors = slave, copyfrom = P#dp.copyfrom}};
				{lockinfo,wait} ->
					{ok,P}
			end;
		P when P#dp.copyfrom == undefined ->
			?ADBG("Actor start ~p, copy=~p, queue=~p, mors=~p startreason=~p",[{P#dp.actorname,P#dp.actortype},P#dp.copyfrom,
							queue:is_empty(P#dp.callqueue),P#dp.mors,butil:ds_val(startreason,Opts)]),
			% Could be normal start after moving to another node though.
			MovedToNode = apply(P#dp.cbmod,cb_checkmoved,[P#dp.actorname,P#dp.actortype]),
			RightCluster = lists:member(MovedToNode,bkdcore:all_cluster_nodes()),
			case butil:readtermfile([P#dp.dbpath,"-term"]) of
				{VotedFor,VotedForTerm} ->
					ok;
				_ ->
					VotedFor = undefined,
					VotedForTerm = 0
			end,
			case ok of
				_ when P#dp.mors == slave ->
					% Read evnum and evterm from wal file if it exists
					case file:open([P#dp.dbpath,"-wal"],[read,binary,raw]) of
						{ok,F} ->
							case file:position(F,eof) of
								{ok,WalSize} when WalSize > 32+40+?PAGESIZE ->
									{ok,_} = file:position(F,{cur,-(?PAGESIZE+40)}),
									{ok,<<_:32,_:32,Evnum:64/big-unsigned,Evterm:64/big-unsigned>>} =
										file:read(F,24),
									file:close(F),
									{ok,P#dp{current_term = VotedForTerm, voted_for = VotedFor, 
												evnum = Evnum, evterm = Evterm}};
								{ok,_} ->
									file:close(F),
									init_opendb(P#dp{current_term = VotedForTerm,voted_for = VotedFor})
							end;
						{error,enoent} ->
							{ok,P#dp{current_term = VotedForTerm, voted_for = VotedFor}}
					end;
				_ when MovedToNode == undefined; RightCluster ->
					init_opendb(P#dp{current_term = VotedForTerm,voted_for = VotedFor});
				_ ->
					?ADBG("Actor moved ~p ~p ~p",[P#dp.actorname,P#dp.actortype,MovedToNode]),
					{ok, P#dp{verified = true, movedtonode = MovedToNode,
								activity_now = actordb_sqlprocutil:actor_start(P)}}
			end;
		P ->
			self() ! start_copy,
			{ok,P#dp{mors = master}}
	end;
init(#dp{} = P) ->
	init(P,noreason).


init_opendb(P) ->
	{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
	NP = P#dp{db = Db},
	case SchemaTables of
		[_|_] ->
			?ADBG("Opening HAVE schema ~p",[{P#dp.actorname,P#dp.actortype}]),
			{ok,[{columns,_},{rows,Rows}]} = actordb_sqlite:exec(Db,
					<<"SELECT * FROM __adb;">>,read),
			Evnum = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
			Vers = butil:toint(butil:ds_val(?SCHEMA_VERSI,Rows)),
			MovedToNode1 = butil:ds_val(?MOVEDTOI,Rows),
			EvTerm = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),

			{ok,actordb_sqlprocutil:start_verify(
						NP#dp{evnum = Evnum, schemavers = Vers,
									evterm = EvTerm,current_term = EvTerm,
									movedtonode = MovedToNode1},true)};
		[] -> 
			?ADBG("Opening NO schema ~p",[{P#dp.actorname,P#dp.actortype}]),
			{ok,actordb_sqlprocutil:start_verify(NP,true)}
	end.


explain(What,Opts) ->
	case lists:keyfind(start_from,1,Opts) of
		{_,{FromPid,FromRef}} ->
			FromPid ! {FromRef,What};
		_ ->
			ok
	end.

reply(undefined,_Msg) ->
	ok;
reply(From,Msg) ->
	gen_server:reply(From,Msg).


check_timer(P) ->
	case P#dp.timerref of
		{undefined,N} ->
			Ref = erlang:send_after(1000,self(),{inactivity_timer,N+1}),
			P#dp{timerref = {Ref,N}};
		_ ->
			P
	end.

