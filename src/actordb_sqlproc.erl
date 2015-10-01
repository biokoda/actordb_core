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
-export([write_call/3, write_call1/4, read_call/3, read_call1/4]).
-include_lib("actordb_sqlproc.hrl").

% Read actor number without creating actor.
try_actornum(Name,Type,CbMod) ->
	case call({Name,Type},[actornum],{state_rw,actornum},CbMod) of
		{error,nocreate} ->
			{"",undefined};
		{ok,Path,NumNow} ->
			{Path,NumNow}
	end.
read(Name,Flags,{[{copy,CopyFrom}],_},Start) ->
	read(Name,Flags,[{copy,CopyFrom}],Start);
read(Name,Flags,[{copy,CopyFrom}],Start) ->
	case distreg:whereis(Name) of
		undefined ->
			R = #read{sql = <<"select * from __adb limit 1;">>, flags = Flags},
			case call(Name,Flags,R,Start) of
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
	call(Name,Flags,#write{sql = delete, flags = Flags},Start);
read(Name,Flags,{Sql,[]},Start) ->
	read(Name,Flags,Sql,Start);
read(Name,Flags,Sql,Start) ->
	call(Name,Flags,#read{sql = Sql, flags = Flags},Start).

write(Name,Flags,{Sql,[]},Start) ->
	write(Name,Flags,Sql,Start);
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
					W = #write{mfa = MFA,sql = delete, transaction = TransactionId, flags = Flags},
					call(Name,Flags,W,Start);
				{Sql0, PreparedStatements} ->
					W = #write{mfa = MFA,sql = iolist_to_binary(Sql0), records = PreparedStatements,
						transaction = TransactionId, flags = Flags},
					call(Name,Flags,W,Start);
				_ ->
					W = #write{mfa = MFA,sql = iolist_to_binary(Sql),
						transaction = TransactionId, flags = Flags},
					call(Name,Flags,W,Start)
			end;
		_ when Sql == undefined ->
			call(Name,Flags,#write{mfa = MFA, flags = Flags},Start);
		_ when tuple_size(Sql) == 2 ->
			{Sql0,Rec} = Sql,
			W = #write{mfa = MFA, sql = iolist_to_binary(Sql0), records = Rec, flags = Flags},
			call(Name,[wait_election|Flags],W,Start);
		_ ->
			W = #write{mfa = MFA, sql = iolist_to_binary(Sql), flags = Flags},
			call(Name,[wait_election|Flags],W,Start)
	end;
write(Name,Flags,[delete],Start) ->
	call(Name,Flags,#write{sql = delete, flags = Flags},Start);
write(Name,Flags,{Sql,Records},Start) ->
	W = #write{sql = iolist_to_binary(Sql), records = Records, flags = Flags},
	call(Name,[wait_election|Flags],W,Start);
write(Name,Flags,Sql,Start) ->
	W = #write{sql = iolist_to_binary(Sql), flags = Flags},
	call(Name,[wait_election|Flags],W,Start).


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
			% ?INF("Call have pid ~p for name ~p, alive ~p",[Pid,Name,erlang:is_process_alive(Pid)]),
			call(Name,Flags,Msg,Start,IsRedirect,Pid)

	end.
call(Name,Flags,Msg,Start,IsRedirect,Pid) ->
	% If call returns redirect, this is slave node not master node.
	% test_mon_calls(Name,Msg),
	case catch gen_server:call(Pid,Msg,infinity) of
		{redirect,Node} when is_binary(Node) ->
			% test_mon_stop(),
			?ADBG("Redirect call to=~p, for=~p, ~p",[Node,Name,Msg]),
			case lists:member(Node,bkdcore:cluster_nodes()) of
				true ->
					case IsRedirect of
						true ->
							double_redirect;
						_ ->
							case actordb:rpc(Node,element(1,Name),{?MODULE,call,[Name,Flags,Msg,Start,true]}) of
								double_redirect ->
									diepls(Pid,nomaster),
									call(Name,Flags,Msg,Start);
								Res ->
									Res
							end
					end;
				false ->
					case IsRedirect of
						onlylocal ->
							{redirect,Node};
						_ ->
							case actordb:rpc(Node,element(1,Name),{?MODULE,call,[Name,Flags,Msg,Start,false]}) of
								{error,Connerr} when Connerr == econnrefused; Connerr == timeout; Connerr == invalidnode ->
									Pid ! doelection,
									call(Name,Flags,Msg,Start,false,Pid);
								Res ->
									?ADBG("Redirect rpc res=~p",[Res]),
									Res
							end
					end
			end;
		{'EXIT',{noproc,_}} = _X  ->
			?ADBG("noproc call again ~p",[_X]),
			call(Name,Flags,Msg,Start);
		{'EXIT',{normal,_}} ->
			?ADBG("died normal"),
			call(Name,Flags,Msg,Start);
		{'EXIT',{nocreate,_}} ->
			% test_mon_stop(),
			{error,nocreate};
		{'EXIT',{error,_} = E} ->
			E;
		Res ->
			% test_mon_stop(),
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

% test_mon_calls(Who,Msg) ->
% 	Ref = make_ref(),
% 	put(ref,Ref),
% 	put(refpid,spawn(fun() -> test_mon_proc(Who,Msg,Ref) end)).
% test_mon_proc(Who,Msg,Ref) ->
% 	receive
% 		Ref ->
% 			ok
% 		after 1000 ->
% 			?AERR("Still waiting on ~p, for ~p",[Who,Msg]),
% 			test_mon_proc(Who,Msg,Ref)
% 	end.
% test_mon_stop() ->
% 	butil:safesend(get(refpid), get(ref)).


call_slave(Cb,Actor,Type,Msg) ->
	call_slave(Cb,Actor,Type,Msg,[]).
call_slave(Cb,Actor,Type,Msg,Flags) ->
	actordb_util:wait_for_startup(Type,Actor,0),
	case apply(Cb,cb_slave_pid,[Actor,Type,[{startreason,Msg}|Flags]]) of
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
	?ADBG("Starting ~p slave=~p",[butil:ds_vals([actor,type],Opts),butil:ds_val(slave,Opts)]),
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
					Res;
				{Ref,nostart} ->
					{error,nostart}
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

print_info(undefined) ->
	ok;
print_info({A,T}) ->
	print_info(distreg:whereis({A,T}));
print_info(Pid) ->
	gen_server:cast(Pid,print_info).

% Call processing.
% Calls are processed here and in actordb_sqlprocutil:doqueue.
% Only in handle_call are we allowed to add calls to callqueue.
handle_call(Msg,_,P) when is_binary(P#dp.movedtonode) ->
	?DBG("REDIRECT BECAUSE MOVED TO NODE ~p ~p",[P#dp.movedtonode,Msg]),
	case apply(P#dp.cbmod,cb_redirected_call,[P#dp.cbstate,P#dp.movedtonode,Msg,moved]) of
		{reply,What,NS,Red} ->
			{reply,What,P#dp{cbstate = NS, movedtonode = Red}};
		ok ->
			{reply,{redirect,P#dp.movedtonode},P}
	end;
handle_call({dbcopy,Msg},CallFrom,P) -> %when element(1,Msg) /= reached_end ->
	Me = actordb_conf:node_name(),
	case ok of
		_ when element(1,Msg) == send_db andalso P#dp.verified == false ->
			{noreply,P#dp{callqueue = queue:in_r({CallFrom,{dbcopy,Msg}},P#dp.callqueue),
				activity = actordb_local:actor_activity(P#dp.activity)}};
		_ when element(1,Msg) == send_db andalso Me /= P#dp.masternode ->
			?DBG("redirect not master node"),
			actordb_sqlprocutil:redirect_master(P);
		_ ->
			actordb_sqlprocutil:dbcopy_call(Msg,CallFrom,
				P#dp{activity = actordb_local:actor_activity(P#dp.activity)})
	end;
handle_call({state_rw,_} = Msg,From, #dp{wasync = #ai{wait = WRef}} = P) when is_reference(WRef) ->
	?DBG("Queuing state call, waitingfor=~p",[WRef]),
	{noreply,P#dp{statequeue = queue:in_r({From,Msg},P#dp.statequeue)}};
handle_call({state_rw,What},From,P) ->
	state_rw_call(What,From,P#dp{activity = actordb_local:actor_activity(P#dp.activity)});
handle_call({commit,Doit,Id},From, P) ->
	commit_call(Doit,Id,From,P#dp{activity = actordb_local:actor_activity(P#dp.activity)});
handle_call(Msg,From,P) ->
	case Msg of
		_ when P#dp.mors == slave ->
			% Now = actordb_local:elapsed_time(),
			case P#dp.masternode of
				undefined when P#dp.election == undefined, is_integer(P#dp.without_master_since) ->
					% P#dp.without_master_since < Now-3000 ->
					% We have given up. But since we are getting a call from outside, try again.
					% Execute election.
					{noreply,actordb_sqlprocutil:start_verify(
						P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue),
						flags = P#dp.flags band (bnot ?FLAG_WAIT_ELECTION)},false)};
				undefined ->
					?DBG("Queing msg no master yet ~p",[Msg]),
					{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue),
						election = actordb_sqlprocutil:election_timer(P#dp.election),
						flags = P#dp.flags band (bnot ?FLAG_WAIT_ELECTION)}};
				_ ->
					case apply(P#dp.cbmod,cb_redirected_call,[P#dp.cbstate,P#dp.masternode,Msg,slave]) of
						{reply,What,NS,_} ->
							{reply,What,P#dp{cbstate = NS}};
						ok ->
							?DBG("Redirecting to master"),
							actordb_sqlprocutil:redirect_master(P)
					end
			end;
		_ when P#dp.verified == false ->
			case is_pid(P#dp.election) andalso P#dp.flags band ?FLAG_WAIT_ELECTION > 0 of
				true ->
					P#dp.election ! exit,
					handle_call(Msg,From,P#dp{flags = P#dp.flags band (bnot ?FLAG_WAIT_ELECTION)});
				_ ->
					case apply(P#dp.cbmod,cb_unverified_call,[P#dp.cbstate,Msg]) of
						queue ->
							{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue)}};
						{moved,Moved} ->
							{noreply,P#dp{movedtonode = Moved}};
						{moved,Moved,NS} ->
							{noreply,P#dp{movedtonode = Moved, cbstate = NS}};
						{reply,What} ->
							{reply,What,P};
						{reinit_master,Mors} ->
							{ok,NP} = init(P#dp{mors = Mors},cb_reinit),
							{noreply,NP};
						{isolate,_OutSql,_State} ->
							write_call(Msg,From,P);
						{reinit,Sql,NS} ->
							{ok,NP} = init(P#dp{cbstate = NS,
								callqueue = queue:in_r({From,#write{sql = Sql}},P#dp.callqueue)},cb_reinit),
							{noreply,NP}
					end
			end;
		% Same transaction can write to an actor more than once
		% Transactions do not use async calls to driver, so if we are in the middle of one, we can execute
		% this write immediately.
		#write{transaction = TransactionId} = Msg1 when
				P#dp.transactionid == TransactionId,P#dp.transactionid /= undefined ->
			write_call1(Msg1,From,P#dp.schemavers,P);
		#read{} when P#dp.movedtonode == undefined ->
			% read call just buffers call
			read_call(Msg,From,P);
		#write{transaction = undefined} when P#dp.movedtonode == undefined ->
			% write_call just buffers call, we can always run it.
			% Actual write is executed at the end of doqueue.
			write_call(Msg,From,P);
		_ when P#dp.movedtonode == deleted andalso (element(1,Msg) == read orelse element(1,Msg) == write) ->
			% #write and #read have flags in same pos
			Flags = element(#write.flags,Msg),
			case lists:member(create,Flags) of
				true ->
					{stop,normal,P};
				false ->
					{reply, {error,nocreate},P}
			end;
		_ ->
			% ?DBG("Queing msg ~p, callres ~p, locked ~p, transactionid ~p",
			% 	[Msg,P#dp.callres,P#dp.locked,P#dp.transactionid]),
			% Continue in doqueue
			self() ! timeout,
			{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue),
				activity = actordb_local:actor_activity(P#dp.activity)}}
	end.


commit_call(Doit,Id,From,P) ->
	?DBG("Commit doit=~p, id=~p, from=~p, trans=~p",[Doit,Id,From,P#dp.transactionid]),
	case P#dp.transactionid == Id of
		true ->
			case P#dp.transactioncheckref of
				undefined ->
					ok;
				_ ->
					erlang:demonitor(P#dp.transactioncheckref)
			end,
			?DBG("Commit write ~p",[P#dp.transactioninfo]),
			{Sql,EvNum,_NewVers} = P#dp.transactioninfo,
			case Sql of
				<<"delete">> when Doit == true ->
					Moved = deleted;
				_ ->
					Moved = P#dp.movedtonode
			end,
			case Doit of
				true when P#dp.follower_indexes == [] ->
					case Moved of
						deleted ->
							Me = self(),
							actordb_sqlprocutil:delete_actor(P),
							spawn(fun() -> ?DBG("Stopping in commit"), stop(Me) end);
						_ ->
							VarHeader = actordb_sqlprocutil:create_var_header(P),
							ok = actordb_sqlite:okornot(actordb_sqlite:exec(
								P#dp.db,<<"#s01;">>,P#dp.evterm,EvNum,VarHeader)),
							actordb_sqlite:replication_done(P#dp.db)
					end,
					{reply,ok,actordb_sqlprocutil:doqueue(P#dp{transactionid = undefined,
						transactioncheckref = undefined,
						transactioninfo = undefined, movedtonode = Moved,
						evnum = EvNum, evterm = P#dp.current_term})};
				true ->
					% We can safely release savepoint.
					% This will send the remaining WAL pages to followers that have commit flag set.
					% Followers will then rpc back appendentries_response.
					% We can also set #dp.evnum now.
					VarHeader = actordb_sqlprocutil:create_var_header(P),
					actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"#s01;">>,
												P#dp.evterm,EvNum,VarHeader)),
					actordb_sqlite:replication_done(P#dp.db),
					{noreply,ae_timer(P#dp{callfrom = From,
						callres = ok,evnum = EvNum,movedtonode = Moved,
						transactionid = undefined, transactioninfo = undefined,
						transactioncheckref = undefined})};
				false when P#dp.follower_indexes == [] ->
					ok = actordb_sqlite:rollback(P#dp.db),
					{reply,ok,actordb_sqlprocutil:doqueue(P#dp{transactionid = undefined,
						transactioninfo = undefined,transactioncheckref = undefined})};
				false ->
					% Transaction failed.
					% Delete it from __transactions.
					% EvNum will actually be the same as transactionsql that we have not finished.
					%  Thus this EvNum section of WAL contains pages from failed transaction and
					%  cleanup of transaction from __transactions.
					{Tid,Updaterid,_} = P#dp.transactionid,
					% case Sql of
					% 	<<"delete">> ->
					% 		ok;
					% 	_ ->
							% actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>,P#dp.evterm,P#dp.evnum,<<>>)
							ok = actordb_sqlite:rollback(P#dp.db),
					% end,
					NewSql = <<"DELETE FROM __transactions WHERE tid=",(butil:tobin(Tid))/binary," AND updater=",
									(butil:tobin(Updaterid))/binary,";">>,
					write_call(#write{sql = NewSql},From,P#dp{callfrom = undefined,
										transactionid = undefined,transactioninfo = undefined,
										transactioncheckref = undefined})
			end;
		_ ->
			{reply,ok,P}
	end.

state_rw_call(donothing,_From,P) ->
	{reply,ok,P};
state_rw_call(recovered,_From,P) ->
	?DBG("No longer in recovery!"),
	{reply,ok,P#dp{inrecovery = false}};
state_rw_call({appendentries_start,Term,LeaderNode,PrevEvnum,PrevTerm,AEType,CallCount} = What,From,P) ->
	% Executed on follower.
	% AE is split into multiple calls (because wal is sent page by page as it is written)
	% Start sets parameters. There may not be any wal append calls after if empty write.
	% AEType = [head,empty,recover]
	?DBG("AE start ~p {PrevEvnum,PrevTerm}=~p leader=~p",[AEType, {PrevEvnum,PrevTerm},LeaderNode]),
	case ok of
		_ when P#dp.inrecovery, AEType == head ->
			?DBG("Ignoring head because inrecovery"),
			% Reply may have gotten lost or leader could have changed.
			case actordb_local:elapsed_time() - P#dp.recovery_age > 2000 of
				true ->
					?ERR("Recovery mode timeout",[]),
					state_rw_call(What,From,P#dp{inrecovery = false});
				false ->
					{reply,false,P}
			end;
		_ when is_pid(P#dp.copyproc) ->
			?DBG("Ignoring AE because copy in progress"),
			{reply,false,P};
		_ when Term < P#dp.current_term ->
			?ERR("AE start, input term too old ~p {InTerm,MyTerm}=~p",
					[AEType,{Term,P#dp.current_term}]),
			reply(From,false),
			actordb_sqlprocutil:ae_respond(P,LeaderNode,false,PrevEvnum,AEType,CallCount),
			% Some node thinks its master and sent us appendentries start.
			% Because we are master with higher term, we turn it down.
			% But we also start a new write so that nodes get synchronized.
			case P#dp.mors of
				master ->
					% {noreply, actordb_sqlprocutil:start_verify(P,false)};
					?DBG("Executing empty write"),
					{noreply,write_call(#write{sql = []},undefined,P)};
				_ ->
					{noreply,P}
			end;
		_ when P#dp.mors == slave, P#dp.masternode /= LeaderNode ->
			?DBG("AE start, slave now knows leader ~p ~p",[AEType,LeaderNode]),
			% If we have any pending replies, this must result in a rewind for this node
			% and a new write to master.
			reply(P#dp.callfrom,{redirect,LeaderNode}),
			actordb_local:actor_mors(slave,LeaderNode),
			NP = P#dp{masternode = LeaderNode,without_master_since = undefined,
			masternodedist = bkdcore:dist_name(LeaderNode),
			callfrom = undefined, callres = undefined,verified = true},
			state_rw_call(What,From,actordb_sqlprocutil:doqueue(actordb_sqlprocutil:reopen_db(NP)));
		% This node is candidate or leader but someone with newer term is sending us log
		_ when P#dp.mors == master ->
			?ERR("AE start, stepping down as leader ~p ~p",
					[AEType,{Term,P#dp.current_term}]),
			reply(P#dp.callfrom,{redirect,LeaderNode}),
			actordb_local:actor_mors(slave,LeaderNode),
			NP = P#dp{mors = slave, verified = true, election = undefined,
				voted_for = undefined,callfrom = undefined, callres = undefined,
				masternode = LeaderNode,without_master_since = undefined,
				masternodedist = bkdcore:dist_name(LeaderNode),
				current_term = Term},
			state_rw_call(What,From,
				actordb_sqlprocutil:save_term(actordb_sqlprocutil:doqueue(actordb_sqlprocutil:reopen_db(NP))));
		_ when P#dp.evnum /= PrevEvnum; P#dp.evterm /= PrevTerm ->
			?ERR("AE start failed, evnum evterm do not match, type=~p, {MyEvnum,MyTerm}=~p, {InNum,InTerm}=~p",
						[AEType,{P#dp.evnum,P#dp.evterm},{PrevEvnum,PrevTerm}]),
			case ok of
				% Node is conflicted, delete last entry
				_ when PrevEvnum > 0, AEType == recover, P#dp.evnum > 0 ->
					NP = actordb_sqlprocutil:rewind_wal(P);
				% If false this node is behind. If empty this is just check call.
				% Wait for leader to send an earlier event.
				_ ->
					NP = P
			end,
			reply(From,false),
			actordb_sqlprocutil:ae_respond(NP,LeaderNode,false,PrevEvnum,AEType,CallCount),
			{noreply,NP};
		_ when Term > P#dp.current_term ->
			?ERR("AE start, my term out of date type=~p {InTerm,MyTerm}=~p",
				[AEType,{Term,P#dp.current_term}]),
			NP = P#dp{current_term = Term,voted_for = undefined,
			masternode = LeaderNode, without_master_since = undefined,verified = true,
			masternodedist = bkdcore:dist_name(LeaderNode)},
			state_rw_call(What,From,actordb_sqlprocutil:doqueue(actordb_sqlprocutil:save_term(NP)));
		_ when AEType == empty ->
			?DBG("AE start, ok for empty"),
			reply(From,ok),
			actordb_sqlprocutil:ae_respond(P,LeaderNode,true,PrevEvnum,AEType,CallCount),
			{noreply,P#dp{verified = true}};
		% Ok, now it will start receiving wal pages
		_ ->
			case AEType == recover of
				true ->
					Age = actordb_local:elapsed_time(),
					?INF("AE start ok for recovery from ~p, evnum=~p, evterm=~p",
						[LeaderNode,P#dp.evnum,P#dp.evterm]);
				false ->
					Age = P#dp.recovery_age,
					?DBG("AE start ok from ~p",[LeaderNode])
			end,
			{reply,ok,P#dp{verified = true, inrecovery = AEType == recover, recovery_age = Age}}
	end;
% Executed on follower.
% Appends pages, a single write is split into multiple calls. 
% Header tells you if this is last call. If we reached header, this means we must have received
% all preceding calls as well.
state_rw_call({appendentries_wal,Header,Body,AEType,CallCount},From,P) ->
	append_wal(P,From,CallCount,Header,Body,AEType);
% Executed on leader.
state_rw_call({appendentries_response,Node,CurrentTerm,Success,
			EvNum,EvTerm,MatchEvnum,AEType,{SentIndex,SentTerm}} = What,From,P) ->
	Follower = lists:keyfind(Node,#flw.node,P#dp.follower_indexes),
	case Follower of
		false ->
			?DBG("Adding node to follower list ~p",[Node]),
			state_rw_call(What,From,actordb_sqlprocutil:store_follower(P,#flw{node = Node}));
		_ when (not (AEType == head andalso Success)) andalso
				(SentIndex /= Follower#flw.match_index orelse
				SentTerm /= Follower#flw.match_term orelse P#dp.verified == false) ->
			% We can get responses from AE calls which are out of date. This is why the other node always sends
			%  back {SentIndex,SentTerm} which are the parameters for follower that we knew of when we sent data.
			% If these two parameters match our current state, then response is valid.
			?DBG("ignoring AE resp, from=~p,success=~p,type=~p,prevevnum=~p,evnum=~p,matchev=~p, sent=~p",
				[Node,Success,AEType,Follower#flw.match_index,EvNum,MatchEvnum,{SentIndex,SentTerm}]),
			{reply,ok,P};
		_ ->
			?DBG("AE resp,from=~p,success=~p,type=~p,prevnum=~p,prevterm=~p evnum=~p,evterm=~p,matchev=~p",
				[Node,Success,AEType,Follower#flw.match_index,Follower#flw.match_term,EvNum,EvTerm,MatchEvnum]),
			Now = actordb_local:elapsed_time(),
			NFlw = Follower#flw{match_index = EvNum, match_term = EvTerm,next_index = EvNum+1,
					wait_for_response_since = undefined, last_seen = Now},
			case Success of
				% An earlier response.
				_ when P#dp.mors == slave ->
					?ERR("Received AE response after stepping down"),
					{reply,ok,P};
				true ->
					reply(From,ok),
					NP = actordb_sqlprocutil:reply_maybe(actordb_sqlprocutil:continue_maybe(
						P,NFlw,AEType == head orelse AEType == empty)),
					?DBG("AE response for node ~p, followers=~p",
						[Node,[{F#flw.node,F#flw.match_index,F#flw.match_term,F#flw.next_index} || F <- NP#dp.follower_indexes]]),
					{noreply,NP};
				% What we thought was follower is ahead of us and we need to step down
				false when P#dp.current_term < CurrentTerm ->
					?DBG("My term is out of date {His,Mine}=~p",[{CurrentTerm,P#dp.current_term}]),
					{reply,ok,actordb_sqlprocutil:reopen_db(actordb_sqlprocutil:save_term(
						P#dp{mors = slave,current_term = CurrentTerm,
							election = actordb_sqlprocutil:election_timer(Now,P#dp.election),
							masternode = undefined, without_master_since = Now,
							masternodedist = undefined,
							voted_for = undefined, follower_indexes = []}))};
				false when NFlw#flw.match_index == P#dp.evnum ->
					% Follower is up to date. He replied false. Maybe our term was too old.
					{reply,ok,actordb_sqlprocutil:reply_maybe(actordb_sqlprocutil:store_follower(P,NFlw))};
				false ->
					% Check if we are copying entire db to that node already, do nothing.
					case [C || C <- P#dp.dbcopy_to, C#cpto.node == Node, C#cpto.actorname == P#dp.actorname] of
						[_|_] ->
							?DBG("Ignoring appendendentries false response because copying to"),
							{reply,ok,P};
						[] ->
							case actordb_sqlprocutil:try_wal_recover(P,NFlw) of
								{false,NP,NF} ->
									?DBG("Can not recover from log, sending entire db"),
									% We can not recover from wal. Send entire db.
									Ref = make_ref(),
									case bkdcore:rpc(NF#flw.node,{?MODULE,call_slave,
											[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
											{dbcopy,{start_receive,actordb_conf:node_name(),Ref}}]}) of
										ok ->
											DC = {send_db,{NF#flw.node,Ref,false,P#dp.actorname}},
											actordb_sqlprocutil:dbcopy_call(DC,From,NP);
										_Err ->
											?ERR("Unable to send db ~p",[_Err]),
											{reply,false,P}
									end;
								{true,NP,NF} ->
									% we can recover from wal
									?DBG("Recovering from wal, for node=~p, match_index=~p, match_term=~p, myevnum=~p",
											[NF#flw.node,NF#flw.match_index,NF#flw.match_term,P#dp.evnum]),
									reply(From,ok),
									{noreply,actordb_sqlprocutil:continue_maybe(NP,NF,false)}
							end
					end
			end
	end;
state_rw_call({request_vote,Candidate,NewTerm,LastEvnum,LastTerm} = What,From,P) ->
	?DBG("Request vote for=~p, mors=~p, {histerm,myterm}=~p, {HisLogTerm,MyLogTerm}=~p {HisEvnum,MyEvnum}=~p",
	[Candidate,P#dp.mors,{NewTerm,P#dp.current_term},{LastTerm,P#dp.evterm},{LastEvnum,P#dp.evnum}]),
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
	Follower = lists:keyfind(Candidate,#flw.node,P#dp.follower_indexes),
	Now = actordb_local:elapsed_time(),
	case Follower of
		false when P#dp.mors == master ->
			?DBG("Adding node to follower list ~p",[Candidate]),
			state_rw_call(What,From,actordb_sqlprocutil:store_follower(P,#flw{node = Candidate}));
		_ ->
			case ok of
				% Candidates term is lower than current_term, ignore.
				_ when NewTerm < P#dp.current_term ->
					DoElection = (P#dp.mors == master andalso P#dp.verified == true),
					reply(From,{outofdate,actordb_conf:node_name(),P#dp.current_term,{P#dp.evnum,P#dp.evterm}}),
					NP = P;
				% We've already seen this term, only vote yes if we have not voted
				%  or have voted for this candidate already.
				_ when NewTerm == P#dp.current_term ->
					case (P#dp.voted_for == undefined orelse P#dp.voted_for == Candidate) of
						true when Uptodate ->
							DoElection = false,

							reply(From,{true,actordb_conf:node_name(),NewTerm,{P#dp.evnum,P#dp.evterm}}),
							NP = actordb_sqlprocutil:save_term(P#dp{voted_for = Candidate,
							current_term = NewTerm,
							election = actordb_sqlprocutil:election_timer(Now,P#dp.election)});
						true ->
							DoElection = (P#dp.mors == master andalso P#dp.verified == true),
							reply(From,{outofdate,actordb_conf:node_name(),NewTerm,{P#dp.evnum,P#dp.evterm}}),
							NP = actordb_sqlprocutil:save_term(P#dp{voted_for = undefined, current_term = NewTerm});
						false ->
							DoElection =(P#dp.mors == master andalso P#dp.verified == true),
							AV = {alreadyvoted,actordb_conf:node_name(),P#dp.current_term,{P#dp.evnum,P#dp.evterm}},
							reply(From,AV),
							NP = P
					end;
				% New candidates term is higher than ours, is he as up to date?
				_ when Uptodate ->
					DoElection = false,
					reply(From,{true,actordb_conf:node_name(),NewTerm,{P#dp.evnum,P#dp.evterm}}),
					NP = actordb_sqlprocutil:save_term(P#dp{voted_for = Candidate, current_term = NewTerm,
					election = actordb_sqlprocutil:election_timer(Now,P#dp.election)});
				% Higher term, but not as up to date. We can not vote for him.
				% We do have to remember new term index though.
				_ ->
					DoElection = (P#dp.mors == master andalso P#dp.verified == true),
					reply(From,{outofdate,actordb_conf:node_name(),NewTerm,{P#dp.evnum,P#dp.evterm}}),
					NP = actordb_sqlprocutil:save_term(P#dp{voted_for = undefined, current_term = NewTerm,
						election = actordb_sqlprocutil:election_timer(Now,P#dp.election)})
			end,
			?DBG("Doing election after request_vote? ~p, mors=~p, verified=~p, election=~p",
					[DoElection,P#dp.mors,P#dp.verified,P#dp.election]),
			{noreply,NP#dp{election = actordb_sqlprocutil:election_timer(Now,P#dp.election)}}
	end;
state_rw_call({delete,_MovedToNode},From,P) ->
	ok = actordb_sqlite:wal_rewind(P,0),
	reply(From,ok),
	{stop,normal,P};
state_rw_call(checkpoint,_From,P) ->
	actordb_sqlprocutil:checkpoint(P),
	{reply,ok,P}.

append_wal(P,From,CallCount,[Header|HT],[Body|BT],AEType) ->
	case append_wal(P,From,CallCount,Header,Body,AEType) of
		{noreply,NP} ->
			{noreply,NP};
		{reply,ok,NP} when HT /= [] ->
			append_wal(NP,From,CallCount,HT,BT,AEType);
		{reply,ok,NP} ->
			{reply,ok,NP}
	end;
append_wal(P,From,CallCount,Header,Body,AEType) ->
	AWR = actordb_sqlprocutil:append_wal(P,Header,Body),
	append_wal1(P,From,CallCount,Header,AEType,AWR).
append_wal1(P,From,CallCount,Header,AEType,AWR) ->
	case AWR of
		{ok,NS} ->
			case Header of
				% dbsize == 0, not last page
				<<_:20/binary,0:32>> ->
					?DBG("AE append ~p",[AEType]),
					{reply,ok,P#dp{locked = [ae], cbstate = NS}};
				% last page
				<<Evterm:64/unsigned-big,Evnum:64/unsigned-big,Pgno:32,Commit:32>> ->
					?DBG("AE WAL done evnum=~p,evterm=~p,aetype=~p,qempty=~p,master=~p,pgno=~p,commit=~p",
							[Evnum,Evterm,AEType,queue:is_empty(P#dp.callqueue),P#dp.masternode,Pgno,Commit]),
					% Prevent any timeouts on next ae since recovery process is progressing.
					case P#dp.inrecovery of
						true ->
							RecoveryAge = actordb_local:elapsed_time();
						false ->
							RecoveryAge = P#dp.recovery_age
					end,
					NP = P#dp{evnum = Evnum, evterm = Evterm,locked = [], recovery_age = RecoveryAge, cbstate = NS},
					reply(From,done),
					actordb_sqlprocutil:ae_respond(NP,NP#dp.masternode,true,P#dp.evnum,AEType,CallCount),
					{noreply,NP}
			end;
		_ ->
			?ERR("Append failed"),
			reply(From,false),
			actordb_sqlprocutil:ae_respond(P,P#dp.masternode,false,P#dp.evnum,AEType,CallCount),
			{noreply,P}
	end.

read_call(#read{sql = [exists]},_From,#dp{mors = master} = P) ->
	{reply,{ok,[{columns,{<<"exists">>}},{rows,[{<<"true">>}]}]},P};
read_call(Msg,From,#dp{mors = master, rasync = AR} = P) ->
	self() ! timeout,
	case Msg#read.sql of
		{Mod,Func,Args} ->
			case apply(Mod,Func,[P#dp.cbstate|Args]) of
				{reply,What,Sql,NS} ->
					AR1 = AR#ai{buffer = [Sql|AR#ai.buffer], buffer_cf = [{tuple,What,From}|AR#ai.buffer_cf],
					buffer_recs = [[]|AR#ai.buffer_recs]},
					{noreply,P#dp{cbstate = NS, rasync = AR1}};
				{reply,What,NS} ->
					{reply,What,P#dp{cbstate = NS}};
				{reply,What} ->
					{reply,What,P};
				{Sql,Recs} when is_list(Recs) ->
					AR1 = AR#ai{buffer = [Sql|AR#ai.buffer], buffer_cf = [From|AR#ai.buffer_cf],
						buffer_recs = [Recs|AR#ai.buffer_recs]},
					{noreply,P#dp{rasync = AR1}};
				{Sql,State} ->
					AR1 = AR#ai{buffer = [Sql|AR#ai.buffer], buffer_cf = [From|AR#ai.buffer_cf],
						buffer_recs = [[]|AR#ai.buffer_recs]},
					{noreply,P#dp{cbstate = State, rasync = AR1}};
				Sql ->
					AR1 = AR#ai{buffer = [Sql|AR#ai.buffer], buffer_cf = [From|AR#ai.buffer_cf],
						buffer_recs = [[]|AR#ai.buffer_recs]},
					{noreply,P#dp{rasync = AR1}}
			end;
		{Sql,{Mod,Func,Args}} ->
			AR1 = AR#ai{buffer = [Sql|AR#ai.buffer], buffer_cf = [{mod,{Mod,Func,Args},From}|AR#ai.buffer_cf],
				buffer_recs = [[]|AR#ai.buffer_recs]},
			{noreply,P#dp{rasync = AR1}};
		{Sql,Recs} ->
			AR1 = AR#ai{buffer = [Sql|AR#ai.buffer], buffer_cf = [From|AR#ai.buffer_cf],
				buffer_recs = [Recs|AR#ai.buffer_recs]},
			{noreply,P#dp{rasync = AR1}};
		Sql ->
			AR1 = AR#ai{buffer = [Sql|AR#ai.buffer], buffer_cf = [From|AR#ai.buffer_cf],
				buffer_recs = [[]|AR#ai.buffer_recs]},
			{noreply,P#dp{rasync = AR1}}
	end;
read_call(_Msg,_From,P) ->
	?DBG("redirect read ~p",[P#dp.masternode]),
	actordb_sqlprocutil:redirect_master(P).

% Execute buffered read sqls
read_call1(_,_,[],P) ->
	P#dp{activity = actordb_local:actor_activity(P#dp.activity)};
read_call1(_,_,From,#dp{mors = slave} = P) ->
	[reply(F,{redirect,P#dp.masternode}) || F <- From],
	P#dp{rasync = #ai{}};
read_call1(Sql,Recs,From,P) ->
	ComplSql = list_to_tuple(Sql),
	Records = list_to_tuple(Recs),
	?DBG("READ SQL=~p, Recs=~p, from=~p",[ComplSql, Records,From]),
	%
	% Direct read mode (not async)
	%
	Res = actordb_sqlite:exec(P#dp.db,ComplSql,Records,read),
	case Res of
		{ok,ResTuples} ->
			?DBG("Read resp=~p",[Res]),
			read_reply(P#dp{rasync = #ai{}, activity = actordb_local:actor_activity(P#dp.activity)}, From, 1, ResTuples);
		{sql_error,ErrMsg,_} = Err ->
			?ERR("Read call error: ~p",[Err]),
			ErrPos = element(1,ErrMsg),

			{Before,[Problem|After]} = lists:split(ErrPos,From),
			reply(Problem, Res),

			{BeforeSql,[_ProblemSql|AfterSql]} = lists:split(ErrPos,Sql),
			{BeforeRecs,[_ProblemRecs|AfterRecs]} = lists:split(ErrPos,Recs),

			read_call1(BeforeSql++AfterSql, BeforeRecs++AfterRecs, Before++After,P#dp{rasync = #ai{}})
	end.

	%
	% Async mode, less safe because it can return pages that have not been replicated.
	% We can make the storage engine level correct (lmdb), but what we can't fix at the moment
	% is sqlite page cache. Any write will store pages in cache. Which means reads will use those
	% unsafe cache pages instead of what is stored in lmdb.
	% This unfortunately means we can't process reads while writes are running. Reads are executed
	% before writes.
	% We could use seperate read/write connections. This also means there is a read and write
	% sqlite page cache. After every write, read connection page cache must be cleared. How
	% detrimental to performance that would be is something that needs to be tested.
	%
	% Recompile driver with threadsafe=1 if using async reads.
	%
	% Res = actordb_sqlite:exec_async(P#dp.db,ComplSql,Records,read),
	% A = P#dp.rasync,
	% NRB = A#ai{wait = Res, info = Sql, callfrom = From, buffer = [], buffer_cf = [], buffer_recs = []},
	% P#dp{rasync = NRB}.

write_call(#write{mfa = MFA, sql = Sql} = Msg,From,P) ->
	A = P#dp.wasync,
	ForceSync = lists:member(fsync,Msg#write.flags),
	?DBG("writecall evnum_prewrite=~p,term=~p writeinfo=~p",[P#dp.evnum,P#dp.current_term,{MFA,Sql}]),
	% Drain message queue.
	self () ! timeout,
	case Sql of
		delete ->
			A1 = A#ai{buffer = [<<"#s02;">>|A#ai.buffer], buffer_cf = [From|A#ai.buffer_cf],
				buffer_recs = [[[[?MOVEDTOI,<<"$deleted$">>]]]|A#ai.buffer_recs],
				buffer_moved = deleted, buffer_fsync = A#ai.buffer_fsync or ForceSync},
			{noreply,P#dp{wasync = A1}};
		{moved,MovedTo} ->
			A1 = A#ai{buffer = [<<"#s02;">>|A#ai.buffer], buffer_cf = [From|A#ai.buffer_cf],
				buffer_recs = [[[[?MOVEDTOI,MovedTo]]]|A#ai.buffer_recs],
				buffer_moved = {moved,MovedTo}, buffer_fsync = A#ai.buffer_fsync or ForceSync},
			{noreply,P#dp{wasync = A1}};
		% If new schema version write, add sql to first place of list of writes.
		_ when Msg#write.newvers /= undefined, MFA == undefined ->
			A1 = A#ai{buffer = A#ai.buffer++[Sql], buffer_recs = A#ai.buffer_recs++[Msg#write.records],
				buffer_cf = A#ai.buffer_cf++[From], buffer_nv = Msg#write.newvers,
				buffer_fsync = A#ai.buffer_fsync or ForceSync},
			{noreply,P#dp{wasync = A1}};
		_ when MFA == undefined ->
			A1 = A#ai{buffer = [Sql|A#ai.buffer], buffer_cf = [From|A#ai.buffer_cf],
				buffer_recs = [Msg#write.records|A#ai.buffer_recs], buffer_fsync = A#ai.buffer_fsync or ForceSync},
			{noreply,P#dp{wasync = A1}};
		_ ->
			{Mod,Func,Args} = MFA,
			case apply(Mod,Func,[P#dp.cbstate|Args]) of
				{reply,What,OutSql,NS} ->
					reply(From,What),
					A1 = A#ai{buffer = [OutSql|A#ai.buffer], buffer_recs = [[]|A#ai.buffer_recs],
						buffer_cf = [undefined|A#ai.buffer_cf], buffer_fsync = A#ai.buffer_fsync or ForceSync},
					{noreply,P#dp{wasync = A1, cbstate = NS}};
				{reply,What,NS} ->
					{reply,What,P#dp{cbstate = NS}};
				{reply,What} ->
					{reply,What,P};
				{exec,OutSql,Recs} ->
					A1 = A#ai{buffer = [OutSql|A#ai.buffer], buffer_recs = [Recs|A#ai.buffer_recs],
						buffer_cf = [From|A#ai.buffer_cf], buffer_fsync = A#ai.buffer_fsync or ForceSync},
					{noreply,P#dp{wasync = A1}};
				% For when a node wants to take its marbles and go play by itself.
				{isolate,OutSql,State} ->
					A1 = A#ai{buffer = [OutSql|A#ai.buffer], buffer_recs = [[]|A#ai.buffer_recs],
						buffer_cf = [From|A#ai.buffer_cf], buffer_fsync = A#ai.buffer_fsync or ForceSync},
					{noreply,P#dp{wasync = A1, cbstate = State, verified = true, mors = master, follower_indexes = []}};
				{OutSql,Recs} when is_list(Recs) ->
					A1 = A#ai{buffer = [OutSql|A#ai.buffer], buffer_recs = [Recs|A#ai.buffer_recs],
						buffer_cf = [From|A#ai.buffer_cf], buffer_fsync = A#ai.buffer_fsync or ForceSync},
					{noreply,P#dp{wasync = A1}};
				{OutSql,State} ->
					A1 = A#ai{buffer = [OutSql|A#ai.buffer], buffer_recs = [[]|A#ai.buffer_recs],
						buffer_cf = [From|A#ai.buffer_cf], buffer_fsync = A#ai.buffer_fsync or ForceSync},
					{noreply,P#dp{wasync = A1, cbstate = State}};
				{OutSql,Recs,State} ->
					A1 = A#ai{buffer = [OutSql|A#ai.buffer], buffer_recs = [Recs|A#ai.buffer_recs],
						buffer_cf = [From|A#ai.buffer_cf], buffer_fsync = A#ai.buffer_fsync or ForceSync},
					{noreply,P#dp{wasync = A1, cbstate = State}};
				OutSql ->
					A1 = A#ai{buffer = [OutSql|A#ai.buffer], buffer_recs = [[]|A#ai.buffer_recs],
						buffer_cf = [From|A#ai.buffer_cf], buffer_fsync = A#ai.buffer_fsync or ForceSync},
					{noreply,P#dp{wasync = A1}}
			end
	end.

% print_sqls(Pos,Sql,Recs) when tuple_size(Sql) >= Pos ->
% 	?ADBG("SQL=~p,     Recs=~p",[element(Pos,Sql),element(Pos,Recs)]),
% 	print_sqls(Pos+1,Sql,Recs);
% print_sqls(_,_,_) ->
% 	ok.

write_call2(#dp{db = queue, wasync = #ai{wait = Ref}} = P) ->
	element(2,handle_info({Ref,ok},P));
write_call2(P) ->
	P.

% Not a multiactor transaction write
write_call1(_W,From,_CF,#dp{mors = slave} = P) ->
	?ADBG("Redirecting write ~p from=~p, w=~p",[P#dp.masternode,From,_W]),
	[reply(F,{redirect,P#dp.masternode}) || F <- From],
	P#dp{wasync = #ai{}};
write_call1(#write{sql = Sql,transaction = undefined} = W,From,NewVers,P) ->
	EvNum = P#dp.evnum+1,
	VarHeader = actordb_sqlprocutil:create_var_header(P),
	case P#dp.db of
		queue ->
			Res = make_ref(),
			CF = [batch,lists:reverse(From)],
			{ok,NS} = actordb_queue:cb_write_exec(P#dp.cbstate,lists:reverse(Sql),P#dp.current_term, EvNum);
		_ ->
			NS = P#dp.cbstate,
			CF = [batch,undefined|lists:reverse([undefined|From])],
			ComplSql = list_to_tuple([<<"#s00;">>|lists:reverse([<<"#s02;#s01;">>|Sql])]),
			ADBW = [[[?EVNUMI,butil:tobin(EvNum)],[?EVTERMI,butil:tobin(P#dp.current_term)]]],
			Records = list_to_tuple([[]|lists:reverse([ADBW|W#write.records])]),
			?DBG("schema = ~p, SQL=~p, Recs=~p, cf=~p",[P#dp.schemavers,ComplSql, Records, CF]),
			% print_sqls(1,ComplSql,Records),
			Res = actordb_sqlite:exec_async(P#dp.db,ComplSql,Records,P#dp.current_term,EvNum,VarHeader)
	end,
	A = P#dp.wasync,
	NWB = A#ai{wait = Res, info = W, newvers = NewVers,
		callfrom = CF, evnum = EvNum, evterm = P#dp.current_term,
		moved = A#ai.buffer_moved, fsync = A#ai.buffer_fsync,
		buffer_moved = undefined, buffer_nv = undefined, buffer_fsync = false,
		buffer = [], buffer_cf = [], buffer_recs = []},
	write_call2(P#dp{wasync = NWB, activity = actordb_local:actor_activity(P#dp.activity), cbstate = NS});
write_call1(#write{sql = Sql1, transaction = {Tid,Updaterid,Node} = TransactionId} = W,From,NewVers,P) ->
	{_CheckPid,CheckRef} = actordb_sqlprocutil:start_transaction_checker(Tid,Updaterid,Node),
	?DBG("Starting transaction write id ~p, curtr ~p, sql ~p",[TransactionId,P#dp.transactionid,Sql1]),
	ForceSync = lists:member(fsync,W#write.flags),
	case P#dp.follower_indexes of
		[] ->
			% If single node cluster, no need to store sql first.
			case P#dp.transactionid of
				TransactionId ->
					% Transaction can write to single actor more than once (especially for KV stores)
					% if we are already in this transaction, just update sql.
					{_OldSql,EvNum,_} = P#dp.transactioninfo,
					case Sql1 of
						delete ->
							ComplSql = <<"delete">>,
							Res = ok;
						_ ->
							ComplSql = Sql1,
							Res = actordb_sqlite:exec(P#dp.db,ComplSql,write)
					end;
				undefined ->
					EvNum = P#dp.evnum+1,
					case Sql1 of
						delete ->
							Res = ok,
							ComplSql = <<"delete">>;
						_ ->
							ComplSql =
								[<<"#s00;">>,
								actordb_sqlprocutil:semicolon(Sql1),
								<<"#s02;">>
								],
							AWR = [[?EVNUMI,butil:tobin(EvNum)],[?EVTERMI,butil:tobin(P#dp.current_term)]],
							Records = W#write.records++[AWR],
							VarHeader = actordb_sqlprocutil:create_var_header(P),
							Res = actordb_sqlite:exec(P#dp.db,ComplSql,Records,P#dp.current_term,EvNum,VarHeader)
					end
			end,
			case actordb_sqlite:okornot(Res) of
				ok ->
					?DBG("Transaction ok"),
					{noreply, actordb_sqlprocutil:reply_maybe(P#dp{transactionid = TransactionId,
						evterm = P#dp.current_term,
						transactioncheckref = CheckRef,force_sync = ForceSync,
						transactioninfo = {ComplSql,EvNum,NewVers},
						activity = actordb_local:actor_activity(P#dp.activity),
						callfrom = From, callres = Res})};
				_Err ->
					ok = actordb_sqlite:rollback(P#dp.db),
					erlang:demonitor(CheckRef),
					?DBG("Transaction not ok ~p",[_Err]),
					{reply,Res,P#dp{transactionid = undefined, activity = actordb_local:actor_activity(P#dp.activity),
						evterm = P#dp.current_term}}
			end;
		_ ->
			EvNum = P#dp.evnum+1,
			case P#dp.transactionid of
				TransactionId when Sql1 /= delete ->
					% Rollback prev version of sql.
					ok = actordb_sqlite:rollback(P#dp.db),
					{OldSql,_EvNum,_} = P#dp.transactioninfo,
					% Combine prev sql with new one.
					Sql = iolist_to_binary([OldSql,Sql1]);
				TransactionId ->
					Sql = <<"delete">>;
				_ ->
					case Sql1 of
						delete ->
							Sql = <<"delete">>;
						_ ->
							Sql = iolist_to_binary(Sql1)
					end
			end,
			ComplSql = <<"#s00;#s02;#s03;#s01;">>,
			TransRecs = [[[butil:tobin(Tid),butil:tobin(Updaterid),Node,butil:tobin(NewVers),base64:encode(Sql)]]],
			Records = [[[?EVNUMI,butil:tobin(EvNum)],[?EVTERMI,butil:tobin(P#dp.current_term)]]|TransRecs],
			VarHeader = actordb_sqlprocutil:create_var_header(P),
			ok = actordb_sqlite:okornot(actordb_sqlite:exec(
				P#dp.db,ComplSql,Records,P#dp.current_term,EvNum,VarHeader)),
			{noreply,ae_timer(P#dp{callfrom = From,callres = undefined, evterm = P#dp.current_term,evnum = EvNum,
				transactioninfo = {Sql,EvNum+1,NewVers}, 
				transactioncheckref = CheckRef,force_sync = ForceSync,
				transactionid = TransactionId})}
	end.

ae_timer(P) ->
	Now = actordb_local:elapsed_time(),
	P#dp{election = actordb_sqlprocutil:election_timer(Now,P#dp.election),
	callat = {Now,0},activity = actordb_local:actor_activity(P#dp.activity),
	follower_indexes = [F#flw{wait_for_response_since = Now} || F <- P#dp.follower_indexes]}.



handle_cast({diepls,_Reason},P) ->
	?DBG("Received diepls ~p",[_Reason]),
	W = P#dp.wasync,
	R = P#dp.rasync,
	Inactive = queue:is_empty(P#dp.callqueue) andalso W#ai.buffer == [] andalso R#ai.buffer == [] andalso
		P#dp.dbcopy_to == [] andalso P#dp.locked == [] andalso P#dp.copyfrom == undefined andalso
		W#ai.wait == undefined andalso R#ai.wait == undefined andalso P#dp.transactioninfo == undefined,
	CanDie = apply(P#dp.cbmod,cb_candie,[P#dp.mors,P#dp.actorname,P#dp.actortype,P#dp.cbstate]),
	?DBG("verified ~p, empty ~p, candie ~p, state=~p",[P#dp.verified,Inactive,CanDie,?R2P(P)]),
	case ok of
		_ when P#dp.verified, Inactive, CanDie /= never ->
			{stop,normal,P};
		_ ->
			{noreply,P}
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
	?INF("sqlproc ~p unhandled cast ~p~n",[P#dp.cbmod,_Msg]),
	{noreply,P}.

% shards/kv can have reads that turn into writes, or have extra data to return along with read.
read_reply(P,[H|T],Pos,Res) ->
	case H of
		{tuple,What,From} ->
			reply(From,{What,actordb_sqlite:exec_res({ok, element(Pos,Res)})}),
			read_reply(P,T,Pos+1,Res);
		{mod,{Mod,Func,Args},From} ->
			case apply(Mod,Func,[P#dp.cbstate,actordb_sqlite:exec_res({ok, element(Pos,Res)})|Args]) of
				{write,Write} ->
					case Write of
						_ when is_binary(Write); is_list(Write) ->
							{noreply,NP} = write_call(#write{sql = iolist_to_binary(Write)},From,P);
						{_,_,_} ->
							{noreply,NP} = write_call(#write{mfa = Write},From,P)
					end,
					read_reply(NP,T,Pos+1,Res);
				{write,Write,NS} ->
					case Write of
						_ when is_binary(Write); is_list(Write) ->
							{noreply,NP} = write_call(#write{sql = iolist_to_binary(Write)},
									   From,P#dp{cbstate = NS});
						{_,_,_} ->
							{noreply,NP} = write_call(#write{mfa = Write},From,P#dp{cbstate = NS})
					end,
					read_reply(NP,T,Pos+1,Res);
				{reply_write,Reply,Write,NS} ->
					reply(From,Reply),
					case Write of
						_ when is_binary(Write); is_list(Write) ->
							{noreply,NP} = write_call(#write{sql = iolist_to_binary(Write)},undefined,P#dp{cbstate = NS});
						{_,_,_} ->
							{noreply,NP} = write_call(#write{mfa = Write},undefined,P#dp{cbstate = NS})
					end,
					read_reply(NP,T,Pos+1,Res);
				{reply,What,NS} ->
					reply(From,What),
					read_reply(P#dp{cbstate = NS},T,Pos+1,Res);
				{reply,What} ->
					reply(From,What),
					read_reply(P,T,Pos+1,Res)
			end;
		From ->
			reply(From,actordb_sqlite:exec_res({ok, element(Pos,Res)})),
			read_reply(P,T,Pos+1,Res)
	end;
read_reply(P,[],_,_) ->
	P.

handle_info(timeout,P) ->
	{noreply,actordb_sqlprocutil:doqueue(P)};
% Async read result. Unlike writes we can reply directly. We don't use async reads atm.
handle_info({Ref,Res}, #dp{rasync = #ai{wait = Ref} = BD} = P) when is_reference(Ref) ->
	NewBD = BD#ai{callfrom = undefined, info = undefined, wait = undefined},
	case Res of
		{ok,ResTuples} ->
			?DBG("Read resp=~p",[Res]),
			{noreply,read_reply(P#dp{rasync = NewBD}, BD#ai.callfrom, 1, ResTuples)}
		% Err ->
		%   TODO: if async reads ever get used...
		% 	?ERR("Read call error: ~p",[Err]),
		% 	{noreply,P#dp{rasync = NewBD}}
	end;
% async write result
handle_info({Ref,Res1}, #dp{wasync = #ai{wait = Ref} = BD} = P) when is_reference(Ref) ->
	?DBG("Write result ~p",[Res1]),
	% ?DBG("Buffer=~p",[BD#ai.buffer]),
	% ?DBG("CQ=~p",[P#dp.callqueue]),
	Res = actordb_sqlite:exec_res(Res1),
	From = BD#ai.callfrom,
	EvNum = BD#ai.evnum,
	EvTerm = BD#ai.evterm,
	% ?DBG("Res1=~p, Callfrom=~p",[Res,From]),
	case BD#ai.newvers of
		undefined ->
			NewVers = P#dp.schemavers;
		NewVers ->
			ok
	end,
	Moved = BD#ai.moved,
	W = BD#ai.info,
	ForceSync = BD#ai.fsync,
	NewAsync = BD#ai{callfrom = undefined, evnum = undefined, evterm = undefined,
		newvers = undefined, info = undefined, wait = undefined, fsync = false},
	case actordb_sqlite:okornot(Res) of
		ok when P#dp.follower_indexes == []  ->
			{noreply,actordb_sqlprocutil:statequeue(actordb_sqlprocutil:reply_maybe(
				P#dp{callfrom = From, callres = Res,evnum = EvNum,
					netchanges = actordb_local:net_changes(), force_sync = ForceSync,
					schemavers = NewVers,evterm = EvTerm,movedtonode = Moved,
					wasync = NewAsync}))};
		ok ->
			% reply on appendentries response or later if nodes are behind.
			case P#dp.callres of
				undefined ->
					Callres = Res;
				Callres ->
					ok
			end,
			{noreply, actordb_sqlprocutil:statequeue(ae_timer(P#dp{callfrom = From, callres = Callres,
				netchanges = actordb_local:net_changes(),force_sync = ForceSync,
				evterm = EvTerm, evnum = EvNum,schemavers = NewVers,movedtonode = Moved,
				wasync = NewAsync}))};
		{sql_error,ErrMsg,_} ->
			actordb_sqlite:rollback(P#dp.db),

			% we don't count #s00
			ErrPos = element(1,ErrMsg)-1,
			[batch,undefined|CF1] = From,
			% Remove cf for last part (#s02, #s01)
			CF = lists:reverse(tl(lists:reverse(CF1))),
			?DBG("Error pos ~p, cf=~p",[ErrPos,CF]),
			{Before,[Problem|After]} = lists:split(ErrPos,CF),
			reply(Problem, Res),

			{BeforeSql,[_ProblemSql|AfterSql]} = lists:split(ErrPos,lists:reverse(W#write.sql)),
			{BeforeRecs,[_ProblemRecs|AfterRecs]} = lists:split(ErrPos,lists:reverse(W#write.records)),

			case BD#ai.newvers of
				undefined ->
					RemainCF = lists:reverse(Before++After),
					RemainSql = lists:reverse(BeforeSql++AfterSql),
					RemainRecs = lists:reverse(BeforeRecs++AfterRecs);
				_ ->
					RemainCF = lists:reverse(tl(Before++After)),
					RemainSql = lists:reverse(tl(BeforeSql++AfterSql)),
					RemainRecs = lists:reverse(tl(BeforeRecs++AfterRecs))
			end,
			% ?DBG("Remain cf=~p",[RemainCF]),
			% ?DBG("Remain sql=~p",[RemainSql]),
			% ?DBG("Remain recs=~p",[RemainRecs]),
			NewAsync1 = NewAsync#ai{buffer = RemainSql++NewAsync#ai.buffer,
				buffer_cf = RemainCF++NewAsync#ai.buffer_cf,
				buffer_recs = RemainRecs++NewAsync#ai.buffer_recs},
			?DBG("New write ~p",[NewAsync1]),
			handle_info(doqueue,actordb_sqlprocutil:statequeue(P#dp{wasync = NewAsync1}))
	end;
handle_info(doqueue, P) ->
	{noreply,actordb_sqlprocutil:doqueue(P)};
handle_info({hibernate,A},P) ->
	?DBG("hibernating"),
	{noreply,P#dp{activity = A},hibernate};
handle_info(copy_timer,P) ->
	case P#dp.dbcopy_to of
		[_|_] ->
			erlang:send_after(1000,self(),copy_timer);
		_ ->
			ok
	end,
	{noreply,P#dp{activity = actordb_local:actor_activity(P#dp.activity)}};
handle_info({'DOWN',Monitor,_,PID,Reason},P) ->
	down_info(PID,Monitor,Reason,P);
handle_info(doelection,P) ->
	self() ! doelection1,
	{noreply,P};
% First check if latencies changed.
handle_info({doelection,_LatencyBefore,_TimerFrom} = Msg,P) ->
	election_timer(Msg,P);
handle_info(doelection1,P) ->
	election_timer(doelection1,P);
handle_info({forget,Nd},P) ->
	?INF("Forgetting node ~p",[Nd]),
	{noreply,P#dp{follower_indexes = lists:keydelete(Nd,#flw.node,P#dp.follower_indexes)}};
handle_info(retry_copy,P) ->
	?DBG("Retry copy mors=~p, ver=~p, cl=~p",[P#dp.mors,P#dp.verified,P#dp.copylater]),
	case P#dp.mors == master andalso P#dp.verified == true of
		true ->
			{noreply,actordb_sqlprocutil:retry_copy(P)};
		_ ->
			{noreply, P}
	end;
handle_info({batch,L},P) ->
	?DBG("Batch=~p",[L]),
	{noreply, lists:foldl(fun({{Pid,Ref},W},NP) -> {noreply, NP1} = handle_call(W, {Pid,Ref}, NP), NP1 end, P, L)};
handle_info(check_locks,P) ->
	case P#dp.locked of
		[] ->
			{noreply,P};
		_ ->
			erlang:send_after(1000,self(),check_locks),
			{noreply, actordb_sqlprocutil:check_locks(P,P#dp.locked,[])}
	end;
handle_info(stop,P) ->
	?DBG("Received stop msg"),
	handle_info({stop,normal},P);
handle_info({stop,Reason},P) ->
	?DBG("Actor stop with reason ~p",[Reason]),
	{stop, normal, P};
handle_info(print_info,P) ->
	handle_cast(print_info,P);
handle_info(commit_transaction,P) ->
	down_info(0,12345,done,P#dp{transactioncheckref = 12345});
handle_info(start_copy,P) ->
	?DBG("Start copy ~p",[P#dp.copyfrom]),
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
		Rpc = {?MODULE,call,[{OldActor,P#dp.actortype},[],Msg,P#dp.cbmod,onlylocal]},
		case actordb:rpc(Node,OldActor,Rpc) of
			ok ->
				?DBG("Ok response for startcopy msg"),
				ok;
			{ok,_} ->
				?DBG("Ok response for startcopy msg"),
				ok;
			{redirect,_} ->
				?DBG("Received redirect, presume job is done"),
				Home ! start_copy_done;
			Err ->
				?ERR("Unable to start copy from ~p, ~p",[P#dp.copyfrom,Err]),
				Home ! {stop,Err}
		end
	end),
	{noreply,P};
handle_info(start_copy_done,P) ->
	{ok,NP} = init(P,copy_done),
	{noreply,NP};
handle_info(Msg,#dp{verified = true} = P) ->
	case apply(P#dp.cbmod,cb_info,[Msg,P#dp.cbstate]) of
		{noreply,S} ->
			{noreply,P#dp{cbstate = S}};
		noreply ->
			{noreply,P}
	end;
handle_info(_Msg,P) ->
	?DBG("sqlproc ~p unhandled info ~p~n",[P#dp.cbmod,_Msg]),
	{noreply,P}.


election_timer({doelection,LatencyBefore,_TimerFrom},P) ->
	LatencyNow = actordb_latency:latency(),
	% Delay if latency significantly increased since start of timer.
	% But only if more than 100ms latency. Which should mean significant load or bad network which
	%  from here means same thing.
	case LatencyNow > (LatencyBefore*1.5) andalso LatencyNow > 100 of
		true ->
			{noreply,P#dp{election = actordb_sqlprocutil:election_timer(undefined),
				activity = actordb_local:actor_activity(P#dp.activity)}};
		false ->
			% Clear out msg queue first.
			self() ! doelection1,
			{noreply,P#dp{activity = actordb_local:actor_activity(P#dp.activity)}}
	end;
% Are any write results pending?
election_timer(doelection1,P) ->
	case P#dp.callfrom of
		undefined ->
			election_timer(doelection2,P);
		_ ->
			LatencyNow = actordb_latency:latency(),
			Now = actordb_local:elapsed_time(),
			{CallTime,Noops} = P#dp.callat,
			% More than a second after write is finished (and sent to followers)
			case Now - CallTime > 1000+LatencyNow of
				true when Noops == 0 ->
					?ERR("Write is taking long to reach consensus"),
					% Try an empty write.
					{noreply,P#dp{callat = {CallTime,1}, election = actordb_sqlprocutil:election_timer(undefined)}};
				true when Noops == 1 ->
					?ERR("Still have not reached consensus"),
					{noreply,P#dp{callat = {CallTime,2}, election = actordb_sqlprocutil:election_timer(undefined)}};
				true when Noops == 2 ->
					?ERR("Write abandon with consensus_timeout"),
					reply(P#dp.callfrom,{error,consensus_timeout}),
					% Step down as leader.
					election_timer(doelection2,P#dp{callfrom = undefined, callres = undefined, 
						masternode = undefined,masternodedist = undefined,
						verified = false, mors = slave, without_master_since = CallTime});
				false ->
					{noreply,P#dp{election = actordb_sqlprocutil:election_timer(undefined),
						activity = actordb_local:actor_activity(P#dp.activity)}}
			end
	end;
% Check if there is anything we need to do, like run another election, issue an empty write or wait some more.
election_timer(doelection2,P) ->
	A = P#dp.wasync,
	Empty = queue:is_empty(P#dp.callqueue) andalso A#ai.buffer_cf == [],
	?DBG("Election timeout, master=~p, verified=~p, followers=~p",
		[P#dp.masternode,P#dp.verified,P#dp.follower_indexes]),
	Now = actordb_local:elapsed_time(),
	Me = actordb_conf:node_name(),
	LatencyNow = actordb_latency:latency(),
	case ok of
		_ when P#dp.verified, P#dp.mors == master, P#dp.dbcopy_to /= [] ->
			% Copying db, wait some more
			{noreply,P#dp{election = actordb_sqlprocutil:election_timer(Now,undefined)}};
		_ when P#dp.verified, P#dp.mors == master ->
			actordb_sqlprocutil:follower_check_handle(P);
		_ when is_pid(P#dp.election) ->
			% We are candidate, wait for election to complete.
			{noreply,P};
		% Unless leader is known and available, start an election.
		_ when P#dp.masternode /= undefined, P#dp.masternode /= Me ->
			% We are follower and masternode is set. This means leader sent us at least one AE.
			% Is connection active?
			case bkdcore_rpc:is_connected(P#dp.masternode) of
				true ->
					?DBG("Election timeout, do nothing, leader=~p",[P#dp.masternode]),
					{noreply,P#dp{without_master_since = undefined}};
				false ->
					% We had leader, but he is gone
					?DBG("Leader is gone, leader=~p, election=~p, empty=~p, me=~p",
						[P#dp.masternode,P#dp.election,Empty,actordb_conf:node_name()]),
					NP = P#dp{election = undefined,without_master_since = Now, 
						masternode = undefined, masternodedist = undefined},
					{noreply,actordb_sqlprocutil:start_verify(NP,false)}
			end;
		_ when P#dp.without_master_since == undefined ->
			?DBG("Leader timeout, leader=~p, election=~p, empty=~p, me=~p",
				[P#dp.masternode,P#dp.election,Empty,actordb_conf:node_name()]),
			% Start counter how long we are looking for leader for.
			NP = P#dp{election = undefined,without_master_since = Now},
			{noreply,actordb_sqlprocutil:start_verify(NP,false)};
		_ when P#dp.election == undefined ->
			% If election undefined this should be a hint from outside. 
			{noreply,actordb_sqlprocutil:start_verify(P,false)};
		_ when Now - P#dp.without_master_since >= 3000+LatencyNow, Empty == false ->
			?ERR("Unable to establish leader, responding with error"),
			% It took too long. Respond with error.
			actordb_sqlprocutil:empty_queue(P#dp.wasync, P#dp.callqueue,{error,consensus_impossible_atm}),
			A1 = A#ai{buffer = [], buffer_recs = [], buffer_cf = [],
				buffer_nv = undefined, buffer_moved = undefined},
			% Give up for now. Do not run elections untill we get a hint from outside.
			% Hint will come from catchup or a client wanting to execute read/write.
			actordb_catchup:report(P#dp.actorname,P#dp.actortype),
			{noreply,P#dp{callqueue = queue:new(),election = undefined,wasync = A1}};
		_ when Now - P#dp.without_master_since >= 3000+LatencyNow ->
			actordb_catchup:report(P#dp.actorname,P#dp.actortype),
			% Give up and wait for hint.
			{noreply,P#dp{election = undefined}};
		_ ->
			?DBG("Election timeout"),
			{noreply,actordb_sqlprocutil:start_verify(P#dp{election = undefined},false)}
	end.

down_info(PID,_,{leader,_,_},#dp{election = PID} = P) when (P#dp.flags band ?FLAG_CREATE) == 0, 
		P#dp.schemavers == undefined ->
	?INF("Stopping with nocreate ",[]),
	Nodes = actordb_sqlprocutil:follower_nodes(P#dp.follower_indexes),
	spawn(fun() -> bkdcore_rpc:multicall(Nodes,{actordb_sqlproc,call_slave,
		[P#dp.cbmod,P#dp.actorname,P#dp.actortype,stop]}) end),
	{stop,nocreate,P};
down_info(PID,_,{leader,_,_},#dp{election = PID} = P) when (P#dp.flags band ?FLAG_CREATE) == 0, 
		P#dp.movedtonode == deleted ->
	?INF("Stopping with nocreate ",[]),
	Nodes = actordb_sqlprocutil:follower_nodes(P#dp.follower_indexes),
	spawn(fun() -> bkdcore_rpc:multicall(Nodes,{actordb_sqlproc,call_slave,
		[P#dp.cbmod,P#dp.actorname,P#dp.actortype,stop]}) end),
	{stop,nocreate,P};
down_info(PID,_Ref,{leader,NewFollowers,AllSynced},#dp{election = PID} = P1) ->
	actordb_local:actor_mors(master,actordb_conf:node_name()),
	P = actordb_sqlprocutil:reopen_db(P1#dp{mors = master, election = undefined,
		masternode = actordb_conf:node_name(),
		without_master_since = undefined,
		masternodedist = bkdcore:dist_name(actordb_conf:node_name()),
		flags = P1#dp.flags band (bnot ?FLAG_WAIT_ELECTION),
		locked = lists:delete(ae,P1#dp.locked)}),
	case P#dp.movedtonode of
		deleted ->
			actordb_sqlprocutil:actually_delete(P1),
			Moved = undefined,
			SchemaVers = undefined;
		_ ->
			Moved = P#dp.movedtonode,
			SchemaVers = P#dp.schemavers
	end,
	ReplType = apply(P#dp.cbmod,cb_replicate_type,[P#dp.cbstate]),
	?DBG("Elected leader term=~p, nodes_synced=~p, moved=~p",[P1#dp.current_term,AllSynced,P#dp.movedtonode]),
	ReplBin = term_to_binary({P#dp.cbmod,P#dp.actorname,P#dp.actortype}),
	ok = actordb_sqlite:replicate_opts(P,ReplBin,ReplType),

	case P#dp.schemavers of
		undefined ->
			Transaction = [],
			Rows = [];
		_ ->
			case actordb_sqlite:exec(P#dp.db,
					<<"SELECT * FROM __adb;",
					  "SELECT * FROM __transactions;">>,read) of
				{ok,[[{columns,_},{rows,Transaction}],[{columns,_},{rows,Rows}]]} ->
					ok;
				Err ->
					?ERR("Unable read from db for, error=~p after election.",[Err]),
					Transaction = Rows = [],
					exit(error)
			end
	end,

	case butil:ds_val(?COPYFROMI,Rows) of
		CopyFrom1 when byte_size(CopyFrom1) > 0 ->
			CbInit = true,%P#dp.cbinit,
			{CopyFrom,CopyReset,CbState} = binary_to_term(base64:decode(CopyFrom1));
		_ ->
			CbInit = false,%P#dp.cbinit,
			CopyFrom = CopyReset = undefined,
			CbState = P#dp.cbstate
	end,
	% After election is won a write needs to be executed. What we will write depends on the situation:
	%  - If this actor has been moving, do a write to clean up after it (or restart it)
	%  - If transaction active continue with write.
	%  - If empty db or schema not up to date create/update it.
	%  - It can also happen that both transaction active and actor move is active. Sqls will be combined.
	%  - Otherwise just empty sql, which still means an increment for evnum and evterm in __adb.
	NP1 = P#dp{verified = true,copyreset = CopyReset,movedtonode = Moved,
		cbstate = CbState, schemavers = SchemaVers, cbinit = CbInit},
	{NP,Sql,AdbRecords,Callfrom} =
		actordb_sqlprocutil:post_election_sql(NP1,Transaction,CopyFrom,[],P#dp.callfrom),
	% If nothing to store and all nodes synced, send an empty AE.
	case is_number(P#dp.schemavers) andalso is_atom(Sql) == false andalso iolist_size(Sql) == 0 of
		true when AllSynced, NewFollowers == [] ->
			?DBG("Nodes synced, no followers"),
			W = NP#dp.wasync,
			{noreply,actordb_sqlprocutil:doqueue(actordb_sqlprocutil:do_cb(
				NP#dp{follower_indexes = [],netchanges = actordb_local:net_changes(),
				wasync = W#ai{nreplies = W#ai.nreplies+1}}))};
		true when AllSynced ->
			?DBG("Nodes synced, running empty AE."),
			NewFollowers1 = [actordb_sqlprocutil:send_empty_ae(P,NF) || NF <- NewFollowers],
			W = NP#dp.wasync,
			{noreply,actordb_sqlprocutil:doqueue(ae_timer(NP#dp{callres = ok,follower_indexes = NewFollowers1,
				wasync = W#ai{nreplies = W#ai.nreplies+1},
				netchanges = actordb_local:net_changes()}))};
		_ ->
			?DBG("Running post election write on nodes ~p, evterm=~p, curterm=~p, vers ~p",
				[P#dp.follower_indexes,P#dp.evterm,P#dp.current_term,NP#dp.schemavers]),
			W = #write{sql = Sql, transaction = NP#dp.transactionid,records = AdbRecords},
			Now = actordb_local:elapsed_time(),
			% Since we won election nodes are accessible.
			Followers = [F#flw{last_seen = Now} || F <- P#dp.follower_indexes],
			write_call(W,Callfrom, NP#dp{follower_indexes = Followers})
	end;
down_info(PID,_Ref,Reason,#dp{election = PID} = P) ->
	case Reason of
		noproc ->
			{noreply, P#dp{election = actordb_sqlprocutil:election_timer(undefined)}};
		{failed,Err} ->
			?ERR("Election failed, retrying later ~p",[Err]),
			{noreply, P#dp{election = actordb_sqlprocutil:election_timer(undefined)}};
		% Election lost. Start an election timer.
		follower when P#dp.without_master_since == undefined ->
			?DBG("Continue as follower"),
			Now = actordb_local:elapsed_time(),
			{noreply,actordb_sqlprocutil:reopen_db(P#dp{
				election = actordb_sqlprocutil:election_timer(Now,undefined),
				masternode = undefined, masternodedist = undefined, mors = slave, 
				without_master_since = Now})};
		follower ->
			?DBG("Continue as follower"),
			Now = actordb_local:elapsed_time(),
			% case Now - P#dp.without_master_since > 3000
			{noreply,P#dp{election = actordb_sqlprocutil:election_timer(Now,undefined),
				masternode = undefined, masternodedist = undefined, mors = slave}};
		_Err ->
			?ERR("Election invalid result ~p",[_Err]),
			{noreply, P#dp{election = actordb_sqlprocutil:election_timer(undefined)}}
	end;
down_info(_PID,Ref,Reason,#dp{transactioncheckref = Ref} = P) ->
	?DBG("Transactioncheck died ~p myid ~p, pid=~p",[Reason,P#dp.transactionid,_PID]),
	case P#dp.transactionid of
		{Tid,Updaterid,Node} ->
			case Reason of
				noproc ->
					{_CheckPid,CheckRef} = actordb_sqlprocutil:start_transaction_checker(Tid,Updaterid,Node),
					{noreply,P#dp{transactioncheckref = CheckRef}};
				abandoned ->
					case handle_call({commit,false,P#dp.transactionid},
							undefined,P#dp{transactioncheckref = undefined}) of
						{stop,normal,NP} ->
							{stop,normal,NP};
						{reply,_,NP} ->
							{noreply,NP};
						{noreply,_} = R ->
							R
					end;
				done ->
					case handle_call({commit,true,P#dp.transactionid},
							undefined,P#dp{transactioncheckref = undefined}) of
						{stop,normal,NP} ->
							{stop,normal,NP};
						{reply,_,NP} ->
							{noreply,NP};
						{noreply,_} = R ->
							R
					end
			end;
		_ ->
			{noreply,P#dp{transactioncheckref = undefined}}
	end;
down_info(PID,_Ref,Reason,#dp{copyproc = PID} = P) ->
	?DBG("copyproc died ~p my_status=~p copyfrom=~p",[Reason,P#dp.mors,P#dp.copyfrom]),
	case Reason of
		unlock ->
			case catch actordb_sqlprocutil:callback_unlock(P) of
				ok when is_binary(P#dp.copyfrom) ->
					{ok,NP} = init(P#dp{mors = slave},copyproc_done),
					{noreply,NP};
				ok ->
					{ok,NP} = init(P#dp{mors = master},copyproc_done),
					{noreply,NP};
				Err ->
					?DBG("Unable to unlock"),
					{stop,Err,P}
			end;
		ok when P#dp.mors == slave ->
			?DBG("Stopping because slave"),
			{stop,normal,P};
		nomajority ->
			{stop,{error,nomajority},P};
		% Error copying.
		%  - There is a chance copy succeeded. If this node was able to send unlock msg
		%    but connection was interrupted before replying.
		%    If this is the case next read/write call will start
		%    actor on this node again and everything will be fine.
		%  - If copy failed before unlock, then it actually did fail. In that case move will restart
		%    eventually.
		_ ->
			?ERR("Coproc died with error ~p~n",[Reason]),
			% actordb_sqlprocutil:empty_queue(P#dp.callqueue,{error,copyfailed}),
			{stop,{error,copyfailed},P}
	end;
down_info(PID,_Ref,Reason,P) ->
	case lists:keyfind(PID,#cpto.pid,P#dp.dbcopy_to) of
		false ->
			?DBG("downmsg, verify maybe? ~p",[P#dp.election]),
			case apply(P#dp.cbmod,cb_info,[{'DOWN',_Ref,process,PID,Reason},P#dp.cbstate]) of
				{noreply,S} ->
					{noreply,P#dp{cbstate = S}};
				noreply ->
					{noreply,P}
			end;
		C ->
			?DBG("Down copyto proc ~p ~p ~p ~p ~p",
				[P#dp.actorname,Reason,C#cpto.ref,P#dp.locked,P#dp.dbcopy_to]),
			case Reason of
				ok ->
					ok;
				_ ->
					?ERR("Copyto process invalid exit ~p",[Reason])
			end,
			WithoutCopy = lists:keydelete(PID,#lck.pid,P#dp.locked),
			NewCopyto = lists:keydelete(PID,#cpto.pid,P#dp.dbcopy_to),
			false = lists:keyfind(C#cpto.ref,2,WithoutCopy),
			% wait_copy not in list add it (2nd stage of lock)
			WithoutCopy1 =  [#lck{ref = C#cpto.ref, ismove = C#cpto.ismove,
				node = C#cpto.node,time = actordb_local:elapsed_time(),
				actorname = C#cpto.actorname}|WithoutCopy],
			erlang:send_after(1000,self(),check_locks),
			{noreply,actordb_sqlprocutil:doqueue(P#dp{dbcopy_to = NewCopyto,locked = WithoutCopy1})}
	end.


terminate(Reason, P) ->
	?DBG("Terminating ~p",[Reason]),
	actordb_sqlite:stop(P),
	distreg:unreg(self()),
	ok.
code_change(_, P, _) ->
	{ok, P}.
init(#dp{} = P,_Why) ->
	% ?DBG("Reinit because ~p, ~p, ~p",[_Why,?R2P(P),get()]),
	?DBG("Reinit because ~p",[_Why]),
	actordb_sqlite:stop(P),
	Flags = P#dp.flags band (bnot ?FLAG_WAIT_ELECTION) band (bnot ?FLAG_STARTLOCK),
	case ok of
		_ when is_reference(P#dp.election) ->
			erlang:cancel_timer(P#dp.election);
		_ when is_pid(P#dp.election) ->
			exit(P#dp.election,reinit);
		_ ->
		 	ok
	end,
	init([{actor,P#dp.actorname},{type,P#dp.actortype},{mod,P#dp.cbmod},{flags,Flags},
		{state,P#dp.cbstate},{slave,P#dp.mors == slave},{wasync,P#dp.wasync},{rasync,P#dp.rasync},
		{queue,P#dp.callqueue},{startreason,{reinit,_Why}}]).
% Never call other processes from init. It may cause deadlocks. Whoever
% started actor is blocking waiting for init to finish.
init([_|_] = Opts) ->
	% put(opt,Opts),
	% Random needs to be unique per-node, not per-actor.
	random:seed(actordb_conf:cfgtime()),
	Now = actordb_local:elapsed_time(),
	P1 = #dp{mors = master, callqueue = queue:new(),statequeue = queue:new(), without_master_since = Now,
		schemanum = catch actordb_schema:num()},
	case actordb_sqlprocutil:parse_opts(P1,Opts) of
		{registered,Pid} ->
			explain({registered,Pid},Opts),
			{stop,normal};
		% P when (P#dp.flags band ?FLAG_ACTORNUM) > 0 ->
		% 	explain({actornum,P#dp.fullpath,actordb_sqlprocutil:read_num(P)},Opts),
		% 	{stop,normal};
		P when (P#dp.flags band ?FLAG_EXISTS) > 0 ->
			case P#dp.movedtonode of
				deleted ->
					explain({ok,[{columns,{<<"exists">>}},{rows,[{<<"false">>}]}]},Opts);
				_ ->
					% {ok,_Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
					% explain({ok,[{columns,{<<"exists">>}},{rows,[{butil:tobin(SchemaTables /= [])}]}]},Opts),
					% {stop,normal}
					LocalShard = actordb_shardmngr:find_local_shard(P#dp.actorname,P#dp.actortype),
					Val =
					case LocalShard of
						{redirect,Shard,Node} ->
							actordb:rpc(Node,Shard,{actordb_shard,is_reg,[Shard,P#dp.actorname,P#dp.actortype]});
						undefined ->
							{Shard,_,Node} = actordb_shardmngr:find_global_shard(P#dp.actorname),
							actordb:rpc(Node,Shard,{actordb_shard,is_reg,[Shard,P#dp.actorname,P#dp.actortype]});
						Shard ->
							actordb_shard:is_reg(Shard,P#dp.actorname,P#dp.actortype)
					end,
					explain({ok,[{columns,{<<"exists">>}},{rows,[{butil:tobin(Val)}]}]},Opts),
					{stop,normal}
			end;
		P when (P#dp.flags band ?FLAG_STARTLOCK) > 0 ->
			case lists:keyfind(lockinfo,1,Opts) of
				{lockinfo,dbcopy,{Ref,CbState,CpFrom,CpReset}} ->
					?DBG("Starting actor slave lock for copy on ref ~p",[Ref]),
					{ok,Db,_,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
					{ok,Pid} = actordb_sqlprocutil:start_copyrec(
						P#dp{db = Db, mors = slave, cbstate = CbState,
							dbcopyref = Ref,  copyfrom = CpFrom, copyreset = CpReset}),
					{ok,P#dp{copyproc = Pid, verified = false,mors = slave, copyfrom = P#dp.copyfrom}};
				{lockinfo,wait} ->
					?DBG("Starting actor lock wait ~p",[P]),
					{ok,P}
			end;
		P when P#dp.copyfrom == undefined ->
			?DBG("Actor start, copy=~p, flags=~p, mors=~p",[P#dp.copyfrom,P#dp.flags,P#dp.mors]),
			% Could be normal start after moving to another node though.
			MovedToNode = apply(P#dp.cbmod,cb_checkmoved,[P#dp.actorname,P#dp.actortype]),
			RightCluster = lists:member(MovedToNode,bkdcore:all_cluster_nodes()),
			case actordb_sqlite:actor_info(P) of
				% {_,VotedFor,VotedCurrentTerm,VoteEvnum,VoteEvTerm} ->
				{{_FCT,LastCheck},{VoteEvTerm,VoteEvnum},_InProg,_MxPage,_AllPages,VotedCurrentTerm,<<>>} ->
					VotedFor = undefined;
				{{_FCT,LastCheck},{VoteEvTerm,VoteEvnum},_InProg,_MxPage,_AllPages,VotedCurrentTerm,VotedFor} ->
					ok;
				_ ->
					VotedFor = undefined,
					LastCheck = VoteEvnum = VotedCurrentTerm = VoteEvTerm = 0
			end,
			case ok of
				_ when P#dp.mors == slave ->
					{ok,actordb_sqlprocutil:init_opendb(P#dp{current_term = VotedCurrentTerm,
						voted_for = VotedFor, evnum = VoteEvnum,evterm = VoteEvTerm,
						election = actordb_sqlprocutil:election_timer(Now,undefined),
						last_checkpoint = LastCheck})};
				_ when MovedToNode == undefined; RightCluster ->
					NP = P#dp{current_term = VotedCurrentTerm,voted_for = VotedFor, evnum = VoteEvnum,
							evterm = VoteEvTerm, last_checkpoint = LastCheck},
					{ok,actordb_sqlprocutil:start_verify(actordb_sqlprocutil:init_opendb(NP),true)};
				_ ->
					?DBG("Actor moved ~p ~p ~p",[P#dp.actorname,P#dp.actortype,MovedToNode]),
					{ok, P#dp{verified = true, movedtonode = MovedToNode}}
			end;
		{stop,Explain} ->
			explain(Explain,Opts),
			{stop,normal};
		P ->
			self() ! start_copy,
			{ok,P#dp{mors = master}}
	end;
init(#dp{} = P) ->
	init(P,noreason).



explain(What,Opts) ->
	case lists:keyfind(start_from,1,Opts) of
		{_,{FromPid,FromRef}} ->
			FromPid ! {FromRef,What};
		_ ->
			ok
	end.

reply(A,B) ->
	actordb_sqlprocutil:reply(A,B).
% reply(undefined,_Msg) ->
% 	ok;
% reply([_|_] = From,Msg) ->
% 	[gen_server:reply(F,Msg) || F <- From];
% reply(From,Msg) ->
% 	gen_server:reply(From,Msg).

