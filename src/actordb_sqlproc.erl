% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_sqlproc).
-behaviour(gen_server).
-define(LAGERDBG,true).
-export([start/1, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([print_info/1]).
-export([read/4,write/4,call/4,call/5,diepls/2]).
-export([call_slave/4,call_slave/5,call_master/4,start_copylock/2]).
-export([write_call/3]).
-include_lib("actordb_sqlproc.hrl").


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

call_master(Cb,Actor,Type,Msg) ->
	case apply(Cb,start,[Actor,Type,[{startreason,Msg}]]) of %
		{ok,Pid} ->
			ok;
		Pid when is_pid(Pid) ->
			ok
	end,
	case catch gen_server:call(Pid,Msg,infinity) of
		{'EXIT',{noproc,_}} ->
			call_master(Cb,Actor,Type,Msg);
		Res ->
			Res
	end.

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

stop(Pid) ->
	gen_server:call(Pid, stop).

print_info(Pid) ->
	gen_server:cast(Pid,print_info).



handle_call(_Msg,_,#dp{movedtonode = <<_/binary>>} = P) ->
	?ADBG("REDIRECT BECAUSE MOVED TO NODE ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.movedtonode,_Msg]),
	{reply,{redirect,P#dp.movedtonode},P#dp{activity = P#dp.activity + 1}};
handle_call({dbcopy_op,_From1,_What,_Data} = Msg,CallFrom,P) ->
	actordb_sqlprocutil:dbcopy_op_call(Msg,CallFrom,P);
handle_call({state_rw,What},From,P) ->
	state_rw_call(What,From,P);
% handle_call({commit,Doit,Id},From, P) ->
% 	?ADBG("Commit ~p ~p ~p ~p",[Doit,Id,From,P#dp.transactionid]),
% 	case P#dp.transactionid == Id of
% 		true ->
% 			case P#dp.transactioncheckref of
% 				undefined ->
% 					ok;
% 				_ ->
% 					erlang:demonitor(P#dp.transactioncheckref)
% 			end,
% 			?ADBG("Commit write ~p ~p",[bkdcore:cluster_nodes_connected(),P#dp.replicate_sql]),
% 			{ConnectedNodes,LenCluster,_LenConnected} = nodes_for_replication(P),
% 			{Sql,EvNum,Crc,NewVers} = P#dp.replicate_sql,
% 			case Doit of
% 				true when LenCluster == 0 ->
% 					case Sql of
% 						delete ->
% 							delete_actor(P),
% 							case From of
% 								undefined ->
% 									{reply,ok,P#dp{transactionid = undefined, replicate_sql = undefined, db = undefined}};
% 								_ ->
% 									reply(From,ok),
% 									{stop,normal,P}
% 							end;
% 						_ ->
% 							ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"RELEASE SAVEPOINT 'adb';">>)),
% 							{reply,ok,P#dp{transactionid = undefined,transactioncheckref = undefined,
% 									 replicate_sql = undefined, activity = P#dp.activity + 1}}
% 					end;
% 				true ->
% 					Commiter = commit_write({commit,Doit,Id},P,LenCluster,ConnectedNodes,EvNum,Sql,Crc,NewVers),
% 					{noreply,P#dp{callfrom = From, commiter = Commiter, activity = P#dp.activity + 1,
% 								  callres = ok,
% 								 transactionid = undefined,transactioncheckref = undefined}};
% 				false when LenCluster == 0 ->
% 					self() ! doqueue,
% 					actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
% 					{reply,ok,P#dp{transactionid = undefined, replicate_sql = undefined,
% 									transactioncheckref = undefined}};
% 				false ->
% 					self() ! doqueue,
% 					{Tid,Updaterid,_} = P#dp.transactionid,
% 					case Sql of
% 						<<"delete">> ->
% 							ok;
% 						_ ->
% 							actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)
% 					end,
% 					NewSql = <<"DELETE FROM __transactions WHERE tid=",(butil:tobin(Tid))/binary," AND updater=",
% 										(butil:tobin(Updaterid))/binary,";">>,
% 					handle_call({write,{undefined,erlang:crc32(NewSql),NewSql,undefined}},From,P#dp{callfrom = undefined,
% 										transactionid = undefined,replicate_sql = undefined,transactioncheckref = undefined})
% 			end;
% 		_ when P#dp.db == undefined ->
% 			reply(From,ok),
% 			{stop,normal,P};
% 		_ ->
% 			{reply,ok,P}
% 	end;
handle_call({delete,Moved},From,P) ->
	?ADBG("deleting actor from node ~p ~p",[P#dp.actorname,P#dp.actortype]),
	actordb_sqlite:stop(P#dp.db),
	actordb_sqlprocutil:delactorfile(P#dp{movedtonode = Moved}),
	distreg:unreg(self()),
	reply(From,ok),
	{stop,normal,P};
% If we are not ready to process calls atm (in the middle of a write or db not verified yet). Queue requests.
handle_call(Msg,From,P) when P#dp.callfrom /= undefined; P#dp.verified /= true; 
								P#dp.transactionid /= undefined; P#dp.locked /= [] ->
	case Msg of
		{write,{_,_,_,TransactionId} = Msg1} when P#dp.transactionid == TransactionId, P#dp.transactionid /= undefined ->
			write_call(Msg1,From,P);
		_ when element(1,Msg) == replicate_start, P#dp.mors == master, P#dp.verified ->
			reply(From,reinit),
			{ok,NP} = init(P,replicate_conflict),
			{noreply,NP};
		_ when element(1,Msg) == replicate_start, P#dp.copyproc /= undefined ->
			{reply,notready,P};
		_ ->
			?DBG("Queueing msg ~p ~p, because ~p",[Msg,P#dp.mors,{P#dp.callfrom,P#dp.verified,P#dp.transactionid}]),
			{noreply,P#dp{callqueue = queue:in_r({From,Msg},P#dp.callqueue), activity = P#dp.activity+1}}
	end;
handle_call({read,Msg},From,P) ->
	read_call(Msg,From,P);
handle_call({write,Msg},From, #dp{mors = master} = P) ->
	write_call(Msg,From,P);
handle_call({write,_},_,#dp{mors = slave} = P) ->
	?DBG("Redirect not master ~p",[P#dp.masternode]),
	actordb_sqlprocutil:redirect_master(P);
handle_call(stop, _, P) ->
	actordb_sqlite:stop(P#dp.db),
	distreg:unreg(self()),
	{stop, shutdown, stopped, P};
handle_call(Msg,From,#dp{mors = master, verified = true} = P) ->
	?DBG("cb_call ~p",[{P#dp.cbmod,Msg}]),
	case apply(P#dp.cbmod,cb_call,[Msg,From,P#dp.cbstate]) of
		{reply,Resp,S} ->
			{reply,Resp,P#dp{cbstate = S}};
		{reply,Resp} ->
			{reply,Resp,P}
	end;
handle_call(_Msg,_From,#dp{mors = slave, verified = true} = P) ->
	?DBG("Redirect not master ~p ~p",[P#dp.masternode,_Msg]),
	% {reply,{redirect,P#dp.masternodedist},P};
	actordb_sqlprocutil:redirect_master(P);
handle_call(_Msg,_,P) ->
	?AINF("sqlproc ~p unhandled call ~p mors ~p verified ~p",[P#dp.cbmod,_Msg,P#dp.mors,P#dp.verified]),
	{reply,{error,unhandled_call},P}.


state_rw_call(What,From,P) ->
	case What of
		% verifyinfo ->
		% 	{reply,{ok,bkdcore:node_name(),P#dp.evcrc,P#dp.evnum,{P#dp.mors,P#dp.verified}},P};
		actornum ->
			{reply,{ok,P#dp.dbpath,actordb_sqlprocutil:read_num(P)},P};
		donothing ->
			{reply,ok,P};
		% AE is split into multiple calls (because wal is sent page by page as it is written)
		% Start sets parameters. There may not be any wal append calls after if empty write.
		{appendentries_start,Term,LeaderNode,PrevEvnum,PrevTerm,_LeaderCommit,IsEmpty} ->
			case ok of
				_ when Term < P#dp.current_term ->
					reply(From,false),
					actordb_sqlprocutil:ae_respond(P,LeaderNode,false),
					{noreply,P};
				% This node is candidate or leader but someone with newer term is sending us log
				_ when P#dp.mors == master ->
					state_rw_call(What,From,
									actordb_sqlprocutil:save_term(actordb_sqlprocutil:reopen_db(
												P#dp{mors = slave, verified = true, 
													voted_for = undefined,
													masternode = LeaderNode,
													masternodedist = bkdcore:dist_name(LeaderNode),
													current_term = Term})));
				_ when P#dp.evnum /= PrevEvnum; P#dp.evterm /= PrevTerm ->
					case P#dp.evnum < PrevEvnum of
						% This node is behind. Return false and 
						%  wait for leader to send an earlier event.
						true ->
							NP = P;
						% Node is conflicted, delete last entry
						false ->
							NP = actordb_sqlprocutil:rewind_wal(P)
					end,
					reply(From,false),
					actordb_sqlprocutil:ae_respond(NP,LeaderNode,false),
					{noreply,P};
				_ when Term > P#dp.current_term ->
					state_rw_call(What,From,actordb_sqlprocutil:save_term(
												P#dp{current_term = Term,voted_for = undefined,
												 masternode = LeaderNode, 
												 masternodedist = bkdcore:dist_name(LeaderNode)}));
				_ when IsEmpty ->
					reply(From,ok),
					actordb_sqlprocutil:ae_respond(P,LeaderNode,true),
					{noreply,P};
				% Ok, now it will start receiving wal pages
				_ ->
					{reply,ok,P}
			end;
		% sqlite wal, header tells you if done (it has db size in header)
		{appendentries_wal,Term,Header,Body} ->
			case ok of
				_ when Term == P#dp.current_term ->
					actordb_sqlprocutil:append_wal(P,Header,Body),
					case Header of
						% dbsize == 0, not last page
						<<_:32,0:32,_/binary>> ->
							{reply,ok,P};
						% last page
						<<_:32,_:32,Evnum:64/unsigned-big,Evterm:64/unsigned-big,_/binary>> ->
							NP = P#dp{evnum = Evnum, evterm = Evterm},
							reply(From,ok),
							actordb_sqlprocutil:ae_respond(NP,NP#dp.masternode,true),
							{noreply,NP}
					end;
				_ ->
					reply(From,false),
					actordb_sqlprocutil:ae_respond(P,P#dp.masternode,false),
					{noreply,P}
			end;
		% called back to leader from every follower that received call
		{appendentries_response,Node,CurrentTerm,Success,EvNum,EvTerm} ->
			Follower = lists:keyfind(Node,#flw.node,P#dp.follower_indexes),
			NFlw = Follower#flw{match_index = EvNum, match_term = EvTerm,next_index = EvNum+1},
			case Success of
				% An earlier response.
				_ when P#dp.mors == slave ->
					{reply,ok,P};
				true ->
					{reply,ok,actordb_sqlprocutil:reply_maybe(actordb_sqlprocutil:continue_maybe(P,NFlw))};
				% What we thought was follower is ahead of us and we need to step down
				false when P#dp.current_term < CurrentTerm ->
					{reply,ok,actordb_sqlprocutil:reopen_db(actordb_sqlprocutil:save_term(
						P#dp{mors = slave,current_term = CurrentTerm,voted_for = undefined, follower_indexes = []}))};
				false ->
					case actordb_sqlprocutil:try_wal_recover(P,NFlw) of
						{false,NP,_NF} ->
							% Send entire db
							{reply,ok,NP};
						{true,NP,NF} ->
							{reply,ok,actordb_sqlprocutil:continue_maybe(NP,NF)}
					end
			end;
		{request_vote,Candidate,NewTerm,LastTerm,LastEvnum} ->
			TrueResp = fun() -> {reply, {true,bkdcore:node_name(),NewTerm}, 
							actordb_sqlprocutil:save_term(P#dp{voted_for = Candidate, current_term = NewTerm})} end,
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
					{reply, {outofdate,bkdcore:node_name(),P#dp.current_term},P};
				% We've already seen this term, only vote yes if we have not voted
				%  or have voted for this candidate already.
				_ when NewTerm == P#dp.current_term ->
					case (P#dp.voted_for == undefined orelse P#dp.voted_for == Candidate) of
						true when Uptodate ->
							TrueResp();
						true ->
							{reply, {outofdate,bkdcore:node_name(),NewTerm},
									 actordb_sqlprocutil:save_term(P#dp{voted_for = undefined, current_term = NewTerm})};
						false ->
							{reply, {alreadyvoted,bkdcore:node_name(),P#dp.current_term},P}
					end;
				% New candidates term is higher than ours, is he as up to date?
				_ when Uptodate ->
					TrueResp();
				% Higher term, but not as up to date. We can not vote for him.
				% We do have to remember new term index though.
				_ ->
					{reply, {outofdate,bkdcore:node_name(),NewTerm},
								actordb_sqlprocutil:save_term(P#dp{voted_for = undefined, current_term = NewTerm})}
			end;
		% Hint from a candidate that this node should start new election, because
		%  it is more up to date.
		doelection ->
			reply(From,ok),
			case P#dp.verified == false andalso P#dp.electionpid == undefined of
				true ->
					{noreply,actordb_sqlprocutil:start_verify(P,false)};
				false ->
					{noreply,P}
			end
		% conflicted ->
		% 	{stop,conflicted,P}
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
									{reply,actordb_sqlite:exec(P#dp.db,Sql,read),check_timer(P#dp{activity = P#dp.activity+1, 
																							  cbstate = State})};
								Sql ->
									{reply,actordb_sqlite:exec(P#dp.db,Sql,read),check_timer(P#dp{activity = P#dp.activity+1})}
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
							{reply,actordb_sqlite:exec(P#dp.db,Sql,read),check_timer(P#dp{activity = P#dp.activity+1})}
					end;
				% Schema has changed. Execute write on schema update.
				% Place this read in callqueue for later execution.
				{NewVers,Sql1} ->
					case write_call(undefined,Sql1,undefined,undefined,NewVers,P) of
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


write_call({MFA,Sql,Transaction} = OrigMsg,From,P) ->
	?ADBG("writecall ~p ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},MFA,Sql,Transaction]),	
	case MFA of
		undefined ->
			case actordb_sqlprocutil:has_schema_updated(P,Sql) of
				ok ->
					write_call(OrigMsg,Sql,Transaction,From,P#dp.schemavers,P);
				{NewVers,Sql1} ->
					write_call(OrigMsg,Sql1,Transaction,From,NewVers,P)
			end;
		{Mod,Func,Args} ->
			case actordb_sqlprocutil:has_schema_updated(P,[]) of
				ok ->
					NewVers = P#dp.schemavers,
					SqlUpdate = [];
				{NewVers,SqlUpdate} ->
					ok
			end,
			?DBLOG(P#dp.db,"writecall mfa ~p",[MFA]),
			case apply(Mod,Func,[P#dp.cbstate|Args]) of
				{reply,What,OutSql1,NS} ->
					reply(From,What),
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(OrigMsg,OutSql,Transaction,undefined,NewVers,P#dp{cbstate = NS});
				{reply,What,NS} ->
					{reply,What,P#dp{cbstate = NS}};
				{reply,What} ->
					{reply,What,P};
				{OutSql1,State} ->
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(OrigMsg,OutSql,Transaction,From,NewVers,P#dp{cbstate = State});
				OutSql1 ->
					OutSql = iolist_to_binary([SqlUpdate,OutSql1]),
					write_call(OrigMsg,OutSql,Transaction,From,NewVers,P)
			end
	end.
% Not a multiactor transaction write
write_call(_OrigMsg,Sql,undefined,From,NewVers,P) ->
	EvNum = P#dp.evnum+1,
	?DBLOG(P#dp.db,"writecall ~p ~p ~p ~p",[EvNum,P#dp.dbcopy_to,From,Sql]),
	% {ConnectedNodes,LenCluster,LenConnected} = nodes_for_replication(P),
	ClusterNodes = bkcore:cluster_nodes(),
	case Sql of
		delete ->
			ReplSql = <<"delete">>,
			Res = ok;
		{moved,Node} ->
			ReplSql = <<"moved,",Node/binary>>,
			Res = ok;
		_ ->
			case actordb_sqlprocutil:actornum(P) of
				<<>> ->
					ReplSql = actordb_sqlprocutil:semicolon(Sql);
				NumSql ->
					ReplSql = [actordb_sqlprocutil:semicolon(Sql),NumSql]
			end,
			ComplSql = 
					[<<"$SAVEPOINT 'adb';">>,
					 ReplSql,
					 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
					 <<"$UPDATE __adb SET val='">>,butil:tobin(P#dp.evterm),<<"' WHERE id=">>,?EVTERM,";",
					 <<"$RELEASE SAVEPOINT 'adb';">>
					 ],
			Res = actordb_sqlite:exec(P#dp.db,ComplSql,write)
	end,
	% ConnectedNodes = bkdcore:cluster_nodes_connected(),
	?DBG("Replicating write ~p",[Sql]),
	case actordb_sqlite:okornot(Res) of
		ok ->
			?DBG("Write result ~p",[Res]),
			case ok of
				_ when ClusterNodes == [] ->
					case Sql of
						delete ->
							actordb_sqlprocutil:delete_actor(P),
							reply(From,Res),
							{stop,normal,P};
						{moved,MovedTo} ->
							actordb_sqlite:stop(P#dp.db),
							reply(From,Res),
							actordb_sqlprocutil:delactorfile(P#dp{movedtonode = MovedTo}),
							{stop,normal,P};
						_ ->
							{reply,Res,P#dp{activity = P#dp.activity+1, evnum = EvNum, schemavers = NewVers}}
					end;
				_ ->
					% reply on appendentries response or later if nodes are behind.
					{noreply, P#dp{callfrom = From, callres = Res, activity = P#dp.activity + 1}}
			end;
		Resp ->
			actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
			{reply,Resp,P#dp{activity = P#dp.activity+1}}
	end.
% write_call(OrigMsg,Sql1,{Tid,Updaterid,Node} = TransactionId,From,NewVers,P) ->
% 	?DBLOG(P#dp.db,"writetransaction ~p ~p ~p ~p",[P#dp.evnum,Crc,{Tid,Updaterid,Node},P#dp.dbcopy_to]),
% 	{_CheckPid,CheckRef} = start_transaction_checker(Tid,Updaterid,Node),
% 	{ConnectedNodes,LenCluster,LenConnected} = nodes_for_replication(P),
% 	?ADBG("Starting transaction write ~p, id ~p, curtr ~p, sql ~p",[LenCluster,TransactionId,P#dp.transactionid,Sql1]),
% 	case LenCluster of
% 		0 ->
% 			% If single node cluster, no need to store sql first.
% 			case P#dp.transactionid of
% 				TransactionId ->
% 					% Transaction can write to single actor more than once (especially for KV stores)
% 					% if we are already in this transaction, just update sql.
% 					{_OldSql,EvNum,EvCrc,_} = P#dp.replicate_sql,
% 					ComplSql = Sql1,
% 					Res = actordb_sqlite:exec(P#dp.db,ComplSql,write);
% 				undefined ->
% 					EvNum = P#dp.evnum+1,
% 					EvCrc = Crc,
% 					case Sql1 of
% 						delete ->
% 							Res = ok,
% 							ComplSql = delete;
% 						_ ->
% 							ComplSql = 
% 								[<<"$SAVEPOINT 'adb';">>,
% 								 semicolon(Sql1),actornum(P),
% 								 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
% 								 <<"$UPDATE __adb SET val='">>,butil:tobin(Crc),<<"' WHERE id=">>,?EVCRC,";"
% 								 ],
% 							Res = actordb_sqlite:exec(P#dp.db,ComplSql,write)
% 					end
% 			end,
% 			case actordb_sqlite:okornot(Res) of
% 				ok ->
% 					?ADBG("Transaction ok"),
% 					{reply, Res, P#dp{activity = P#dp.activity+1, evnum = EvNum, evcrc = EvCrc,
% 								 transactionid = TransactionId, schemavers = NewVers,
% 								transactioncheckref = CheckRef,replicate_sql = {ComplSql,EvNum,EvCrc,NewVers}}};
% 				_Err ->
% 					ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)),
% 					erlang:demonitor(CheckRef),
% 					?ADBG("Transaction not ok ~p",[_Err]),
% 					{reply,Res,P#dp{actortype = P#dp.activity + 1, transactionid = undefined}}
% 			end;
% 		_ ->
% 			EvNum = P#dp.evnum+1,
% 			case P#dp.transactionid of
% 				TransactionId ->
% 					% Rollback prev version of sql.
% 					ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)),
% 					{OldSql,_EvNum,_EvCrc,NewVers} = P#dp.replicate_sql,
% 					% Combine prev sql with new one.
% 					Sql = iolist_to_binary([OldSql,Sql1]),
% 					TransactionInfo = [<<"$INSERT OR REPLACE INTO __transactions (id,tid,updater,node,schemavers,sql) VALUES (1,">>,
% 											(butil:tobin(Tid)),",",(butil:tobin(Updaterid)),",'",Node,"',",
% 								 				(butil:tobin(NewVers)),",",
% 								 				"'",(base64:encode(Sql)),"');"];
% 				_ ->
% 					case Sql1 of
% 						delete ->
% 							Sql = <<"delete">>;
% 						_ ->
% 							Sql = iolist_to_binary(Sql1)
% 					end,
% 					% First store transaction info 
% 					% Once that is stored (on all nodes), execute the sql to see if there are errors (but only on this master node).
% 					TransactionInfo = [<<"$INSERT INTO __transactions (id,tid,updater,node,schemavers,sql) VALUES (1,">>,
% 											(butil:tobin(Tid)),",",(butil:tobin(Updaterid)),",'",Node,"',",
% 											(butil:tobin(NewVers)),",",
% 								 				"'",(base64:encode(Sql)),"');"]
% 			end,
% 			CrcTransaction = erlang:crc32(TransactionInfo),
% 			ComplSql = 
% 					[<<"$SAVEPOINT 'adb';">>,
% 					 TransactionInfo,
% 					 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
% 					 <<"$UPDATE __adb SET val='">>,butil:tobin(CrcTransaction),<<"' WHERE id=">>,?EVCRC,";"
% 					 ],
% 			ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,ComplSql,write)),
% 			?DBG("Replicating transaction write, connected ~p",[ConnectedNodes]),
% 			case ok of
% 				_ when (LenConnected+1)*2 > (LenCluster+1) ->
% 					Commiter = commit_write(OrigMsg,P,LenCluster,ConnectedNodes,EvNum,TransactionInfo,CrcTransaction,P#dp.schemavers),
% 					{noreply,P#dp{callfrom = From,callres = undefined, commiter = Commiter, 
% 								  activity = P#dp.activity + 1,replicate_sql = {Sql,EvNum+1,Crc,NewVers},
% 								  transactioncheckref = CheckRef,
% 								  transactionid = TransactionId}};
% 				_ ->
% 					actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>),
% 					erlang:demonitor(CheckRef),
% 					{reply,{error,{replication_failed_2,LenConnected,LenCluster}},P}
% 			end
% 	end.





handle_cast({diepls,Reason},P) ->
	?ADBG("diepls ~p",[{P#dp.actorname,P#dp.actortype}]),
	case Reason of
		nomaster ->
			?AERR("Die because nomaster"),
			actordb_sqlite:stop(P#dp.db),
			distreg:unreg(self()),
			{stop,normal,P};
		_ ->
			case handle_info(check_inactivity,P) of
				{noreply,_,hibernate} ->
					% case apply(P#dp.cbmod,cb_candie,[P#dp.mors,P#dp.actorname,P#dp.actortype,P#dp.cbstate]) of
					% 	true ->
							?DBLOG(P#dp.db,"reqdie",[]),
							actordb_sqlite:stop(P#dp.db),
							distreg:unreg(self()),
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
	io:format("~p~n",[?R2P(P)]),
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


handle_info(doqueue, P) when P#dp.callfrom == undefined, P#dp.verified /= false, P#dp.transactionid == undefined ->
	case queue:is_empty(P#dp.callqueue) of
		true ->
			?DBG("doqueue empty"),
			{noreply,P};
		false ->
			{{value,Call},CQ} = queue:out_r(P#dp.callqueue),
			{From,Msg} = Call,
			?DBG("doqueue ~p",[Call]),
			case P#dp.verified of
				failed ->
					?AINF("stop cant verify"),
					reply(From,cant_verify_db),
					distreg:unreg(self()),
					{stop,cant_verify_db,P};
				_ ->
					case handle_call(Msg,From,P#dp{callqueue = CQ}) of
						{reply,Res,NP} ->
							reply(From,Res),
							handle_info(doqueue,NP);
						% If call returns noreply, it will continue processing later.
						{noreply,NP} ->
							{noreply,NP}
					end
			end
	end;
handle_info(doqueue,P) ->
	?DBG("doqueue ~p",[{P#dp.verified,P#dp.callfrom}]),
	case P#dp.verified of
		failed ->
			?AERR("Verify failed, actor stop ~p ~p",[P#dp.actorname,P#dp.actortype]),
			distreg:unreg(self()),
			?DBLOG(P#dp.db,"verify failed",[]),
			{stop,normal,P};
		_ ->
			{noreply,P}
	end;
handle_info({'DOWN',Monitor,_,PID,Reason},P) ->
	down_info(PID,Monitor,Reason,P);
handle_info({inactivity_timer,Ref,N},P) ->
	case Ref == P#dp.timerref of
		true ->
			handle_info({check_inactivity,N},P#dp{timerref = undefined});
		false ->
			handle_info({check_inactivity,N},P)
	end;
handle_info({check_inactivity,N}, P) ->
	timer_info(N,P);
handle_info(check_inactivity,P) ->
	handle_info({check_inactivity,P#dp.activity+1},P#dp{activity = P#dp.activity + 1});
handle_info(stop,P) ->
	handle_info({stop,normal},P);
handle_info({stop,Reason},P) ->
	distreg:unreg(self()),
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
handle_info({check_redirect,Db,IsMove},P) ->
	case actordb_sqlprocutil:check_redirect(P,P#dp.copyfrom) of
		false ->
			file:delete(P#dp.dbpath),
			file:delete(P#dp.dbpath++"-wal"),
			file:delete(P#dp.dbpath++"-shm"),
			{stop,{error,failed}};
		Red  ->
			?ADBG("Returned redirect ~p ~p ~p",[P#dp.actorname,Red,P#dp.copyreset]),
			case Red of
				{true,NewShard} when NewShard /= undefined ->
					ok = actordb_shard:reg_actor(NewShard,P#dp.actorname,P#dp.actortype);
				_ ->
					ok
			end,
			ResetSql = actordb_sqlprocutil:do_copy_reset(IsMove,P#dp.copyreset,P#dp.cbstate),
			case actordb_sqlite:exec(Db,[<<"BEGIN;DELETE FROM __adb WHERE id=">>,(?COPYFROM),";",
												  ResetSql,"COMMIT;"],write) of
				{ok,_} ->
					ok;
				ok ->
					ok
			end,
			actordb_sqlite:stop(Db),
			{ok,NP} = init(P#dp{db = undefined,copyfrom = undefined, copyreset = false, mors = master},cleanup_copymove),
			{noreply,NP}
	end;
handle_info(_Msg,P) ->
	?DBG("sqlproc ~p unhandled info ~p~n",[P#dp.cbmod,_Msg]),
	{noreply,P}.


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

timer_info(N,P) ->
		% ?AINF("check_inactivity ~p ~p ~p~n",[{N,P#dp.activity},{P#dp.actorname,P#dp.callfrom},
	% 				{P#dp.dbcopyref,P#dp.dbcopy_to,P#dp.locked,P#dp.copyproc,P#dp.verified,P#dp.transactionid}]),
	Empty = queue:is_empty(P#dp.callqueue),
	case P of
		% If true, process is inactive and can die (or go to sleep)
		#dp{activity = N, callfrom = undefined, verified = true, transactionid = undefined,
			dbcopyref = undefined, dbcopy_to = [], locked = [], copyproc = undefined} when Empty ->
			case P#dp.movedtonode of
				undefined ->
					case apply(P#dp.cbmod,cb_candie,[P#dp.mors,P#dp.actorname,P#dp.actortype,P#dp.cbstate]) of
						true ->
							?ADBG("Die because temporary ~p ~p master ~p",[P#dp.actorname,P#dp.actortype,P#dp.masternode]),
							distreg:unreg(self()),
							?DBLOG(P#dp.db,"die temporary ",[]),
							{stop,normal,P};
						_ when P#dp.activity == 0 ->
							case timer:now_diff(os:timestamp(),P#dp.start_time) > 10*1000000 of
								true ->
									?ADBG("die after 10sec inactive"),
									?DBLOG(P#dp.db,"die 0 after 10sec",[]),
									actordb_sqlite:stop(P#dp.db),
									distreg:unreg(self()),
									{stop,normal,P};
								false ->
									Now = actordb_local:actor_activity(P#dp.activity_now),
									{noreply,check_timer(P#dp{activity_now = Now})}
							end;
						_ when (P#dp.flags band ?FLAG_NOHIBERNATE) > 0 ->
							actordb_sqlite:stop(P#dp.db),
							distreg:unreg(self()),
							{stop,normal,P};
						_ ->
							?DBG("Process hibernate ~p",[P#dp.actorname]),
							case P#dp.timerref /= undefined of
								true ->
									erlang:cancel_timer(P#dp.timerref),
									Timer = undefined;
								false ->
									Timer = P#dp.timerref
							end,
							{noreply,P#dp{timerref = Timer},hibernate}
					end;
				_ ->
					case P#dp.db of
						undefined ->
							ok;
						_ ->
							actordb_sqlite:stop(P#dp.db),
							actordb_sqlprocutil:delactorfile(P),
							[rpc:async_call(Nd,?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
																	{delete,P#dp.movedtonode},[{flags,P#dp.flags}]]) 
									|| Nd <- bkdcore:cluster_nodes_connected()]
					end,
					case timer:now_diff(os:timestamp(),P#dp.start_time) > 10*1000000 of
						true ->
							?ADBG("Die because moved"),
							?DBLOG(P#dp.db,"die moved ",[]),
							distreg:unreg(self()),
							{stop,normal,P};
						false ->
							Now = actordb_local:actor_activity(P#dp.activity_now),
							{noreply,check_timer(P#dp{activity_now = Now, db = undefined})}
					end
			end;
		_ when P#dp.electionpid == undefined, P#dp.verified /= true ->
			case P#dp.verified of
				failed ->
					?AERR("verify fail ~p",[?R2P(P)]);
				_ ->
					ok
			end,
			case timer:now_diff(os:timestamp(),P#dp.start_time) > 20*1000000 of
				true ->
					{stop,verifyfail,P};
				false ->
					{noreply,P}
			end;
		_ ->
			Now = actordb_local:actor_activity(P#dp.activity_now),

			case ok of
				_ when P#dp.dbcopyref == undefined, P#dp.dbcopy_to == [], P#dp.db /= undefined ->
					{_,NPages} = actordb_sqlite:wal_pages(P#dp.db),
					case NPages*(?PAGESIZE+24) > 1024*1024 of
						true ->
							actordb_sqlite:checkpoint(P#dp.db);
						_ ->
							ok
					end;
				_ ->
					ok
			end,
			{noreply,check_timer(P#dp{activity_now = Now, locked = abandon_locks(P,P#dp.locked,[])})}
	end.


down_info(PID,_Ref,Reason,#dp{electionpid = PID} = P1) ->
	% TODO: - unfinished transaction
	% 		- copy/move from another node
	case Reason of
		% We are leader, evnum == 0, which means no other node has any data.
		% If create flag not set stop.
		leader when (P1#dp.flags band ?FLAG_CREATE) == 0, P1#dp.evnum == 0 ->
			{stop,nocreate,P1};
		leader ->
			FollowerIndexes = [#flw{node = Nd,match_index = 0,next_index = P1#dp.evnum+1} || Nd <- bkdcore:cluster_nodes()],
			P = P1#dp{mors = master, electionpid = undefined, evterm = P1#dp.current_term,
					  follower_indexes = FollowerIndexes},
			% After election is won a write needs to be executed.
			% If empty db or schema not up to date use that.
			% Otherwise just empty sql, which still means an increment for evnum and evterm in __adb.
			case P#dp.evnum of
				0 ->
					{SchemaVers,Schema} = apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,0]),
					Sql = [actordb_sqlprocutil:base_schema(SchemaVers,P#dp.actortype),
								 Schema];
				_ ->
					case apply(P#dp.cbmod,cb_schema,[P#dp.cbstate,P#dp.actortype,P#dp.schemavers]) of
						{_,[]} ->
							SchemaVers = P#dp.schemavers,
							Sql = <<>>;
						{SchemaVers,Schema} ->
							Sql = [Schema,
									<<"UPDATE __adb SET val='">>,(butil:tobin(SchemaVers)),
										<<"' WHERE id=",?SCHEMA_VERS/binary,";">>]
					end
			end,
			case write_call({undefined,Sql,undefined},undefined,
								actordb_sqlprocutil:reopen_db(P#dp{schemavers = SchemaVers})) of
				{noreply,NP} ->
					{noreply,NP};
				{reply,_,NP} ->
					{noreply,NP}
			end;
		follower ->
			{noreply,actordb_sqlprocutil:reopen_db(P1#dp{electionpid = undefined, mors = slave})}
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
						{_,_,NP} ->
							{noreply,NP};
						{noreply,NP} ->
							{noreply,NP}
					end;
				done ->
					case handle_call({commit,true,P#dp.transactionid},undefined,P#dp{transactioncheckref = undefined}) of
						{_,_,NP} ->
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
		_ ->
			?AINF("Coproc died ~p~n",[?R2P(P)]),
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
						activity = P#dp.activity + 1},
			case queue:is_empty(P#dp.callqueue) of
				true ->
					{noreply,NP};
				false ->
					handle_info(doqueue,NP)
			end;
		false ->
			?ADBG("downmsg, verify maybe? ~p",[P#dp.electionpid]),
			case apply(P#dp.cbmod,cb_info,[{'DOWN',_Ref,process,PID,Reason},P#dp.cbstate]) of
				{noreply,S} ->
					{noreply,P#dp{cbstate = S}};
				noreply ->
					{noreply,P}
			end
	end.


terminate(_, _) ->
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
	case actordb_sqlprocutil:parse_opts(check_timer(#dp{mors = master, callqueue = queue:new(), start_time = os:timestamp(), 
									schemanum = actordb_schema:num()}),Opts) of
		{registered,Pid} ->
			?ADBG("die already registered"),
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
					erlang:monitor(process,Pid),
					{ok,P#dp{copyproc = Pid, verified = false,mors = slave, copyfrom = P#dp.copyfrom}}
			end;
		P ->
			random:seed(P#dp.start_time),
			ClusterNodes = bkdcore:cluster_nodes(),
			?ADBG("Actor start ~p ~p ~p ~p ~p ~p, startreason ~p",[P#dp.actorname,P#dp.actortype,P#dp.copyfrom,
													queue:is_empty(P#dp.callqueue),ClusterNodes,
					bkdcore:node_name(),butil:ds_val(startreason,Opts)]),
			
			case P#dp.copyfrom of
				% Normal start (not start and copy/move actor from another node)
				undefined ->
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
							{ok,F} = file:open([P#dp.dbpath,"-wal"],[read,binary,raw]),
							case file:position(F,eof) of
								{ok,WalSize} when WalSize > 32+40+?PAGESIZE ->
									{ok,_} = file:position(F,{cur,-(?PAGESIZE+40)}),
									{ok,<<_:32,_:32,Evnum:64/big-unsigned,Evterm:64/big-unsigned>>} =
										file:read(F,24),
									{ok,_} = file:position(F,eof),
									{ok,P#dp{db = F, current_term = VotedForTerm, voted_for = VotedFor, 
												evnum = Evnum, evterm = Evterm}};
								{ok,_} ->
									file:close(F),
									init_opendb(P#dp{current_term = VotedForTerm,voted_for = VotedFor})
							end;
						_ when MovedToNode == undefined; RightCluster ->
							init_opendb(P#dp{current_term = VotedForTerm,voted_for = VotedFor});
						_ ->
							?ADBG("Actor moved ~p ~p ~p",[P#dp.actorname,P#dp.actortype,MovedToNode]),
							{ok, P#dp{verified = true, movedtonode = MovedToNode,
										activity_now = actordb_sqlprocutil:actor_start(P)}}
					end;
				% Either create a copy of an actor or move an actor from one cluster to another.
				_ ->
					init_copy(P)
			end
	end;
init(#dp{} = P) ->
	init(P,noreason).


init_opendb(P) ->
	{ok,Db,SchemaTables,_PageSize} = actordb_sqlite:init(P#dp.dbpath,wal),
	NP = P#dp{db = Db},
	case SchemaTables of
		[_|_] ->
			?ADBG("Opening HAVE schema ~p",[{P#dp.actorname,P#dp.actortype}]),
			?DBLOG(Db,"init normal have schema",[]),
			{ok,[[{columns,_},{rows,Transaction}],
				[{columns,_},{rows,Rows}]]} = actordb_sqlite:exec(Db,
					<<"SELECT * FROM __adb;",
					  "SELECT * FROM __transactions;">>,read),
			Evnum = butil:toint(butil:ds_val(?EVNUMI,Rows,0)),
			Vers = butil:toint(butil:ds_val(?SCHEMA_VERSI,Rows)),
			MovedToNode1 = butil:ds_val(?MOVEDTOI,Rows),
			CopyFrom = butil:ds_val(?COPYFROMI,Rows),
			EvTerm = butil:toint(butil:ds_val(?EVTERMI,Rows,0)),

			% case actordb_sqlite:exec(Db,<<"SELECT min(id),max(id) FROM __wlog;">>,read) of
			% 	{sql_error,_,_} ->
			% 		HaveWlog = ?WLOG_NONE,
			% 		WlogLen = 0,
			% 		actordb_sqlite:exec(Db,<<"CREATE TABLE __wlog (id INTEGER PRIMARY KEY, crc INTEGER, sql TEXT);">>);
			% 	[{columns,_},{rows,[{MinWL,MaxWL}]}] when is_integer(MinWL), 
			% 												MinWL > 0, MinWL < MaxWL ->
			% 		WlogLen = MaxWL - MinWL,
			% 		HaveWlog = ?WLOG_ACTIVE;
			% 	_ ->
			% 		WlogLen = 0,
			% 		HaveWlog = ?WLOG_NONE
			% end,
			% case butil:toint(butil:ds_val(?WLOG_STATUSI,Rows,?WLOG_NONE)) of
			% 	?WLOG_ABANDONDED ->
			% 		WlogStatus = ?WLOG_ABANDONDED;
			% 	_ ->
			% 		WlogStatus = HaveWlog
			% end,
			case Transaction of
				[] when CopyFrom /= undefined ->
					CPFrom = binary_to_term(base64:decode(CopyFrom)),
					case CPFrom of
						{{move,_NewShard,_Node},CopyReset,CopyState} ->
							TypeOfMove = move;
						{{split,_Mfa,_Node,_FromActor},CopyReset,CopyState} ->
							TypeOfMove = split;
						{_,CopyReset,CopyState} ->
							TypeOfMove = false
					end,
					self() ! {check_redirect,Db,TypeOfMove},
					{ok,P#dp{copyreset = CopyReset,copyfrom = CPFrom,cbstate = CopyState,
								evterm = EvTerm,
								schemavers = Vers
							 % wlog_status = WlogStatus,wlog_len = WlogLen
							 }};
				[] ->
					{ok,actordb_sqlprocutil:start_verify(
								NP#dp{evnum = Evnum, schemavers = Vers,
											% wlog_status = WlogStatus,
											% wlog_len = WlogLen,
											evterm = EvTerm,
											movedtonode = MovedToNode1},true)};
				[{1,Tid,Updid,Node,SchemaVers,MSql1}] ->
					case base64:decode(MSql1) of
						<<"delete">> ->
							MSql = delete;
						MSql ->
							ok
					end,
					ReplSql = {MSql,Evnum+1,SchemaVers},
					Transid = {Tid,Updid,Node},
					{ok,actordb_sqlprocutil:start_verify(
								NP#dp{evnum = Evnum, replicate_sql = ReplSql, 
									transactionid = Transid, %wlog_status = WlogStatus, wlog_len = WlogLen,
									movedtonode = MovedToNode1,
									evterm = EvTerm,
									schemavers = SchemaVers},true)}
			end;
		[] -> %when (P#dp.flags band ?FLAG_CREATE) > 0 ->
			?ADBG("Opening NO schema create ~p",[{P#dp.actorname,P#dp.actortype}]),
			?DBLOG(Db,"init normal created schema",[]),
			{ok,actordb_sqlprocutil:start_verify(NP,true)}
		% [] ->
		% 	actordb_sqlite:stop(NP#dp.db),
		% 	?ADBG("Opening NO schema nocreate ~p",[{P#dp.actorname,P#dp.actortype}]),
		% 	explain(nocreate,Opts),
		% 	{stop,normal}
	end.

init_copy(P) ->
	?AINF("start copyfrom ~p ~p ~p",[P#dp.actorname,P#dp.actortype,P#dp.copyfrom]),
	case element(1,P#dp.copyfrom) of
		move ->
			TypeOfMove = move;
		split ->
			TypeOfMove = split;
		_ ->
			TypeOfMove = false
	end,
	% First check if movement is already done.
	case actordb_sqlite:init(P#dp.dbpath,wal) of
		{ok,Db,[_|_],_PageSize} ->
			?DBLOG(P#dp.db,"init copyfrom ~p",[P#dp.copyfrom]),
			case actordb_sqlite:exec(Db,[<<"select * from __adb where id=">>,?COPYFROM,";"],read) of
				{ok,[{columns,_},{rows,[]}]} ->
					case TypeOfMove of
						false ->
							Doit = true;
						_ ->
							Doit = check
					end;
				{ok,[{columns,_},{rows,[{_,_Copyf}]}]} ->
					Doit = false
			end;
		{ok,Db,[],_PageSize} ->
			actordb_sqlite:stop(Db),
			Doit = true;
		_ ->
			Db = undefined,
			Doit = true
	end,
	case Doit of
		true  ->
			{EPid,_} = spawn_monitor(fun() -> 
						actordb_sqlprocutil:verify_getdb(P#dp.actorname,P#dp.actortype,P#dp.copyfrom,
							undefined,master,P#dp.cbmod,P#dp.evnum) 
			end),
			{ok,P#dp{verified = false, electionpid = EPid, mors = master,
				activity_now = actordb_sqlprocutil:actor_start(P)}};
		_ ->
			?AINF("Started for copy but copy already done or need check (~p) ~p ~p",
						[Doit,P#dp.actorname,P#dp.actortype]),
			self() ! {check_redirect,Db,TypeOfMove},
			{ok,P}
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
		undefined ->
			Ref = make_ref(),
			erlang:send_after(1000,self(),{inactivity_timer,Ref,P#dp.activity}),
			P#dp{timerref = Ref};
		_ ->
			P
	end.

	% Called from master
% handle_call({replicate_start,_Ref,Node,PrevEvnum,PrevCrc,Sql,EvNum,Crc,NewVers} = Msg,From,P) ->
% 	?ADBG("Replicate start ~p ~p ~p ~p ~p ~p ~p",[P#dp.actorname,P#dp.actortype,P#dp.evnum, PrevEvnum, P#dp.evcrc, PrevCrc,Sql]),
% 	?DBLOG(P#dp.db,"replicatestart ~p ~p ~p ~p",[_Ref,butil:encode_percent(_Node),EvNum,Crc]),
% 	case Sql of
% 		<<"delete">> ->
% 			Trump = true;
% 		<<"moved,",_/binary>> ->
% 			Trump = true;
% 		_ ->
% 			Trump = false
% 	end,
% 	case ok of
% 		_ when Trump; P#dp.mors == slave, Node == P#dp.masternodedist, P#dp.evnum == PrevEvnum, P#dp.evcrc == PrevCrc ->
% 			{reply,ok,check_timer(P#dp{replicate_sql = {Sql,EvNum,Crc,NewVers}, activity = P#dp.activity + 1})};
% 		% _ when P#dp.mors == slave, Node == P#dp.masternodedist, P#dp.prev_evnum == PrevEvnum, P#dp.prev_evcrc == PrevCrc ->
% 		% 	case forget_write(P) of
% 		% 		ok ->
% 		% 			{reply,ok,check_timer(P#dp{replicate_sql = {Sql,EvNum,Crc,NewVers}, activity = P#dp.activity + 1})};
% 		% 		_ ->
% 		% 			handle_call(Msg,From,P#dp{prev_evnum = -1})
% 		% 	end;
% 		_ ->
% 			actordb_sqlite:stop(P#dp.db),
% 			case ok of
% 				_ when Node == P#dp.masternodedist ->
% 					?DBLOG(P#dp.db,"replicate conflict!!! ~p ~p in ~p ~p, cur ~p ~p",[_Ref,_Node,EvNum,Crc,P#dp.evnum,P#dp.evcrc]),
% 					?AERR("Replicate conflict!!!!! ~p ~p ~p ~p ~p, master ~p",[{P#dp.actorname,P#dp.actortype},
% 												P#dp.evnum, PrevEvnum, P#dp.evcrc, PrevCrc,{Node,P#dp.masternodedist}]),
% 					reply(From,desynced),
% 					{ok,NP} = init(P,replicate_conflict),
% 					{noreply,NP};
% 				_ ->
% 					?ADBG("Reinit to reastablish master node ~p",[{P#dp.actorname,P#dp.actortype}]),
% 					{ok,NP} = init(P#dp{callqueue = queue:in({From,Msg},P#dp.callqueue)},replicate_conflict),
% 					{reply,ok,NP}
% 			end
% 	end;
% % Called from master
% handle_call({replicate_commit,_StoreLog},From,P) ->
% 	?ADBG("Replicate commit! ~p ~p",[{P#dp.actorname,P#dp.actortype},P#dp.replicate_sql]),
% 	case P#dp.replicate_sql of
% 		<<>> ->
% 			?DBLOG(P#dp.db,"replicatecommit empty",[]),
% 			{reply,false,check_timer(P#dp{activity = P#dp.activity+1})};
% 		_ ->
% 			{Sql,EvNum,Crc,NewVers} = P#dp.replicate_sql,
% 			?DBLOG(P#dp.db,"replicatecommit ok ~p ~p",[EvNum,Crc]),
% 			case Sql of
% 				<<"delete">> ->
% 					actordb_sqlite:stop(P#dp.db),
% 					actordb_sqlprocutil:delactorfile(P),
% 					reply(From,ok),
% 					{stop,normal,P};
% 				<<"moved,",MovedTo/binary>> ->
% 					actordb_sqlite:stop(P#dp.db),
% 					?DBG("Stopping because moved ~p ~p",[P#dp.actorname,MovedTo]),
% 					actordb_sqlprocutil:delactorfile(P#dp{movedtonode = MovedTo}),
% 					reply(From,ok),
% 					{stop,normal,P};
% 				_ ->
% 					Sql1 = semicolon(Sql),
% 					Res = actordb_sqlite:exec(P#dp.db,[
% 						 <<"$SAVEPOINT 'adb';">>,
% 						 Sql1,%add_wlog(P,StoreLog,Sql1,EvNum,Crc),
% 						 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNum),<<"' WHERE id=">>,?EVNUM,";",
% 						 <<"$UPDATE __adb SET val='">>,butil:tobin(Crc),<<"' WHERE id=">>,?EVCRC,";",
% 						 <<"$RELEASE SAVEPOINT 'adb';">>
% 						 ],write),
% 					{reply,actordb_sqlite:okornot(Res),check_timer(%wlog_after_write(StoreLog,
% 									P#dp{replicate_sql = <<>>,evnum = EvNum, prev_evnum = P#dp.evnum,prev_evcrc = P#dp.evcrc,
% 										 evcrc = Crc, activity = P#dp.activity + 1, schemavers = NewVers})}
% 			end
% 	end;
% handle_call(replicate_rollback,_,P) ->
% 	?ERR("replicate_rollback"),
% 	{reply,ok,P#dp{replicate_sql = <<>>}};
% handle_call({replicate_bad_commit,EvNum,Crc},_,P) ->
% 	?ERR("replicate_bad_commit in ~p, my ~p",[{EvNum,Crc},{P#dp.evnum,P#dp.evcrc}]),
% 	case P#dp.evnum == EvNum andalso P#dp.evcrc == Crc of
% 		true ->
% 			% case forget_write(P) of
% 			% 	ok ->
% 			% 		{reply,ok,P#dp{evcrc = P#dp.prev_evcrc, evnum = P#dp.prev_evnum}};
% 			% 	_ ->
% 					actordb_sqlite:stop(P#dp.db),
% 					actordb_sqlprocutil:delactorfile(P),
% 					{ok,NP} = init(P,replicate_bad_commit),
% 	 				{reply,ok,NP#dp{callqueue = P#dp.callqueue}};
% 			% end;
% 		_ ->
% 			{reply,ok,P#dp{}}
% 	end;


% forget_write(P) ->
% 	% We can find out how many pages last write contained and truncate wal file manually.
% 	% After closing db handle, last write will be lost and bad commit will be forgotten.
% 	ForgetWrite = fun() ->
% 		wal = P#dp.journal_mode,
% 		{NPrev,NPages} = actordb_sqlite:wal_pages(P#dp.db),
% 		true = NPages > NPrev,
% 		{ok,File} = file:open(P#dp.dbpath++"-wal",[write,read,binary,raw]),
% 		{ok,_} = file:position(File,32+(P#dp.page_size+24)*NPrev),
% 		ok = file:truncate(File),
% 		file:close(File),
% 		actordb_sqlite:stop(P#dp.db),
% 		ok
% 	end,
% 	case catch ForgetWrite() of
% 		Res ->
% 			Res
% 	end.

% wlog_dbcopy(P,Home,ActorTo) ->
% 	case gen_server:call(Home,{dbcopy_op,{self(),P#dp.dbcopyref},wlog_read,P#dp.evnum}) of
% 		{ok,Done,Rows} ->
% 			{LastNum,LastCrc,CompleteSql} = lists:foldl(fun({Id,Crc,_,_,Sql},{_,_,Sqls}) ->
% 				{Id,Crc,[Sqls,base64:decode(Sql)]}
% 			end,{0,0,[]},Rows),
% 			ok = rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,{LastNum,LastCrc,CompleteSql},sql,original]}),
% 			case Done of
% 				done ->
% 					exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,<<>>,done,original]}));
% 				continue ->
% 					wlog_dbcopy(P#dp{evnum = LastNum},Home,ActorTo)
% 			end;
% 		done ->
% 			exit(rpc(P#dp.dbcopy_to,{?MODULE,dbcopy_send,[P,P#dp.dbcopyref,<<>>,done,original]}))
% 	end.



% printfail(_A,_T,_,[]) ->
% 	ok;
% printfail(A,T,N,L) ->
% 	?AERR("commit failed on ~p ~p  ~p",[{A,T},N,L]).

% commit_write(OrigMsg,P1,LenCluster,ConnectedNodes,EvNum,Sql,Crc,SchemaVers) ->
% 	Actor = P1#dp.actorname,
% 	Type = P1#dp.actortype,
% 	OldEvnum = P1#dp.evnum,
% 	OldCrc = P1#dp.evcrc,
% 	Flags = P1#dp.flags,
% 	Cbmod = P1#dp.cbmod,
% 	% WL = P1#dp.writelog,
% 	{Commiter,_} = spawn_monitor(fun() ->
% 			Ref = make_ref(),
% 			SqlBin = iolist_to_binary(Sql),
% 			{ResultsStart,StartFailed} = rpc:multicall(ConnectedNodes,?MODULE,call_slave,
% 						[Cbmod,Actor,Type,{replicate_start,Ref,node(),OldEvnum,
% 																	OldCrc,SqlBin,EvNum,Crc,SchemaVers},[{flags,Flags}]]),
% 			printfail(Actor,Type,1,StartFailed),
% 			% Only count ok responses
% 			{LenStarted,NProblems} = lists:foldl(fun(X,{NRes,NProblems}) -> 
% 									case X of 
% 										ok -> {NRes+1,NProblems}; 
% 										reinit -> exit({reinit,OrigMsg});
% 										_ -> {NRes,NProblems+1}
% 									end
% 									end,{0,0},ResultsStart),
% 			case (LenStarted+1)*2 > LenCluster+1 of
% 				true ->
% 					NodesToCommit = lists:subtract(ConnectedNodes,StartFailed),
% 					DoWlog = LenCluster > length(ConnectedNodes) orelse 
% 							StartFailed /= [] orelse 
% 							NProblems > 0,
% 					{ResultsCommit,CommitFailedOn} = rpc:multicall(NodesToCommit,?MODULE,call_slave,
% 										[Cbmod,Actor,Type,{replicate_commit,DoWlog},[{flags,Flags}]]),
% 					CommitBadres = [X || X <- ResultsCommit, X /= ok],
% 					printfail(Actor,Type,2,CommitFailedOn),
% 					LenCommited = length(ResultsCommit),
% 					case (LenCommited+1)*2 > LenCluster+1 of
% 						true ->
% 							% WLog = <<(trim_wlog(WL))/binary,
% 							% 					EvNum:64/unsigned,Crc:32/unsigned,(byte_size(SqlBin)):32/unsigned,
% 							% 						(SqlBin)/binary>>,
% 							exit({ok, EvNum, Crc, DoWlog orelse CommitFailedOn /= [] orelse CommitBadres /= []});
% 						false ->
% 							CommitOkOn = lists:subtract(NodesToCommit,CommitFailedOn),
% 							[rpc:async_call(Nd,?MODULE,call_slave,
% 										[Cbmod,Actor,Type,
% 													{replicate_bad_commit,EvNum,Crc},[{flags,Flags}]]) || 
% 														Nd <- CommitOkOn],
% 							exit({replication_failed_3,LenCommited,LenCluster})
% 					end;
% 				false ->
% 					?AERR("replicate failed ~p ~p ~p",[Sql,{Actor,Type},{ResultsStart,StartFailed}]),
% 					rpc:multicall(ConnectedNodes,?MODULE,call_slave,[Cbmod,Actor,
% 																Type,replicate_rollback,[{flags,Flags}]]),
% 					exit({replication_failed_4,LenStarted,LenCluster})
% 			end
% 	end),
% 	Commiter.


% handle_info({'DOWN',_Monitor,Ref,PID,_Result},#dp{commiter = PID, callfrom = dbcopy} = P) ->
% 	handle_info({'DOWN',_Monitor,Ref,PID,_Result},P#dp{callfrom = undefined,commiter = undefined});
% handle_info({'DOWN',_Monitor,_,PID,Result},#dp{commiter = PID} = P) ->
% 	case Result of
% 		{ok,EvNum,Crc,_DoWlog} ->
% 			?DBLOG(P#dp.db,"commiterdown ok ~p ~p",[EvNum,Crc]),
% 			?DBG("Commiter down ~p ok ~p callres ~p ~p",[{P#dp.actorname,P#dp.actortype},EvNum,P#dp.callres,P#dp.callqueue]),
% 			{Sql,EvNumNew,CrcSql,NewVers} = P#dp.replicate_sql,
% 			case Sql of 
% 				<<"delete">> ->
% 					case P#dp.transactionid == undefined of
% 						true ->
% 							Die = true,
% 							delete_actor(P);
% 						false ->
% 							Die = false
% 					end;
% 				<<"moved,",MovedTo/binary>> ->
% 					?DBG("Stopping because moved ~p ~p",[P#dp.actorname,MovedTo]),
% 					Die = true,
% 					actordb_sqlite:stop(P#dp.db),
% 					actordb_sqlprocutil:delactorfile(P#dp{movedtonode = MovedTo});
% 				_ ->
% 					Die = false,
% 					% add_wlog(P,DoWlog,Sql,EvNum,Crc),
% 					ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,[<<"RELEASE SAVEPOINT 'adb';">>]))
% 			end,
% 			case P#dp.transactionid of
% 				undefined ->
% 					ReplicateSql = undefined,
% 					reply(P#dp.callfrom,P#dp.callres);
% 				_ ->
% 					{Tid,Updaterid,_} = P#dp.transactionid,
% 					case Sql of
% 						<<"delete">> ->
% 							ReplicateSql = {<<"delete">>,EvNumNew,CrcSql,NewVers},
% 							reply(P#dp.callfrom,ok);
% 						_ ->
% 							NewSql = [Sql,<<"$DELETE FROM __transactions WHERE tid=">>,(butil:tobin(Tid)),
% 												<<" AND updater=">>,(butil:tobin(Updaterid)),";"],
% 							% Execute transaction sql and at the same time delete transaction sql from table.
% 							ComplSql = 
% 									[<<"$SAVEPOINT 'adb';">>,
% 									 NewSql,
% 									 <<"$UPDATE __adb SET val='">>,butil:tobin(EvNumNew),<<"' WHERE id=">>,?EVNUM,";",
% 									 <<"$UPDATE __adb SET val='">>,butil:tobin(CrcSql),<<"' WHERE id=">>,?EVCRC,";"
% 									 ],
% 							Res = actordb_sqlite:exec(P#dp.db,ComplSql,write),
% 							reply(P#dp.callfrom,Res),
% 							% Store sql for later execution on slave nodes.
% 							ReplicateSql = {NewSql,EvNumNew,CrcSql,NewVers},
% 							case actordb_sqlite:okornot(Res) of
% 								ok ->
% 									ok;
% 								_ ->
% 									Me = self(),
% 									spawn(fun() -> gen_server:call(Me,{commit,false,P#dp.transactionid}) end)
% 							end
% 					end
% 			end,
% 			case ok of
% 				_ when Die ->
% 					{stop,normal,P};
% 				_ ->
% 					handle_info(doqueue,check_timer(%wlog_after_write(DoWlog,
% 												P#dp{commiter = undefined,callres = undefined, 
% 												callfrom = undefined,activity = P#dp.activity+1, 
% 												evnum = EvNum, evcrc = Crc,
% 												schemavers = NewVers,
% 												replicate_sql = ReplicateSql}))
% 			end;
% 		{reinit,Msg} ->
% 			ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>)),
% 			{ok,NP} = init(P#dp{callqueue = queue:in_r({P#dp.callfrom,Msg},P#dp.callqueue)}),
% 			{noreply,NP};
% 		% Should always be: {replication_failed,HasNodes,NeedsNodes}
% 		Err ->
% 			?AERR("Commiter down ~p error ~p",[{P#dp.actorname,P#dp.actortype},Err]),
% 			?DBLOG(P#dp.db,"commiterdown error ~p",[Err]),
% 			{Sql,_EvNumNew,_CrcSql,_NewVers} = P#dp.replicate_sql,
% 			case Sql of
% 				<<"delete">> ->
% 					ok;
% 				_ ->
% 					ok = actordb_sqlite:okornot(actordb_sqlite:exec(P#dp.db,<<"ROLLBACK;">>))
% 			end,
% 			reply(P#dp.callfrom,{error,Err}),
% 			handle_info(doqueue,P#dp{callfrom = undefined,commiter = undefined, transactionid = undefined, replicate_sql = undefined})
% 	end;
% handle_info({'DOWN',_Monitor,_,PID,Reason},#dp{verifypid = PID} = P) ->
% 	case Reason of
% 		{verified,Mors,MasterNode,_AllSynced} when P#dp.transactionid == undefined; Mors == slave ->
% 			actordb_local:actor_mors(Mors,MasterNode),
% 			?ADBG("Verify down ~p ~p ~p ~p ~p ~p",[P#dp.actorname, P#dp.actortype, P#dp.evnum,
% 						Reason, P#dp.mors, queue:is_empty(P#dp.callqueue)]),
% 			?DBLOG(P#dp.db,"verified ~p ~p",[Mors,MasterNode]),
% 			case Mors of
% 				master ->
% 					% If any uncommited transactions, check if they are abandonded or to be executed
% 					NS = do_cb_init(P);
% 				_ ->
% 					NS = P#dp.cbstate
% 			end,
% 			handle_info(doqueue,%wlog_stillneed(AllSynced,?WLOG_NONE,
% 									P#dp{verified = true, verifypid = undefined, mors = Mors, masternode = MasterNode, 
% 									masternodedist = bkdcore:dist_name(MasterNode),
% 									cbstate = NS});
% 		{verified,Mors,MasterNode,_AllSynced} ->
% 			?ADBG("Verify down ~p ~p ~p ~p ~p ~p",[P#dp.actorname, P#dp.actortype, P#dp.evnum,
% 						Reason, P#dp.mors, queue:is_empty(P#dp.callqueue)]),
% 			?DBLOG(P#dp.db,"verified ~p ~p",[Mors,MasterNode]),
% 			actordb_local:actor_mors(Mors,MasterNode),
% 			{Tid,Updid,Node} = P#dp.transactionid,
% 			{Sql,Evnum,Crc,_NewVers} = P#dp.replicate_sql, 
% 			% NP = wlog_stillneed(AllSynced,?WLOG_NONE,
% 			NP = P#dp{verified = true,verifypid = undefined, mors = Mors,
% 					 			masternode = MasterNode,masternodedist = bkdcore:dist_name(MasterNode), cbstate = do_cb_init(P)},
% 			case actordb:rpc(Node,Updid,{actordb_multiupdate,transaction_state,[Updid,Tid]}) of
% 				{ok,State} when State == 0; State == 1 ->
% 					ComplSql = 
% 						[<<"$SAVEPOINT 'adb';">>,
% 						 semicolon(Sql),actornum(P),
% 						 <<"$DELETE FROM __transactions WHERE tid=">>,(butil:tobin(Tid)),
% 						 		<<" AND updater=">>,(butil:tobin(Updid)),";",
% 						 <<"$UPDATE __adb SET val='">>,butil:tobin(Evnum),<<"' WHERE id=">>,?EVNUM,";",
% 						 <<"$UPDATE __adb SET val='">>,butil:tobin(Crc),<<"' WHERE id=">>,?EVCRC,";"
% 						 ],
% 					actordb_sqlite:exec(P#dp.db,ComplSql,write),
% 					% 0 - transaction still running, wait for done.
% 					% 1 - finished, do commit straight away.
% 					case State of
% 						0 ->
% 							{_CheckPid,CheckRef} = start_transaction_checker(Tid,Updid,Node),
% 							{noreply,NP#dp{transactioncheckref = CheckRef}};
% 						1 ->
% 							CQ = queue:in({undefined,{commit,true,{Tid,Updid,Node}}},NP#dp.callqueue),
% 							handle_info(doqueue,NP#dp{callqueue = CQ})
% 					end;
% 				% Lets forget this ever happened.
% 				{ok,-1} ->
% 					Sql = <<"DELETE FROM __transactions id=",(butil:tobin(Tid))/binary,
% 								" AND updater=",(butil:tobin(Updid))/binary,";">>,
% 					CQ = queue:in({undefined,{write,{erlang:crc32(Sql),Sql,undefined}}},NP#dp.callqueue),
% 					handle_info(doqueue,NP#dp{callqueue = CQ});
% 				% In case of error, process should crash, because it can not process sql if it can not verify last transaction
% 				_Err ->
% 					exit({error,{unable_to_verify_transaction,_Err}})
% 			end;
% 		{update_from,Node,Mors,MasterNode,Ref} ->
% 			?ADBG("Verify down ~p ~p ~p master ~p, update from ~p",[P#dp.actorname,PID,Reason,MasterNode,Node]),
% 			% handle_info(doqueue,P#dp{verified = false, verifypid = undefined, mors = Mors});
% 			case P#dp.copyfrom of
% 				undefined ->
% 					Copyfrom = Node;
% 				Copyfrom ->
% 					ok
% 			end,
% 			actordb_sqlite:stop(P#dp.db),
% 			{ok,RecvPid} = start_copyrec(P#dp{copyfrom = Copyfrom, mors = master, dbcopyref = Ref}),
% 			erlang:monitor(process,RecvPid),

% 			% In case db needs to be restored from another node, reject any writes in callqueue
% 			QL = butil:sparsemap(fun({MsgFrom,Msg}) ->
% 				case ok of
% 					_ when element(1,Msg) == replicate_start ->
% 						reply(MsgFrom,notready),
% 						undefined;
% 					_ ->
% 						{MsgFrom,Msg}
% 				end
% 			end,queue:to_list(P#dp.callqueue)),

% 			{noreply,P#dp{db = undefined,
% 							verifypid = undefined, verified = false, mors = Mors, masternode = MasterNode,callqueue = queue:from_list(QL),
% 							masternodedist = bkdcore:dist_name(MasterNode),dbcopyref = Ref, copyfrom = Copyfrom, copyproc = RecvPid}};
% 		{update_direct,Mors,Bin} ->
% 			?AINF("Verify down update direct ~p ~p ~p",[Mors,P#dp.actorname,P#dp.actortype]),
% 			actordb_sqlite:stop(P#dp.db),
% 			actordb_sqlprocutil:delactorfile(P),
% 			ok = file:write_file(P#dp.dbpath,Bin),
% 			{ok,NP} = init(P#dp{mors = Mors},update_direct),
% 			{noreply,NP};
% 		{redirect,Nd} ->
% 			?AINF("verify redirect ~p ~p",[P#dp.actorname,Nd]),
% 			{stop,normal,P};
% 		{nomajority,Groups} ->
% 			?AERR("Verify nomajority ~p ~p",[{P#dp.actorname,P#dp.actortype},Groups]),
% 			% self() ! stop,
% 			% handle_info(doqueue,P#dp{verified = failed, verifypid = undefined});
% 			{stop,nomajority,P};
% 		{nomajority,Groups,Failed} ->
% 			?AERR("Verify nomajority ~p ~p ~p",[{P#dp.actorname,P#dp.actortype},Groups,Failed]),
% 			% self() ! stop,
% 			% handle_info(doqueue,P#dp{verified = failed, verifypid = undefined});
% 			{stop,nomajority,P};
% 		{error,enoent} ->
% 			?AERR("error enoent result of verify ~p ~p",[P#dp.actorname,P#dp.actortype]),
% 			distreg:unreg(self()),
% 			{stop,normal,P};
% 		nomaster ->
% 			?AERR("No master found for verify ~p ~p",[?R2P(P),get()]),
% 			{stop,nomaster,P};
% 		_ ->
% 			case queue:is_empty(P#dp.callqueue) of
% 				true ->
% 					?AERR("Verify down for ~p error ~p",[P#dp.actorname,Reason]);
% 				false ->
% 					?AERR("Verify down for ~p error ~p ~p",[P#dp.actorname,Reason,queue:out_r(P#dp.callqueue)])
% 			end,
% 			{Verifypid,_} = spawn_monitor(fun() -> timer:sleep(500), 
% 													verifydb(P#dp.actorname,P#dp.actortype,P#dp.evcrc,
% 																P#dp.evnum,P#dp.mors,P#dp.cbmod,P#dp.flags) 
% 												end),
% 			{noreply,P#dp{verified = false, verifypid = Verifypid}}
% 	end;



% % Once wlog started continue untill we know all nodes are in sync.
% add_wlog(P,DoWlog,Sql,Evnum,Crc) when P#dp.wlog_status == ?WLOG_ACTIVE; 
% 									  DoWlog, P#dp.wlog_status /= ?WLOG_ABANDONDED ->
% 	[<<"$INSERT INTO __wlog VALUES (">>,butil:tobin(Evnum),$,,butil:tobin(Crc),$,,butil:tobin(P#dp.evnum),$,,butil:tobin(P#dp.evcrc),$,,
% 			$',base64:encode(iolist_to_binary(Sql)),$',");"];
% % If status ?WLOG_NONE, ?WLOG_ABANDONED or dowlog == false do not do it.
% add_wlog(_P,_DoWlog,_Sql,_Evnum,_Crc) ->
% 	[].

% wlog_after_write(DoWlog,P) ->
% 	case ok of
% 		_ when P#dp.wlog_status == ?WLOG_ACTIVE, P#dp.wlog_len >= 10000 ->
% 			wlog_stillneed(true,?WLOG_ABANDONDED,P);
% 		_ when DoWlog, P#dp.wlog_status == ?WLOG_NONE ->
% 			P#dp{wlog_status = ?WLOG_ACTIVE, wlog_len = P#dp.wlog_len + 1};
% 		_ when P#dp.wlog_status == ?WLOG_ACTIVE ->
% 			P#dp{wlog_len = P#dp.wlog_len + 1};
% 		_ ->
% 			P
% 	end.

% can_stop_wlog(P) ->
% 	Home = self(),
% 	spawn(fun() -> can_stop_wlog(Home,P) end).
% can_stop_wlog(Home,P) ->
% 	ClusterNodes = bkdcore:cluster_nodes(),
% 	ConnectedNodes = bkdcore:cluster_nodes_connected(),
% 	LenConnected = length(ConnectedNodes),
% 	case LenConnected  == length(ClusterNodes) of
% 		true ->
% 			{Results,GetFailed} = rpc:multicall(ConnectedNodes,?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
% 															{getinfo,verifyinfo},[{flags,P#dp.flags}]]),
% 			case GetFailed of
% 				[] ->
% 					MatchingResults = [ok || {ok,_,Crc,Evnum,_} <- Results, Crc == P#dp.evcrc, Evnum == P#dp.evnum],
% 					case length(MatchingResults) == LenConnected of
% 						true ->
% 							% If all nodes match, there is no need to still be doing wlog
% 							[rpc:async_call(Nd,?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
% 															{dbcopy_op,undefined,wlog_unneeded,?WLOG_NONE}])
% 								|| Nd <- ConnectedNodes],
% 							gen_server:call(Home,{dbcopy_op,undefined,wlog_unneeded,?WLOG_NONE});
% 						false ->
% 							ok
% 					end;
% 				_ ->
% 					ok
% 			end;
% 		false ->
% 			ok
% 	end.

% Either log has been abandonded for being too large or all nodes are in sync. Thus there are
%  two reasons for abandoning, ?WLOG_NONE or ?WLOG_ABANDONED.
% wlog_stillneed(AllSynced,How,P) ->
% 	case ok of
% 		_ when AllSynced andalso (P#dp.wlog_status == ?WLOG_ACTIVE orelse P#dp.wlog_status == ?WLOG_ABANDONDED) ->
% 			[rpc:async_call(Nd,?MODULE,call_slave,[P#dp.cbmod,P#dp.actorname,P#dp.actortype,
% 															{dbcopy_op,undefined,wlog_unneeded,How}])
% 							|| Nd <- bkdcore:cluster_nodes_connected()],
% 			actordb_sqlite:exec(P#dp.db,[<<"$INSERT OR REPLACE INTO __adb VALUES (">>,
% 									?WLOG_STATUS,$,,butil:tobin(How),<<");">>,
% 									<<"$DELETE * FROM __wlog;">>]),
% 			P#dp{wlog_status = How, wlog_len = 0};
% 		_ ->
% 			P
% 	end.


% verified_response(MeMors,MasterNode,AllSynced) ->
% 	?ADBG("verified_response ~p ~p",[MeMors,MasterNode]),
% 	Me = bkdcore:node_name(),
% 	case ok of
% 		_ when MeMors == master, MasterNode == undefined ->
% 			exit({verified,master,Me,AllSynced});
% 		_ when MeMors == master, MasterNode /= Me, MasterNode /= undefined ->
% 			exit({verified,slave,MasterNode,AllSynced});
% 		_ when MasterNode /= undefined ->
% 			exit({verified,MeMors,MasterNode,AllSynced});
% 		_ when MeMors == master ->
% 			exit({verified,master,Me,AllSynced});
% 		_ ->
% 			exit(nomaster)
% 	end.
% verifydb(Actor,Type,Evcrc,Evnum,MeMors,Cb,Flags) ->
% 	?ADBG("Verifydb ~p ~p ~p ~p ~p ~p",[Actor,Type,Evcrc,Evnum,MeMors,Cb]),
% 	ClusterNodes = bkdcore:cluster_nodes(),
% 	LenCluster = length(ClusterNodes),
% 	ConnectedNodes = bkdcore:cluster_nodes_connected(),
% 	{Results,GetFailed} = rpc:multicall(ConnectedNodes,?MODULE,call_slave,[Cb,Actor,Type,{getinfo,verifyinfo},[{flags,Flags}]]),
% 	?ADBG("verify from others ~p",[Results]),
% 	printfail(Actor,Type,3,GetFailed),
% 	Me = bkdcore:node_name(),
% 	% Count how many nodes have db with same last evnum and evcrc and gather nodes that are different.
% 	{Yes,Masters} = lists:foldl(
% 			fun({redirect,Nd},_) -> 
% 				exit({redirect,Nd});
% 			 ({ok,Node,NodeCrc,NodeEvnum,NodeMors},{YesVotes,Masters}) -> 
% 			 	case NodeMors of
% 			 		{master,true} ->
% 			 			Masters1 = [{Node,true}|Masters];
% 			 		{_,failed} ->
% 			 			Masters1 = Masters;
% 			 		{master,false} ->
% 			 			Masters1 = [{Node,false}|Masters];
% 			 		{slave,_} ->
% 			 			Masters1 = Masters;
% 			 		master ->
% 			 			Masters1 = [{Node,true}|Masters];
% 			 		slave ->
% 			 			Masters1 = Masters
% 			 	end,
% 				case Evcrc == NodeCrc andalso Evnum == NodeEvnum of
% 					true ->
% 						{YesVotes+1,Masters1};
% 					false ->
% 						{YesVotes,Masters1}
% 				end;
% 			(_,{YesVotes,Masters}) ->
% 				{YesVotes,Masters}
% 			end,
% 			{0,[]},Results),
% 	case Masters of
% 		[] when MeMors == master ->
% 			MasterNode = bkdcore:node_name();
% 		[] ->
% 			?AERR("No master node set ~p ~p ~p",[{Actor,Type},MeMors,Results]),
% 			MasterNode = undefined,
% 			exit(nomaster);
% 		[{MasterNode,true}] ->
% 			ok;
% 		[{MasterNode1,_}] when MeMors == master, Me < MasterNode1 ->
% 			MasterNode = Me;
% 		[{MasterNode,_}] ->
% 			ok;
% 		[_,_|_] ->
% 			case [MN || {MN,true} <- Masters] of
% 				[MasterNode] ->
% 					ok;
% 				% This should not be possible, kill all actors on all nodes
% 				[_,_|_] ->
% 					?AERR("Received multiple confirmed masters?? ~p ~p",[{Actor,Type},Results]),
% 					rpc:multicall(ConnectedNodes,?MODULE,call_slave,[Cb,Actor,Type,{getinfo,conflicted},[{flags,Flags}]]),
% 					MasterNode = undefined,
% 					exit(nomaster);
% 				[] ->
% 					case MeMors of
% 						master ->
% 							[MasterNode|_] = lists:sort([Me|[MN || {MN,_} <- Masters]]);
% 						_ ->
% 							[MasterNode|_] = lists:sort([MN || {MN,_} <- Masters])
% 					end
% 			end
% 	end,
% 	case MasterNode == Me of
% 		true ->
% 			MeMorsOut = master;
% 		_ ->
% 			MeMorsOut = slave
% 	end,
% 	% This node is in majority group.
% 	case (Yes+1)*2 > (LenCluster+1) of
% 		true ->
% 			case Evnum == 0 of
% 				true ->
% 					case butil:findtrue(fun({ok,_,_,NodeEvnum,_}) -> NodeEvnum > 0 end,Results) of
% 						false ->
% 							verified_response(MeMorsOut,MasterNode,Yes == LenCluster);
% 						_ ->
% 							?ADBG("Node does not have db some other node does! ~p",[Actor]),
% 							% Majority has evnum 0, but there is more than one group.
% 							% This is an exception. In this case highest evnum db is the right one.
% 							[{ok,Oknode,_,_,_}|_] = lists:reverse(lists:keysort(4,Results)),
% 							verify_getdb(Actor,Type,Oknode,MasterNode,MeMorsOut,Cb,Evnum,Evcrc)
% 					end;
% 				_ ->
% 					verified_response(MeMorsOut,MasterNode,Yes == LenCluster)
% 			end;
% 		false ->
% 			Grouped = butil:group(fun({ok,_Node,NodeCrc,NodeEvnum,_NodeMors}) -> {NodeEvnum,NodeCrc} end,
% 										[{ok,bkdcore:node_name(),Evcrc,Evnum,MeMors}|Results]),
% 			case butil:find(fun({Key,Group}) -> 
% 					case length(Group)*2 > (LenCluster+1) of
% 						true ->
% 							{Key,Group};
% 						false ->
% 							undefined
% 					end
% 				 end,Grouped) of
% 				% Group with a majority of nodes and evnum > 0. This is winner.
% 				{MajorityKey,MajorityGroup} when element(1,MajorityKey) > 0 ->
% 					% There is a group with a majority of nodes, if it has a node running set as master, 
% 					% 		then local node must be slave
% 					% If it does not have master and no master found, master for local node is unchanged.
% 					case butil:findtrue(fun({ok,Node,_,_,_}) -> Node == MasterNode end,MajorityGroup) of
% 						false ->
% 							[{ok,Oknode,_,_,_}|_]  = MajorityGroup,
% 							?ADBG("Restoring db from another node ~p",[Actor]),
% 							verify_getdb(Actor,Type,Oknode,MasterNode,MeMorsOut,Cb,Evnum,Evcrc);
% 						{ok,MasterNode,_,_,_} ->
% 							verify_getdb(Actor,Type,MasterNode,MasterNode,MeMorsOut,Cb,Evnum,Evcrc)
% 					end;
% 				% No clear majority or majority has no writes.
% 				_ ->
% 					% If only two types of actors and one has no events, the other type must
% 					%   have some events and consider it correct.
% 					% If local node part of that group db is verified. If not it needs to restore from that node.
% 					case Grouped of
% 						[_,_] ->
% 							case lists:keyfind({0,0},1,Grouped) of
% 								false ->
% 									exit({nomajority,Grouped});
% 								{_,_ZeroGroup} when Evnum == 0 ->
% 									?ADBG("Node does not have db some other node does! ~p",[Actor]),
% 									[{_,OtherGroup}] = lists:keydelete({0,0},1,Grouped),
% 									[{ok,Oknode,_,_,_}|_]  = OtherGroup,
% 									verify_getdb(Actor,Type,Oknode,MasterNode,MeMorsOut,Cb,Evnum,Evcrc);
% 								{_,_ZeroGroup} ->
% 									verified_response(MeMorsOut,MasterNode,false)
% 							end;
% 						_ ->
% 							exit({nomajority,Grouped,GetFailed})
% 					end
% 			end
% 	end.