% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module(actordb_sqlparse).
-compile(export_all).
-export([parse_statements/1]).
-include("actordb.hrl").


is_write([<<_/binary>> = Bin|_]) ->
	is_write(Bin);
is_write({Bin,_}) ->
	is_write(Bin);
is_write(<<$$,Bin/binary>>) ->
	is_write(Bin);
is_write(Bin) ->
	case Bin of
		<<"#w",_/binary>> ->
			true;
		<<"#r",_/binary>> ->
			false;
		<<"select ",_/binary>> ->
			false;
		<<"SELECT ",_/binary>> ->
			false;
		<<"Select ",_/binary>> ->
			false;
		<<"INSERT ",_/binary>> ->
			true;
		<<"Insert ",_/binary>> ->
			true;
		<<"insert ",_/binary>> ->
			true;
		<<"UPDATE ",_/binary>> ->
			true;
		<<"Update ",_/binary>> ->
			true;
		<<"update ",_/binary>> ->
			true;
		<<"replace ",_/binary>> ->
			true;
		<<"REPLACE ",_/binary>> ->
			true;
		<<"Replace ",_/binary>> ->
			true;
		<<"Delete ",_/binary>> ->
			true;
		<<"DELETE ",_/binary>> ->
			true;
		<<"delete ",_/binary>> ->
			true;
		<<"CREATE ",_/binary>> ->
			true;
		<<"Create ",_/binary>> ->
			true;
		<<"create ",_/binary>> ->
			true;
		<<"_insert ",_/binary>> ->
			true;
		<<"_INSERT ",_/binary>> ->
			true;
		<<"_Insert ",_/binary>> ->
			true;
		<<"pragma ",Rem/binary>> ->
			{pragma,Rem};
		<<"Pragma ",Rem/binary>> ->
			{pragma,Rem};
		<<"PRAGMA ",Rem/binary>> ->
			{pragma,Rem};
		<<"show ",Rem/binary>> ->
			{show,Rem};
		<<"Show ",Rem/binary>> ->
			{show,Rem};
		<<"SHOW ",Rem/binary>> ->
			{show,Rem};
		<<"with ",Rem/binary>> ->
			{with,Rem};
		<<"With ",Rem/binary>> ->
			{with,Rem};
		<<"WITH ",Rem/binary>> ->
			{with,Rem};
		<<"PREPARE ",Rem/binary>> ->
			{prepare, Rem};
		<<"Prepare ",Rem/binary>> ->
			{prepare, Rem};	
		<<"prepare ",Rem/binary>> ->
			{prepare, Rem};	
		<<"Execute ",Rem/binary>> ->
			{execute,Rem};
		<<"execute ",Rem/binary>> ->
			{execute,Rem};
		<<"EXECUTE ",Rem/binary>> ->
			{execute,Rem};
		% If you write sql like a moron then you get to these slow parts.
		<<C,R,E,A,T,E," ",_/binary>> when (C == $c orelse C == $C) andalso 
											(R == $r orelse R == $R) andalso
											(E == $e orelse E == $E) andalso
											(A == $a orelse A == $A) andalso
											(T == $t orelse T == $T) ->
			true;
		<<I,N,S,E,R,T," ",_/binary>> when (I == $i orelse I == $I) andalso 
											(N == $n orelse N == $N) andalso
											(S == $s orelse S == $S) andalso
											(E == $e orelse E == $E) andalso
											(R == $r orelse R == $R) andalso
											(T == $t orelse T == $T) ->
			true;
		<<"_",I,N,S,E,R,T," ",_/binary>> when (I == $i orelse I == $I) andalso 
											(N == $n orelse N == $N) andalso
											(S == $s orelse S == $S) andalso
											(E == $e orelse E == $E) andalso
											(R == $r orelse R == $R) andalso
											(T == $t orelse T == $T) ->
			true;
		<<U,P,D,A,T,E," ",_/binary>> when (U == $u orelse U == $U) andalso 
											(P == $p orelse P == $P) andalso
											(D == $d orelse D == $D) andalso
											(A == $a orelse A == $A) andalso
											(T == $t orelse T == $T) andalso
											(E == $e orelse E == $E) ->
			true;	
		<<D,E,L,E,T,E," ",_/binary>> when (D == $d orelse D == $D) andalso 
											(E == $e orelse E == $E) andalso
											(L == $l orelse L == $L) andalso
											(T == $t orelse T == $T)  ->
			true;
		<<R,E,P,L,A,C,E," ",_/binary>> when (R == $r orelse R == $R) andalso 
											(E == $e orelse E == $E) andalso
											(P == $p orelse P == $P) andalso
											(L == $l orelse L == $L) andalso
											(A == $a orelse A == $A) andalso
											(C == $c orelse C == $C) ->
			true;
		<<P,R,A,G,M,A," ",Rem/binary>> when (P == $p orelse P == $P) andalso 
											(R == $r orelse R == $R) andalso
											(A == $a orelse A == $A) andalso
											(G == $g orelse G == $G) andalso
											(M == $m orelse M == $M) ->
			{pragma,Rem};
		<<S,H,O,W," ",Rem/binary>> when (S == $s orelse S == $S) andalso
										(H == $h orelse H == $H) andalso
										(O == $o orelse O == $O) andalso
										(W == $w orelse W == $W) ->
			{show,Rem};
		<<W,I,T,H," ",Rem/binary>> when (W == $w orelse W == $W) andalso
										(I == $i orelse I == $I) andalso
										(T == $t orelse T == $T) andalso
										(H == $h orelse H == $H) ->
			{with,Rem};
		<<P,R,E,P,A,R,E," ",Rem/binary>> when (P == $p orelse P == $P) andalso
										(R == $r orelse R == $R) andalso
										(E == $e orelse E == $E) andalso
										(A == $a orelse A == $A) ->
			{prepare,Rem};
		<<E,X,E,C,U,T,E," ",Rem/binary>> when (E == $e orelse E == $E) andalso
										(X == $x orelse X == $X) andalso
										(C == $c orelse C == $C) andalso
										(U == $u orelse U == $U) andalso
										(T == $t orelse T == $T) ->
			{prepare,Rem};
		_ ->
			false
	end.

% Returns: {[{Actors,IsWrite,Statements},..],IsWrite}
% Actors: {Type,[Actor1,Actor2,Actor3,...],[Flag1,Flag2]} |
% 		  {Type,$*,[Flag1,Flag2]} |
% 		  {Type,GlobalVarName,Column,BlockVarName,[Flag1,Flag2]}
% IsWrite - boolean if any of the statements in the block change the db
% Statements: [Statement]
% Statement: {ResultGlobalVarName,SqlStatement]} |
% 					 SqlStatement 
% SqlStatement = list of: binary | char | {VariableName,VariableColumn} | uniqid
% Actors:
% 1. Fixed list of actors
% 2. all actors of some type 
% 3. actors as a result of a query preceing current block
parse_statements(Bin) ->
	parse_statements(undefined,Bin).
parse_statements(BP,Bin) ->
	L = split_statements(rem_spaces(Bin)),
	parse_statements(BP,L,[],undefined,[],false,false).
parse_statements(BP,[<<>>|T],L,CurUse,CurStatements,IsWrite,GIsWrite) ->
	parse_statements(BP,T,L,CurUse,CurStatements,IsWrite,GIsWrite);
parse_statements(BP,[H|T],L,CurUse,CurStatements,IsWrite,GIsWrite) ->
	case is_actor(H) of
		undefined ->
			case is_write(H) of
				{pragma,PragmaRem} ->
					case CurUse of
						undefined ->
							[];
						_ ->
							case parse_pragma(PragmaRem) of
								delete ->
									parse_statements(BP,T,L,CurUse,[delete],true, true);
								exists ->
									case split_actor(CurUse) of
										{Type,Actors,Flags} ->
											NewUse = {Type,Actors,butil:lists_add(exists,Flags)};
										{Type,Global,Col,Var,Flags} ->
											NewUse = {Type,Global,Col,Var,butil:lists_add(exists,Flags)}
									end,
									parse_statements(BP,T,L,NewUse,[exists],IsWrite, GIsWrite);
								{copy,Name} ->
									{Type,[Actor],Flags} = split_actor(CurUse),
									{_,_,Node} = actordb_shardmngr:find_global_shard(Name),
									parse_statements(BP,T,L,{Type,[Actor],[{copyfrom,{Node,Name}}|Flags]},[{copy,Name}],false, false);
								Pragma when Pragma == list; Pragma == count ->
									case split_actor(CurUse) of
										{Type,_Actors,_Flags} ->
											ok;
										{Type,_Global,_Col,_Var,_Flags} ->
											ok
									end,
									parse_statements(BP,T,L,{Type,$*,[]},[Pragma],false,false)
							end
					end;
				{show,ShowRem} ->
					case parse_show(ShowRem) of
						tables when CurUse /= undefined ->
							ST = <<"select name from sqlite_master where type='table';">>,
							parse_statements(BP,T,L,CurUse,[ST|CurStatements],true, true);
						_ ->
							parse_statements(BP,T,L,CurUse,[H|CurStatements],IsWrite,GIsWrite)
					end;
				{prepare,PrepRem} ->
					{Name,ActorType,Types,Sql} = parse_prepare(rem_spaces(PrepRem)),
					IsWriteH = is_write(Sql),
					Id = actordb_sharedstate:save_prepared(ActorType,IsWriteH,Sql),
					<<_/binary>> = Id,
					actordb_backpressure:save(BP,{prepared,Name},{ActorType,Id,Types}),
					parse_statements(BP,T,L,CurUse,CurStatements,IsWrite, GIsWrite);
				{execute,ExecRem} ->
					{Name,Vals} = parse_execute(rem_spaces(ExecRem)),
					?AINF("Parsed execute ~p ~p",[Name,Vals]),
					parse_statements(BP,T,L,CurUse,CurStatements,IsWrite, GIsWrite);
				{with,WithRem} ->
					IsWriteH = find_as(WithRem),
					parse_statements(BP,T,L,CurUse,[H|CurStatements],IsWrite orelse IsWriteH, GIsWrite orelse IsWriteH);
				% ignore ->
				% 	parse_statements(T,L,CurUse,CurStatements,IsWrite,GIsWrite);
				IsWriteH ->
					parse_statements(BP,T,L,CurUse,[H|CurStatements],IsWrite orelse IsWriteH, GIsWrite orelse IsWriteH)
			end;
		% Started first block
		Use when CurUse == undefined, CurStatements == [] ->
			parse_statements(BP,T,L,Use,[],IsWrite,GIsWrite);
		% New block and there was a block before this one. Finish up that and start new.
		Use when CurUse /= undefined ->
			parse_statements(BP,T,[{split_actor(CurUse),IsWrite,lists:reverse(CurStatements)}|L],Use,[],false,GIsWrite)
	end;
parse_statements(_BP,[],_L,undefined,CurStatements,_,_) ->
	Lines = [string:to_lower(butil:tolist(N)) || N <- lists:reverse(CurStatements), is_binary(N) orelse is_list(N)],
	case meta_call(Lines,[]) of
		[] ->
			Lines;
		R ->
			R
	end;
parse_statements(_BP,[],L,Use,S,IsWrite,GIsWrite) ->
	{lists:reverse([{split_actor(Use),IsWrite,lists:reverse(S)}|L]),GIsWrite}.

meta_call(["show schema;"|T],Out) ->
	All = [begin
		Tuple = apply(actordb_schema,Type,[]),
		[{butil:tobin(Type),iolist_to_binary(Line)} || Line <- tuple_to_list(Tuple)]
	 end || Type <- actordb_schema:types()],
	meta_call(T,[[{columns,{<<"type">>,<<"sql">>}},{rows,lists:flatten(All)}]|Out]);
meta_call([_|T],O) ->
	meta_call(T,O);
meta_call([],O) ->
	O.

parse_execute(Vals) ->
	NameSize = count_exec_name(Vals,0),
	<<Name:NameSize/binary,Typesrem/binary>> = Vals,
	<<"(",Typesrem1/binary>> = rem_spaces(Typesrem),
	Params = execute_params(rem_spaces(Typesrem1)),
	{Name,Params}.

execute_params(Bin) ->
	case count_exec_param(Bin,0,0) of
		{Count,Skip} ->
			<<Val:Count/binary,_:Skip/binary,Rem/binary>> = Bin,
			[Val|execute_params(rem_spaces(Rem))];
		done ->
			[]
	end.

count_exec_param(<<")",_/binary>>,0,0) ->
	done;
count_exec_param(<<")",_/binary>>,N,NSkip) ->
	{N,NSkip};
count_exec_param(<<" ",Rem/binary>>,N,NSkip) ->
	count_exec_param(Rem,N,NSkip+1);
count_exec_param(<<",",_/binary>>,N,NSkip) ->
	{N,NSkip+1};
count_exec_param(<<_,B/binary>>,N,NS) ->
	count_exec_param(B,N+1,NS).


count_exec_name(<<"(",_/binary>>,N) ->
	N;
count_exec_name(<<" ",_/binary>>,N) ->
	N;
count_exec_name(<<_,Rem/binary>>,N) ->
	count_exec_name(Rem,N+1).


parse_prepare(Sql) ->
	NameSize = count_name(Sql,0),
	<<Name:NameSize/binary,Typesrem/binary>> = Sql,
	<<"(",Typesrem1/binary>> = rem_spaces(Typesrem),
	{Params,RemFor} = prepare_params(Typesrem1,<<>>,[]),
	<<_,_,_," ",Remtype1/binary>> = rem_spaces(RemFor),
	Remtype = rem_spaces(Remtype1),
	Type = get_name(Remtype),
	Typesize = byte_size(Type),
	<<Type:Typesize/binary,RemAs/binary>> = Remtype,
	<<_,_,SqlOut/binary>> = rem_spaces(RemAs),
	{Name,Type,Params,rem_spaces(SqlOut)}.

prepare_params(<<",",Rem/binary>>,Word,L) ->
	prepare_params(Rem,<<>>,[type_to_atom(Word)|L]);
prepare_params(<<" ",Rem/binary>>,Word,L) ->
	prepare_params(Rem,Word,L);
prepare_params(<<")",Rem/binary>>,Word,L) ->
	{lists:reverse([type_to_atom(Word)|L]),Rem};
prepare_params(<<C,Rem/binary>>,Word,L) ->
	prepare_params(Rem,<<Word/binary,C>>,L).

type_to_atom(Type) ->
	case Type of
		<<"INT">> ->
			int;
		<<"int">> ->
			int;
		<<"Int">> ->
			int;
		<<"real">> ->
			real;
		<<"REAL">> ->
			real;
		<<"Real">> ->
			real;
		<<"text">> ->
			text;
		<<"Text">> ->
			text;
		<<"TEST">> ->
			text;
		<<"BLOB">> ->
			blob;
		<<"Blob">> ->
			blob;
		<<"blob">> ->
			blob
	end.

parse_show(Bin) ->
	case rem_spaces(Bin) of
		<<"tables",_/binary>> ->
			tables;
		<<"TABLES",_/binary>> ->
			tables;
		<<"Tables",_/binary>> ->
			tables;
		<<_/binary>> = Str ->
			case string:to_lower(butil:tolist(Str)) of
				"tables"++_ ->
					tables;
				_ ->
					ok
			end;
		_ ->
			undefined
	end.

parse_pragma(Bin) ->
	case rem_spaces(Bin) of
		<<"delete",_/binary>> ->
			delete;
		<<"DELETE",_/binary>> ->
			delete;
		<<"Delete",_/binary>> ->
			delete;
		<<"exists",_/binary>> ->
			exists;
		<<"Exists",_/binary>> ->
			exists;
		<<"EXISTS",_/binary>> ->
			exists;
		<<"list",_/binary>> ->
			list;
		<<"List",_/binary>> ->
			list;
		<<"LIST",_/binary>> ->
			list;
		<<"count",_/binary>> ->
			count;
		<<"COUNT",_/binary>> ->
			count;
		<<"Count",_/binary>> ->
			count;
		<<"copy",R/binary>> ->
			<<"=",Aname/binary>> = rem_spaces(R),
			{copy,get_name(Aname)};
		<<D,E,L,E,T,E,_/binary>> when (D == $d orelse D == $D) andalso 
										(E == $e orelse E == $E) andalso
										(L == $l orelse L == $L) andalso
										(T == $t orelse T == $T) ->
			delete;
		<<E,X,I,S,T,S,_/binary>> when (E == $e orelse E == $E) andalso
									  (X == $x orelse X == $X) andalso
									  (I == $i orelse I == $I) andalso
									  (S == $s orelse S == $S) andalso
									  (T == $t orelse T == $T) ->
			exists;
		<<L,I,S,T,_/binary>> when (L == $l orelse L == $L) andalso
								  (I == $i orelse I == $I) andalso
								  (S == $s orelse S == $S) andalso
								  (T == $t orelse T == $T) ->
			list;
		<<C,O,U,N,T,_/binary>> when (C == $c orelse C == $C) andalso
								  (O == $o orelse O == $O) andalso
								  (U == $u orelse U == $U) andalso
								  (N == $n orelse N == $N) andalso
								  (T == $t orelse T == $T) ->
			count;
		<<C,O,P,Y,R/binary>> when (C == $c orelse C == $C) andalso
								  (O == $o orelse O == $O) andalso
								  (P == $p orelse P == $P) andalso
								  (Y == $y orelse Y == $Y) ->
			<<"=",Aname/binary>> = rem_spaces(R),
			{copy,get_name(Aname)};
		_ ->
			undefined
	end.


split_statements(<<>>) ->
	[];
split_statements(Bin1) ->
	case Bin1 of
		<<"{{",WithGlobal/binary>> ->
			Len = count_param(WithGlobal,0),
			case WithGlobal of
				<<GlobalVar1:Len/binary,"}}",SB/binary>> ->
					case rem_spaces(SB) of
						<<";",HaveNext/binary>> ->
							StatementBin = <<>>,
							GlobalVar = split_param(GlobalVar1,<<>>,[]);
						<<>> ->
							HaveNext = undefined,
							StatementBin = <<>>,
							GlobalVar = split_param(GlobalVar1,<<>>,[]);
						_ ->
							HaveNext = undefined,
							GlobalVar = GlobalVar1,
							StatementBin = SB
					end;
				<<"result}}",StatementBin/binary>> ->
					HaveNext = undefined,
					GlobalVar = <<"RESULT">>
			end;
		StatementBin ->
			HaveNext = undefined,
			GlobalVar = undefined
	end,
	case HaveNext of
		undefined ->
			case find_ending(rem_spaces(StatementBin),0,[],true) of
				BytesToEnd when is_integer(BytesToEnd) ->
					case rem_spaces(StatementBin) of
						% prepared statements must not have spaces between # and ; (like "#wXXXX;")
						<<"#",ReadWrite,A1,A2,A3,A4,";",Next/binary>> when ReadWrite == $w; ReadWrite == $r ->
							Statement = <<"#",ReadWrite,A1,A2,A3,A4,";">>;
						<<"#",ReadWrite,A1,A2,A3,A4,Next1/binary>> when ReadWrite == $w; ReadWrite == $r ->
							Statement = <<"#",ReadWrite,A1,A2,A3,A4,";">>,
							case rem_spaces(Next1) of
								<<";",Next/binary>> ->
									ok;
								<<>> = Next ->
									ok
							end;
						<<Statement:BytesToEnd/binary,Next/binary>> ->
							ok
					end;
				{<<_/binary>> = Statement,Next} ->
					ok;
				{Statement1,Next} ->
					Statement = lists:reverse(Statement1)
			end;
		Next ->
			Statement = StatementBin
	end,
	case GlobalVar of
		undefined ->
			[Statement|split_statements(rem_spaces(Next))];
		_ ->
			[{GlobalVar,Statement}|split_statements(rem_spaces(Next))]
	end.

find_ending(Bin,Offset1,Prev,IsIolist) ->
	case esqlite3:parse_helper(Bin,Offset1) of
		ok ->
			case Prev of
				[] ->
					byte_size(Bin);
				_ when IsIolist ->
					{iolist_to_binary(lists:reverse([Bin|Prev])),<<>>};
				_ ->
					{[Bin|Prev],<<>>}
			end;
		Offset ->
			case Bin of
				<<SkippingBin:Offset/binary,";",Rem/binary>> ->
					case Prev of
						[] ->
							Offset+1;
						_ when IsIolist ->
							{iolist_to_binary(lists:reverse([$;,SkippingBin|Prev])),Rem};
						_ ->
							{[$;,SkippingBin|Prev],Rem}
					end;
				<<SkippingBin:Offset/binary,"{{hash(",Rem/binary>> ->
					case count_hash(Rem,0) of
						undefined ->
							find_ending(Bin,Offset+7,Prev,IsIolist);
						Paramlen ->
							<<Hashid1:Paramlen/binary,"}}",After/binary>> = Rem,
							HSize = byte_size(Hashid1)-1,
							<<Hashid:HSize/binary,")">> = Hashid1,
							find_ending(After,0,[butil:tobin(actordb_util:hash(Hashid)),SkippingBin|Prev],false)
					end;
				<<SkippingBin:Offset/binary,"{{",Rem/binary>> ->
					case count_param(Rem,0) of
						undefined ->
							find_ending(Bin,Offset+2,Prev,IsIolist);
						Paramlen ->
							<<Param:Paramlen/binary,"}}",After/binary>> = Rem,
							io:format("SPIT ~p~n",[Param]),
							case Param of
								<<"curactor">> ->
									find_ending(After,0,[curactor,SkippingBin|Prev],false);
								<<"uniqid">> ->
									find_ending(After,0,[uniqid,SkippingBin|Prev],false);
								<<"uniqueid">> ->
									find_ending(After,0,[uniqid,SkippingBin|Prev],false);
								_ ->
									case split_param(Param,<<>>,[]) of
										[<<"uniqueid">>,Column] ->
											find_ending(After,0,[{uniqid,Column},SkippingBin|Prev],false);
										[<<"uniqid">>,Column] ->
											find_ending(After,0,[{uniqid,Column},SkippingBin|Prev],false);
										[Actor,Column] ->
											find_ending(After,0,[{Actor,Column},SkippingBin|Prev],false);
										{A1,C1,A2,C2} ->
											find_ending(After,0,[{A1,C1,A2,C2},SkippingBin|Prev],false);
										_X ->
											find_ending(Bin,Offset+2,Prev,IsIolist)
									end
							end
					end;
				<<SkippingBin:Offset/binary,"/*",Rem/binary>> ->
					find_ending(remove_comment(Rem),0,[SkippingBin|Prev],IsIolist)
			end
	end.
remove_comment(<<"*/",Rem/binary>>) ->
	Rem;
remove_comment(<<_,Rem/binary>>) ->
	remove_comment(Rem).


% {{X.column=A.column}}
split_param(<<"=",Rem/binary>>,Column,Words) ->
	case Words of
		[Actor] ->
			[NextActor,NextColumn] = split_param(Rem,<<>>,[]),
			{Actor,Column,NextActor,NextColumn};
		_ ->
			invalid
	end;
split_param(<<".",Rem/binary>>,Word,Words) ->
	case Words of
		[] when Word /= <<>> ->
			split_param(Rem,<<>>,[Word|Words]);
		_ ->
			invalid
	end;
split_param(<<" ",Rem/binary>>,W,Wo) ->
	split_param(Rem,W,Wo);
split_param(<<C,Rem/binary>>,W,Wo) ->
	split_param(Rem,<<W/binary,C>>,Wo);
split_param(<<>>,Word,Words) ->
	case Words of
		[Actor] ->
			[Actor,Word];
		_ ->
			invalid
	end.

count_hash(<<"}}",_/binary>>,N) ->
	N;
count_hash(<<_,R/binary>>,N) ->
	count_hash(R,N+1);
count_hash(<<>>,_) ->
	undefined.

count_param(<<"}}",_/binary>>,N) ->
	N;
count_param(<<C,Rem/binary>>,N) when C >= $a, C =< z; 
									 C >= $A, C =< $Z; 
									 C >= $0, C =< $9;
									 C == $.; C == $=; C == $(; 
									 C == $); C == $_; C == $- ->
	count_param(Rem,N+1);
count_param(<<>>,_) ->
	undefined;
count_param(_,_) ->
	undefined.


get_name(Bin) ->
	Count = count_name(rem_spaces(Bin),0),
	<<Name:Count/binary,_/binary>> = Bin,
	Name.
count_name(<<>>,N) ->
	N;
count_name(<<" ",_/binary>>,N) ->
	N;
count_name(<<";",_/binary>>,N) ->
	N;
count_name(<<C,Rem/binary>>,N) when C /= $', C > 32, C /= $`, C /= $", C /= $(, C /= $) ->
	count_name(Rem,N+1).

count_string(<<"''",Rem/binary>>,N) ->
	count_string(Rem,N+2);
count_string(<<"'",_/binary>>,N) ->
	N;
count_string(<<_,Rem/binary>>,N) ->
	count_string(Rem,N+1);
count_string(<<>>,N) ->
	N.

is_actor({Bin,_}) ->
	is_actor(Bin);
% If actor name is from variable and there are no flags
is_actor([H,{Var,Column},<<")",Flags/binary>>|_]) when is_binary(Flags) ->
	case is_actor(H) of
		undefined ->
			undefined;
		Actor ->
			[Actor,{Var,Column},rem_spaces(Flags)]
	end;
is_actor(Bin) ->
	case Bin of 
		<<"actor ",Rem/binary>> ->
			Rem;
		<<"ACTOR ",Rem/binary>> ->
			Rem;
		<<"Actor ",Rem/binary>> ->
			Rem;
		<<A,C,T,O,R," ",Rem/binary>>  when (A == $a orelse A == $A) andalso
										(C == $c orelse C == $C) andalso
										(T == $t orelse T == $T) andalso
										(O == $o orelse O == $O) andalso
										(R == $t orelse R == $T) ->
			Rem;
		_ ->
			undefined
	end.

% actordb_sqlparse:split_actor(<<"type(asdf,234,asdisfpsouf);">>).
% actordb_sqlparse:split_actor(<<"type(for X.column in RES);">>).
split_actor({_,_,_} = A) ->
	A;
split_actor({_,__,_,_} = A) ->
	A;
% Variable actor name
split_actor([Type,{K,V},Flags]) ->
	{<< <<Char:8>> || <<Char:8>> <= Type, Char /= $\s, Char /= $(>>, [{K,V}], check_flags(Flags,[])};
split_actor(Bin) ->
	case split_actor(Bin,<<>>,undefined,[]) of
		{Type,[<<"*">>],Flags} ->
			{Type,$*,Flags};
		Res ->
			Res
	end.
split_actor(<<" ",Bin/binary>>,Word,Type,L) ->
	split_actor(Bin,Word,Type,L);
split_actor(<<"(",Bin/binary>>,Word,_,L) ->
	split_actor(Bin,<<>>,Word,L);
split_actor(<<"'",Bin/binary>>,Word,T,L) ->
	split_actor(Bin,Word,T,L);
split_actor(<<"`",Bin/binary>>,Word,undefined,L) ->
	split_actor(Bin,Word,undefined,L);
split_actor(<<",",Bin/binary>>,Word,Type,L) ->
	split_actor(Bin,<<>>,Type,[is_not_empty(Word)|L]);
split_actor(<<")",FlagsBin/binary>>,Word,Type,L) ->
	{Type,[is_not_empty(Word)|L],check_flags(FlagsBin,[])};
split_actor(<<"for ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global,Flags} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var,Flags};
split_actor(<<"FOR ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global,Flags} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var,Flags};
split_actor(<<"For ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global,Flags} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var,Flags};
split_actor(<<"foreach ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global,Flags} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var,Flags};
split_actor(<<"FOREACH ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global,Flags} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var,Flags};
split_actor(<<";",_/binary>>,Word,Type,L) ->
	% {Type,[Word|L],[]};
	split_actor(<<>>,Word,Type,L);
split_actor(<<C,Bin/binary>>,Word,Type,L) ->
	split_actor(Bin,<<Word/binary,C>>,Type,L);
split_actor(<<>>,Word,Type,L) ->
	case catch count_name(Word,0) of
		0 ->
			throw({error,empty_actor_name});
		{'EXIT',_} ->
			throw({error,invalid_actor_name});
		_ ->
			{Type,[Word|L],[]}
	end.

is_not_empty(<<>>) ->
	throw({error,empty_actor_name});
is_not_empty(W) ->
	W.

check_flags(<<" ",Rem/binary>>,L) ->
	check_flags(Rem,L);
check_flags(<<",",Rem/binary>>,L) ->
	check_flags(Rem,L);
check_flags(<<"create",Rem/binary>>,L) ->
	check_flags(Rem,[create|L]);
check_flags(<<"CREATE",Rem/binary>>,L) ->
	check_flags(Rem,[create|L]);
check_flags(<<";">>,L) ->
	L;
check_flags(<<>>,L) ->
	L;
check_flags(<<_,Rem/binary>>,L) ->
	check_flags(Rem,L).

split_foru(<<" in ",Bin/binary>>,Word,Var,undefined) ->
	split_foru(Bin,<<>>,Var,Word);
split_foru(<<" IN ",Bin/binary>>,Word,Var,undefined) ->
	split_foru(Bin,<<>>,Var,Word);
split_foru(<<" ",Bin/binary>>,Word,Var,Col) ->
	case ok of
		% Is this end of column name
		_ when Var /= undefined, byte_size(Word) > 0, Col == undefined ->
			split_foru(Bin,<<>>,Var,Word);
		% This is end of global var name
		_ when Var /= undefined, byte_size(Word) > 0, Col /= undefined ->
			{Var,Col,Word,check_flags(Bin,[])};
		_ ->
			split_foru(Bin,Word,Var,Col)
	end;
split_foru(<<".",Bin/binary>>,Word,undefined,undefined) ->
	split_foru(Bin,<<>>,Word,undefined);
split_foru(<<")",FlagsBin/binary>>,Word,Var,Col) ->
	{Var,Col,Word,check_flags(FlagsBin,[])};
split_foru(<<";",_/binary>>,Word,Var,Col) ->
	{Var,Col,Word,[]};
split_foru(<<C,Bin/binary>>,Word,Var,Col) ->
	split_foru(Bin,<<Word/binary,C>>,Var,Col).


rem_spaces(<<" ",X/binary>>) ->
	rem_spaces(X);
rem_spaces(<<"\n",X/binary>>) ->
	rem_spaces(X);
rem_spaces(<<"\r",X/binary>>) ->
	rem_spaces(X);
rem_spaces(X) ->
	X.

% For with statements, we need to move past the entire with
% to find out if we are dealing with a read (select) or write (delete, update, insert).
% First find "as", then find the last ) after the first one.
find_as(<<C1,"AS",C2,Rem/binary>>) when (C1 == $\s orelse C1 == $\n) andalso (C2 == $\s orelse C2 == $\n) ->
	move_to_para(Rem);
find_as(<<C1,"as",C2,Rem/binary>>)  when (C1 == $\s orelse C1 == $\n) andalso (C2 == $\s orelse C2 == $\n) ->
	move_to_para(Rem);
find_as(<<C1,"As",C2,Rem/binary>>)  when (C1 == $\s orelse C1 == $\n) andalso (C2 == $\s orelse C2 == $\n) ->
	move_to_para(Rem);
find_as(<<C1,"aS",C2,Rem/binary>>)  when (C1 == $\s orelse C1 == $\n) andalso (C2 == $\s orelse C2 == $\n) ->
	move_to_para(Rem);
find_as(<<_,Rem/binary>>) ->
	find_as(Rem).

move_to_para(<<"(",Rem/binary>>) ->
	move_to_endpara(Rem,1,false);
move_to_para(<<_,Rem/binary>>) ->
	move_to_para(Rem).

move_to_endpara(Rem,0,false) ->
	find_comma_or_char(Rem);
move_to_endpara(<<"(",Rem/binary>>,N,false) ->
	move_to_endpara(Rem,N+1,false);
move_to_endpara(<<")",Rem/binary>>,N,false) ->
	move_to_endpara(Rem,N-1,false);
move_to_endpara(<<"'",Rem/binary>>,N,IsString) ->
	move_to_endpara(Rem,N,not IsString);
move_to_endpara(<<"`",Rem/binary>>,N,IsString) ->
	move_to_endpara(Rem,N,not IsString);
move_to_endpara(<<_,Rem/binary>>,N,IsString) ->
	move_to_endpara(Rem,N,IsString).

find_comma_or_char(<<",",Rem/binary>>) ->
	find_as(Rem);
find_comma_or_char(<<C,Rem/binary>>) when (C >= $a andalso C =< $z) orelse
										  (C >= $A andalso C =< $Z) ->
	is_write(<<C,Rem/binary>>);
find_comma_or_char(<<_,Rem/binary>>) ->
	find_comma_or_char(Rem).

