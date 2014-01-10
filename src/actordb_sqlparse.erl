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
is_write(Bin) ->
	case Bin of
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
		% <<"SET ",_Rem/binary>> ->
		% 	ignore;
		% <<"set ",_Rem/binary>> ->
		% 	ignore;
		% <<"Set ",_Rem/binary>> ->
		% 	ignore;
		% If you write sql like a moron then you get to these slow parts.
		<<C,R,E,A,T,EE," ",_/binary>> when (C == $c orelse C == $C) andalso 
											(R == $r orelse R == $R) andalso
											(E == $e orelse E == $E) andalso
											(A == $a orelse A == $A) andalso
											(T == $t orelse T == $T) andalso
											(EE == $e orelse EE == $E) ->
			true;
		<<I,N,S,E,R,T," ",_/binary>> when (I == $i orelse I == $I) andalso 
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
		<<D,E,L,E1,T,E2," ",_/binary>> when (D == $d orelse D == $D) andalso 
											(E == $e orelse E == $E) andalso
											(L == $l orelse L == $L) andalso
											(E1 == $e orelse E1 == $E) andalso
											(T == $t orelse T == $T) andalso
											(E2 == $e orelse E2 == $E) ->
			true;
		<<R,E,P,L,A,C,E1," ",_/binary>> when (R == $r orelse R == $R) andalso 
											(E == $e orelse E == $E) andalso
											(P == $p orelse P == $P) andalso
											(L == $l orelse L == $L) andalso
											(A == $a orelse A == $A) andalso
											(C == $c orelse C == $C) andalso
											(E1 == $e orelse E1 == $E) ->
			true;
		<<P,R,A,G,M,A1," ",Rem/binary>> when (P == $p orelse P == $P) andalso 
											(R == $r orelse R == $R) andalso
											(A == $a orelse A == $A) andalso
											(G == $g orelse G == $G) andalso
											(M == $m orelse M == $M) andalso
											(A1 == $a orelse A1 == $A) ->
			{pragma,Rem};
		<<S,H,O,W," ",Rem/binary>> when (S == $s orelse S == $S) andalso
										(H == $h orelse H == $H) andalso
										(O == $o orelse O == $O) andalso
										(W == $w orelse W == $W) ->
			{show,Rem};
		% <<S,E,T," ",_Rem/binary>> when (S == $s orelse S == $S) andalso
		% 															(E == $e orelse E == $E) andalso
		% 															(T == $t orelse T == $T) ->
		% 	ignore;
		_ ->
			false
	end.

% Returns: {[{Actors,IsWrite,Statements},..],IsWrite}
% Actors: {Type,[Actor1,Actor2,Actor3,...]} |
% 				{Type,$*} |
% 				{Type,GlobalVarName,Column,BlockVarName}
% IsWrite - boolean if any of the statements in the block change the db
% Statements: [Statement]
% Statement: {ResultGlobalVarName,SqlStatement]} |
% 					 SqlStatement 
% SqlStatement = list of: binary | char | {VariableName,VariableColumn} | uniqid
% Actors:
% 1. Fixed list of actors
% 2. all actors of some type 
%	3. actors as a result of a query preceing current block
parse_statements(Bin) ->
	L = split_statements(rem_spaces(Bin)),
	parse_statements(L,[],undefined,[],false,false).
parse_statements([<<>>|T],L,CurUse,CurStatements,IsWrite,GIsWrite) ->
	parse_statements(T,L,CurUse,CurStatements,IsWrite,GIsWrite);
parse_statements([H|T],L,CurUse,CurStatements,IsWrite,GIsWrite) ->
	case is_use(H) of
		undefined ->
			case is_write(H) of
				{pragma,PragmaRem} ->
					case parse_pragma(PragmaRem) of
						delete ->
							parse_statements(T,L,CurUse,[delete],true, true)
					end;
				{show,ShowRem} ->
					case parse_show(ShowRem) of
						tables when CurUse /= undefined ->
							ST = <<"select name from sqlite_master where type='table';">>,
							parse_statements(T,L,CurUse,[ST|CurStatements],true, true);
						_ ->
							parse_statements(T,L,CurUse,[H|CurStatements],IsWrite,GIsWrite)
					end;
				% ignore ->
				% 	parse_statements(T,L,CurUse,CurStatements,IsWrite,GIsWrite);
				IsWriteH ->
					parse_statements(T,L,CurUse,[H|CurStatements],IsWrite orelse IsWriteH, GIsWrite orelse IsWriteH)
			end;
		% Started first block
		Use when CurUse == undefined, CurStatements == [] ->
			parse_statements(T,L,Use,[],IsWrite,GIsWrite);
		% New block and there was a block before this one. Finish up that and start new.
		Use when CurUse /= undefined ->
			parse_statements(T,[{split_use(CurUse),IsWrite,lists:reverse(CurStatements)}|L],Use,[],false,GIsWrite)
	end;
parse_statements([],_L,undefined,CurStatements,_,_) ->
	Lines = [string:to_lower(butil:tolist(N)) || N <- lists:reverse(CurStatements)],
	case meta_call(Lines,[]) of
		[] ->
			Lines;
		R ->
			R
	end;
parse_statements([],L,Use,S,IsWrite,GIsWrite) ->
	{lists:reverse([{split_use(Use),IsWrite,lists:reverse(S)}|L]),GIsWrite}.

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

parse_show(Bin) ->
	case rem_spaces(Bin) of
		<<"tables",_/binary>> ->
			tables;
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
		<<D,E,L,E1,T,E2,_/binary>> when (D == $d orelse D == $D) andalso 
										(E == $e orelse E == $E) andalso
										(L == $l orelse L == $L) andalso
										(E1 == $e orelse E1 == $E) andalso
										(T == $t orelse T == $T) andalso
										(E2 == $e orelse E2 == $E) ->
			delete;
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
				<<GlobalVar1:Len/binary,"}}",SB/binary>> when SB == <<>>; SB == <<";">> ->
					StatementBin = <<>>,
					GlobalVar = split_param(GlobalVar1,<<>>,[]);
				<<"result}}",StatementBin/binary>> ->
					GlobalVar = <<"RESULT">>;
				<<GlobalVar:Len/binary,"}}",StatementBin/binary>> ->
					ok
			end;
		StatementBin ->
			GlobalVar = undefined
	end,
	case find_ending(rem_spaces(StatementBin),0,[],true) of
		BytesToEnd when is_integer(BytesToEnd) ->
			<<Statement:BytesToEnd/binary,Next/binary>> = StatementBin;
		{<<_/binary>> = Statement,Next} ->
			ok;
		{Statement1,Next} ->
			Statement = lists:reverse(Statement1)
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
					{iolist_to_binary([Bin|Prev]),<<>>};
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
				<<SkippingBin:Offset/binary,"{{",Rem/binary>> ->
					case count_param(Rem,0) of
						undefined ->
							find_ending(Bin,Offset+2,Prev,IsIolist);
						Paramlen ->
							<<Param:Paramlen/binary,"}}",After/binary>> = Rem,
							case Param of
								<<"hash(",Hashid1/binary>> ->
									HSize = byte_size(Hashid1)-1,
									<<Hashid:HSize/binary,")">> = Hashid1,
									find_ending(After,0,[butil:tobin(actordb_util:hash(Hashid)),SkippingBin|Prev],false);
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

% find_ending(Bin,Offset,Prev,Instring) ->
% 	case Bin of
% 		<<_SkippingBin:Offset/binary,"'",_Rem/binary>> ->
% 			find_ending(Bin,Offset+1,Prev,not Instring);
% 		<<SkippingBin:Offset/binary,"{{",Rem/binary>> ->
% 				case count_param(Rem,0) of
% 					undefined ->
% 						find_ending(Bin,Offset+2,Prev,Instring);
% 					Paramlen ->
% 						<<Param:Paramlen/binary,"}}",After/binary>> = Rem,
% 						case Param of
% 							<<"hash(",Hashid1/binary>> ->
% 								HSize = byte_size(Hashid1)-1,
% 								<<Hashid:HSize/binary,")">> = Hashid1,
% 								find_ending(After,0,[butil:tobin(actordb_util:hash(Hashid)),SkippingBin|Prev],Instring);
% 							<<"uniqid">> ->
% 								find_ending(After,0,[uniqid,SkippingBin|Prev],Instring);
% 							<<"uniqueid">> ->
% 								find_ending(After,0,[uniqid,SkippingBin|Prev],Instring);
% 							_ ->
% 								case split_param(Param,<<>>,[]) of
% 									[<<"uniqueid">>,Column] ->
% 										find_ending(After,0,[{uniqid,Column},SkippingBin|Prev],Instring);
% 									[<<"uniqid">>,Column] ->
% 										find_ending(After,0,[{uniqid,Column},SkippingBin|Prev],Instring);
% 									[Actor,Column] ->
% 										find_ending(After,0,[{Actor,Column},SkippingBin|Prev],Instring);
% 									{A1,C1,A2,C2} ->
% 										find_ending(After,0,[{A1,C1,A2,C2},SkippingBin|Prev],Instring);
% 									_X ->
% 										find_ending(Bin,Offset+2,Prev,Instring)
% 								end
% 						end
% 				end;
% 		<<SkippingBin:Offset/binary,";",Rem/binary>> when Instring == false ->
% 			case Prev of
% 				[] ->
% 					Offset+1;
% 				_ ->
% 					{[$;,SkippingBin|Prev],Rem}
% 			end;
% 		<<_Skip:Offset/binary,_,_/binary>> ->
% 			find_ending(Bin,Offset+1,Prev,Instring);
% 		_ ->
% 			case Prev of
% 				[] ->
% 					Offset;
% 				_ ->
% 					{[Bin|Prev],<<>>}
% 			end
% 	end.

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

count_param(<<"}}",_/binary>>,N) ->
	N;
count_param(<<C,Rem/binary>>,N) when C >= $a, C =< z; 
									 C >= $A, C =< $Z; 
									 C >= $0, C =< $9;
									 C == $.; C == $=; C == $(; C == $) ->
	count_param(Rem,N+1);
count_param(<<>>,_) ->
	undefined;
count_param(_,_) ->
	undefined.

count_string(<<"''",Rem/binary>>,N) ->
	count_string(Rem,N+2);
count_string(<<"'",_/binary>>,N) ->
	N;
count_string(<<_,Rem/binary>>,N) ->
	count_string(Rem,N+1);
count_string(<<>>,N) ->
	N.

is_use({Bin,_}) ->
	is_use(Bin);
is_use(Bin) ->
	case Bin of 
		<<"use ",Rem/binary>> ->
			Rem;
		<<"USE ",Rem/binary>> ->
			Rem;
		<<"Use ",Rem/binary>> ->
			Rem;
		<<"uSe ",Rem/binary>> ->
			Rem;
		<<"usE ",Rem/binary>> ->
			Rem;
		_ ->
			undefined
	end.

% actordb_sqlparse:split_use(<<"type(asdf,234,asdisfpsouf);">>).
% actordb_sqlparse:split_use(<<"type(for X.column in RES);">>).
split_use(Bin) ->
	case split_use(Bin,<<>>,undefined,[]) of
		{Type,[<<"*">>]} ->
			{Type,$*};
		Res ->
			Res
	end.
split_use(<<" ",Bin/binary>>,Word,Type,L) ->
	split_use(Bin,Word,Type,L);
split_use(<<"(",Bin/binary>>,Word,_,L) ->
	split_use(Bin,<<>>,Word,L);
split_use(<<"'",Bin/binary>>,Word,T,L) ->
	split_use(Bin,Word,T,L);
split_use(<<"`",Bin/binary>>,Word,undefined,L) ->
	split_use(Bin,Word,undefined,L);
split_use(<<",",Bin/binary>>,Word,Type,L) ->
	split_use(Bin,<<>>,Type,[Word|L]);
split_use(<<")",_/binary>>,Word,Type,L) ->
	{Type,[Word|L]};
split_use(<<"for ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var};
split_use(<<"FOR ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var};
split_use(<<"foreach ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var};
split_use(<<"FOREACH ",Bin/binary>>,<<>>,Type,[]) ->
	{Var,Col,Global} = split_foru(Bin,<<>>,undefined,undefined),
	{Type,Global,Col,Var};
split_use(<<";",_/binary>>,Word,Type,L) ->
	{Type,[Word|L]};
split_use(<<C,Bin/binary>>,Word,Type,L) ->
	split_use(Bin,<<Word/binary,C>>,Type,L);
split_use(<<>>,Word,Type,L) ->
	{Type,[Word|L]}.

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
			{Var,Col,Word};
		_ ->
			split_foru(Bin,Word,Var,Col)
	end;
split_foru(<<".",Bin/binary>>,Word,undefined,undefined) ->
	split_foru(Bin,<<>>,Word,undefined);
split_foru(<<")",_/binary>>,Word,Var,Col) ->
	{Var,Col,Word};
split_foru(<<";",_/binary>>,Word,Var,Col) ->
	{Var,Col,Word};
split_foru(<<C,Bin/binary>>,Word,Var,Col) ->
	split_foru(Bin,<<Word/binary,C>>,Var,Col).


rem_spaces(<<" ",X/binary>>) ->
	rem_spaces(X);
rem_spaces(X) ->
	X.


