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
	L = split_statements(rem_spaces(Bin)),
	parse_statements(L,[],undefined,[],false,false).
parse_statements([<<>>|T],L,CurUse,CurStatements,IsWrite,GIsWrite) ->
	parse_statements(T,L,CurUse,CurStatements,IsWrite,GIsWrite);
parse_statements([H|T],L,CurUse,CurStatements,IsWrite,GIsWrite) ->
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
									parse_statements(T,L,CurUse,[delete],true, true);
								exists ->
									case split_actor(CurUse) of
										{Type,Actors,Flags} ->
											NewUse = {Type,Actors,butil:lists_add(exists,Flags)};
										{Type,Global,Col,Var,Flags} ->
											NewUse = {Type,Global,Col,Var,butil:lists_add(exists,Flags)}
									end,
									parse_statements(T,L,NewUse,[exists],IsWrite, GIsWrite);
								{copy,Name} ->
									{Type,[Actor],Flags} = split_actor(CurUse),
									{_,_,Node} = actordb_shardmngr:find_global_shard(Name),
									parse_statements(T,L,{Type,[Actor],[{copyfrom,{Node,Name}}|Flags]},[{copy,Name}],false, false);
								Pragma when Pragma == list; Pragma == count ->
									case split_actor(CurUse) of
										{Type,_Actors,_Flags} ->
											ok;
										{Type,_Global,_Col,_Var,_Flags} ->
											ok
									end,
									parse_statements(T,L,{Type,$*,[]},[Pragma],false,false)
							end
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
			parse_statements(T,[{split_actor(CurUse),IsWrite,lists:reverse(CurStatements)}|L],Use,[],false,GIsWrite)
	end;
parse_statements([],_L,undefined,CurStatements,_,_) ->
	Lines = [string:to_lower(butil:tolist(N)) || N <- lists:reverse(CurStatements), is_binary(N) orelse is_list(N)],
	case meta_call(Lines,[]) of
		[] ->
			Lines;
		R ->
			R
	end;
parse_statements([],L,Use,S,IsWrite,GIsWrite) ->
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
			<<Statement:BytesToEnd/binary,Next/binary>> = rem_spaces(StatementBin);
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


get_name(Bin) ->
	Count = count_name(rem_spaces(Bin),0),
	<<Name:Count/binary,_/binary>> = Bin,
	Name.
count_name(<<C,Rem/binary>>,N) when C >= $a, C =< z; 
									 C >= $A, C =< $Z; 
									 C >= $0, C =< $9;
									 C == $.; C == $_ ->
	count_name(Rem,N+1);
count_name(<<>>,N) ->
	N;
count_name(<<" ">>,N) ->
	N;
count_name(<<";",_/binary>>,N) ->
	N.

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
	split_actor(Bin,<<>>,Type,[Word|L]);
split_actor(<<")",FlagsBin/binary>>,Word,Type,L) ->
	{Type,[Word|L],check_flags(FlagsBin,[])};
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
	{Type,[Word|L],[]};
split_actor(<<C,Bin/binary>>,Word,Type,L) ->
	split_actor(Bin,<<Word/binary,C>>,Type,L);
split_actor(<<>>,Word,Type,L) ->
	{Type,[Word|L],[]}.

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
rem_spaces(X) ->
	X.


