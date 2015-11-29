% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

%% @author  Biokoda d.o.o.
%% @doc Utility functions for myactor_proto implementation.

-module(myactor_util).

-include_lib("myactor.hrl").

-compile(export_all).

%% @spec rem_spaces(binary()) -> binary()
%% @doc  Remove leading spaces from binary.
rem_spaces(<<" ",X/binary>>) ->
	rem_spaces(X);
rem_spaces(X) ->
	X.

% decoding binary protocol
exec_vals(<<?T_NULL,0,Types/binary>>,Vals) ->
	[undefined|exec_vals(Types,Vals)];
exec_vals(<<?T_VAR_STRING,0,Types/binary>>,Vals) ->
	{Val,Rem} = myactor_util:read_lenenc_string(Vals),
	[Val|exec_vals(Types,Rem)];
exec_vals(<<?T_STRING,0,Types/binary>>,Vals) ->
	{Val,Rem} = myactor_util:read_lenenc_string(Vals),
	[Val|exec_vals(Types,Rem)];
exec_vals(<<?T_TINY,16#80,Types/binary>>,<<V,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_TINY,0,Types/binary>>,<<V:8/signed,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_SHORT,16#80,Types/binary>>,<<V:16/unsigned-little,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_SHORT,0,Types/binary>>,<<V:16/signed-little,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_LONG,16#80,Types/binary>>,<<V:32/unsigned-little,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_LONG,0,Types/binary>>,<<V:32/little,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_LONGLONG,16#80,Types/binary>>,<<V:64/unsigned-little,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_LONGLONG,0,Types/binary>>,<<V:64/signed-little,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_FLOAT,0,Types/binary>>,<<V:32/float-signed-little,Vals/binary>>) ->
	Factor = math:pow(10, floor(6 - math:log10(abs(V)))),
	[round(V * Factor) / Factor|exec_vals(Types,Vals)];
exec_vals(<<?T_DOUBLE,0,Types/binary>>,<<V:64/float-signed-little,Vals/binary>>) ->
	[V|exec_vals(Types,Vals)];
exec_vals(<<?T_DATE,0,Types/binary>>,<<4, Y:16/little, M, D,Vals/binary>>) ->
	[butil:date_to_bstring({Y,M,D},<<"-">>)|exec_vals(Types,Vals)];
exec_vals(<<?T_DATETIME,0,Types/binary>>,<<4, Y:16/little, M, D,Vals/binary>>) ->
	[butil:date_to_bstring({Y,M,D},<<"-">>)|exec_vals(Types,Vals)];
exec_vals(<<?T_DATETIME,0,Types/binary>>,<<7, Y:16/little, M, D, H, Mi, S,Vals/binary>>) ->
	[<<(butil:date_to_bstring({Y,M,D},<<"-">>))/binary," ",(butil:time_to_bstring({H,Mi,S},<<":">>))/binary>>|exec_vals(Types,Vals)];
exec_vals(<<?T_DATETIME,0,Types/binary>>,<<11, Y:16/little, M, D, H, Mi, S,Micro:32/little,Vals/binary>>) ->
	[<<(butil:date_to_bstring({Y,M,D},<<"-">>))/binary," ",(butil:time_to_bstring({H,Mi,S+0.000001 * Micro},<<":">>))/binary>>|exec_vals(Types,Vals)];
exec_vals(<<?T_TIME,0,Types/binary>>,<<8, 1, 0:32/little, H1, M1, S1,Vals/binary>>) ->
	[(butil:time_to_bstring({H1,M1,S1},<<":">>))|exec_vals(Types,Vals)];
exec_vals(<<?T_TIME,0,Types/binary>>,<<8, 1, D:32/little, H1, M1, S1,Vals/binary>>) ->
	[butil:tobin(D),"d ",(butil:time_to_bstring({H1,M1,S1},<<":">>))|exec_vals(Types,Vals)];
exec_vals(<<?T_TIME,0,Types/binary>>,<<12, 0, D:32/little, H, M, S1, Micro:32/little,Vals/binary>>) ->
	[butil:tobin(D),"d ",(butil:time_to_bstring({H,M,S1+0.000001 * Micro},<<":">>))|exec_vals(Types,Vals)];
exec_vals(<<>>,<<>>) ->
	[].


enc_vals([H|T]) ->
	[enc_val(H)|enc_vals(T)];
enc_vals([]) ->
	[].

enc_val(undefined) ->
	[];
enc_val(null) ->
	[];
enc_val(<<_/binary>> = V) ->
	binary_to_varchar(V); %<<?T_VAR_STRING,0>>,
% enc_val(H) when is_integer(H), H =< 16#ff, H >= 0 ->
% 	% [<<?T_TINY,16#80,H:8>>];
% 	<<H>>;
% enc_val(H) when is_integer(H), H >= -16#80, H < 0 ->
% 	% [<<?T_TINY,0,H:8>>];
% 	<<H>>;
% enc_val(H) when is_integer(H), H =< 16#ffff, H >= 0 ->
% 	% [<<?T_SHORT,16#80,H:16/little>>];
% 	<<H:16/little>>;
% enc_val(H) when is_integer(H), H >= -16#8000, H < 0 ->
% 	% [<<?T_SHORT,0,H:16/little>>];
% 	<<H:16/little>>;
% enc_val(H) when is_integer(H), H =< 16#ffffffff, H >= 0 ->
% 	% [<<?T_LONG,16#80,H:32/little>>];
% 	<<H:32/little>>;
% enc_val(H) when is_integer(H), H >= -16#80000000, H < 0 ->
% 	% [<<?T_LONG,0,H:32/little>>];
% 	<<H:32/little>>;
% enc_val(H) when is_integer(H), H =< 16#ffffffffffffffff, H >= 0 ->
% 	% [<<?T_LONGLONG,16#80,H:64/little>>];
% 	<<H:64/little>>;
% enc_val(H) when is_integer(H), H >= -16#8000000000000000, H < 0 ->
% 	% [<<?T_LONGLONG,0,H:64/little>>];
% 	<<H:64/little>>;
enc_val(H) when is_integer(H) ->
	% enc_val(butil:tobin(H));
	<<H:64/little>>;
enc_val(H) when is_float(H) ->
	% [<<?T_DOUBLE,0,H:64/float-little>>];
	<<H:64/float-little>>;
enc_val({blob,V}) when byte_size(V) < 256 ->
	% [<<?T_TINY_BLOB,0>>,binary_to_varchar(V)];
	binary_to_varchar(V);
enc_val({blob,V}) when byte_size(V) < 65536 ->
	binary_to_varchar(V);
enc_val({blob,V}) when byte_size(V) < 16777216 ->
	binary_to_varchar(V);
enc_val({blob,V}) when byte_size(V) < 4294967296 ->
	binary_to_varchar(V).

floor(Value) ->
	case trunc(Value) of
		Trunc when Trunc =< Value -> 
			Trunc;
		Trunc when Trunc > Value ->
			Trunc - 1
	end.

null_bitmap(Vals) when is_tuple(Vals) ->
	null_bitmap(tuple_to_list(Vals));
null_bitmap(Vals) ->
	NullBits = << <<(bit(V)):1>> || V <- Vals >>,
	OrigLen = bit_size(NullBits) + 2,
	PadLen = ((OrigLen + 7) band (bnot 7)) - OrigLen,
	Full = <<0:2, NullBits/bitstring, 0:PadLen>>,
	<< <<H:1, G:1, F:1, E:1, D:1, C:1, B:1, A:1>> || <<A:1, B:1, C:1, D:1, E:1, F:1, G:1, H:1>> <= Full >>.

bit(undefined) ->
	1;
bit(null) ->
	1;
bit(_) ->
	0.

%% @spec is_actor(binary()) -> true | false
%% @doc  Detect if string/query starts with "actor" statement.
is_actor({Bin,_}) ->
	is_actor(Bin);
is_actor(Bin) ->
	case Bin of 
		<<"actor ",_Rem/binary>> ->
			true;
		<<"ACTOR ",_Rem/binary>> ->
			true;
		<<"Actor ",_Rem/binary>> ->
			true;
		<<A,C,T,O,R," ",_Rem/binary>>  when (A == $a orelse A == $A) andalso
				(C == $c orelse C == $C) andalso
				(T == $t orelse T == $T) andalso
				(O == $o orelse O == $O) andalso
				(R == $t orelse R == $T) ->
			true;
		_ ->
			false
	end.

%% @spec genhash() -> binary()
%% @doc  Generate a random 20 byte long hash for password encryption.
genhash() ->    % 20 bytes long id
	<< <<(nozero(B)):8>> || <<B:8>> <= crypto:rand_bytes(20) >>.
nozero(0) ->
	1;
nozero(N) ->
	N.
%% @spec binary_to_varchar(binary()) -> binary()
%% @doc  Creates length-encoded binary that can be used for string representation within packets.
binary_to_varchar(undefined) ->
		<<16#fb>>;
binary_to_varchar(null) ->
		<<16#fb>>;
binary_to_varchar(Binary) ->
	[mysql_var_integer(byte_size(Binary)), Binary].

%% @spec mysql_var_integer(integer()) -> binary()
%% @doc  Encodes integer that it can be used for integer representation within packets.
mysql_var_integer(Int) when Int < 251 ->
	<<Int:8>>;
mysql_var_integer(Int) when Int < 16#10000 ->
	<<16#fc, Int:16/little>>;
mysql_var_integer(Int) when Int < 16#1000000 ->
	<<16#fd, Int:24/little>>;
mysql_var_integer(Int) ->
	<<16#fe, Int:64/little>>.

%% @spec read_lenenc_string(binary()) -> {binary(),binary()}
%% @doc  Read a length-encoded string from packet representation. Returns the rest of te binary.
read_lenenc_string(<<16#fc, Len:16/little, Bin:Len/binary, Rest/binary>>) -> {Bin, Rest};
read_lenenc_string(<<16#fd, Len:24/little, Bin:Len/binary, Rest/binary>>) -> {Bin, Rest};
read_lenenc_string(<<16#fe, Len:64/little, Bin:Len/binary, Rest/binary>>) -> {Bin, Rest};
read_lenenc_string(<<Len:8/little, Bin:Len/binary, Rest/binary>>) -> {Bin, Rest}.


%% @spec build_ids(list()) -> binary()
%% @doc  Converts a list of ActorIds (retrieved from myactor_sqlparse:parse_statements structure) to binary representation splitted with commas
%%       Structure sample: {[{{_,<b>ActorIds</b>},false,[]}],false}
build_idsbin($*) ->
	butil:tobin(<<"*">>);
build_idsbin([AId]) ->
	butil:tobin(AId);
build_idsbin(Ids) when is_list(Ids) ->
	build_idsbin(Ids,<<>>).
build_idsbin([],Bin) ->
	Bin;
build_idsbin([H|T],Bin) ->
	build_idsbin(T,<<Bin/binary,$,,H/binary>>).

%% @spec gen_doc() -> ok
%% @doc  Generates this documentation.
gen_doc() ->
	edoc:application(myactor,butil:project_rootpath()++"/apps/myactor/src/",[{dir,butil:project_rootpath()++"/doc/myactor/"},{private,true},{todo,true}]).