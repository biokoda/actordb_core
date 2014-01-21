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

%% @spec is_use(binary()) -> true | false
%% @doc  Detect if string/query starts with "use" statement.
is_use({Bin,_}) ->
    is_use(Bin);
is_use(Bin) ->
    case Bin of 
        <<"use ",_Rem/binary>> ->
            true;
        <<"USE ",_Rem/binary>> ->
            true;
        <<"Use ",_Rem/binary>> ->
            true;
        <<"uSe ",_Rem/binary>> ->
            true;
        <<"usE ",_Rem/binary>> ->
            true;
        <<"USe ",_Rem/binary>> ->
            true;
        <<"uSE ",_Rem/binary>> ->
            true;
        _ ->
            false
    end.

%% @spec is_actor(binary()) -> true | false
%% @doc  Detect if string/query starts with "actor" statement.
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

%% @spec genhash() -> binary()
%% @doc  Generate a random 20 byte long hash for password encryption.
genhash() ->    % 20 bytes long id
   Bin = << <<B:8>> || <<B:8>> <= term_to_binary(make_ref()), B /= 0 >>,
   case size(Bin) of
    L when L < 20 ->       
        <<Bin/binary,11111111111111111111:(20 - L)/unit:8>>;
    _ ->
        <<Bin0:160/bitstring, _R/binary>> = Bin,
        Bin0
   end.

%% @spec binary_to_varchar(binary()) -> binary()
%% @doc  Creates length-encoded binary that can be used for string representation within packets.
binary_to_varchar(undefined) ->
        <<16#fb>>;
binary_to_varchar(null) ->
        <<16#fb>>;
binary_to_varchar(Binary) ->
    Len = mysql_var_integer(byte_size(Binary)),
    <<Len/binary, Binary/binary>>.

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