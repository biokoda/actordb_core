% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_cmd).
-export([compare_schema/2]).
% -include_lib("actordb_core/include/actordb.hrl").
-define(ERR(F,P),lists:flatten(io_lib:fwrite(F,P))).

compare_schema(Types,New) ->
	N1 = [Data || Data <- New, (tuple_size(Data) == 2 orelse element(1,Data) == iskv)],
	compare_schema1(Types,N1).
% Move over existing tyes of actors.
% For every type check if it exists in new schema and if any sql statements added.
compare_schema1([Type|T],New) when Type == ids; Type == types; Type == iskv; Type == num ->
	compare_schema1(T,New);
compare_schema1([Type|T],New) ->
	case lists:keyfind(Type,1,New) of
		false ->
			{error,?ERR("Missing type ~p in new config. Config invalid.~n",[Type])};
		{_,_,_} ->
			compare_schema1(T,New);
		{_,SqlNew} ->
			case apply(actordb_schema,Type,[]) of
				SqlNew ->
					compare_schema1(T,lists:keydelete(Type,1,New));
				SqlCur ->
					SizeCur = tuple_size(SqlCur),
					check_sql(Type,SizeCur,SqlNew,actordb_schema:iskv(Type)),
					compare_schema1(T,lists:keydelete(Type,1,New))
			end
	end;
compare_schema1([],New) ->
	NewTypes = lists:keydelete(ids,1,lists:keydelete(types,1,lists:keydelete(iskv,1,lists:keydelete(num,1,New)))),
	case NewTypes of
		[] ->
			ok;
		_ ->
			{iskv,multihead,MultiheadList1} = lists:keyfind(iskv,1,New),
			MultiheadList = [Name || {Name,true} <- lists:keydelete(any,1,MultiheadList1)],
			[check_sql(Type,0,SqlNew,lists:member(Type,MultiheadList)) || {Type,SqlNew} <- NewTypes],
			ok
	end.

check_sql(Type,SizeCur,SqlNew,IsKv) ->
	SizeNew = tuple_size(SqlNew),
	{ok,Db,_,_} = actordb_sqlite:init(":memory:"),
	% io:format("~p~n",[Type]),
	[begin
		% io:format("Check sql running: ~s\n",[element(N,SqlNew)]),
		case actordb_sqlite:exec(Db,[element(N,SqlNew)]) of
			ok ->
				ok;
			{ok,_} ->
				ok;
			{sql_error,E,_E1} ->
				actordb_sqlite:stop(Db),
				throw({error,?ERR("SQL Error for type \"~p\"~n~p~n~p~n",[Type,E,
									binary_to_list(iolist_to_binary(element(N,SqlNew)))])});
			{error,E} ->
				actordb_sqlite:stop(Db),
				throw({error,?ERR("SQL Error for type ~p, ~p~n",[Type,E])})
		end,
		case N == SizeCur of
			true ->
				compare_tables(Type,Db);
			false ->
				ok
		end
	end || N <- lists:seq(1,SizeNew)],
	case IsKv of
		true ->
			case actordb_sqlite:exec(Db,"select name from sqlite_master where type='table';",read) of
				{ok,[{columns,{<<"name">>}},{rows,Tables}]} ->
					case lists:member({<<"actors">>},Tables) of
						true ->
							check_actor_table(Db,Type);
						_ ->
							throw({error,?ERR("KV requires an \"actors\" table."++
												"~nType ~p has tables: ~p",
								[Type,[X || {X} <- Tables]])})
					end
			end;
		false ->
			ok
	end.

compare_tables(Type,NewDb) ->
	{ok,Db,_,_} = actordb_sqlite:init(":memory:"),
	% io:format("Running: ~s~n",[tuple_to_list(apply(actordb_schema,Type,[]))]),
	ok = actordb_sqlite:exec(Db,tuple_to_list(apply(actordb_schema,Type,[])),write),
	compare_tables1(Type,actordb:tables(Type),Db,NewDb).
compare_tables1(Type,[Table|T],Db,NewDb) ->
	OldColumns = actordb_sqlite:exec(Db,["pragma table_info(",Table,");"],read),
	NewColumns = actordb_sqlite:exec(NewDb,["pragma table_info(",Table,");"],read),
	case OldColumns == NewColumns of
		true ->
			compare_tables1(Type,T,Db,NewDb);
		false ->
			throw({error,?ERR("For type=~p, schema was not modified correctly, add lines to the end of the list."++
				" Do not modify existing SQL statements.~nMismatch was found for table: ~s~n",[Type,Table])})
	end;
compare_tables1(_,[],_,_) ->
	ok.

check_actor_table(Db,Type) ->
	{ok,[{columns,Columns},{rows,Rows1}]} = actordb_sqlite:exec(Db,"pragma table_info(actors);",read),
	Rows = [lists:zip(tuple_to_list(Columns),tuple_to_list(Row)) || Row <- Rows1],
	case butil:findobj(<<"name">>,<<"id">>,Rows) of
		false ->
			throw({error,?ERR("KV data type ~p does not contain \"id\" column of type TEXT.",[Type])});
		IdCol ->
			case butil:ds_val(<<"type">>,IdCol) of
				<<"TEXT">> ->
					ok;
				IdColType ->
					throw({error,?ERR("KV data type ~p \"id\" column should be TEXT, but is ~p.",
							[Type,butil:tolist(IdColType)])})
			end
	end,
	case butil:findobj(<<"name">>,<<"hash">>,Rows) of
		false ->
			throw({error,?ERR("KV data type ~p does not contain \"hash\" column of type INTEGER.",
					[Type])});
		HashCol ->
			case butil:ds_val(<<"type">>,HashCol) of
				<<"INTEGER">> ->
					ok;
				ColType ->
					throw({error,?ERR("KV data type ~p \"hash\" column should be INTEGER, but is ~p.",
							[Type,butil:tolist(ColType)])})
			end
	end.
