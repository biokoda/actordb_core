-module(actordb_sql).
-export([parse/1,file/1]).
-define(p_anything,true).
-define(p_charclass,true).
-define(p_choose,true).
-define(p_label,true).
-define(p_not,true).
-define(p_one_or_more,true).
-define(p_optional,true).
-define(p_regexp,true).
-define(p_scan,true).
-define(p_seq,true).
-define(p_string,true).
-define(p_zero_or_more,true).



-include_lib("actordb_core/include/actordb.hrl").

-spec file(file:name()) -> any().
file(Filename) -> case file:read_file(Filename) of {ok,Bin} -> parse(Bin); Err -> Err end.

-spec parse(binary() | list()) -> any().
parse(List) when is_list(List) -> parse(unicode:characters_to_binary(List));
parse(Input) when is_binary(Input) ->
  _ = setup_memo(),
  Result = case 'sql'(Input,{{line,1},{column,1}}) of
             {AST, <<>>, _Index} -> AST;
             Any -> Any
           end,
  release_memo(), Result.

-spec 'sql'(input(), index()) -> parse_result().
'sql'(Input, Index) ->
  p(Input, Index, 'sql', fun(I,D) -> (p_choose([fun 'set_query'/2, fun 'select_query'/2, fun 'update_query'/2, fun 'insert_query'/2, fun 'delete_query'/2, fun 'show_query'/2, fun 'desc_query'/2, fun 'use_query'/2, fun 'account_management_query'/2, fun 'commit'/2, fun 'rollback'/2, fun 'print'/2, fun 'actor'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'actor'(input(), index()) -> parse_result().
'actor'(Input, Index) ->
  p(Input, Index, 'actor', fun(I,D) -> (p_seq([p_regexp(<<"(?i)actor">>), p_optional(fun 'space'/2), fun 'key'/2, p_optional(fun 'space'/2), p_optional(fun 'actor_kv'/2)]))(I,D) end, fun(Node, _Idx) ->
    [_,_,Type,_,Type1|_] = Node,
    {actor,binary_to_atom(Type,latin1),Type1}
 end).

-spec 'actor_kv'(input(), index()) -> parse_result().
'actor_kv'(Input, Index) ->
  p(Input, Index, 'actor_kv', fun(I,D) -> (p_optional(p_regexp(<<"(?i)kv">>)))(I,D) end, fun(Node, _Idx) ->
    case Node of
        <<_/binary>> when byte_size(Node) > 0 ->
            kv;
        _ ->
            actor
    end
 end).

-spec 'use_query'(input(), index()) -> parse_result().
'use_query'(Input, Index) ->
  p(Input, Index, 'use_query', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'use'/2, fun 'space'/2, fun 'database'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    [_,_Use,_,Database,_] = Node,
    {use, Database}
 end).

-spec 'desc_query'(input(), index()) -> parse_result().
'desc_query'(Input, Index) ->
  p(Input, Index, 'desc_query', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'describe'/2, fun 'space'/2, fun 'table'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    [_,_Desc,_,Table,_] = Node,
    {describe, Table}
 end).

-spec 'show_query'(input(), index()) -> parse_result().
'show_query'(Input, Index) ->
  p(Input, Index, 'show_query', fun(I,D) -> (p_choose([fun 'show_status'/2, fun 'show_create_table'/2, fun 'show_tables_from'/2, fun 'show_tables_like'/2, fun 'show_tables'/2, fun 'show_databases'/2, fun 'show_fields'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'show_status'(input(), index()) -> parse_result().
'show_status'(Input, Index) ->
  p(Input, Index, 'show_status', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'show'/2, fun 'space'/2, fun 'table_keyword'/2, fun 'space'/2, fun 'status'/2, fun 'space'/2, fun 'like'/2, fun 'space'/2, p_label('like', fun 'string'/2), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #show{type=status, from=proplists:get_value(like, Node)}
 end).

-spec 'show_tables'(input(), index()) -> parse_result().
'show_tables'(Input, Index) ->
  p(Input, Index, 'show_tables', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'show'/2, p_optional(p_seq([fun 'space'/2, fun 'full'/2])), fun 'space'/2, p_choose([fun 'tables_keyword'/2, fun 'schemas'/2]), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [_,show,[],_,_Tables,_] ->
            #show{type=tables, full=false, from=undefined};
        [_,show,[_,full],_,_Tables,_] ->
            #show{type=tables, full=true, from=undefined}
    end
 end).

-spec 'show_create_table'(input(), index()) -> parse_result().
'show_create_table'(Input, Index) ->
  p(Input, Index, 'show_create_table', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'show'/2, fun 'space'/2, p_regexp(<<"(?i)create">>), fun 'space'/2, fun 'table_keyword'/2, fun 'space'/2, p_label('key', fun 'key'/2), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #show{type = create_table, from=proplists:get_value(key,Node)}
 end).

-spec 'show_tables_from'(input(), index()) -> parse_result().
'show_tables_from'(Input, Index) ->
  p(Input, Index, 'show_tables_from', fun(I,D) -> (p_seq([p_label('show_tables', fun 'show_tables'/2), fun 'space'/2, fun 'from'/2, fun 'space'/2, p_label('key', fun 'key'/2), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    ShowTables = proplists:get_value(show_tables, Node),
    ShowTables#show{from=proplists:get_value(key,Node)}
 end).

-spec 'show_tables_like'(input(), index()) -> parse_result().
'show_tables_like'(Input, Index) ->
  p(Input, Index, 'show_tables_like', fun(I,D) -> (p_seq([p_label('show_tables', fun 'show_tables'/2), fun 'space'/2, fun 'like'/2, fun 'space'/2, p_label('pattern', fun 'string'/2), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    ShowTables = proplists:get_value(show_tables, Node),
    ShowTables#show{from={like,proplists:get_value(pattern,Node)}}
 end).

-spec 'show_databases'(input(), index()) -> parse_result().
'show_databases'(Input, Index) ->
  p(Input, Index, 'show_databases', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'show'/2, fun 'space'/2, fun 'databases'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(_Node, _Idx) ->
    #show{type=databases}
 end).

-spec 'show_fields'(input(), index()) -> parse_result().
'show_fields'(Input, Index) ->
  p(Input, Index, 'show_fields', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'show'/2, p_label('full', p_optional(p_seq([fun 'space'/2, fun 'full'/2]))), fun 'space'/2, fun 'fields_keyword'/2, fun 'space'/2, fun 'from'/2, fun 'space'/2, p_label('key', fun 'key'/2), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    Full = lists:member(full,proplists:get_value(full,Node)),
    #show{type=fields, full=Full, from = proplists:get_value(key,Node)}
 end).

-spec 'set_query'(input(), index()) -> parse_result().
'set_query'(Input, Index) ->
  p(Input, Index, 'set_query', fun(I,D) -> (p_seq([fun 'set'/2, fun 'space'/2, p_label('head', fun 'system_set'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), p_label('s', fun 'system_set'/2)]))), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    Head = proplists:get_value(head,Node),
    Tail = [proplists:get_value(s,N) || N <- proplists:get_value(tail,Node)],
    #system_set{'query' = [Head|Tail]}
 end).

-spec 'system_set'(input(), index()) -> parse_result().
'system_set'(Input, Index) ->
  p(Input, Index, 'system_set', fun(I,D) -> (p_choose([p_seq([p_label('var', fun 'set_var'/2), p_optional(fun 'space'/2), p_string(<<"=">>), p_optional(fun 'space'/2), p_label('val', fun 'value'/2)]), p_seq([p_label('names', p_string(<<"NAMES">>)), fun 'space'/2, p_label('val', fun 'string'/2)])]))(I,D) end, fun(Node, _Idx) ->
    case proplists:get_value(names,Node) of
        undefined ->
            {proplists:get_value(var,Node), proplists:get_value(val,Node)};
        N ->
            {#variable{name = N, scope = session}, proplists:get_value(val,Node)}
    end
 end).

-spec 'select_query'(input(), index()) -> parse_result().
'select_query'(Input, Index) ->
  p(Input, Index, 'select_query', fun(I,D) -> (p_choose([fun 'select_limit'/2, fun 'select_order'/2, fun 'select_group'/2, fun 'select_where'/2, fun 'select_from'/2, fun 'select_simple'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'select_simple'(input(), index()) -> parse_result().
'select_simple'(Input, Index) ->
  p(Input, Index, 'select_simple', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'select'/2, fun 'space'/2, fun 'params'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #select{params=lists:nth(4, Node)}
 end).

-spec 'select_from'(input(), index()) -> parse_result().
'select_from'(Input, Index) ->
  p(Input, Index, 'select_from', fun(I,D) -> (p_seq([fun 'select_simple'/2, fun 'space'/2, fun 'from'/2, fun 'space'/2, fun 'tables'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    [#select{params=Query}, _, _From, _, Tables, _] = Node,
    #select{params=Query, tables=Tables}
 end).

-spec 'select_where'(input(), index()) -> parse_result().
'select_where'(Input, Index) ->
  p(Input, Index, 'select_where', fun(I,D) -> (p_seq([fun 'select_from'/2, fun 'space'/2, fun 'where'/2, fun 'space'/2, fun 'conditions'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    [Select, _, _Where, _, Conditions, _] = Node,
    Select#select{conditions=Conditions}
 end).

-spec 'select_group'(input(), index()) -> parse_result().
'select_group'(Input, Index) ->
  p(Input, Index, 'select_group', fun(I,D) -> (p_seq([p_choose([fun 'select_where'/2, fun 'select_from'/2]), fun 'space'/2, fun 'group_by'/2, fun 'space'/2, fun 'group_datas'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    [Select, _, _GroupBy, _, Groups, _] = Node,
    Select#select{group=Groups}
 end).

-spec 'select_order'(input(), index()) -> parse_result().
'select_order'(Input, Index) ->
  p(Input, Index, 'select_order', fun(I,D) -> (p_seq([p_choose([fun 'select_group'/2, fun 'select_where'/2, fun 'select_from'/2]), fun 'space'/2, fun 'order_by'/2, fun 'space'/2, fun 'order_datas'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    [Select, _, _OrderBy, _, Orders, _] = Node,
    Select#select{order=Orders}
 end).

-spec 'select_limit'(input(), index()) -> parse_result().
'select_limit'(Input, Index) ->
  p(Input, Index, 'select_limit', fun(I,D) -> (p_seq([p_choose([fun 'select_order'/2, fun 'select_group'/2, fun 'select_where'/2, fun 'select_from'/2, fun 'select_simple'/2]), fun 'space'/2, fun 'limit'/2, fun 'space'/2, fun 'integer'/2, p_optional(p_seq([fun 'space'/2, fun 'offset'/2, fun 'space'/2, fun 'integer'/2])), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [Select,_,limit,_,Limit,[],_] ->
            Select#select{limit=Limit};
        [Select,_,limit,_,Limit,[_,offset,_,Offset],_] ->
            Select#select{limit=Limit, offset=Offset}
    end
 end).

-spec 'order_datas'(input(), index()) -> parse_result().
'order_datas'(Input, Index) ->
  p(Input, Index, 'order_datas', fun(I,D) -> (p_seq([p_label('head', fun 'order_data'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), fun 'order_data'/2])))]))(I,D) end, fun(Node, _Idx) ->
    [proplists:get_value(head, Node)|[ lists:nth(4,I) || I <- proplists:get_value(tail, Node) ]]
 end).

-spec 'order_data'(input(), index()) -> parse_result().
'order_data'(Input, Index) ->
  p(Input, Index, 'order_data', fun(I,D) -> (p_seq([p_choose([fun 'key'/2, fun 'integer'/2]), p_optional(p_seq([fun 'space'/2, fun 'sort'/2]))]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [Key, [_, Sort]] -> #order{key=Key, sort=Sort};
        [Key, []] -> #order{key=Key, sort=asc}
    end
 end).

-spec 'sort'(input(), index()) -> parse_result().
'sort'(Input, Index) ->
  p(Input, Index, 'sort', fun(I,D) -> (p_choose([fun 'asc'/2, fun 'desc'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'group_datas'(input(), index()) -> parse_result().
'group_datas'(Input, Index) ->
  p(Input, Index, 'group_datas', fun(I,D) -> (p_seq([p_label('head', fun 'group_data'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), fun 'group_data'/2])))]))(I,D) end, fun(Node, _Idx) ->
    [proplists:get_value(head, Node)|[ lists:nth(4,I) || I <- proplists:get_value(tail, Node) ]]
 end).

-spec 'group_data'(input(), index()) -> parse_result().
'group_data'(Input, Index) ->
  p(Input, Index, 'group_data', fun(I,D) -> (p_choose([fun 'key'/2, fun 'integer'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'update_query'(input(), index()) -> parse_result().
'update_query'(Input, Index) ->
  p(Input, Index, 'update_query', fun(I,D) -> (p_choose([fun 'update_where'/2, fun 'update_simple'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'update_simple'(input(), index()) -> parse_result().
'update_simple'(Input, Index) ->
  p(Input, Index, 'update_simple', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'update'/2, fun 'space'/2, fun 'table_general'/2, fun 'space'/2, fun 'set'/2, fun 'space'/2, fun 'sets'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #update{table=lists:nth(4, Node), set=lists:nth(8, Node)}
 end).

-spec 'update_where'(input(), index()) -> parse_result().
'update_where'(Input, Index) ->
  p(Input, Index, 'update_where', fun(I,D) -> (p_seq([fun 'update_simple'/2, fun 'space'/2, fun 'where'/2, fun 'space'/2, fun 'conditions'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    [Update, _, _Where, _, Conditions, _] = Node,
    Update#update{conditions=Conditions}
 end).

-spec 'sets'(input(), index()) -> parse_result().
'sets'(Input, Index) ->
  p(Input, Index, 'sets', fun(I,D) -> (p_seq([p_label('head', fun 'set_value'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), fun 'set_value'/2])))]))(I,D) end, fun(Node, _Idx) ->
    [proplists:get_value(head, Node)| [ lists:nth(4, I) || I <- proplists:get_value(tail, Node) ] ]
 end).

-spec 'set_value'(input(), index()) -> parse_result().
'set_value'(Input, Index) ->
  p(Input, Index, 'set_value', fun(I,D) -> (p_seq([fun 'key'/2, p_optional(fun 'space'/2), p_string(<<"=">>), p_optional(fun 'space'/2), fun 'param_general'/2]))(I,D) end, fun(Node, _Idx) ->
    #set{key=lists:nth(1, Node), value=lists:nth(5, Node)}
 end).

-spec 'delete_query'(input(), index()) -> parse_result().
'delete_query'(Input, Index) ->
  p(Input, Index, 'delete_query', fun(I,D) -> (p_choose([fun 'delete_where'/2, fun 'delete_simple'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'delete_simple'(input(), index()) -> parse_result().
'delete_simple'(Input, Index) ->
  p(Input, Index, 'delete_simple', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'delete'/2, fun 'space'/2, fun 'table_general'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #delete{table=lists:nth(4, Node)}
 end).

-spec 'delete_where'(input(), index()) -> parse_result().
'delete_where'(Input, Index) ->
  p(Input, Index, 'delete_where', fun(I,D) -> (p_seq([fun 'delete_simple'/2, fun 'space'/2, fun 'where'/2, fun 'space'/2, fun 'conditions'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    [Delete, _, _Where, _, Conditions, _] = Node,
    Delete#delete{conditions=Conditions}
 end).

-spec 'insert_query'(input(), index()) -> parse_result().
'insert_query'(Input, Index) ->
  p(Input, Index, 'insert_query', fun(I,D) -> (p_choose([fun 'insert_values_keys'/2, fun 'insert_values'/2, fun 'insert_set'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'insert_values_keys'(input(), index()) -> parse_result().
'insert_values_keys'(Input, Index) ->
  p(Input, Index, 'insert_values_keys', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'insert'/2, fun 'space'/2, fun 'table_general'/2, p_optional(fun 'space'/2), p_string(<<"(">>), p_optional(fun 'space'/2), fun 'keys'/2, p_optional(fun 'space'/2), p_string(<<")">>), fun 'space'/2, fun 'values'/2, fun 'params_blocks'/2]))(I,D) end, fun(Node, _Idx) ->
    Values = [lists:zipwith(fun(X,Y) ->
        #set{key=X, value=Y}
    end, lists:nth(8, Node), Vals) || Vals <- lists:nth(13, Node) ],
    #insert{table=lists:nth(4, Node), values=Values}
 end).

-spec 'insert_values'(input(), index()) -> parse_result().
'insert_values'(Input, Index) ->
  p(Input, Index, 'insert_values', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'insert'/2, fun 'space'/2, fun 'table_general'/2, fun 'space'/2, fun 'values'/2, fun 'params_blocks'/2]))(I,D) end, fun(Node, _Idx) ->
    #insert{table=lists:nth(4, Node), values=lists:nth(7, Node)}
 end).

-spec 'insert_set'(input(), index()) -> parse_result().
'insert_set'(Input, Index) ->
  p(Input, Index, 'insert_set', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'insert'/2, fun 'space'/2, fun 'table_general'/2, fun 'space'/2, fun 'set'/2, fun 'space'/2, fun 'sets'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #insert{table=lists:nth(4, Node), values=lists:nth(8, Node)}
 end).

-spec 'account_management_query'(input(), index()) -> parse_result().
'account_management_query'(Input, Index) ->
  p(Input, Index, 'account_management_query', fun(I,D) -> (p_choose([fun 'insert_user'/2, fun 'grant_sql'/2, fun 'drop_user'/2, fun 'rename_sql'/2, fun 'revoke_sql'/2, fun 'set_password'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'drop_user'(input(), index()) -> parse_result().
'drop_user'(Input, Index) ->
  p(Input, Index, 'drop_user', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'drop'/2, fun 'space'/2, fun 'user'/2, fun 'space'/2, fun 'user_at_host'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #management{action = drop, data = #account{access = lists:nth(6, Node)} }
 end).

-spec 'insert_user'(input(), index()) -> parse_result().
'insert_user'(Input, Index) ->
  p(Input, Index, 'insert_user', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'create_user'/2, fun 'space'/2, fun 'user_at_host'/2, fun 'space'/2, fun 'identified'/2, p_optional(fun 'password'/2), fun 'space'/2, fun 'param'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #value{name = undefined,value = Password} = lists:nth(9, Node),
    #management{action = create, data = #account{access = [#value{name = <<"password">>, value = Password}|lists:nth(4, Node)]}}
 end).

-spec 'grant_sql'(input(), index()) -> parse_result().
'grant_sql'(Input, Index) ->
  p(Input, Index, 'grant_sql', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'grant'/2, fun 'space'/2, fun 'permission'/2, p_optional(fun 'space'/2), fun 'on'/2, fun 'space'/2, fun 'priv_level'/2, p_choose([p_seq([p_string(<<".">>), fun 'priv_level'/2, fun 'space'/2]), fun 'space'/2]), fun 'to'/2, fun 'space'/2, fun 'user_at_host'/2, p_optional(fun 'space'/2), p_optional(p_seq([fun 'with'/2, p_zero_or_more(fun 'grant_options'/2)])), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    case lists:nth(14,Node) of
    [_|_] ->
      #management{action = grant, data = #permission{on = lists:nth(8,Node), account = lists:nth(12,Node), conditions = [lists:nth(4,Node)|lists:nth(2,lists:nth(14,Node))]}};
    _ ->
      #management{action = grant, data = #permission{on = lists:nth(8,Node), account = lists:nth(12,Node), conditions = lists:nth(4,Node)}}
    end
 end).

-spec 'rename_sql'(input(), index()) -> parse_result().
'rename_sql'(Input, Index) ->
  p(Input, Index, 'rename_sql', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'rename_user'/2, fun 'space'/2, fun 'user_at_host'/2, fun 'space'/2, fun 'to'/2, fun 'space'/2, fun 'user_at_host'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #management{action = rename, data = [#account{access = lists:nth(4, Node)}|lists:nth(8, Node)]}
 end).

-spec 'revoke_sql'(input(), index()) -> parse_result().
'revoke_sql'(Input, Index) ->
  p(Input, Index, 'revoke_sql', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'revoke'/2, fun 'space'/2, fun 'permission'/2, fun 'space'/2, fun 'on'/2, fun 'space'/2, fun 'priv_level'/2, p_choose([p_seq([p_string(<<".">>), fun 'priv_level'/2, fun 'space'/2]), fun 'space'/2]), fun 'from'/2, fun 'space'/2, fun 'user_at_host'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #management{action = revoke, data = #permission{on = lists:nth(8,Node), account = lists:nth(12,Node), conditions = lists:nth(4,Node)}}
 end).

-spec 'set_password'(input(), index()) -> parse_result().
'set_password'(Input, Index) ->
  p(Input, Index, 'set_password', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'set'/2, fun 'space'/2, fun 'password'/2, fun 'space'/2, fun 'for'/2, fun 'space'/2, fun 'user_at_host'/2, fun 'space'/2, p_string(<<"=">>), fun 'space'/2, fun 'param'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    case lists:nth(12, Node) of
      #value{name = undefined,value = Password} -> ok;
      #key{name = Password, alias = _} -> ok
    end,
    #management{action = setpasswd, data = #account{access =[#value{name = <<"password">>, value = Password}|lists:nth(8, Node)]}}
 end).

-spec 'tables'(input(), index()) -> parse_result().
'tables'(Input, Index) ->
  p(Input, Index, 'tables', fun(I,D) -> (p_seq([p_label('head', fun 'table'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), fun 'table'/2])))]))(I,D) end, fun(Node, _Idx) ->
    [proplists:get_value(head, Node)|[ lists:nth(4,I) || I <- proplists:get_value(tail, Node) ]]
 end).

-spec 'table_general'(input(), index()) -> parse_result().
'table_general'(Input, Index) ->
  p(Input, Index, 'table_general', fun(I,D) -> (p_choose([fun 'table_alias'/2, fun 'table_value'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'table'(input(), index()) -> parse_result().
'table'(Input, Index) ->
  p(Input, Index, 'table', fun(I,D) -> (p_choose([fun 'table_alias'/2, fun 'table_value'/2, fun 'param_sql'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'table_alias'(input(), index()) -> parse_result().
'table_alias'(Input, Index) ->
  p(Input, Index, 'table_alias', fun(I,D) -> (p_seq([fun 'key'/2, fun 'space'/2, fun 'as'/2, fun 'space'/2, fun 'key'/2]))(I,D) end, fun(Node, _Idx) ->
    #table{name=lists:nth(1, Node), alias=lists:nth(5, Node)}
 end).

-spec 'table_value'(input(), index()) -> parse_result().
'table_value'(Input, Index) ->
  p(Input, Index, 'table_value', fun(I,D) -> (fun 'key'/2)(I,D) end, fun(Node, _Idx) ->
    #table{name=Node, alias=Node}
 end).

-spec 'comparator'(input(), index()) -> parse_result().
'comparator'(Input, Index) ->
  p(Input, Index, 'comparator', fun(I,D) -> (p_choose([p_string(<<"<=">>), p_string(<<"=<">>), p_string(<<"=>">>), p_string(<<">=">>), p_string(<<"<>">>), p_string(<<"!=">>), p_string(<<"<">>), p_string(<<">">>), p_string(<<"=">>), fun 'like'/2]))(I,D) end, fun(Node, _Idx) ->
case Node of
    <<"<=">> -> lte;
    <<"=<">> -> lte;
    <<">=">> -> gte;
    <<"=>">> -> gte;
    <<"!=">> -> neq;
    <<"<>">> -> neq;
    <<"<">> -> lt;
    <<">">> -> gt;
    <<"=">> -> eq;
    like -> like
end
 end).

-spec 'priv_level'(input(), index()) -> parse_result().
'priv_level'(Input, Index) ->
  p(Input, Index, 'priv_level', fun(I,D) -> (p_choose([fun 'priv_part'/2, fun 'priv_all'/2, fun 'all_for_all'/2, fun 'db_name_all'/2, fun 'db_name_table'/2, fun 'table'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'priv_part'(input(), index()) -> parse_result().
'priv_part'(Input, Index) ->
  p(Input, Index, 'priv_part', fun(I,D) -> (p_seq([p_string(<<".">>), p_string(<<"*">>)]))(I,D) end, fun(_Node, _Idx) ->
  #all{}
 end).

-spec 'priv_all'(input(), index()) -> parse_result().
'priv_all'(Input, Index) ->
  p(Input, Index, 'priv_all', fun(I,D) -> (p_string(<<"*">>))(I,D) end, fun(_Node, _Idx) ->
  #all{}
 end).

-spec 'all_for_all'(input(), index()) -> parse_result().
'all_for_all'(Input, Index) ->
  p(Input, Index, 'all_for_all', fun(I,D) -> (p_seq([p_string(<<"*">>), p_string(<<".">>), p_string(<<"*">>)]))(I,D) end, fun(_Node, _Idx) ->
  #all{table = #all{}}
 end).

-spec 'db_name_all'(input(), index()) -> parse_result().
'db_name_all'(Input, Index) ->
  p(Input, Index, 'db_name_all', fun(I,D) -> (p_seq([fun 'database'/2, p_string(<<".">>), p_string(<<"*">>)]))(I,D) end, fun(Node, _Idx) ->
    [DBName,_,_] = Node,
    #value{name = DBName, value = #all{}}
 end).

-spec 'db_name_table'(input(), index()) -> parse_result().
'db_name_table'(Input, Index) ->
  p(Input, Index, 'db_name_table', fun(I,D) -> (p_seq([fun 'database'/2, p_string(<<".">>), fun 'table'/2]))(I,D) end, fun(Node, _Idx) ->
    [DBName,_,TableName] = Node,
    %table could also be routine name
    #value{name = DBName, value = #table{name = TableName}}
 end).

-spec 'permission'(input(), index()) -> parse_result().
'permission'(Input, Index) ->
  p(Input, Index, 'permission', fun(I,D) -> (p_seq([p_label('head', fun 'perms'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), fun 'perms'/2])))]))(I,D) end, fun(Node, _Idx) ->
  [proplists:get_value(head, Node)|[ lists:nth(4,I) || I <- proplists:get_value(tail, Node) ]]
 end).

-spec 'perms'(input(), index()) -> parse_result().
'perms'(Input, Index) ->
  p(Input, Index, 'perms', fun(I,D) -> (p_choose([fun 'all'/2, fun 'all_privileges'/2, fun 'alter_routine'/2, fun 'alter'/2, fun 'create_routine'/2, fun 'create_temp_tables'/2, fun 'create_user'/2, fun 'create_view'/2, fun 'event'/2, fun 'file'/2, fun 'grant_option'/2, fun 'index'/2, fun 'lock_tables'/2, fun 'process'/2, fun 'references'/2, fun 'reload'/2, fun 'repl_client'/2, fun 'repl_slave'/2, fun 'show_dbs'/2, fun 'show_view'/2, fun 'shutdown'/2, fun 'super'/2, fun 'trigger'/2, fun 'update'/2, fun 'usage'/2, fun 'insert'/2, fun 'create'/2, fun 'delete'/2, fun 'drop'/2, fun 'execute'/2, fun 'select'/2, fun 'update'/2, fun 'read'/2, fun 'write'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'user_at_host'(input(), index()) -> parse_result().
'user_at_host'(Input, Index) ->
  p(Input, Index, 'user_at_host', fun(I,D) -> (p_seq([fun 'param'/2, p_optional(p_string(<<"@">>)), p_optional(fun 'param'/2)]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [{value,undefined,Username},<<"@">>,{value,undefined,Host}] ->
            ok;
        [{value,undefined,Username}|_] ->
            Host = <<>>
    end,
    [#value{name = <<"username">>, value = Username},
    #value{name = <<"host">>, value = Host}]
 end).

-spec 'grant_options'(input(), index()) -> parse_result().
'grant_options'(Input, Index) ->
  p(Input, Index, 'grant_options', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), p_choose([fun 'max_queries_per_hour'/2, fun 'max_updates_per_hour'/2, fun 'max_connections_per_hour'/2, fun 'max_user_connections'/2]), p_optional(fun 'space'/2), p_choose([p_seq([p_string(<<"=">>), p_optional(fun 'space'/2), fun 'integer'/2]), fun 'integer'/2])]))(I,D) end, fun(Node, _Idx) ->
  case Node of
  [_,What,_,[_Operator,_,Value]] -> #value{name = What, value = Value};
  [_,What,_,Value] -> #value{name = What, value = Value}
  end
 end).

-spec 'conditions'(input(), index()) -> parse_result().
'conditions'(Input, Index) ->
  p(Input, Index, 'conditions', fun(I,D) -> (p_choose([fun 'conditions_normal_chain'/2, fun 'conditions_normal'/2, fun 'conditions_parens_chain'/2, fun 'conditions_parens'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'conditions_parens_chain'(input(), index()) -> parse_result().
'conditions_parens_chain'(Input, Index) ->
  p(Input, Index, 'conditions_parens_chain', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), p_string(<<"(">>), fun 'conditions'/2, p_string(<<")">>), p_optional(fun 'space'/2), fun 'nexo'/2, fun 'space'/2, fun 'conditions'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [_,_,Cond,_,_,Nexo,_,Next,_] -> #condition{nexo=Nexo, op1=Cond, op2=Next}
    end
 end).

-spec 'conditions_parens'(input(), index()) -> parse_result().
'conditions_parens'(Input, Index) ->
  p(Input, Index, 'conditions_parens', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), p_string(<<"(">>), p_label('first', fun 'conditions'/2), p_string(<<")">>), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    proplists:get_value(first, Node)
 end).

-spec 'conditions_normal_chain'(input(), index()) -> parse_result().
'conditions_normal_chain'(Input, Index) ->
  p(Input, Index, 'conditions_normal_chain', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'condition'/2, fun 'space'/2, fun 'nexo'/2, fun 'space'/2, fun 'conditions'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [_,Cond,_,Nexo,_,Next,_] -> #condition{nexo=Nexo, op1=Cond, op2=Next}
    end
 end).

-spec 'conditions_normal'(input(), index()) -> parse_result().
'conditions_normal'(Input, Index) ->
  p(Input, Index, 'conditions_normal', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), fun 'condition'/2, p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->lists:nth(2, Node) end).

-spec 'condition'(input(), index()) -> parse_result().
'condition'(Input, Index) ->
  p(Input, Index, 'condition', fun(I,D) -> (p_choose([fun 'condition_comp'/2, fun 'condition_set'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'condition_set'(input(), index()) -> parse_result().
'condition_set'(Input, Index) ->
  p(Input, Index, 'condition_set', fun(I,D) -> (p_seq([fun 'param'/2, p_optional(fun 'space'/2), fun 'set_comp'/2, fun 'subquery'/2]))(I,D) end, fun(Node, _Idx) ->
    #condition{nexo=lists:nth(3,Node), op1=lists:nth(1,Node), op2=lists:nth(4,Node)}
 end).

-spec 'condition_comp'(input(), index()) -> parse_result().
'condition_comp'(Input, Index) ->
  p(Input, Index, 'condition_comp', fun(I,D) -> (p_seq([fun 'param'/2, p_optional(fun 'space'/2), fun 'comparator'/2, p_optional(fun 'space'/2), fun 'param'/2]))(I,D) end, fun(Node, _Idx) ->
    #condition{nexo=lists:nth(3,Node), op1=lists:nth(1,Node), op2=lists:nth(5,Node)}
 end).

-spec 'subquery'(input(), index()) -> parse_result().
'subquery'(Input, Index) ->
  p(Input, Index, 'subquery', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), p_string(<<"(">>), p_optional(fun 'space'/2), p_choose([fun 'select_query'/2, fun 'set_datas'/2]), p_optional(fun 'space'/2), p_string(<<")">>), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) ->
    #subquery{subquery=lists:nth(4, Node)}
 end).

-spec 'set_datas'(input(), index()) -> parse_result().
'set_datas'(Input, Index) ->
  p(Input, Index, 'set_datas', fun(I,D) -> (p_seq([p_label('head', fun 'set_data'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), fun 'set_data'/2])))]))(I,D) end, fun(Node, _Idx) ->
    [proplists:get_value(head, Node)|[ lists:nth(4,I) || I <- proplists:get_value(tail, Node) ]]
 end).

-spec 'set_data'(input(), index()) -> parse_result().
'set_data'(Input, Index) ->
  p(Input, Index, 'set_data', fun(I,D) -> (fun 'value'/2)(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'nexo'(input(), index()) -> parse_result().
'nexo'(Input, Index) ->
  p(Input, Index, 'nexo', fun(I,D) -> (p_choose([fun 'nexo_and'/2, fun 'nexo_or'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'set_comp'(input(), index()) -> parse_result().
'set_comp'(Input, Index) ->
  p(Input, Index, 'set_comp', fun(I,D) -> (p_choose([fun 'in'/2, fun 'exist'/2, fun 'all'/2, fun 'any'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'params_blocks'(input(), index()) -> parse_result().
'params_blocks'(Input, Index) ->
  p(Input, Index, 'params_blocks', fun(I,D) -> (p_seq([p_label('head', fun 'params_block'/2), p_label('tail', p_zero_or_more(p_seq([p_string(<<",">>), fun 'params_block'/2])))]))(I,D) end, fun(Node, _Idx) ->
    [proplists:get_value(head, Node)|[ lists:nth(2,I) || I <- proplists:get_value(tail, Node) ]]
 end).

-spec 'params_block'(input(), index()) -> parse_result().
'params_block'(Input, Index) ->
  p(Input, Index, 'params_block', fun(I,D) -> (p_seq([p_optional(fun 'space'/2), p_string(<<"(">>), p_optional(fun 'space'/2), fun 'params'/2, p_optional(fun 'space'/2), p_string(<<")">>), p_optional(fun 'space'/2)]))(I,D) end, fun(Node, _Idx) -> lists:nth(4,Node) end).

-spec 'params'(input(), index()) -> parse_result().
'params'(Input, Index) ->
  p(Input, Index, 'params', fun(I,D) -> (p_seq([p_label('head', fun 'param'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), fun 'param'/2])))]))(I,D) end, fun(Node, _Idx) ->
    [proplists:get_value(head, Node)|[ lists:nth(4,I) || I <- proplists:get_value(tail, Node) ]]
 end).

-spec 'param_general'(input(), index()) -> parse_result().
'param_general'(Input, Index) ->
  p(Input, Index, 'param_general', fun(I,D) -> (p_choose([fun 'param_var'/2, fun 'param_function'/2, fun 'param_value'/2, fun 'param_key'/2, fun 'param_sql'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'param'(input(), index()) -> parse_result().
'param'(Input, Index) ->
  p(Input, Index, 'param', fun(I,D) -> (p_choose([fun 'param_var'/2, fun 'param_arithmetic'/2, fun 'param_function'/2, fun 'param_value'/2, fun 'param_all'/2, fun 'param_all_alias'/2, fun 'param_key_alias'/2, fun 'param_key'/2, fun 'param_sql'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'param_sql'(input(), index()) -> parse_result().
'param_sql'(Input, Index) ->
  p(Input, Index, 'param_sql', fun(I,D) -> (p_seq([fun 'subquery'/2, p_optional(p_seq([fun 'space'/2, fun 'as'/2, fun 'space'/2, fun 'key'/2]))]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [#subquery{subquery=Query}, [_,_As,_,Key]] -> #subquery{name=Key, subquery=Query};
        [#subquery{subquery=Query}, []] -> #subquery{subquery=Query}
    end
 end).

-spec 'param_key_alias'(input(), index()) -> parse_result().
'param_key_alias'(Input, Index) ->
  p(Input, Index, 'param_key_alias', fun(I,D) -> (p_seq([fun 'key'/2, p_string(<<"\.">>), fun 'key'/2, p_optional(p_seq([fun 'space'/2, fun 'as'/2, fun 'space'/2, fun 'key'/2]))]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [Alias, _, Val, [_, _As, _, Key]] -> #key{alias=Key, name=Val, table=Alias};
        [Alias, _, Val, []] -> #key{alias=Val, name=Val, table=Alias}
    end
 end).

-spec 'param_key'(input(), index()) -> parse_result().
'param_key'(Input, Index) ->
  p(Input, Index, 'param_key', fun(I,D) -> (p_seq([fun 'key'/2, p_optional(p_seq([fun 'space'/2, fun 'as'/2, fun 'space'/2, fun 'key'/2]))]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [Val, [_, _As, _, Key]] -> #key{alias=Key, name=Val};
        [Val, []] -> #key{alias=Val, name=Val}
    end
 end).

-spec 'param_value'(input(), index()) -> parse_result().
'param_value'(Input, Index) ->
  p(Input, Index, 'param_value', fun(I,D) -> (p_seq([fun 'value'/2, p_optional(p_seq([fun 'space'/2, fun 'as'/2, fun 'space'/2, fun 'key'/2]))]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [Val, [_, _As, _, Key]] -> #value{name=Key, value=Val};
        [Val, []] -> #value{value=Val}
    end
 end).

-spec 'param_var'(input(), index()) -> parse_result().
'param_var'(Input, Index) ->
  p(Input, Index, 'param_var', fun(I,D) -> (p_seq([fun 'var'/2, p_optional(p_seq([fun 'space'/2, fun 'as'/2, fun 'space'/2, fun 'key'/2]))]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [Var, [_, _As, _, Key]] -> Var#variable{label=Key};
        [Var, []] -> Var
    end
 end).

-spec 'param_all'(input(), index()) -> parse_result().
'param_all'(Input, Index) ->
  p(Input, Index, 'param_all', fun(I,D) -> (p_string(<<"*">>))(I,D) end, fun(_Node, _Idx) ->#all{} end).

-spec 'param_all_alias'(input(), index()) -> parse_result().
'param_all_alias'(Input, Index) ->
  p(Input, Index, 'param_all_alias', fun(I,D) -> (p_seq([fun 'key'/2, p_string(<<"\.">>), p_string(<<"*">>)]))(I,D) end, fun(Node, _Idx) ->#all{table=lists:nth(1,Node)} end).

-spec 'param_function'(input(), index()) -> parse_result().
'param_function'(Input, Index) ->
  p(Input, Index, 'param_function', fun(I,D) -> (p_seq([fun 'key'/2, p_optional(fun 'space'/2), p_string(<<"(">>), p_optional(fun 'space'/2), p_optional(fun 'params'/2), p_optional(fun 'space'/2), p_string(<<")">>), p_optional(p_seq([fun 'space'/2, fun 'as'/2, fun 'space'/2, fun 'key'/2]))]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [Name, _, _, _, Params, _, _, [_, _As, _, Key]] -> 
            #function{name=Name, params=Params, alias=Key};
        [Name, _, _, _, Params, _, _, []] -> 
            #function{name=Name, params=Params}
    end
 end).

-spec 'param_wa'(input(), index()) -> parse_result().
'param_wa'(Input, Index) ->
  p(Input, Index, 'param_wa', fun(I,D) -> (p_choose([fun 'param_function'/2, fun 'param_value'/2, fun 'param_all'/2, fun 'param_all_alias'/2, fun 'param_key_alias'/2, fun 'param_key'/2, fun 'param_sql'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'param_arithmetic'(input(), index()) -> parse_result().
'param_arithmetic'(Input, Index) ->
  p(Input, Index, 'param_arithmetic', fun(I,D) -> (fun 'additive'/2)(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'additive'(input(), index()) -> parse_result().
'additive'(Input, Index) ->
  p(Input, Index, 'additive', fun(I,D) -> (p_choose([p_seq([fun 'multitive'/2, p_optional(fun 'space'/2), p_choose([p_string(<<"+">>), p_string(<<"-">>)]), p_optional(fun 'space'/2), fun 'additive'/2]), p_label('mul', fun 'multitive'/2)]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [A, _, Type, _, B] -> #operation{type=Type,op1=A,op2=B};
        {mul,Param} -> Param
    end
 end).

-spec 'multitive'(input(), index()) -> parse_result().
'multitive'(Input, Index) ->
  p(Input, Index, 'multitive', fun(I,D) -> (p_choose([p_seq([fun 'primary'/2, p_optional(fun 'space'/2), p_choose([p_string(<<"*">>), p_string(<<"\/">>)]), p_optional(fun 'space'/2), p_label('Mul', fun 'multitive'/2)]), p_label('pri', fun 'primary'/2)]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [A, _, Type, _, {'Mul', B}] -> #operation{type=Type, op1=A, op2=B};
        {pri,Param} -> Param
    end
 end).

-spec 'primary'(input(), index()) -> parse_result().
'primary'(Input, Index) ->
  p(Input, Index, 'primary', fun(I,D) -> (p_choose([p_label('par', p_seq([p_string(<<"(">>), p_optional(fun 'space'/2), p_label('add', fun 'additive'/2), p_optional(fun 'space'/2), p_string(<<")">>)])), p_label('dec', fun 'param_wa'/2)]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        {dec,Param} -> Param;
        {par,List} -> proplists:get_value(add,List)
    end
 end).

-spec 'status'(input(), index()) -> parse_result().
'status'(Input, Index) ->
  p(Input, Index, 'status', fun(I,D) -> (p_regexp(<<"(?i)status">>))(I,D) end, fun(_Node, _Idx) ->status end).

-spec 'like'(input(), index()) -> parse_result().
'like'(Input, Index) ->
  p(Input, Index, 'like', fun(I,D) -> (p_regexp(<<"(?i)like">>))(I,D) end, fun(_Node, _Idx) ->like end).

-spec 'use'(input(), index()) -> parse_result().
'use'(Input, Index) ->
  p(Input, Index, 'use', fun(I,D) -> (p_regexp(<<"(?i)use">>))(I,D) end, fun(_Node, _Idx) ->use end).

-spec 'describe'(input(), index()) -> parse_result().
'describe'(Input, Index) ->
  p(Input, Index, 'describe', fun(I,D) -> (p_choose([p_regexp(<<"(?i)desc">>), p_regexp(<<"(?i)describe">>)]))(I,D) end, fun(_Node, _Idx) ->describe end).

-spec 'limit'(input(), index()) -> parse_result().
'limit'(Input, Index) ->
  p(Input, Index, 'limit', fun(I,D) -> (p_regexp(<<"(?i)limit">>))(I,D) end, fun(_Node, _Idx) ->limit end).

-spec 'offset'(input(), index()) -> parse_result().
'offset'(Input, Index) ->
  p(Input, Index, 'offset', fun(I,D) -> (p_regexp(<<"(?i)offset">>))(I,D) end, fun(_Node, _Idx) ->offset end).

-spec 'full'(input(), index()) -> parse_result().
'full'(Input, Index) ->
  p(Input, Index, 'full', fun(I,D) -> (p_regexp(<<"(?i)full">>))(I,D) end, fun(_Node, _Idx) ->full end).

-spec 'schemas'(input(), index()) -> parse_result().
'schemas'(Input, Index) ->
  p(Input, Index, 'schemas', fun(I,D) -> (p_regexp(<<"(?i)schemas">>))(I,D) end, fun(_Node, _Idx) ->schemas end).

-spec 'show'(input(), index()) -> parse_result().
'show'(Input, Index) ->
  p(Input, Index, 'show', fun(I,D) -> (p_regexp(<<"(?i)show">>))(I,D) end, fun(_Node, _Idx) ->show end).

-spec 'fields_keyword'(input(), index()) -> parse_result().
'fields_keyword'(Input, Index) ->
  p(Input, Index, 'fields_keyword', fun(I,D) -> (p_regexp(<<"(?i)fields">>))(I,D) end, fun(_Node, _Idx) ->fields end).

-spec 'tables_keyword'(input(), index()) -> parse_result().
'tables_keyword'(Input, Index) ->
  p(Input, Index, 'tables_keyword', fun(I,D) -> (p_regexp(<<"(?i)tables">>))(I,D) end, fun(_Node, _Idx) ->tables end).

-spec 'table_keyword'(input(), index()) -> parse_result().
'table_keyword'(Input, Index) ->
  p(Input, Index, 'table_keyword', fun(I,D) -> (p_regexp(<<"(?i)table">>))(I,D) end, fun(_Node, _Idx) ->table end).

-spec 'databases'(input(), index()) -> parse_result().
'databases'(Input, Index) ->
  p(Input, Index, 'databases', fun(I,D) -> (p_regexp(<<"(?i)databases">>))(I,D) end, fun(_Node, _Idx) ->databases end).

-spec 'update'(input(), index()) -> parse_result().
'update'(Input, Index) ->
  p(Input, Index, 'update', fun(I,D) -> (p_regexp(<<"(?i)update">>))(I,D) end, fun(_Node, _Idx) ->update end).

-spec 'select'(input(), index()) -> parse_result().
'select'(Input, Index) ->
  p(Input, Index, 'select', fun(I,D) -> (p_regexp(<<"(?i)select">>))(I,D) end, fun(_Node, _Idx) ->select end).

-spec 'set'(input(), index()) -> parse_result().
'set'(Input, Index) ->
  p(Input, Index, 'set', fun(I,D) -> (p_regexp(<<"(?i)set">>))(I,D) end, fun(_Node, _Idx) ->set end).

-spec 'from'(input(), index()) -> parse_result().
'from'(Input, Index) ->
  p(Input, Index, 'from', fun(I,D) -> (p_regexp(<<"(?i)from">>))(I,D) end, fun(_Node, _Idx) ->from end).

-spec 'where'(input(), index()) -> parse_result().
'where'(Input, Index) ->
  p(Input, Index, 'where', fun(I,D) -> (p_regexp(<<"(?i)where">>))(I,D) end, fun(_Node, _Idx) ->where end).

-spec 'as'(input(), index()) -> parse_result().
'as'(Input, Index) ->
  p(Input, Index, 'as', fun(I,D) -> (p_regexp(<<"(?i)as">>))(I,D) end, fun(_Node, _Idx) ->as end).

-spec 'nexo_or'(input(), index()) -> parse_result().
'nexo_or'(Input, Index) ->
  p(Input, Index, 'nexo_or', fun(I,D) -> (p_regexp(<<"(?i)or">>))(I,D) end, fun(_Node, _Idx) ->nexo_or end).

-spec 'nexo_and'(input(), index()) -> parse_result().
'nexo_and'(Input, Index) ->
  p(Input, Index, 'nexo_and', fun(I,D) -> (p_regexp(<<"(?i)and">>))(I,D) end, fun(_Node, _Idx) ->nexo_and end).

-spec 'in'(input(), index()) -> parse_result().
'in'(Input, Index) ->
  p(Input, Index, 'in', fun(I,D) -> (p_regexp(<<"(?i)in">>))(I,D) end, fun(_Node, _Idx) ->in end).

-spec 'any'(input(), index()) -> parse_result().
'any'(Input, Index) ->
  p(Input, Index, 'any', fun(I,D) -> (p_regexp(<<"(?i)any">>))(I,D) end, fun(_Node, _Idx) ->in end).

-spec 'exist'(input(), index()) -> parse_result().
'exist'(Input, Index) ->
  p(Input, Index, 'exist', fun(I,D) -> (p_regexp(<<"(?i)exist">>))(I,D) end, fun(_Node, _Idx) ->exist end).

-spec 'all'(input(), index()) -> parse_result().
'all'(Input, Index) ->
  p(Input, Index, 'all', fun(I,D) -> (p_regexp(<<"(?i)all">>))(I,D) end, fun(_Node, _Idx) ->all end).

-spec 'group_by'(input(), index()) -> parse_result().
'group_by'(Input, Index) ->
  p(Input, Index, 'group_by', fun(I,D) -> (p_regexp(<<"(?i)group +by">>))(I,D) end, fun(_Node, _Idx) ->group_by end).

-spec 'asc'(input(), index()) -> parse_result().
'asc'(Input, Index) ->
  p(Input, Index, 'asc', fun(I,D) -> (p_regexp(<<"(?i)asc">>))(I,D) end, fun(_Node, _Idx) ->asc end).

-spec 'desc'(input(), index()) -> parse_result().
'desc'(Input, Index) ->
  p(Input, Index, 'desc', fun(I,D) -> (p_regexp(<<"(?i)desc">>))(I,D) end, fun(_Node, _Idx) ->desc end).

-spec 'order_by'(input(), index()) -> parse_result().
'order_by'(Input, Index) ->
  p(Input, Index, 'order_by', fun(I,D) -> (p_regexp(<<"(?i)order +by">>))(I,D) end, fun(_Node, _Idx) ->order_by end).

-spec 'delete'(input(), index()) -> parse_result().
'delete'(Input, Index) ->
  p(Input, Index, 'delete', fun(I,D) -> (p_regexp(<<"(?i)delete +from">>))(I,D) end, fun(_Node, _Idx) ->delete end).

-spec 'insert'(input(), index()) -> parse_result().
'insert'(Input, Index) ->
  p(Input, Index, 'insert', fun(I,D) -> (p_regexp(<<"(?i)insert +into">>))(I,D) end, fun(_Node, _Idx) ->insert end).

-spec 'values'(input(), index()) -> parse_result().
'values'(Input, Index) ->
  p(Input, Index, 'values', fun(I,D) -> (p_regexp(<<"(?i)values">>))(I,D) end, fun(_Node, _Idx) ->values end).

-spec 'for'(input(), index()) -> parse_result().
'for'(Input, Index) ->
  p(Input, Index, 'for', fun(I,D) -> (p_regexp(<<"(?i)for">>))(I,D) end, fun(_Node, _Idx) ->for end).

-spec 'revoke'(input(), index()) -> parse_result().
'revoke'(Input, Index) ->
  p(Input, Index, 'revoke', fun(I,D) -> (p_regexp(<<"(?i)revoke">>))(I,D) end, fun(_Node, _Idx) ->revoke end).

-spec 'max_queries_per_hour'(input(), index()) -> parse_result().
'max_queries_per_hour'(Input, Index) ->
  p(Input, Index, 'max_queries_per_hour', fun(I,D) -> (p_regexp(<<"(?i)max_queries_per_hour">>))(I,D) end, fun(_Node, _Idx) ->max_queries_per_hour end).

-spec 'max_updates_per_hour'(input(), index()) -> parse_result().
'max_updates_per_hour'(Input, Index) ->
  p(Input, Index, 'max_updates_per_hour', fun(I,D) -> (p_regexp(<<"(?i)max_updates_per_hour">>))(I,D) end, fun(_Node, _Idx) ->max_updates_per_hour end).

-spec 'max_connections_per_hour'(input(), index()) -> parse_result().
'max_connections_per_hour'(Input, Index) ->
  p(Input, Index, 'max_connections_per_hour', fun(I,D) -> (p_regexp(<<"(?i)max_connections_per_hour">>))(I,D) end, fun(_Node, _Idx) ->max_connections_per_hour end).

-spec 'max_user_connections'(input(), index()) -> parse_result().
'max_user_connections'(Input, Index) ->
  p(Input, Index, 'max_user_connections', fun(I,D) -> (p_regexp(<<"(?i)max_user_connections">>))(I,D) end, fun(_Node, _Idx) ->max_user_connections end).

-spec 'rename_user'(input(), index()) -> parse_result().
'rename_user'(Input, Index) ->
  p(Input, Index, 'rename_user', fun(I,D) -> (p_regexp(<<"(?i)rename +user">>))(I,D) end, fun(_Node, _Idx) ->rename_user end).

-spec 'create_user'(input(), index()) -> parse_result().
'create_user'(Input, Index) ->
  p(Input, Index, 'create_user', fun(I,D) -> (p_regexp(<<"(?i)create +user">>))(I,D) end, fun(_Node, _Idx) ->create_user end).

-spec 'create_view'(input(), index()) -> parse_result().
'create_view'(Input, Index) ->
  p(Input, Index, 'create_view', fun(I,D) -> (p_regexp(<<"(?i)create +view">>))(I,D) end, fun(_Node, _Idx) ->create_view end).

-spec 'grant_option'(input(), index()) -> parse_result().
'grant_option'(Input, Index) ->
  p(Input, Index, 'grant_option', fun(I,D) -> (p_regexp(<<"(?i)grant +option">>))(I,D) end, fun(_Node, _Idx) ->grant_option end).

-spec 'print'(input(), index()) -> parse_result().
'print'(Input, Index) ->
  p(Input, Index, 'print', fun(I,D) -> (p_regexp(<<"(?i)print">>))(I,D) end, fun(_Node, _Idx) ->print end).

-spec 'write'(input(), index()) -> parse_result().
'write'(Input, Index) ->
  p(Input, Index, 'write', fun(I,D) -> (p_regexp(<<"(?i)write">>))(I,D) end, fun(_Node, _Idx) ->write end).

-spec 'read'(input(), index()) -> parse_result().
'read'(Input, Index) ->
  p(Input, Index, 'read', fun(I,D) -> (p_regexp(<<"(?i)read">>))(I,D) end, fun(_Node, _Idx) ->read end).

-spec 'execute'(input(), index()) -> parse_result().
'execute'(Input, Index) ->
  p(Input, Index, 'execute', fun(I,D) -> (p_regexp(<<"(?i)execute">>))(I,D) end, fun(_Node, _Idx) ->execute end).

-spec 'create'(input(), index()) -> parse_result().
'create'(Input, Index) ->
  p(Input, Index, 'create', fun(I,D) -> (p_regexp(<<"(?i)create">>))(I,D) end, fun(_Node, _Idx) ->create end).

-spec 'usage'(input(), index()) -> parse_result().
'usage'(Input, Index) ->
  p(Input, Index, 'usage', fun(I,D) -> (p_regexp(<<"(?i)usage">>))(I,D) end, fun(_Node, _Idx) ->usage end).

-spec 'trigger'(input(), index()) -> parse_result().
'trigger'(Input, Index) ->
  p(Input, Index, 'trigger', fun(I,D) -> (p_regexp(<<"(?i)trigger">>))(I,D) end, fun(_Node, _Idx) ->trigger end).

-spec 'super'(input(), index()) -> parse_result().
'super'(Input, Index) ->
  p(Input, Index, 'super', fun(I,D) -> (p_regexp(<<"(?i)super">>))(I,D) end, fun(_Node, _Idx) ->super end).

-spec 'shutdown'(input(), index()) -> parse_result().
'shutdown'(Input, Index) ->
  p(Input, Index, 'shutdown', fun(I,D) -> (p_regexp(<<"(?i)shutdown">>))(I,D) end, fun(_Node, _Idx) ->shutdown end).

-spec 'show_view'(input(), index()) -> parse_result().
'show_view'(Input, Index) ->
  p(Input, Index, 'show_view', fun(I,D) -> (p_regexp(<<"(?i)show +view">>))(I,D) end, fun(_Node, _Idx) ->show_view end).

-spec 'show_dbs'(input(), index()) -> parse_result().
'show_dbs'(Input, Index) ->
  p(Input, Index, 'show_dbs', fun(I,D) -> (p_regexp(<<"(?i)show +databases">>))(I,D) end, fun(_Node, _Idx) ->show_dbs end).

-spec 'repl_slave'(input(), index()) -> parse_result().
'repl_slave'(Input, Index) ->
  p(Input, Index, 'repl_slave', fun(I,D) -> (p_regexp(<<"(?i)replication +slave">>))(I,D) end, fun(_Node, _Idx) ->repl_slave end).

-spec 'repl_client'(input(), index()) -> parse_result().
'repl_client'(Input, Index) ->
  p(Input, Index, 'repl_client', fun(I,D) -> (p_regexp(<<"(?i)replication +client">>))(I,D) end, fun(_Node, _Idx) ->repl_client end).

-spec 'reload'(input(), index()) -> parse_result().
'reload'(Input, Index) ->
  p(Input, Index, 'reload', fun(I,D) -> (p_regexp(<<"(?i)reload">>))(I,D) end, fun(_Node, _Idx) ->reload end).

-spec 'references'(input(), index()) -> parse_result().
'references'(Input, Index) ->
  p(Input, Index, 'references', fun(I,D) -> (p_regexp(<<"(?i)references">>))(I,D) end, fun(_Node, _Idx) ->references end).

-spec 'process'(input(), index()) -> parse_result().
'process'(Input, Index) ->
  p(Input, Index, 'process', fun(I,D) -> (p_regexp(<<"(?i)process">>))(I,D) end, fun(_Node, _Idx) ->process end).

-spec 'lock_tables'(input(), index()) -> parse_result().
'lock_tables'(Input, Index) ->
  p(Input, Index, 'lock_tables', fun(I,D) -> (p_regexp(<<"(?i)lock +tables">>))(I,D) end, fun(_Node, _Idx) ->lock_tables end).

-spec 'index'(input(), index()) -> parse_result().
'index'(Input, Index) ->
  p(Input, Index, 'index', fun(I,D) -> (p_regexp(<<"(?i)index">>))(I,D) end, fun(_Node, _Idx) ->index end).

-spec 'file'(input(), index()) -> parse_result().
'file'(Input, Index) ->
  p(Input, Index, 'file', fun(I,D) -> (p_regexp(<<"(?i)file">>))(I,D) end, fun(_Node, _Idx) ->file end).

-spec 'event'(input(), index()) -> parse_result().
'event'(Input, Index) ->
  p(Input, Index, 'event', fun(I,D) -> (p_regexp(<<"(?i)event">>))(I,D) end, fun(_Node, _Idx) ->event end).

-spec 'create_temp_tables'(input(), index()) -> parse_result().
'create_temp_tables'(Input, Index) ->
  p(Input, Index, 'create_temp_tables', fun(I,D) -> (p_regexp(<<"(?i)create +temporary +tables">>))(I,D) end, fun(_Node, _Idx) ->create_temp_tables end).

-spec 'create_routine'(input(), index()) -> parse_result().
'create_routine'(Input, Index) ->
  p(Input, Index, 'create_routine', fun(I,D) -> (p_regexp(<<"(?i)create +routine">>))(I,D) end, fun(_Node, _Idx) ->create_routine end).

-spec 'alter'(input(), index()) -> parse_result().
'alter'(Input, Index) ->
  p(Input, Index, 'alter', fun(I,D) -> (p_regexp(<<"(?i)alter">>))(I,D) end, fun(_Node, _Idx) ->alter end).

-spec 'alter_routine'(input(), index()) -> parse_result().
'alter_routine'(Input, Index) ->
  p(Input, Index, 'alter_routine', fun(I,D) -> (p_regexp(<<"(?i)alter +routine">>))(I,D) end, fun(_Node, _Idx) ->alter_routine end).

-spec 'all_privileges'(input(), index()) -> parse_result().
'all_privileges'(Input, Index) ->
  p(Input, Index, 'all_privileges', fun(I,D) -> (p_regexp(<<"(?i)all +privileges">>))(I,D) end, fun(_Node, _Idx) ->all_privileges end).

-spec 'with'(input(), index()) -> parse_result().
'with'(Input, Index) ->
  p(Input, Index, 'with', fun(I,D) -> (p_regexp(<<"(?i)with">>))(I,D) end, fun(_Node, _Idx) ->with end).

-spec 'to'(input(), index()) -> parse_result().
'to'(Input, Index) ->
  p(Input, Index, 'to', fun(I,D) -> (p_regexp(<<"(?i)to">>))(I,D) end, fun(_Node, _Idx) ->to end).

-spec 'on'(input(), index()) -> parse_result().
'on'(Input, Index) ->
  p(Input, Index, 'on', fun(I,D) -> (p_regexp(<<"(?i)on">>))(I,D) end, fun(_Node, _Idx) ->on end).

-spec 'grant'(input(), index()) -> parse_result().
'grant'(Input, Index) ->
  p(Input, Index, 'grant', fun(I,D) -> (p_regexp(<<"(?i)grant">>))(I,D) end, fun(_Node, _Idx) ->grant end).

-spec 'password'(input(), index()) -> parse_result().
'password'(Input, Index) ->
  p(Input, Index, 'password', fun(I,D) -> (p_regexp(<<"(?i)password">>))(I,D) end, fun(_Node, _Idx) ->password end).

-spec 'identified'(input(), index()) -> parse_result().
'identified'(Input, Index) ->
  p(Input, Index, 'identified', fun(I,D) -> (p_regexp(<<"(?i)identified +by ">>))(I,D) end, fun(_Node, _Idx) ->identified end).

-spec 'user'(input(), index()) -> parse_result().
'user'(Input, Index) ->
  p(Input, Index, 'user', fun(I,D) -> (p_regexp(<<"(?i)user">>))(I,D) end, fun(_Node, _Idx) ->user end).

-spec 'drop'(input(), index()) -> parse_result().
'drop'(Input, Index) ->
  p(Input, Index, 'drop', fun(I,D) -> (p_regexp(<<"(?i)drop">>))(I,D) end, fun(_Node, _Idx) ->drop end).

-spec 'database'(input(), index()) -> parse_result().
'database'(Input, Index) ->
  p(Input, Index, 'database', fun(I,D) -> (fun 'key'/2)(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'commit'(input(), index()) -> parse_result().
'commit'(Input, Index) ->
  p(Input, Index, 'commit', fun(I,D) -> (p_regexp(<<"(?i)commit">>))(I,D) end, fun(_Node, _Idx) ->commit end).

-spec 'rollback'(input(), index()) -> parse_result().
'rollback'(Input, Index) ->
  p(Input, Index, 'rollback', fun(I,D) -> (p_regexp(<<"(?i)rollback">>))(I,D) end, fun(_Node, _Idx) ->rollback end).

-spec 'keys'(input(), index()) -> parse_result().
'keys'(Input, Index) ->
  p(Input, Index, 'keys', fun(I,D) -> (p_seq([p_label('head', fun 'key'/2), p_label('tail', p_zero_or_more(p_seq([p_optional(fun 'space'/2), p_string(<<",">>), p_optional(fun 'space'/2), fun 'key'/2])))]))(I,D) end, fun(Node, _Idx) ->
    [proplists:get_value(head, Node)|[ lists:nth(4,I) || I <- proplists:get_value(tail, Node) ]]
 end).

-spec 'key'(input(), index()) -> parse_result().
'key'(Input, Index) ->
  p(Input, Index, 'key', fun(I,D) -> (p_choose([p_seq([p_string(<<"`">>), p_label('chars', p_one_or_more(p_seq([p_not(p_string(<<"`">>)), p_choose([p_string(<<"\\\\">>), p_string(<<"\\`">>), p_anything()])]))), p_string(<<"`">>)]), p_seq([p_charclass(<<"[a-zA-Z]">>), p_zero_or_more(p_charclass(<<"[A-zA-Z0-9_]">>))])]))(I,D) end, fun(Node, _Idx) ->
    case length(Node) of
        3 -> iolist_to_binary(proplists:get_value(chars, Node));
        2 -> iolist_to_binary([lists:nth(1,Node)|lists:nth(2,Node)])
    end
 end).

-spec 'value'(input(), index()) -> parse_result().
'value'(Input, Index) ->
  p(Input, Index, 'value', fun(I,D) -> (p_choose([fun 'string'/2, fun 'null'/2, fun 'number'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'var'(input(), index()) -> parse_result().
'var'(Input, Index) ->
  p(Input, Index, 'var', fun(I,D) -> (p_seq([p_string(<<"@">>), p_optional(p_string(<<"@">>)), fun 'key'/2]))(I,D) end, fun(Node, _Idx) ->
    [_L,G,Key] = Node,
    Scope = case G of
        [] -> global;
        _ -> local
    end,
    #variable{name=Key, scope=Scope}
 end).

-spec 'set_var'(input(), index()) -> parse_result().
'set_var'(Input, Index) ->
  p(Input, Index, 'set_var', fun(I,D) -> (p_seq([p_optional(p_string(<<"@">>)), p_optional(p_string(<<"@">>)), fun 'key'/2]))(I,D) end, fun(Node, _Idx) ->
    [L,G,Key] = Node,
    Scope = if
        L == [] andalso G == [] -> session;
        G == [] -> global;
        true -> local
    end,
    #variable{name=Key, scope=Scope}
 end).

-spec 'string'(input(), index()) -> parse_result().
'string'(Input, Index) ->
  p(Input, Index, 'string', fun(I,D) -> (p_seq([p_string(<<"\'">>), p_zero_or_more(p_seq([p_seq([p_not(p_string(<<"\'">>)), p_choose([p_string(<<"\\\\">>), p_string(<<"\\\'">>), p_anything()])]), p_optional(p_string(<<"\'\'">>))])), p_string(<<"\'">>)]))(I,D) end, fun(Node, _Idx) ->binary:replace(iolist_to_binary(lists:nth(2, Node)), <<"''">>, <<"'">>) end).

-spec 'number'(input(), index()) -> parse_result().
'number'(Input, Index) ->
  p(Input, Index, 'number', fun(I,D) -> (p_choose([fun 'float'/2, fun 'integer'/2]))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'integer'(input(), index()) -> parse_result().
'integer'(Input, Index) ->
  p(Input, Index, 'integer', fun(I,D) -> (p_one_or_more(p_charclass(<<"[0-9]">>)))(I,D) end, fun(Node, _Idx) ->
    list_to_integer(lists:flatten([ binary_to_list(I) || I <- Node ]))
 end).

-spec 'float'(input(), index()) -> parse_result().
'float'(Input, Index) ->
  p(Input, Index, 'float', fun(I,D) -> (p_seq([p_zero_or_more(p_charclass(<<"[0-9]">>)), p_string(<<".">>), p_one_or_more(p_charclass(<<"[0-9]">>))]))(I,D) end, fun(Node, _Idx) ->
    case Node of
        [Int,_,Dec] when Int =/= [] ->
            list_to_float(
                lists:flatten([ binary_to_list(I) || I <- Int ]) ++ "." ++
                lists:flatten([ binary_to_list(D) || D <- Dec ])
            );
        [_,_,[Dec]] ->
            list_to_float("0." ++ lists:flatten([ binary_to_list(D) || D <- Dec ]))
    end
 end).

-spec 'space'(input(), index()) -> parse_result().
'space'(Input, Index) ->
  p(Input, Index, 'space', fun(I,D) -> (p_zero_or_more(p_charclass(<<"[\s\t\n\s\r]">>)))(I,D) end, fun(Node, _Idx) ->Node end).

-spec 'null'(input(), index()) -> parse_result().
'null'(Input, Index) ->
  p(Input, Index, 'null', fun(I,D) -> (p_seq([p_charclass(<<"[nN]">>), p_charclass(<<"[uU]">>), p_charclass(<<"[lL]">>), p_charclass(<<"[lL]">>)]))(I,D) end, fun(_Node, _Idx) ->null end).



-file("peg_includes.hrl", 1).
-type index() :: {{line, pos_integer()}, {column, pos_integer()}}.
-type input() :: binary().
-type parse_failure() :: {fail, term()}.
-type parse_success() :: {term(), input(), index()}.
-type parse_result() :: parse_failure() | parse_success().
-type parse_fun() :: fun((input(), index()) -> parse_result()).
-type xform_fun() :: fun((input(), index()) -> term()).

-spec p(input(), index(), atom(), parse_fun(), xform_fun()) -> parse_result().
p(Inp, StartIndex, Name, ParseFun, TransformFun) ->
  case get_memo(StartIndex, Name) of      % See if the current reduction is memoized
    {ok, Memo} -> %Memo;                     % If it is, return the stored result
      Memo;
    _ ->                                        % If not, attempt to parse
      Result = case ParseFun(Inp, StartIndex) of
        {fail,_} = Failure ->                       % If it fails, memoize the failure
          Failure;
        {Match, InpRem, NewIndex} ->               % If it passes, transform and memoize the result.
          Transformed = TransformFun(Match, StartIndex),
          {Transformed, InpRem, NewIndex}
      end,
      memoize(StartIndex, Name, Result),
      Result
  end.

-spec setup_memo() -> ets:tid().
setup_memo() ->
  put({parse_memo_table, ?MODULE}, ets:new(?MODULE, [set])).

-spec release_memo() -> true.
release_memo() ->
  ets:delete(memo_table_name()).

-spec memoize(index(), atom(), parse_result()) -> true.
memoize(Index, Name, Result) ->
  Memo = case ets:lookup(memo_table_name(), Index) of
              [] -> [];
              [{Index, Plist}] -> Plist
         end,
  ets:insert(memo_table_name(), {Index, [{Name, Result}|Memo]}).

-spec get_memo(index(), atom()) -> {ok, term()} | {error, not_found}.
get_memo(Index, Name) ->
  case ets:lookup(memo_table_name(), Index) of
    [] -> {error, not_found};
    [{Index, Plist}] ->
      case proplists:lookup(Name, Plist) of
        {Name, Result}  -> {ok, Result};
        _  -> {error, not_found}
      end
    end.

-spec memo_table_name() -> ets:tid().
memo_table_name() ->
    get({parse_memo_table, ?MODULE}).

-ifdef(p_eof).
-spec p_eof() -> parse_fun().
p_eof() ->
  fun(<<>>, Index) -> {eof, [], Index};
     (_, Index) -> {fail, {expected, eof, Index}} end.
-endif.

-ifdef(p_optional).
-spec p_optional(parse_fun()) -> parse_fun().
p_optional(P) ->
  fun(Input, Index) ->
      case P(Input, Index) of
        {fail,_} -> {[], Input, Index};
        {_, _, _} = Success -> Success
      end
  end.
-endif.

-ifdef(p_not).
-spec p_not(parse_fun()) -> parse_fun().
p_not(P) ->
  fun(Input, Index)->
      case P(Input,Index) of
        {fail,_} ->
          {[], Input, Index};
        {Result, _, _} -> {fail, {expected, {no_match, Result},Index}}
      end
  end.
-endif.

-ifdef(p_assert).
-spec p_assert(parse_fun()) -> parse_fun().
p_assert(P) ->
  fun(Input,Index) ->
      case P(Input,Index) of
        {fail,_} = Failure-> Failure;
        _ -> {[], Input, Index}
      end
  end.
-endif.

-ifdef(p_seq).
-spec p_seq([parse_fun()]) -> parse_fun().
p_seq(P) ->
  fun(Input, Index) ->
      p_all(P, Input, Index, [])
  end.

-spec p_all([parse_fun()], input(), index(), [term()]) -> parse_result().
p_all([], Inp, Index, Accum ) -> {lists:reverse( Accum ), Inp, Index};
p_all([P|Parsers], Inp, Index, Accum) ->
  case P(Inp, Index) of
    {fail, _} = Failure -> Failure;
    {Result, InpRem, NewIndex} -> p_all(Parsers, InpRem, NewIndex, [Result|Accum])
  end.
-endif.

-ifdef(p_choose).
-spec p_choose([parse_fun()]) -> parse_fun().
p_choose(Parsers) ->
  fun(Input, Index) ->
      p_attempt(Parsers, Input, Index, none)
  end.

-spec p_attempt([parse_fun()], input(), index(), none | parse_failure()) -> parse_result().
p_attempt([], _Input, _Index, Failure) -> Failure;
p_attempt([P|Parsers], Input, Index, FirstFailure)->
  case P(Input, Index) of
    {fail, _} = Failure ->
      case FirstFailure of
        none -> p_attempt(Parsers, Input, Index, Failure);
        _ -> p_attempt(Parsers, Input, Index, FirstFailure)
      end;
    Result -> Result
  end.
-endif.

-ifdef(p_zero_or_more).
-spec p_zero_or_more(parse_fun()) -> parse_fun().
p_zero_or_more(P) ->
  fun(Input, Index) ->
      p_scan(P, Input, Index, [])
  end.
-endif.

-ifdef(p_one_or_more).
-spec p_one_or_more(parse_fun()) -> parse_fun().
p_one_or_more(P) ->
  fun(Input, Index)->
      Result = p_scan(P, Input, Index, []),
      case Result of
        {[_|_], _, _} ->
          Result;
        _ ->
          {fail, {expected, Failure, _}} = P(Input,Index),
          {fail, {expected, {at_least_one, Failure}, Index}}
      end
  end.
-endif.

-ifdef(p_label).
-spec p_label(atom(), parse_fun()) -> parse_fun().
p_label(Tag, P) ->
  fun(Input, Index) ->
      case P(Input, Index) of
        {fail,_} = Failure ->
           Failure;
        {Result, InpRem, NewIndex} ->
          {{Tag, Result}, InpRem, NewIndex}
      end
  end.
-endif.

-ifdef(p_scan).
-spec p_scan(parse_fun(), input(), index(), [term()]) -> {[term()], input(), index()}.
p_scan(_, <<>>, Index, Accum) -> {lists:reverse(Accum), <<>>, Index};
p_scan(P, Inp, Index, Accum) ->
  case P(Inp, Index) of
    {fail,_} -> {lists:reverse(Accum), Inp, Index};
    {Result, InpRem, NewIndex} -> p_scan(P, InpRem, NewIndex, [Result | Accum])
  end.
-endif.

-ifdef(p_string).
-spec p_string(binary()) -> parse_fun().
p_string(S) ->
    Length = erlang:byte_size(S),
    fun(Input, Index) ->
      try
          <<S:Length/binary, Rest/binary>> = Input,
          {S, Rest, p_advance_index(S, Index)}
      catch
          error:{badmatch,_} -> {fail, {expected, {string, S}, Index}}
      end
    end.
-endif.

-ifdef(p_anything).
-spec p_anything() -> parse_fun().
p_anything() ->
  fun(<<>>, Index) -> {fail, {expected, any_character, Index}};
     (Input, Index) when is_binary(Input) ->
          <<C/utf8, Rest/binary>> = Input,
          {<<C/utf8>>, Rest, p_advance_index(<<C/utf8>>, Index)}
  end.
-endif.

-ifdef(p_charclass).
-spec p_charclass(string() | binary()) -> parse_fun().
p_charclass(Class) ->
    {ok, RE} = re:compile(Class, [unicode, dotall]),
    fun(Inp, Index) ->
            case re:run(Inp, RE, [anchored]) of
                {match, [{0, Length}|_]} ->
                    {Head, Tail} = erlang:split_binary(Inp, Length),
                    {Head, Tail, p_advance_index(Head, Index)};
                _ -> {fail, {expected, {character_class, binary_to_list(Class)}, Index}}
            end
    end.
-endif.

-ifdef(p_regexp).
-spec p_regexp(binary()) -> parse_fun().
p_regexp(Regexp) ->
    {ok, RE} = re:compile(Regexp, [unicode, dotall, anchored]),
    fun(Inp, Index) ->
        case re:run(Inp, RE) of
            {match, [{0, Length}|_]} ->
                {Head, Tail} = erlang:split_binary(Inp, Length),
                {Head, Tail, p_advance_index(Head, Index)};
            _ -> {fail, {expected, {regexp, binary_to_list(Regexp)}, Index}}
        end
    end.
-endif.

-ifdef(line).
-spec line(index() | term()) -> pos_integer() | undefined.
line({{line,L},_}) -> L;
line(_) -> undefined.
-endif.

-ifdef(column).
-spec column(index() | term()) -> pos_integer() | undefined.
column({_,{column,C}}) -> C;
column(_) -> undefined.
-endif.

-spec p_advance_index(input() | unicode:charlist() | pos_integer(), index()) -> index().
p_advance_index(MatchedInput, Index) when is_list(MatchedInput) orelse is_binary(MatchedInput)-> % strings
  lists:foldl(fun p_advance_index/2, Index, unicode:characters_to_list(MatchedInput));
p_advance_index(MatchedInput, Index) when is_integer(MatchedInput) -> % single characters
  {{line, Line}, {column, Col}} = Index,
  case MatchedInput of
    $\n -> {{line, Line+1}, {column, 1}};
    _ -> {{line, Line}, {column, Col+1}}
  end.
