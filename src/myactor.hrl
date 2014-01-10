% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-include_lib("actordb.hrl").

-compile({parse_transform,lager_transform}).

%% Version tag
-define(MYACTOR_VER,<<"5.5.0-myactor-proto">>).

%% Error description macro. After this macro is used ErrDesc variable can be used alongiside of myactor_proto:send_err/2 function.
-define(ERR_DESC(Cst,Err), 	_ErrId = butil:tolist(butil:uuid(list_to_binary(io_lib:format("~p",[Err])))), 
							lager:error("myactor exception (errid=~p) (state=~p) (query=~p) : ~p",[_ErrId, Cst, Query, Err]), 
							ErrDesc = list_to_binary(io_lib:format("Error-Id: ~p~nException: ~p ",[_ErrId, Err])) ).

%% Capability flag definitions
-define(CAPABILITY_LONG_PASSWORD, 1).
-define(CAPABILITY_FOUND_ROWS, 2).
-define(CAPABILITY_LONG_FLAG, 4).
-define(CAPABILITY_CONNECT_WITH_DB, 8).
-define(CAPABILITY_NO_SCHEMA, 16#10).
-define(CAPABILITY_COMPRESS, 16#20).
-define(CAPABILITY_ODBC, 16#40).
-define(CAPABILITY_LOCAL_FILES, 16#80).
-define(CAPABILITY_IGNORE_SPACE, 16#100).
-define(CAPABILITY_PROTOCOL_41, 16#200).
-define(CAPABILITY_INTERACTIVE, 16#400).
-define(CAPABILITY_SSL, 16#800).
-define(CAPABILITY_IGNORE_SIGPIPE, 16#1000).
-define(CAPABILITY_TRANSACTIONS, 16#2000).
-define(CAPABILITY_RESERVED, 16#4000).
-define(CAPABILITY_SECURE_CONNECTION, 16#8000).
-define(CAPABILITY_MULTI_STATEMENTS, 16#10000).
-define(CAPABILITY_MULTI_RESULTS, 16#20000).
-define(CAPABILITY_PS_MULTI_RESULTS, 16#40000).
-define(CAPABILITY_PLUGIN_AUTH, 16#80000).
-define(CAPABILITY_CONNECT_ATTRS, 16#100000).
-define(CAPABILITY_PLUGIN_AUTH_LENENC_CLIENT_DATA, 16#200000).

-define(CAPABILITY_MAP,[
        {?CAPABILITY_LONG_PASSWORD,long_password},
        {?CAPABILITY_FOUND_ROWS,found_rows},
        {?CAPABILITY_LONG_FLAG, long_flag},
        {?CAPABILITY_CONNECT_WITH_DB, connect_with_db},
        {?CAPABILITY_NO_SCHEMA, no_schema},
        {?CAPABILITY_COMPRESS, compress},
        {?CAPABILITY_ODBC, odbc},
        {?CAPABILITY_LOCAL_FILES, local_files},
        {?CAPABILITY_IGNORE_SPACE, ignore_space},
        {?CAPABILITY_PROTOCOL_41, protocol_41},
        {?CAPABILITY_INTERACTIVE, interactive},
        {?CAPABILITY_SSL, ssl},
        {?CAPABILITY_IGNORE_SIGPIPE, ignore_sigpipe},
        {?CAPABILITY_TRANSACTIONS, transactions},
        {?CAPABILITY_SECURE_CONNECTION, secure_connection},
        {?CAPABILITY_MULTI_STATEMENTS, multi_statements},
        {?CAPABILITY_MULTI_RESULTS, multi_results},
        {?CAPABILITY_PS_MULTI_RESULTS, ps_multi_results},
        {?CAPABILITY_PLUGIN_AUTH, plugin_auth},
        {?CAPABILITY_CONNECT_ATTRS, connect_attrs},
        {?CAPABILITY_PLUGIN_AUTH_LENENC_CLIENT_DATA, auth_lenenc_client_data} ]).

%% Header response codes 
-define(OK_HEADER,16#00).
-define(ERR_HEADER,16#ff).
-define(EOF_HEADER, 16#FE).

%% Null definition
-define(NULL, <<16#fb>>).

%% Command code values
-define(COM_SLEEP, 0).
-define(COM_QUIT, 1).
-define(COM_INIT_DB, 2).
-define(COM_QUERY, 3).
-define(COM_FIELD_LIST, 4).
-define(COM_CREATE_DB, 5).
-define(COM_DROP_DB, 6).
-define(COM_REFRESH, 7).
-define(COM_SHUTDOWN, 8).
-define(COM_STATISTICS, 9).
-define(COM_PROCESS_INFO, 10).
-define(COM_CONNECT, 11).
-define(COM_PROCESS_KILL, 12).
-define(COM_DEBUG, 13).
-define(COM_PING, 14).
-define(COM_TIME, 15).
-define(COM_DELAYED_INSERT, 16).
-define(COM_CHANGE_USER, 17).
-define(COM_BINLOG_DUMP, 18).
-define(COM_TABLE_DUMP, 19).
-define(COM_CONNECT_OUT, 20).
-define(COM_REGISTER_SLAVE, 21).
-define(COM_STMT_PREPARE, 22).
-define(COM_STMT_EXECUTE, 23).
-define(COM_STMT_SEND_LONG_DATA, 24).
-define(COM_STMT_CLOSE, 25).
-define(COM_STMT_RESET, 26).
-define(COM_SET_OPTION, 27).
-define(COM_STMT_FETCH, 28).
-define(COM_DAEMON, 29).
-define(COM_BINLOG_DUMP_GTID, 30).

%% Charset values
-define(CHARSET_UTF8, 33).
-define(CHARSET_BINARY, 63).

%% Data type definitions
-define(T_DECIMAL, 0).
-define(T_TINY, 1).
-define(T_SHORT, 2).
-define(T_LONG ,3).
-define(T_FLOAT ,4).
-define(T_DOUBLE ,5).
-define(T_NULL ,6).
-define(T_TIMESTAMP ,7).
-define(T_LONGLONG, 8).
-define(T_INT24, 9).
-define(T_DATE, 10).
-define(T_TIME, 11).
-define(T_DATETIME, 12).
-define(T_YEAR, 13).
-define(T_NEWDATE, 14).
-define(T_VARCHAR, 15).
-define(T_BIT, 16).
-define(T_NEWDECIMAL, 16#f6).
-define(T_ENUM, 16#f7).
-define(T_SET, 16#f8).
-define(T_TINY_BLOB, 16#f9).
-define(T_MEDIUM_BLOB, 16#fa).
-define(T_LONG_BLOB, 16#fb).
-define(T_BLOB, 16#fc).
-define(T_VAR_STRING, 16#fd).
-define(T_STRING, 16#fe).
-define(T_GEOMETRY, 16#ff).

-record(bp_action,{	action = undefined :: atom(),
					state = undefined :: term() }).

% connection state record
-record(cst,{	socket = undefined :: port() , 
				transport = undefined :: module() , 
				phase = undefined :: atom() , 
				buf = <<>> :: binary() , 
				hash = undefined :: binary() , 
				sequenceid = 0 :: integer() , 
				capabilities = [] :: list() , 
				current_actor = undefined :: list() , 
				bp_action = undefined :: #bp_action{} ,  
				queueing = false :: boolean(), 
				query_queue = <<>> :: binary() }).


-record(coltypes,{ 
		defined = false :: boolean(),
		cols = undefined :: term()
	}).