% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-compile([{parse_transform, lager_transform}]).
-define(NUM_SHARDS,12).
-define(PAGESIZE,4096).
-define(DEF_CACHE_PAGES,10).
% -define(NAMESPACE_MAX,134217728).
-define(NAMESPACE_MAX,4294967295).
-define(MULTIUPDATE_TYPE,'__mupdate__').
-define(CLUSTEREVENTS_TYPE,'__clusterevents__').
-define(STATE_TYPE,'__state__').

-define(STATE_NM_LOCAL,<<"local">>).
-define(STATE_NM_GLOBAL,<<"global">>).

-define(MAXCOUNTERS,10).
-define(COUNTER_SQLSIZE,0).
-define(COUNTER_REQSNOW,1).
-define(COUNTER_READS,2).
-define(COUNTER_WRITES,3).
-define(COUNTER_TIME,4).


% -define(ADBG(F),lager:debug([$s,$=,$~,$p,$\s|F],[erlang:system_info(scheduler_id)])).
% -define(ADBG(F,A),lager:debug([$s,$=,$~,$p,$\s|F],[erlang:system_info(scheduler_id)|A])).
-define(ADBG(F),lager:debug(F)).
-define(ADBG(F,A),lager:debug(F,A)).
-define(AINF(F),lager:info(F)).
-define(AINF(F,A),lager:info(F,A)).
-define(AERR(F),lager:error(F)).
-define(AERR(F,A),lager:error(F,A)).
% -define(AERR(F),lager:error([$s,$=,$~,$p,$\s|F],[erlang:system_info(scheduler_id)])).
% -define(AERR(F,A),lager:error([$s,$=,$~,$p,$\s|F],[erlang:system_info(scheduler_id)|A])).

% Database Administration Statements
-record(table, {name, alias}).
-record(all, {table}).
-record(subquery, {name, subquery }).
-record(key, {alias, name, table}).
-record(value, {name, value}).
-record(condition, {nexo, op1, op2}).
-record(function, {name, params, alias}).
-record(operation, {type, op1, op2}).
-record(variable, {name, label, scope}).

-record(system_set, {'query'}).

% SHOW
-record(show, {type, full, from}).

-type show() :: #show{}.

% SELECT
-record(select, {params, tables, conditions, group, order, limit, offset}).
-record(order, {key, sort}).

-type select() :: #select{}.

% UPDATE
-record(update, {table, set, conditions}).
-record(set, {key, value}).

-type update() :: #update{}.

% DELETE
-record(delete, {table, conditions}).

-type delete() :: #delete{}.

% INSERT
-record(insert, {table, values}).

-type insert() :: #insert{}.

-type sql() :: show() | select() | update() | delete() | insert().

-record(management, {action :: action(), data :: account() | permission() }).
-record(account, {access}).
-record(permission, {on, account, conditions}).

-type action() :: create | drop | grant | rename | revoke | setpasswd.
-type account() :: #account{}.
-type permission() :: #permission{}.
