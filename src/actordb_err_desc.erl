-module(actordb_err_desc).
-export([desc/1]).

desc({error,E}) ->
	desc(E);
desc({sql_error,E}) ->
	desc(E);
desc({unknown_actor_type,Type}) ->
	case actordb:types() of
		schema_not_loaded ->
			desc(not_initialized);
		_ ->
			butil:tobin([butil:tobin(Type)," is not a valid type."])
	end;
desc(schema_not_loaded) ->
	case actordb:types() of
		schema_not_loaded ->
			desc(not_initialized);
		_ ->
			<<"Invalid type.">>
	end;
desc(Err) ->
	case Err of
		consensus_timeout ->
			<<"Replication timeout. Write may or may not be replicated successfully.">>;
		consensus_impossible_atm ->
			<<"Cluster can not reach consensus at this time. Query was not executed.">>;
		no_permission ->
			<<"User lacks permission for this query.">>;
		invalid_login ->
			<<"Username and/or password incorrect.">>;
		local_node_missing ->
			<<"This node is not a part of supplied node list.">>;
		missing_group_insert ->
			<<"No valid groups for initialization.">>;
		missing_nodes_insert ->
			<<"No valid nodes for initalization.">>;
		missing_root_user ->
			<<"No valid root user for initialization">>;
		not_initialized ->
			<<"ActorDB needs to be initialized before this query can be executed.">>;
		nocreate ->
			<<"Query without create flag was attempted on an actor which does not exist.">>;
		"not_iolist" ->
			<<"not_iolist, are you using {{...}} in a single actor call?">>;
		_ when is_tuple(Err) ->
			iolist_to_binary([butil:tolist(E)++" "||E<-tuple_to_list(Err)]);
		_ ->
			butil:tobin(Err)
	end.
