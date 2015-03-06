namespace java com.actordb.thrift
namespace cpp com.actordb
namespace csharp Actordb
namespace py actordb
namespace php actordb
namespace perl Actordb
namespace rb ActordbThrift

const string VERSION = "1.0.0"

enum Null
{
  IsNull = 1
}

union Val {
  1: i64 bigint,
  2: i32 integer,
  3: i16 smallint,
  4: double real,
  5: bool bval,
  6: string text,
  7: Null isnull
}


struct ReadResult
{
  1: required bool success,
  2: optional string error,
  3: optional bool hasMore, // not used yet
  4: optional list<string> columns,
  5: optional list<map<string,Val>> rows
}

struct WriteResult
{
  1: required bool success,
  2: optional string error,
  3: optional i64 lastChangeRowid,
  4: optional i64 rowsChanged
}

struct LoginResult
{
  1: required bool success,
  2: optional string error
  3: optional list<string> readaccess;
  4: optional list<string> writeaccess;
}

union Result
{
  1: ReadResult read,
  2: WriteResult write
}

service Actordb {
  LoginResult login(1: required string username, 2: required string password),

  // query for 1 actor of type
  Result exec_single(1: required string actorname, 2: required string actortype, 3: required string sql, 4: list<string> flags = []),

  // query over some actors of type
  Result exec_multi(1: required list<string> actors, 2: required string actortype, 3: required string sql, 4: list<string> flags = []),

  // query over all actors for type
  Result exec_all(1: required string actortype, 2: required string sql, 3: list<string> flags = []),

  // all in sql: actor sometype(actorname) create; select * from mytab;
  Result exec_sql(1: required string sql)
}

