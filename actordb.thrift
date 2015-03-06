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
  3: optional list<string> columns,
  3: optional list<map<string,Val>> rows
}

struct WriteResult
{
  1: required bool success,
  2: optional string error,
  3: optional i64 lastChangeRowid,
  4: optional i64 rowsChanged
}
