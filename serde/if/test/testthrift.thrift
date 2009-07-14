
namespace java org.apache.hadoop.hive.serde.test


struct InnerStruct {
  1: i32 field0
}

struct ThriftTestObj {
  1: i32 field1,
  2: string field2,
  3: list<InnerStruct> field3
}
