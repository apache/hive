namespace java org.apache.hcatalog.hbase.snapshot.transaction.thrift
namespace cpp Apache.HCatalog.HBase

struct StoreFamilyRevision {
  1: i64 revision,
  2: i64 timestamp
}

struct StoreFamilyRevisionList {
  1: list<StoreFamilyRevision> revisionList
}
