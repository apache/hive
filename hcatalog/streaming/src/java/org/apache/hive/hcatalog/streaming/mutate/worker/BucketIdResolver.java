package org.apache.hive.hcatalog.streaming.mutate.worker;

/** Computes and appends bucket ids to records that are due to be inserted. */
public interface BucketIdResolver {

  Object attachBucketIdToRecord(Object record);

  /** See: {@link org.apache.hadoop.hive.ql.exec.ReduceSinkOperator#computeBucketNumber(Object, int)}. */
  int computeBucketId(Object record);

}
