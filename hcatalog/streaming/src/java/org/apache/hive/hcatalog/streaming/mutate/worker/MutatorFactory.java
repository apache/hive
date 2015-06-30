package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;

public interface MutatorFactory {

  Mutator newMutator(AcidOutputFormat<?, ?> outputFormat, long transactionId, Path partitionPath, int bucketId) throws IOException;
  
  RecordInspector newRecordInspector();
  
  BucketIdResolver newBucketIdResolver(int totalBuckets);
  
}
