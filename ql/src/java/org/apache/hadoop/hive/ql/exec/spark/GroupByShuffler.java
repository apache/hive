package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

public class GroupByShuffler implements SparkShuffler {

  @Override
  public JavaPairRDD<HiveKey, Iterable<BytesWritable>> shuffle(
      JavaPairRDD<HiveKey, BytesWritable> input, int numPartitions) {
    if (numPartitions > 0) {
      return input.groupByKey(numPartitions);
    }
    return input.groupByKey();
  }

}
