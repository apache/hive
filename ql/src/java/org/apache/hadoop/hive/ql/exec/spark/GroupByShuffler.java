package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

public class GroupByShuffler implements SparkShuffler {

  @Override
  public JavaPairRDD<BytesWritable, Iterable<BytesWritable>> shuffle(
      JavaPairRDD<BytesWritable, BytesWritable> input) {
    return input.groupByKey(/* default to hash partition */);
  }

}
