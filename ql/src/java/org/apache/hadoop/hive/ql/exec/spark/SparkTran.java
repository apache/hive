package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

public interface SparkTran {
  JavaPairRDD<BytesWritable, BytesWritable> transform(
		  JavaPairRDD<BytesWritable, BytesWritable> input);
}
