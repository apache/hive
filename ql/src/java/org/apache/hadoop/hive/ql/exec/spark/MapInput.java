/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MapInput implements SparkTran<BytesWritable, BytesWritable, HiveKey, BytesWritable> {
  private JavaPairRDD<HiveKey, BytesWritable> hadoopRDD;
  private boolean toCache;

  public MapInput(JavaPairRDD<HiveKey, BytesWritable> hadoopRDD) {
    this.hadoopRDD = hadoopRDD;
  }

  public MapInput(JavaPairRDD<HiveKey, BytesWritable> hadoopRDD, boolean toCache) {
    this.hadoopRDD = hadoopRDD;
    this.toCache = toCache;
  }

  public void setToCache(boolean toCache) {
    this.toCache = toCache;
  }

  @Override
  public JavaPairRDD<HiveKey, BytesWritable> transform(
      JavaPairRDD<BytesWritable, BytesWritable> input) {
    Preconditions.checkArgument(input == null,
        "AssertionError: MapInput doesn't take any input");
    JavaPairRDD result = hadoopRDD;
    if (toCache) {
      result = result.mapToPair(new CopyFunction());
      return result.cache();
    } else {
      return result;
    }
  }

  private static class CopyFunction implements PairFunction<Tuple2<BytesWritable, BytesWritable>,
        BytesWritable, BytesWritable> {

    @Override
    public Tuple2<BytesWritable, BytesWritable> call(Tuple2<BytesWritable, BytesWritable> tup) throws Exception {
      // no need to copy key since it never get used in HiveMapFunction
      BytesWritable value = SparkUtilities.copyBytesWritable(tup._2());
      return new Tuple2<BytesWritable, BytesWritable>(tup._1(), value);
    }
  }

}
