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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import com.google.common.base.Preconditions;


public class MapInput extends CacheTran<WritableComparable, Writable,
    WritableComparable, Writable> {
  private JavaPairRDD<WritableComparable, Writable> hadoopRDD;
  private final SparkPlan sparkPlan;
  private String name = "MapInput";

  public MapInput(SparkPlan sparkPlan, JavaPairRDD<WritableComparable, Writable> hadoopRDD) {
    this(sparkPlan, hadoopRDD, false);
  }

  public MapInput(SparkPlan sparkPlan,
      JavaPairRDD<WritableComparable, Writable> hadoopRDD, boolean toCache) {
    super(toCache);
    this.hadoopRDD = hadoopRDD;
    this.sparkPlan = sparkPlan;
  }

  @Override
  public JavaPairRDD<WritableComparable, Writable> doTransform(
      JavaPairRDD<WritableComparable, Writable> input) {
    Preconditions.checkArgument(input == null,
        "AssertionError: MapInput doesn't take any input");
    JavaPairRDD<WritableComparable, Writable> result;
    if (isCacheEnable()) {
      result = hadoopRDD.mapToPair(new CopyFunction());
    } else {
      result = hadoopRDD;
    }
    return result;
  }

  private static class CopyFunction implements PairFunction<Tuple2<WritableComparable, Writable>,
    WritableComparable, Writable> {

    private transient Configuration conf;

    @Override
    public Tuple2<WritableComparable, Writable>
    call(Tuple2<WritableComparable, Writable> tuple) throws Exception {
      if (conf == null) {
        conf = new Configuration();
      }

      return new Tuple2<WritableComparable, Writable>(tuple._1(),
          WritableUtils.clone(tuple._2(), conf));
    }

  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }
}
