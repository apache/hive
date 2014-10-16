/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark.counter;

import java.io.Serializable;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkCounter implements Serializable {

  private String name;
  private String displayName;
  private Accumulator<Long> accumulator;

  public SparkCounter(
    String name,
    String displayName,
    String groupName,
    long initValue,
    JavaSparkContext sparkContext) {

    this.name = name;
    this.displayName = displayName;
    LongAccumulatorParam longParam = new LongAccumulatorParam();
    String accumulatorName = groupName + "_" + name;
    this.accumulator = sparkContext.accumulator(initValue, accumulatorName, longParam);
  }

  public long getValue() {
    return accumulator.value();
  }

  public void increment(long incr) {
    accumulator.add(incr);
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  class LongAccumulatorParam implements AccumulatorParam<Long> {

    @Override
    public Long addAccumulator(Long t1, Long t2) {
      return t1 + t2;
    }

    @Override
    public Long addInPlace(Long r1, Long r2) {
      return r1 + r2;
    }

    @Override
    public Long zero(Long initialValue) {
      return 0L;
    }
  }

}
