/*
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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;

import scala.Tuple2;

public class HiveReduceFunctionResultList<V> extends
    HiveBaseFunctionResultList<Tuple2<HiveKey, V>> {
  private static final long serialVersionUID = 1L;
  private final SparkReduceRecordHandler reduceRecordHandler;

  /**
   * Instantiate result set Iterable for Reduce function output.
   *
   * @param inputIterator Input record iterator.
   * @param reducer Initialized {@link org.apache.hadoop.hive.ql.exec.mr.ExecReducer} instance.
   */
  public HiveReduceFunctionResultList(
      Iterator<Tuple2<HiveKey, V>> inputIterator,
      SparkReduceRecordHandler reducer) {
    super(inputIterator);
    this.reduceRecordHandler = reducer;
  }

  @Override
  protected void processNextRecord(Tuple2<HiveKey, V> inputRecord)
      throws IOException {
    HiveKey key = inputRecord._1();
    V value = inputRecord._2();
    if (value instanceof Iterable) {
      @SuppressWarnings("unchecked")
      Iterable<BytesWritable> values = (Iterable<BytesWritable>)value;
      reduceRecordHandler.<BytesWritable>processRow(key, values.iterator());
    } else {
      reduceRecordHandler.processRow(key, value);
    }
  }

  @Override
  protected boolean processingDone() {
    return false;
  }

  @Override
  protected void closeRecordProcessor() {
    reduceRecordHandler.close();
  }
}
