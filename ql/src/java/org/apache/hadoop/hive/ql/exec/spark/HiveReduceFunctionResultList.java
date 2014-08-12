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
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Reporter;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class HiveReduceFunctionResultList extends
    HiveBaseFunctionResultList<Tuple2<BytesWritable, Iterable<BytesWritable>>> {
  private final ExecReducer reducer;

  /**
   * Instantiate result set Iterable for Reduce function output.
   *
   * @param inputIterator Input record iterator.
   * @param reducer Initialized {@link org.apache.hadoop.hive.ql.exec.mr.ExecReducer} instance.
   */
  public HiveReduceFunctionResultList(Configuration conf,
      Iterator<Tuple2<BytesWritable, Iterable<BytesWritable>>> inputIterator,
      ExecReducer reducer) {
    super(conf, inputIterator);
    this.reducer = reducer;
    setOutputCollector();
  }

  @Override
  protected void processNextRecord(Tuple2<BytesWritable, Iterable<BytesWritable>> inputRecord)
      throws IOException {
    reducer.reduce(inputRecord._1(), inputRecord._2().iterator(), this, Reporter.NULL);
  }

  @Override
  protected boolean processingDone() {
    return false;
  }

  @Override
  protected void closeRecordProcessor() {
    reducer.close();
  }

  private void setOutputCollector() {
    if (reducer != null && reducer.getReducer() != null) {
      OperatorUtils.setChildrenCollector(
          Arrays.<Operator<? extends OperatorDesc>>asList(reducer.getReducer()), this);
    }
  }
}
