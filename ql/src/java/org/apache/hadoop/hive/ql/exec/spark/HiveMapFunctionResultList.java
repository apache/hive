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

import org.apache.hadoop.io.BytesWritable;

import scala.Tuple2;

public class HiveMapFunctionResultList extends
    HiveBaseFunctionResultList<Tuple2<BytesWritable, BytesWritable>> {
  private static final long serialVersionUID = 1L;
  private final SparkRecordHandler recordHandler;

  /**
   * Instantiate result set Iterable for Map function output.
   *
   * @param inputIterator Input record iterator.
   * @param handler Initialized {@link SparkMapRecordHandler} instance.
   */
  public HiveMapFunctionResultList(
      Iterator<Tuple2<BytesWritable, BytesWritable>> inputIterator,
      SparkRecordHandler handler) {
    super(inputIterator);
    recordHandler = handler;
  }

  @Override
  protected void processNextRecord(Tuple2<BytesWritable, BytesWritable> inputRecord)
      throws IOException {
    recordHandler.processRow(inputRecord._1(), inputRecord._2());
  }

  @Override
  protected boolean processingDone() {
    return recordHandler.getDone();
  }

  @Override
  protected void closeRecordProcessor() {
    recordHandler.close();
  }
}
