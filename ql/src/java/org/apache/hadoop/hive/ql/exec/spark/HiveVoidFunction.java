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
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Implementation of a voidFunction that does nothing.
 */
public class HiveVoidFunction implements VoidFunction<Tuple2<HiveKey, BytesWritable>> {
  private static final long serialVersionUID = 1L;

  private static HiveVoidFunction instance = new HiveVoidFunction();

  public static HiveVoidFunction getInstance() {
    return instance;
  }

  private HiveVoidFunction() {
  }

  @Override
  public void call(Tuple2<HiveKey, BytesWritable> t) throws Exception {
  }

}
