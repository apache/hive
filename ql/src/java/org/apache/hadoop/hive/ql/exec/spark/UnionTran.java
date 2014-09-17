/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

public class UnionTran implements SparkTran<HiveKey, HiveKey> {
  List<JavaPairRDD<HiveKey, BytesWritable>> otherInputsList = new LinkedList<JavaPairRDD<HiveKey, BytesWritable>>();

  @Override
  public JavaPairRDD<HiveKey, BytesWritable> transform(
      JavaPairRDD<HiveKey, BytesWritable> input) {
    JavaPairRDD<HiveKey, BytesWritable> result = input;
    for (JavaPairRDD<HiveKey, BytesWritable> otherInput : otherInputsList) {
      result = result.union(otherInput);
    }
    return result;
  }

  public void addOtherInput(JavaPairRDD<HiveKey, BytesWritable> input) {
    otherInputsList.add(input);
  }

  public List<JavaPairRDD<HiveKey, BytesWritable>> getOtherInputList() {
    return this.otherInputsList;
  }
}
