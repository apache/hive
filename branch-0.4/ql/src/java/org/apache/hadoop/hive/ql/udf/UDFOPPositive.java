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

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@description(
    name = "positive",
    value = "_FUNC_ a - Returns a"
)
public class UDFOPPositive extends UDFBaseNumericUnaryOp {

  private static Log LOG = LogFactory.getLog(UDFOPPositive.class.getName());

  public UDFOPPositive() {
  }


  @Override
  public ByteWritable evaluate(ByteWritable a) {
    return a;
  }

  @Override
  public ShortWritable evaluate(ShortWritable a) {
    return a;
  }

  @Override
  public IntWritable evaluate(IntWritable a) {
    return a;
  }

  @Override
  public LongWritable evaluate(LongWritable a) {
    return a;
  }

  @Override
  public FloatWritable evaluate(FloatWritable a) {
    return a;
  }

  @Override
  public DoubleWritable evaluate(DoubleWritable a) {
    return a;
  }

}
