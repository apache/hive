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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@description(
    name = "abs",
    value = "_FUNC_(x) - returns the absolute value of x",
    extended = "Example:\n" +
    		"  > SELECT _FUNC_(0) FROM src LIMIT 1;\n" +
    		"  0\n" +
    		"  > SELECT _FUNC_(-5) FROM src LIMIT 1;\n" +
    		"  5"
    )
public class UDFAbs extends UDF { 
  
  private DoubleWritable resultDouble = new DoubleWritable();
  private LongWritable resultLong = new LongWritable();
  private IntWritable resultInt = new IntWritable();
  
  public DoubleWritable evaluate(DoubleWritable n) {
    if (n == null) {
      return null;
    }
    
    resultDouble.set(Math.abs(n.get()));
      
    return resultDouble;
  }
  
  public LongWritable evaluate(LongWritable n) {
    if (n == null) {
      return null;
    }
    
    resultLong.set(Math.abs(n.get()));
      
    return resultLong;
  }
  
  public IntWritable evaluate(IntWritable n) {
    if (n == null) {
      return null;
    }
    
    resultInt.set(Math.abs(n.get()));
      
    return resultInt;
  }
}
