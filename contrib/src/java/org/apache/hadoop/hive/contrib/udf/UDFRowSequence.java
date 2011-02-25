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

package org.apache.hadoop.hive.contrib.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

/**
 * UDFRowSequence.
 */
@Description(name = "row_sequence",
    value = "_FUNC_() - Returns a generated row sequence number starting from 1")
@UDFType(deterministic = false, stateful = true)
public class UDFRowSequence extends UDF
{
  private LongWritable result = new LongWritable();

  public UDFRowSequence() {
    result.set(0);
  }

  public LongWritable evaluate() {
    result.set(result.get() + 1);
    return result;
  }
}

// End UDFRowSequence.java
