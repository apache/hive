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

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@Description(name = "unbase64",
    value = "_FUNC_(str) - Convert the argument from a base 64 string to binary")
public class UDFUnbase64 extends UDF {
  private final transient BytesWritable result = new BytesWritable();

  public BytesWritable evaluate(Text value){
    if (value == null) {
      return null;
    }
    byte[] bytes = new byte[value.getLength()];
    System.arraycopy(value.getBytes(), 0, bytes, 0, value.getLength());
    byte[] decoded = Base64.decodeBase64(bytes);
    result.set(decoded, 0, decoded.length);
    return result;
  }
}