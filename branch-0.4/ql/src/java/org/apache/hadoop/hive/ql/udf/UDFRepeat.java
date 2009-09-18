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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(
    name = "repeat",
    value = "_FUNC_(str, n) - repeat str n times ",
    extended = "Example:\n" +
        "  > SELECT _FUNC_('123', 2) FROM src LIMIT 1;\n" +
        "  '123123'"
    )
public class UDFRepeat extends UDF { 
  private Text result = new Text();
  
  public Text evaluate(Text s, IntWritable n) {
    if (n == null || s == null) {
      return null;
    }
    
    int len = n.get()*s.getLength();
    if(len < 0) {
      len = 0;
    }
    
    byte[] data = result.getBytes();
    
    if(data.length < len) {
      data = new byte[len];
    }
    
    for(int i = 0; i < len; i += s.getLength()) {
      for(int j = 0; j < s.getLength(); j++) {
        data[i + j] = s.getBytes()[j]; 
      }
    }
    
    result.set(data, 0, len);
    return result;
  }
}
