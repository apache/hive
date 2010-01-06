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
package org.apache.hadoop.hive.contrib.mr.example;

import java.util.Iterator;

import org.apache.hadoop.hive.contrib.mr.GenericMR;
import org.apache.hadoop.hive.contrib.mr.Output;
import org.apache.hadoop.hive.contrib.mr.Reducer;

/**
 * Example Reducer (WordCount). 
 */
public final class WordCountReduce {
  public static void main(final String[] args) throws Exception {
    new GenericMR().reduce(System.in, System.out, new Reducer() {
      public void reduce(String key, Iterator<String[]> records, Output output) throws Exception {
        int count = 0;
        
        while (records.hasNext()) {
          // note we use col[1] -- the key is provided again as col[0]
          count += Integer.parseInt(records.next()[1]);
        }
        
        output.collect(new String[] { key, String.valueOf(count) });
      }
    });
  }
}
