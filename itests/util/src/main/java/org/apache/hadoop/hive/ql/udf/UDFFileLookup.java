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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * A UDF for testing, which does key/value lookup from a file
 */
@Description(name = "lookup",
value = "_FUNC_(col) - UDF for key/value lookup from a file")
public class UDFFileLookup extends UDF {

  IntWritable result = new IntWritable();

  static Map<String, Integer> data = null;

  private static void loadData() throws Exception {
    // sales.txt will need to be added as a resource.
    File file = new File("./sales.txt");
    BufferedReader br = new BufferedReader(new FileReader(file));
    data = new HashMap<String, Integer>();
    String line = br.readLine();
    while (line != null) {
      String[] parts = line.split("\t", 2);
      if (parts.length == 2) {
        data.put(parts[0], Integer.valueOf(parts[1]));
      }
      line = br.readLine();
    }
    br.close();
  }

  public IntWritable evaluate(Text s) throws Exception {
    if (data == null) {
      loadData();
    }
    Integer val = data.get(s.toString());
    if (val == null) {
      return null;
    }
    result.set(val.intValue());
    return result;
  }
}
