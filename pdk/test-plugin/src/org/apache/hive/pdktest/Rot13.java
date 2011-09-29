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
package org.apache.hive.pdktest;

import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Example UDF for rot13 transformation.
 */
@Description(name = "rot13",
  value = "_FUNC_(str) - Returns str with all characters transposed via rot13",
  extended = "Example:\n"
  + "  > SELECT _FUNC_('Facebook') FROM src LIMIT 1;\n" + "  'Snprobbx'")
@HivePdkUnitTests(
    setup = "create table rot13_data(s string); "
    + "insert overwrite table rot13_data select 'Facebook' from onerow;",
    cleanup = "drop table if exists rot13_data;",
    cases = {
      @HivePdkUnitTest(
        query = "SELECT tp_rot13('Mixed Up!') FROM onerow;",
        result = "Zvkrq Hc!"),
      @HivePdkUnitTest(
        query = "SELECT tp_rot13(s) FROM rot13_data;",
        result = "Snprobbx")
    }
  )
public class Rot13 extends UDF {
  private Text t = new Text();

  public Rot13() {
  }

  public Text evaluate(Text s) {
    StringBuilder out = new StringBuilder(s.getLength());
    char[] ca = s.toString().toCharArray();
    for (char c : ca) {
      if (c >= 'a' && c <= 'm') {
        c += 13;
      } else if (c >= 'n' && c <= 'z') {
        c -= 13;
      } else if (c >= 'A' && c <= 'M') {
        c += 13;
      } else if (c >= 'N' && c <= 'Z') {
        c -= 13;
      }
      out.append(c);
    }
    t.set(out.toString());
    return t;
  }
}
