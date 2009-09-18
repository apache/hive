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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

@description(
    name = "rlike,regexp",
    value = "str _FUNC_ regexp - Returns true if str matches regexp and " +
    		"false otherwise",
    extended = "Example:\n" +
        "  > SELECT 'fb' _FUNC_ '.*' FROM src LIMIT 1;\n" +
        "  true"
    )
public class UDFRegExp extends UDF {

  private Text lastRegex = new Text();
  private Pattern p = null;

  BooleanWritable result = new BooleanWritable();
  public UDFRegExp() {
  }

  public BooleanWritable evaluate(Text s, Text regex) {
    if (s == null || regex == null) {
      return null;
    }
    if (!regex.equals(lastRegex) || p == null) {
      lastRegex.set(regex);
      p = Pattern.compile(regex.toString());
    }
    Matcher m = p.matcher(s.toString());
    result.set(m.matches());
    return result;
  }

}
