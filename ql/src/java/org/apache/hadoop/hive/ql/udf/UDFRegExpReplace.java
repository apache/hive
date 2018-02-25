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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDFRegExpReplace.
 *
 */
@Description(name = "regexp_replace",
    value = "_FUNC_(str, regexp, rep) - replace all substrings of str that "
    + "match regexp with rep", extended = "Example:\n"
    + "  > SELECT _FUNC_('100-200', '(\\d+)', 'num') FROM src LIMIT 1;\n"
    + "  'num-num'")
public class UDFRegExpReplace extends UDF {

  private final Text lastRegex = new Text();
  private Pattern p = null;

  private final Text lastReplacement = new Text();
  private String replacementString = "";

  private Text result = new Text();

  public UDFRegExpReplace() {
  }

  public Text evaluate(Text s, Text regex, Text replacement) {
    if (s == null || regex == null || replacement == null) {
      return null;
    }
    // If the regex is changed, make sure we compile the regex again.
    if (!regex.equals(lastRegex) || p == null) {
      lastRegex.set(regex);
      p = Pattern.compile(regex.toString());
    }
    Matcher m = p.matcher(s.toString());
    // If the replacement is changed, make sure we redo toString again.
    if (!replacement.equals(lastReplacement)) {
      lastReplacement.set(replacement);
      replacementString = replacement.toString();
    }

    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replacementString);
    }
    m.appendTail(sb);

    result.set(sb.toString());
    return result;
  }

}
