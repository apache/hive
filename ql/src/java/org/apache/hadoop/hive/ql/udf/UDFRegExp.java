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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

@description(name = "rlike,regexp", value = "str _FUNC_ regexp - Returns true if str matches regexp and "
    + "false otherwise", extended = "Example:\n"
    + "  > SELECT 'fb' _FUNC_ '.*' FROM src LIMIT 1;\n" + "  true")
public class UDFRegExp extends UDF {

  static final Log LOG = LogFactory.getLog(UDFRegExp.class.getName());

  private final Text lastRegex = new Text();
  private Pattern p = null;
  boolean warned = false;

  BooleanWritable result = new BooleanWritable();

  public UDFRegExp() {
  }

  public BooleanWritable evaluate(Text s, Text regex) {
    if (s == null || regex == null) {
      return null;
    }
    if (regex.getLength() == 0) {
      if (!warned) {
        warned = true;
        LOG.warn(getClass().getSimpleName() + " regex is empty. Additional "
            + "warnings for an empty regex will be suppressed.");
      }
      result.set(false);
      return result;
    }
    if (!regex.equals(lastRegex) || p == null) {
      lastRegex.set(regex);
      p = Pattern.compile(regex.toString());
    }
    Matcher m = p.matcher(s.toString());
    result.set(m.find(0));
    return result;
  }

}
