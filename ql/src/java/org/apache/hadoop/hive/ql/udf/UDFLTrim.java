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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringLTrim;
import org.apache.hadoop.io.Text;

/**
 * UDFLTrim.
 *
 */
@Description(name = "ltrim",
    value = "_FUNC_(str) - Removes the leading space characters from str ",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('   facebook') FROM src LIMIT 1;\n" + "  'facebook'")
@VectorizedExpressions({StringLTrim.class})
public class UDFLTrim extends UDF {
  private final Text result = new Text();

  public UDFLTrim() {
  }

  public Text evaluate(Text s) {
    if (s == null) {
      return null;
    }
    result.set(StringUtils.stripStart(s.toString(), " "));
    return result;
  }

}
