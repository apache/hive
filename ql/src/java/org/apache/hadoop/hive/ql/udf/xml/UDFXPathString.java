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

package org.apache.hadoop.hive.ql.udf.xml;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(name = "xpath_string",
    value = "_FUNC_(xml, xpath) - Returns the text contents of the first xml node that matches the xpath expression",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('<a><b>b</b><c>cc</c></a>','a/c') FROM src LIMIT 1;\n"
    + "  'cc'\n"
    + "  > SELECT _FUNC_('<a><b>b1</b><b>b2</b></a>','a/b') FROM src LIMIT 1;\n"
    + "  'b1'\n"
    + "  > SELECT _FUNC_('<a><b>b1</b><b>b2</b></a>','a/b[2]') FROM src LIMIT 1;\n"
    + "  'b2'\n"
    + "  > SELECT _FUNC_('<a><b>b1</b><b>b2</b></a>','a') FROM src LIMIT 1;\n"
    + "  'b1b2'"
    )
public class UDFXPathString extends UDF {

  private final UDFXPathUtil xpath = new UDFXPathUtil();
  private final Text result = new Text();

  public Text evaluate(String xml, String path) {
    String s = xpath.evalString(xml, path);
    if (s == null) {
      return null;
    }

    result.set(s);
    return result;
  }
}
