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

@Description(
    name = "xpath_float",
    value = "_FUNC_(xml, xpath) - Returns a float value that matches the xpath expression",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('<a><b>1</b><b>2</b></a>','sum(a/b)') FROM src LIMIT 1;\n"
    + "  3.0")
public class UDFXPathFloat extends UDF {

  private final UDFXPathUtil xpath = new UDFXPathUtil();

  public float evaluate(String xml, String path) {
    return xpath.evalNumber(xml, path).floatValue();
  }
}
