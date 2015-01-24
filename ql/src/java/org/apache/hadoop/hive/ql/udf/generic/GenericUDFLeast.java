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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;

/**
 * GenericUDF Class for SQL construct "least(v1, v2, .. vn)".
 *
 * NOTES: 1. v1, v2 and vn should have the same TypeInfo, or an exception will
 * be thrown.
 */
@Description(name = "least",
    value = "_FUNC_(v1, v2, ...) - Returns the least value in a list of values",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(2, 3, 1) FROM src LIMIT 1;\n" + "  1")
public class GenericUDFLeast extends GenericUDFGreatest {

  @Override
  protected String getFuncName() {
    return "least";
  }

  @Override
  protected boolean isGreatest() {
    return false;
  }
}
