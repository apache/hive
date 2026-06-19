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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;

// this function is for internal use only
public class GenericUDFOPNotEqualNS extends GenericUDFOPNotEqual {

  @Description(name = "IS DISTINCT FROM", value = "a _FUNC_ b - Returns same result with NOTEQUALNS (IS DISTINCT " +
          "FROM) operator for non-null operands, but returns FALSE if both are NULL, TRUE if one of the them is NULL")
  public GenericUDFOPNotEqualNS(){
    this.opName = "NOTEQUALNS";
    this.opDisplayName = "IS DISTINCT FROM";
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    Object o1 = arguments[1].get();
    if (o0 == null && o1 == null) {
      result.set(false);
      return result;
    }
    if (o0 == null || o1 == null) {
      result.set(true);
      return result;
    }
    return super.evaluate(arguments);
  }

  @Override
  public GenericUDF negative() {
    return new GenericUDFOPEqualNS();
  }
}
