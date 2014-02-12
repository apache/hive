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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class FunctionUtils {

  public static boolean isQualifiedFunctionName(String functionName) {
    return functionName.indexOf('.') >= 0;
  }

  public static String qualifyFunctionName(String functionName, String dbName) {
    if (isQualifiedFunctionName(functionName)) {
      return functionName;
    }
    return dbName + "." + functionName;
  }

  /**
   * Splits a qualified function name into an array containing the database name and function name.
   * If the name is not qualified, the database name is null.
   * If there is more than one '.', an exception will be thrown.
   * @param functionName Function name, which may or may not be qualified
   * @return
   */
  public static String[] splitQualifiedFunctionName(String functionName) throws HiveException {
    String[] names = functionName.split("\\.");
    if (names.length == 1) {
      String[] retval = { null, functionName };
      return retval;
    } else if (names.length > 2) {
      throw new HiveException("Function name does not have correct format: " + functionName);
    }
    return names;
  }

}
