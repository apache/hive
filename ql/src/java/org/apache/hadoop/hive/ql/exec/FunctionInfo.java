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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

public class FunctionInfo {

  private final boolean isNative;

  private final String displayName;

  private GenericUDF genericUDF;

  private GenericUDTF genericUDTF;

  private GenericUDAFResolver genericUDAFResolver;

  public FunctionInfo(boolean isNative, String displayName,
      GenericUDF genericUDF) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDF = genericUDF;
  }

  public FunctionInfo(boolean isNative, String displayName,
      GenericUDAFResolver genericUDAFResolver) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDAFResolver = genericUDAFResolver;
  }

  public FunctionInfo(boolean isNative, String displayName,
      GenericUDTF genericUDTF) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDTF = genericUDTF;
  }

  /**
   * Get a new GenericUDF object for the function.
   */
  public GenericUDF getGenericUDF() {
    // GenericUDF is stateful - we have to make a copy here
    if (genericUDF == null) {
      return null;
    }
    return FunctionRegistry.cloneGenericUDF(genericUDF);
  }

  /**
   * Get a new GenericUDTF object for the function.
   */
  public GenericUDTF getGenericUDTF() {
    // GenericUDTF is stateful too, copy
    if (genericUDTF == null) {
      return null;
    }
    return FunctionRegistry.cloneGenericUDTF(genericUDTF);
  }

  /**
   * Get the GenericUDAFResolver object for the function.
   */
  public GenericUDAFResolver getGenericUDAFResolver() {
    return genericUDAFResolver;
  }

  /**
   * Get the Class of the UDF
   */
  public Class<?> getFunctionClass() {
    if (isGenericUDF()) {
      if (genericUDF instanceof GenericUDFBridge) {
        return ((GenericUDFBridge) genericUDF).getUdfClass();
      } else {
        return genericUDF.getClass();
      }
    } else if (isGenericUDAF()) {
      if (genericUDAFResolver instanceof GenericUDAFBridge) {
        return ((GenericUDAFBridge) genericUDAFResolver).getUDAFClass();
      } else {
        return genericUDAFResolver.getClass();
      }
    } else if (isGenericUDTF()) {
      return genericUDTF.getClass();
    }
    return null;
  }

  /**
   * Get the display name for this function. This should be transfered into
   * exprNodeGenericUDFDesc, and will be used as the first parameter to
   * GenericUDF.getDisplayName() call, instead of hard-coding the function name.
   * This will solve the problem of displaying only one name when a udf is
   * registered under 2 names.
   */
  public String getDisplayName() {
    return displayName;
  }

  /**
   * Native functions cannot be unregistered.
   */
  public boolean isNative() {
    return isNative;
  }

  /**
   * @return TRUE if the function is a GenericUDF
   */
  public boolean isGenericUDF() {
    return null != genericUDF;
  }

  /**
   * @return TRUE if the function is a GenericUDAF
   */
  public boolean isGenericUDAF() {
    return null != genericUDAFResolver;
  }

  /**
   * @return TRUE if the function is a GenericUDTF
   */
  public boolean isGenericUDTF() {
    return null != genericUDTF;
  }
}
