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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

public class FunctionInfo {
  
  private boolean isNative;

  private String displayName;

  private GenericUDF genericUDF;

  private GenericUDAFResolver genericUDAFResolver;

  public FunctionInfo(boolean isNative, String displayName, GenericUDF genericUDF) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDF = genericUDF;
    this.genericUDAFResolver = null;
  }

  public FunctionInfo(boolean isNative, String displayName, GenericUDAFResolver genericUDAFResolver) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDF = null;
    this.genericUDAFResolver = genericUDAFResolver;
  }

  /**
   * Get a new GenericUDF object for the function. 
   */
  public GenericUDF getGenericUDF() {
    // GenericUDF is stateful - we have to make a copy here
    return FunctionRegistry.cloneGenericUDF(genericUDF);
  }
  
  /**
   * Get the GenericUDAFResolver object for the function. 
   */
  public GenericUDAFResolver getGenericUDAFResolver() {
    return genericUDAFResolver;
  }
  
  /**
   * Get the display name for this function.
   * This should be transfered into exprNodeGenericUDFDesc, and will be 
   * used as the first parameter to GenericUDF.getDisplayName() call, instead
   * of hard-coding the function name.  This will solve the problem of 
   * displaying only one name when a udf is registered under 2 names.
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
}
