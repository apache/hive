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

  private OperatorType opType;

  private boolean isOperator;
  
  private Class<? extends UDF> udfClass;
  
  private Class<? extends GenericUDF> genericUDFClass;

  private GenericUDAFResolver genericUDAFResolver;

  public static enum OperatorType { NO_OP, PREFIX, INFIX, POSTFIX };

  public FunctionInfo(String displayName, Class<? extends UDF> udfClass,
      Class<? extends GenericUDF> genericUdfClass) {
    this(true, displayName, udfClass, genericUdfClass);
  }

  public FunctionInfo(boolean isNative, String displayName, Class<? extends UDF> udfClass,
      Class<? extends GenericUDF> genericUdfClass) {
    this.isNative = isNative;
    this.displayName = displayName;
    opType = OperatorType.NO_OP;
    isOperator = false;
    this.udfClass = udfClass;
    this.genericUDFClass = genericUdfClass;
    this.genericUDAFResolver = null;
  }

  public FunctionInfo(String displayName, GenericUDAFResolver genericUDAFResolver) {
    this(true, displayName, genericUDAFResolver);
  }

  public FunctionInfo(boolean isNative, String displayName, GenericUDAFResolver genericUDAFResolver) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.opType = OperatorType.NO_OP;
    this.udfClass = null;
    this.genericUDFClass = null;
    this.genericUDAFResolver = genericUDAFResolver;
  }

  public boolean isAggFunction() {
    return genericUDAFResolver != null;
  }

  public boolean isOperator() {
    return isOperator;
  }

  public void setIsOperator(boolean val) {
    isOperator = val;
  }
  
  public void setOpType(OperatorType opt) {
    opType = opt;
  }
  
  public OperatorType getOpType() {
    return opType;
  }

  public Class<? extends UDF> getUDFClass() {
    return udfClass;
  }

  public Class<? extends GenericUDF> getGenericUDFClass() {
    return genericUDFClass;
  }
  
  public GenericUDAFResolver getGenericUDAFResolver() {
    return genericUDAFResolver;
  }
  
  public String getDisplayName() {
    return displayName;
  }
  
  public boolean isNative() {
    return isNative;
  }
}
