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

package org.apache.hadoop.hive.ql.plan.impala.funcmapper;

import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;

/**
 * Contains details for Scalar functions.  These functions are currently
 * stored in a resource file.
 * Implements FunctionDetails because ImpalaFunctionSignature can be used
 * as a key for both AggFunctionDetails and ScalarFunctionDetails.
 */
public class ScalarFunctionDetails implements FunctionDetails {

  public String dbName;
  public String fnName;
  public String impalaFnName;
  public TPrimitiveType retType;
  public TPrimitiveType[] argTypes;
  public String symbolName;
  public String prepareFnSymbol;
  public String closeFnSymbol;
  public boolean userVisible;
  public boolean isAgg;
  public boolean hasVarArgs;
  public boolean isPersistent;
  public boolean castUp;
  public TFunctionBinaryType binaryType;
  public String hdfsUriLoc;
  public HdfsUri hdfsUri;
  public ImpalaFunctionSignature ifs;

  public ScalarFunctionDetails() {
  }

  public void setFnName(String fnName) {
    this.fnName = fnName;
  }

  public void setImpalaFnName(String impalaFnName) {
    this.impalaFnName = impalaFnName;
  }

  public void setRetType(TPrimitiveType retType) {
    this.retType = retType;
  }

  public void setArgTypes(TPrimitiveType[] argTypes) {
    this.argTypes = argTypes;
  }

  public void setSymbolName(String symbolName) {
    this.symbolName = symbolName;
  }

  public void setPrepareFnSymbol(String prepareFnSymbol) {
    this.prepareFnSymbol = prepareFnSymbol;
  }

  public void setCloseFnSymbol(String closeFnSymbol) {
    this.closeFnSymbol = closeFnSymbol;
  }

  public void setUserVisible(boolean userVisible) {
    this.userVisible = userVisible;
  }

  public void setIsAgg(boolean isAgg) {
    this.isAgg = isAgg;
  }

  public void setHasVarArgs(boolean hasVarArgs) {
    this.hasVarArgs = hasVarArgs;
  }

  public void setIsPersistent(boolean isAgg) {
    this.isAgg = isAgg;
  }

  public void setCastUp(boolean castUp) {
    this.castUp = castUp;
  }

  public void setBinaryType(TFunctionBinaryType binaryType) {
    this.binaryType = binaryType;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setHdfsUriLoc(String hdfsUriLoc) {
    this.hdfsUriLoc = hdfsUriLoc;
    this.hdfsUri = new HdfsUri(hdfsUriLoc);
  }

  @Override
  public ImpalaFunctionSignature getSignature() {
    return ifs;
  }

  public static ScalarFunctionDetails get(String name, SqlTypeName retType,
      List<SqlTypeName> operandTypes) {
    ImpalaFunctionSignature sig = new DefaultFunctionSignature(name, operandTypes, retType);
    return get(sig);
  }

  /** 
   * Retrieve function details about a scalar function given a signature
   * containing the function name, return type, and operand types.
   */
  public static ScalarFunctionDetails get(ImpalaFunctionSignature sig) {
    return (ScalarFunctionDetails) ImpalaBuiltins.SCALAR_BUILTINS_INSTANCE.get(sig);
  }
}
