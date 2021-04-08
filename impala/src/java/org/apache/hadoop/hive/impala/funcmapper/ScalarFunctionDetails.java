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

package org.apache.hadoop.hive.impala.funcmapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
  private TPrimitiveType retType;
  private TPrimitiveType[] argTypes;
  @Expose(serialize=false,deserialize=false)
  public Type impalaRetType;
  @Expose(serialize=false,deserialize=false)
  public List<Type> impalaArgTypes;
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
  public boolean retTypeAlwaysNullable;
  public ImpalaFunctionSignature ifs;

  // Set of all scalar functions available in Impala
  static final Set<String> SCALAR_BUILTINS = new HashSet<>();
  // Map containing a scalar  Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  static final Map<ImpalaFunctionSignature, ScalarFunctionDetails> SCALAR_BUILTINS_MAP = Maps.newHashMap();

  // populate all functions from the resource file.
  static {
    Reader reader =
        new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/impala_scalars.json"));
    Gson gson = new Gson();
    java.lang.reflect.Type scalarFuncDetailsType = new TypeToken<ArrayList<ScalarFunctionDetails>>(){}.getType();
    List<ScalarFunctionDetails> scalarDetails = gson.fromJson(reader, scalarFuncDetailsType);

    for (ScalarFunctionDetails sfd : scalarDetails) {
      SCALAR_BUILTINS.add(sfd.fnName.toUpperCase());
      sfd.setDbName(BuiltinsDb.NAME);
      ImpalaFunctionSignature ifs = ImpalaFunctionSignature.create(sfd.fnName, sfd.getArgTypes(),
          sfd.getRetType(), sfd.hasVarArgs, sfd.retTypeAlwaysNullable);
      sfd.ifs = ifs;
      SCALAR_BUILTINS_MAP.put(ifs, sfd);
    }
  }

  public static Collection<ScalarFunctionDetails> getAllFuncDetails() {
    return SCALAR_BUILTINS_MAP.values();
  }

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

  public List<Type> getArgTypes() {
    if (impalaArgTypes == null) {
      impalaArgTypes = (argTypes != null)
          ? ImpalaTypeConverter.getImpalaTypesList(argTypes)
          : Lists.newArrayList();
    }
    return impalaArgTypes;
  }

  public Type getRetType() {
    if (impalaRetType == null) {
      impalaRetType = ImpalaTypeConverter.getImpalaType(retType);
    }
    return impalaRetType;
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

  public void setRetTypeAlwaysAllowsNulls(boolean retTypeAlwaysNullable) {
    this.retTypeAlwaysNullable = retTypeAlwaysNullable;
  }

  @Override
  public ImpalaFunctionSignature getSignature() {
    return ifs;
  }

  /**
   * Shortcut for getting the ScalarFunctionDetails when the Impala operand types,
   * the return type, and the function name are hardcoded.
   */
  public static ScalarFunctionDetails get(String name, List<Type> operandTypes,
       Type retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.create(name,
        operandTypes, retType, false, null);

    return SCALAR_BUILTINS_MAP.get(sig);
  }

  public static ScalarFunctionDetails get(String name, List<RelDataType> operandTypes,
       RelDataType retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.fetch(
        SCALAR_BUILTINS_MAP, name, operandTypes, retType);

    if (sig != null) {
      return SCALAR_BUILTINS_MAP.get(sig);
    }
    return null;
  }
}
