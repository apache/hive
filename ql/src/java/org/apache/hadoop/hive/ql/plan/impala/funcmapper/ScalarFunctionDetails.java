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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
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

  // Map containing a scalar  Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  static public Map<ImpalaFunctionSignature, ScalarFunctionDetails> SCALAR_BUILTINS_INSTANCE = Maps.newHashMap();

  // populate all functions from the resource file.
  static {
    Reader reader =
        new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/impala_scalars.json"));
    Gson gson = new Gson();
    Type scalarFuncDetailsType = new TypeToken<ArrayList<ScalarFunctionDetails>>(){}.getType();
    List<ScalarFunctionDetails> scalarDetails = gson.fromJson(reader, scalarFuncDetailsType);

    try {
      for (ScalarFunctionDetails sfd : scalarDetails) {
        sfd.setDbName(BuiltinsDb.NAME);
        List<SqlTypeName> argTypes = (sfd.argTypes == null)
            ? Lists.newArrayList()
            : ImpalaTypeConverter.getSqlTypeNames(sfd.argTypes);
        SqlTypeName retType = ImpalaTypeConverter.getSqlTypeName(sfd.retType);
        ImpalaFunctionSignature ifs =
            ImpalaFunctionSignature.create(sfd.fnName, argTypes, retType, sfd.hasVarArgs);
        sfd.ifs = ifs;
        SCALAR_BUILTINS_INSTANCE.put(ifs, sfd);
        BuiltinsDb.getInstance(true).addFunction(ImpalaFunctionUtil.create(sfd));
      }
    } catch (HiveException e) {
      // if an exception is hit here, we have a problem in our resource file.
      throw new RuntimeException("Problem processing resource file impala_scalars.json:" + e);
    }
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

  public static ScalarFunctionDetails get(String name, List<SqlTypeName> operandTypes,
       SqlTypeName retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.fetch(
        SCALAR_BUILTINS_INSTANCE, name, operandTypes, retType);

    if (sig != null) {
      return SCALAR_BUILTINS_INSTANCE.get(sig);
    }
    return null;
  }
}
