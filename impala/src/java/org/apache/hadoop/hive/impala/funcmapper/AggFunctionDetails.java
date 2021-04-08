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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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
 * Contains details for Aggregation functions.  These functions are currently
 * stored in a resource file.
 * Implements FunctionDetails because ImpalaFunctionSignature can be used
 * as a key for both AggFunctionDetails and ScalarFunctionDetails.
 */
public class AggFunctionDetails implements FunctionDetails {

  public String fnName;
  public String impalaFnName;
  private TPrimitiveType retType;
  private TPrimitiveType[] argTypes;
  private TPrimitiveType intermediateType;
  @Expose(serialize=false,deserialize=false)
  private Type impalaRetType;
  @Expose(serialize=false,deserialize=false)
  private List<Type> impalaArgTypes;
  @Expose(serialize=false,deserialize=false)
  private Type impalaIntermediateType;
  public int intermediateTypeLength;
  public boolean isAnalyticFn;
  public String updateFnSymbol;
  public String initFnSymbol;
  public String mergeFnSymbol;
  public String finalizeFnSymbol;
  public String getValueFnSymbol;
  public String removeFnSymbol;
  public String serializeFnSymbol;
  public boolean ignoresDistinct;
  public boolean returnsNonNullOnEmpty;
  public boolean isAgg;
  public TFunctionBinaryType binaryType;
  private ImpalaFunctionSignature ifs;

  // Set of all aggregate functions available in Impala
  static final Set<String> AGG_BUILTINS = new HashSet<>();
  // Set of all analytic functions available in Impala
  static final Set<String> ANALYTIC_BUILTINS = new HashSet<>();
  // Map containing an aggregate Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  static final Map<ImpalaFunctionSignature, AggFunctionDetails> AGG_BUILTINS_MAP = Maps.newHashMap();

  // populate all agg functions from the resource file.
  static {
    Reader reader =
        new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/impala_aggs.json"));
    Gson gson = new Gson();
    java.lang.reflect.Type aggFuncDetailsType = new TypeToken<ArrayList<AggFunctionDetails>>(){}.getType();
    List<AggFunctionDetails> aggDetails = gson.fromJson(reader, aggFuncDetailsType);

    for (AggFunctionDetails afd : aggDetails) {
      Preconditions.checkState(afd.isAgg || afd.isAnalyticFn);
      if (afd.isAgg) {
        AGG_BUILTINS.add(afd.fnName.toUpperCase());
      }
      if (afd.isAnalyticFn) {
        ANALYTIC_BUILTINS.add(afd.fnName.toUpperCase());
      }
      ImpalaFunctionSignature ifs = ImpalaFunctionSignature.create(afd.fnName, afd.getArgTypes(),
          afd.getRetType(), false, false);
      afd.ifs = ifs;
      AGG_BUILTINS_MAP.put(ifs, afd);
    }
  }

  public static Collection<AggFunctionDetails> getAllFuncDetails() {
    return AGG_BUILTINS_MAP.values();
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

  public void setIntermediateType(TPrimitiveType intermediateType) {
    this.intermediateType = intermediateType;
  }

  public void setIntermediateTypeLength(int intermediateTypeLength) {
    this.intermediateTypeLength = intermediateTypeLength;
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

  public Type getIntermediateType() {
    if (intermediateType == null) {
      return getRetType();
    }
    if (impalaIntermediateType == null) {
      impalaIntermediateType = ImpalaTypeConverter.getImpalaType(intermediateType);
      // The only case where intermediateTypeLength is set is for FIXED_UDA_INTERMEDIATE.
      if (intermediateTypeLength > 0) {
        Preconditions.checkState(intermediateType == TPrimitiveType.FIXED_UDA_INTERMEDIATE);
        impalaIntermediateType =
            ImpalaTypeConverter.createImpalaType(impalaIntermediateType, intermediateTypeLength, 0);
      }
    }
    return impalaIntermediateType;
  }

  public void setIsAnalyticFn(boolean isAnalyticFn) {
    this.isAnalyticFn = isAnalyticFn;
  }

  public void setUpdateFnSymbol(String updateFnSymbol) {
    this.updateFnSymbol = updateFnSymbol;
  }

  public void setInitFnSymbol(String initFnSymbol) {
    this.initFnSymbol = initFnSymbol;
  }

  public void setMergeFnSymbol(String mergeFnSymbol) {
    this.mergeFnSymbol = mergeFnSymbol;
  }

  public void setFinalizeFnSymbol(String finalizeFnSymbol) {
    this.finalizeFnSymbol = finalizeFnSymbol;
  }

  public void setGetValueFnSymbol(String getValueFnSymbol) {
    this.getValueFnSymbol = getValueFnSymbol;
  }

  public void setRemoveFnSymbol(String removeFnSymbol) {
    this.removeFnSymbol = removeFnSymbol;
  }

  public void setIgnoresDistinct(boolean ignoresDistinct) {
    this.ignoresDistinct = ignoresDistinct;
  }

  public void setReturnsNonNullOnEmpty(boolean returnsNonNullOnEmpty) {
    this.returnsNonNullOnEmpty = returnsNonNullOnEmpty;
  }

  public void setIsAgg(boolean isAgg) {
    this.isAgg = isAgg;
  }

  public void setBinaryType(TFunctionBinaryType binaryType) {
    this.binaryType = binaryType;
  }

  @Override
  public ImpalaFunctionSignature getSignature() {
    return ifs;
  }

  /**
   * Retrieve function details about an agg function given a signature
   * containing the function name, return type, and operand types.
   */
  public static AggFunctionDetails get(String name, List<RelDataType> operandTypes,
       RelDataType retType) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.fetch(AGG_BUILTINS_MAP,
        name, operandTypes, retType);

    if (sig != null) {
      return AGG_BUILTINS_MAP.get(sig);
    }
    return null;
  }

  public static boolean isAggFunction(String fnName) {
    return AGG_BUILTINS.contains(fnName.toUpperCase());
  }

  public static AggFunctionDetails get(String name, List<Type> operandTypes,
      Type retType, boolean hasVarArgs) {

    ImpalaFunctionSignature sig = ImpalaFunctionSignature.create(name.toLowerCase(), operandTypes, retType,
        hasVarArgs, null);

    if (sig != null) {
      return AGG_BUILTINS_MAP.get(sig);
    }
    return null;
  }
}
