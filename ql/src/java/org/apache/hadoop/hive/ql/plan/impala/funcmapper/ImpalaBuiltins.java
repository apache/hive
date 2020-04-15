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

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.thrift.TPrimitiveType;

/**
 * Contains details for Aggregation functions.  These functions are currently
 * stored in a resource file.
 * Implements FunctionDetails because ImpalaFunctionSignature can be used
 * as a key for both AggFunctionDetails and ScalarFunctionDetails.
 */
public class ImpalaBuiltins {

  // Map containing an aggregate Impala signature to the details associated with the signature.
  // A signature consists of the function name, the operand types and the return type.
  static public Map<ImpalaFunctionSignature, AggFunctionDetails> AGG_BUILTINS_INSTANCE = Maps.newHashMap();

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
        ImpalaFunctionSignature ifs = new DefaultFunctionSignature(sfd.fnName, argTypes, retType, sfd.hasVarArgs);
        sfd.ifs = ifs;
        SCALAR_BUILTINS_INSTANCE.put(ifs, sfd);
        BuiltinsDb.getInstance(true).addFunction(ScalarFunctionUtil.create(sfd));
      }
    } catch (HiveException e) {
      // if an exception is hit here, we have a problem in our resource file.
      throw new RuntimeException("Problem processing resource file impala_scalars.json:" + e);
    }
  }

  // populate all agg functions from the resource file.
  static {
    Reader reader =
        new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/impala_aggs.json"));
    Gson gson = new Gson();
    Type aggFuncDetailsType = new TypeToken<ArrayList<AggFunctionDetails>>(){}.getType();
    List<AggFunctionDetails> aggDetails = gson.fromJson(reader, aggFuncDetailsType);

    try {
      for (AggFunctionDetails afd : aggDetails) {
        Preconditions.checkState(afd.isAgg || afd.isAnalyticFn);
        List<SqlTypeName> argTypes = (afd.argTypes == null)
            ? Lists.newArrayList()
            : ImpalaTypeConverter.getSqlTypeNames(afd.argTypes);
        SqlTypeName retType = ImpalaTypeConverter.getSqlTypeName(afd.retType);
        ImpalaFunctionSignature ifs = new DefaultFunctionSignature(afd.fnName, argTypes, retType, false);
        afd.ifs = ifs;
        AGG_BUILTINS_INSTANCE.put(ifs, afd);
      }
    } catch (HiveException e) {
      // if an exception is hit here, we have a problem in our resource file.
      throw new RuntimeException("Problem processing resource file impala_aggs.json:" + e);
    }
  }

  // populate all scalar functions from the resource file.
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
        ImpalaFunctionSignature ifs = new DefaultFunctionSignature(sfd.fnName, argTypes, retType, sfd.hasVarArgs);
        sfd.ifs = ifs;
        SCALAR_BUILTINS_INSTANCE.put(ifs, sfd);
        BuiltinsDb.getInstance(true).addFunction(ScalarFunctionUtil.create(sfd));
      }
    } catch (HiveException e) {
      // if an exception is hit here, we have a problem in our resource file.
      throw new RuntimeException("Problem processing resource file impala_scalars.json:" + e);
    }
  }

}
