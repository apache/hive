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
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class ImpalaFunctionSignatureFactory {

  static public ImpalaFunctionSignature create(SqlOperator operator, List<SqlTypeName> argTypes,
      SqlTypeName retType) throws HiveException {
    return create(operator.getKind(), operator.getName().toLowerCase(), argTypes, retType);
  }

  static public ImpalaFunctionSignature create(String func, List<SqlTypeName> argTypes,
      SqlTypeName retType) throws HiveException {
    return new DefaultFunctionSignature(func, argTypes, retType, false);
  }

  static public ImpalaFunctionSignature create(SqlKind kind, String func, List<SqlTypeName> argTypes,
      SqlTypeName retType) throws HiveException {
    switch(kind) {
      case CAST:
        return new CastFunctionSignature(argTypes, retType);
      case CASE:
        return new CaseFunctionSignature(argTypes, retType);
      case EXTRACT:
        return new ExtractFunctionSignature(func, argTypes, retType);
      case PLUS:
      case MINUS:
        if (argTypes.contains(SqlTypeName.TIMESTAMP)) {
          return new TimeIntervalOpFunctionSignature(kind, argTypes, retType);
        }
        return new DefaultFunctionSignature(func, argTypes, retType, false);
      default:
        return new DefaultFunctionSignature(func, argTypes, retType, false);
    }
  }
}
