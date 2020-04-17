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

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TPrimitiveType;

import java.util.List;

/**
 * Utility class to create an Impala ScalarFunction and AggregateFunction
 */
public class ImpalaFunctionUtil {
  public static ScalarFunction create(ScalarFunctionDetails sfd) {
    ScalarFunction retVal =
        new ScalarFunction(new FunctionName(sfd.dbName, sfd.impalaFnName),
            ImpalaTypeConverter.getImpalaTypesList(sfd.argTypes),
            ImpalaTypeConverter.getImpalaType(sfd.retType),
            sfd.hdfsUri,
            sfd.symbolName,
            sfd.prepareFnSymbol,
            sfd.closeFnSymbol);
    retVal.setBinaryType(sfd.binaryType);
    retVal.setHasVarArgs(sfd.hasVarArgs);
    return retVal;
  }

  public static AggregateFunction create(AggFunctionDetails afd) {
    FunctionName impalaFuncName = new FunctionName(BuiltinsDb.NAME, afd.fnName);
    List<Type> impalaArgTypes = ImpalaTypeConverter.getImpalaTypesList(afd.argTypes);
    Preconditions.checkNotNull(afd.retType);
    Preconditions.checkNotNull(afd.intermediateType);
    Type impalaRetType =
        ImpalaTypeConverter.getImpalaType(afd.retType);
    Type impalaIntermediateType =
        (afd.intermediateType == TPrimitiveType.FIXED_UDA_INTERMEDIATE) ?
            ImpalaTypeConverter.getImpalaType(afd.intermediateType, afd.intermediateTypeLength, 0)
            : ImpalaTypeConverter.getImpalaType(afd.intermediateType);

    AggregateFunction retVal =
        new AggregateFunction(impalaFuncName, impalaArgTypes,
            impalaRetType, impalaIntermediateType, null,
            afd.updateFnSymbol, afd.initFnSymbol, afd.serializeFnSymbol,
            afd.mergeFnSymbol, afd.getValueFnSymbol, afd.removeFnSymbol,
            afd.finalizeFnSymbol);
    return retVal;
  }
}
