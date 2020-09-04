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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.common.AnalysisException;

/**
 * Utility class to create an Impala ScalarFunction and AggregateFunction
 */
public class ImpalaFunctionUtil {
  public static ScalarFunction create(ScalarFunctionDetails sfd, Analyzer analyzer)
      throws HiveException {
    FunctionName funcName = new FunctionName(sfd.dbName, sfd.impalaFnName);
    if (analyzer != null && !sfd.impalaFnName.equals("not implemented")) {
      try {
        funcName.analyze(analyzer);
      } catch (AnalysisException e) {
        throw new HiveException("Encountered Impala exception: ", e);
      }
    }
    ScalarFunction retVal =
        new ScalarFunction(funcName,
            sfd.getArgTypes(),
            sfd.getRetType(),
            sfd.hdfsUri,
            sfd.symbolName,
            sfd.prepareFnSymbol,
            sfd.closeFnSymbol);
    retVal.setBinaryType(sfd.binaryType);
    retVal.setHasVarArgs(sfd.hasVarArgs);
    return retVal;
  }

  public static ScalarFunction create(ScalarFunctionDetails sfd) throws HiveException {
    return create(sfd, null);
  }

  public static AggregateFunction create(AggFunctionDetails afd, Analyzer analyzer)
      throws HiveException {
    FunctionName impalaFuncName = new FunctionName(BuiltinsDb.NAME, afd.fnName);
    if (analyzer != null && !afd.fnName.equals("not implemented")) {
      try {
        impalaFuncName.analyze(analyzer);
      } catch (AnalysisException e) {
        throw new HiveException("Encountered Impala exception: ", e);
      }
    }
    AggregateFunction retVal =
        new AggregateFunction(impalaFuncName, afd.getArgTypes(),
            afd.getRetType(), afd.getIntermediateType(), null,
            afd.updateFnSymbol, afd.initFnSymbol, afd.serializeFnSymbol,
            afd.mergeFnSymbol, afd.getValueFnSymbol, afd.removeFnSymbol,
            afd.finalizeFnSymbol);
    retVal.setBinaryType(afd.binaryType);
    return retVal;
  }

  public static AggregateFunction create(AggFunctionDetails afd) throws HiveException {
    return create(afd, null);
  }
}
