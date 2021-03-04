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

package org.apache.hadoop.hive.impala;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSConverter;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.engine.EngineCompileHelper;
import org.apache.hadoop.hive.ql.engine.EngineEventSequence;
import org.apache.hadoop.hive.ql.engine.EngineQueryHelper;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.impala.parse.ImpalaCompiler;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;
import org.apache.hadoop.hive.impala.plan.ImpalaHMSConverter;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryHelperImpl;
import org.apache.hadoop.hive.impala.calcite.ImpalaTypeSystemImpl;

import java.util.List;

public class ImpalaCompileHelper implements EngineCompileHelper {

  public HMSConverter getHMSConverter() {
    return new ImpalaHMSConverter();
  }

  public EngineEventSequence getEventSequence(String event) {
    return new ImpalaEventSequence(event);
  }

  public EngineQueryHelper getQueryHelper(HiveConf conf, String dbname, String username,
                                             HiveTxnManager txnMgr, Context ctx,
                                             QueryState queryState) throws SemanticException {
    return new ImpalaQueryHelperImpl(conf, dbname, username, txnMgr, ctx, queryState);
  }

  public RelDataTypeSystem getRelDataTypeSystem() {
    return new ImpalaTypeSystemImpl();
  }

  public TaskCompiler getCompiler(HiveConf conf) {
    long fetchSize = conf.getLongVar(HiveConf.ConfVars.HIVE_IMPALA_FETCH_SIZE);
    return new ImpalaCompiler(fetchSize);
  }
}
