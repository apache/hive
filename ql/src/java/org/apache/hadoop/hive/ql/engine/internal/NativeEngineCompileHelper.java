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

package org.apache.hadoop.hive.ql.engine.internal;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSConverter;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.engine.EngineCompileHelper;
import org.apache.hadoop.hive.ql.engine.EngineEventSequence;
import org.apache.hadoop.hive.ql.engine.EngineQueryHelper;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.parse.MapReduceCompiler;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;
import org.apache.hadoop.hive.ql.parse.TezCompiler;
import org.apache.hadoop.hive.ql.parse.spark.SparkCompiler;

public class NativeEngineCompileHelper implements EngineCompileHelper {

  public HMSConverter getHMSConverter() {
    return null;
  }

  public EngineEventSequence getEventSequence(String event) {
    return new DummyEventSequence();
  }

  public EngineQueryHelper getQueryHelper(HiveConf conf, String dbname, String username,
                                             HiveTxnManager txnMgr, Context ctx,
                                             QueryState queryState) throws SemanticException {
    return null;
  }

  public EngineQueryHelper resetQueryHelper(
      EngineQueryHelper queryHelper) throws SemanticException {
    return null;
  }

  public RelDataTypeSystem getRelDataTypeSystem() {
    return new HiveTypeSystemImpl();
  }

  public TaskCompiler getCompiler(HiveConf conf) {
    switch (conf.getRuntime()) {
      case MR:
        return new MapReduceCompiler();
      case TEZ:
        return new TezCompiler();
      case SPARK:
        return new SparkCompiler();
      case INVALID_RUNTIME:
        throw new UnsupportedOperationException("Invalid execution engine specified.");
    }

    throw new UnsupportedOperationException("Invalid execution engine specified.");
  }
}
