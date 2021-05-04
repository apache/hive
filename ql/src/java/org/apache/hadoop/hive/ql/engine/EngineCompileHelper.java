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

package org.apache.hadoop.hive.ql.engine;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSConverter;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;

import java.util.List;

/**
 * EngineCompileHelper.  This interface is used to create compilation objects that are
 * engine specific.
 */
@InterfaceStability.Unstable
public interface EngineCompileHelper {

  public HMSConverter getHMSConverter();

  public EngineEventSequence getEventSequence(String event);

  public EngineQueryHelper getQueryHelper(HiveConf conf, String dbname, String username,
                                             HiveTxnManager txnMgr, Context ctx) throws SemanticException;

  public RelDataTypeSystem getRelDataTypeSystem();

  public TaskCompiler getCompiler(HiveConf conf);

  public static EngineCompileHelper getInstance(HiveConf conf) {
    return EngineLoader.getInstance(conf).getCompileHelper();
  }
}
