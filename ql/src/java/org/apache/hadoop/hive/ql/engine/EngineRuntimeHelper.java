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

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;

/**
 * EngineRuntimeHelper.  This interface is used to create runtime objects that are
 * engine specific.
 */
@InterfaceStability.Unstable
public interface EngineRuntimeHelper {

  public Class getTaskClass();

  public Class getQueryDescClass();

  public Class getQueryOperatorClass();

  public FetchOperator createFetchOperator(HiveConf conf, FetchWork work, JobConf job,
      Operator<?> op, List<VirtualColumn> vcCols, Schema resultSchema,
      HiveOperation hiveOp) throws HiveException;

  public static EngineRuntimeHelper getInstance(HiveConf conf) {
    return EngineLoader.getInstance(conf).getRuntimeHelper();
  }
}
