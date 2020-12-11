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

package org.apache.hadoop.hive.ql.impala;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ResultMethod;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.impala.exec.ImpalaQueryOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.impala.exec.ImpalaStreamingFetchOperator;
import org.apache.hadoop.hive.ql.impala.exec.ImpalaTask;
import org.apache.hadoop.hive.ql.engine.EngineRuntimeHelper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.impala.plan.ImpalaQueryDesc;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;

public class ImpalaRuntimeHelper implements EngineRuntimeHelper {

  public Class getTaskClass() {
    return ImpalaTask.class;
  }

  public Class getQueryDescClass() {
    return ImpalaQueryDesc.class;
  }

  public Class getQueryOperatorClass() {
    return ImpalaQueryOperator.class;
  }

  public FetchOperator createFetchOperator(HiveConf conf, FetchWork work, JobConf job,
      Operator<?> operator, List<VirtualColumn> vcCols, Schema resultSchema,
      HiveOperation hiveOp) throws HiveException {
    return hiveOp == HiveOperation.ANALYZE_TABLE ||
        (hiveOp == HiveOperation.QUERY && conf.getResultMethod() == ResultMethod.STREAMING)
      ? new ImpalaStreamingFetchOperator(work, job, operator, vcCols, resultSchema)
      : new FetchOperator(work, job, operator, vcCols);
  }
}
