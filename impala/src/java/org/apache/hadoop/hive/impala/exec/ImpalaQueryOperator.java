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

package org.apache.hadoop.hive.impala.exec;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.impala.plan.ImpalaCompiledPlan;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.impala.thrift.TExecRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impala Query Operator implementation.
 **/
public class ImpalaQueryOperator extends FileSinkOperator {

  public static final Logger LOG = LoggerFactory.getLogger(ImpalaQueryOperator.class);

  public ImpalaQueryOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FILESINK;
  }

  public ImpalaCompiledPlan getCompiledPlan() {
    // CDPD-8398: try to avoid cast, if possible
    return ((ImpalaQueryDesc)conf).getCompiledPlan();
  }
}
