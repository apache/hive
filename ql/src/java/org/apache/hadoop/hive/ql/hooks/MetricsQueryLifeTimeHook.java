/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;

/**
 * LifeTimeHook gathering metrics for the query lifecycle if the
 * metrics are enabled
 */
public class MetricsQueryLifeTimeHook implements QueryLifeTimeHook {

  private Metrics metrics = MetricsFactory.getInstance();
  private MetricsScope compilingQryScp;
  private MetricsScope executingQryScp;

  @Override
  public void beforeCompile(QueryLifeTimeHookContext ctx) {
    if (metrics != null) {
      compilingQryScp = metrics.createScope(MetricsConstant.HS2_COMPILING_QUERIES);
    }
  }

  @Override
  public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
    if (metrics != null && compilingQryScp != null) {
      metrics.endScope(compilingQryScp);
    }
  }

  @Override
  public void beforeExecution(QueryLifeTimeHookContext ctx) {
    if (metrics != null) {
      executingQryScp = metrics.createScope(MetricsConstant.HS2_EXECUTING_QUERIES);
    }
  }

  @Override
  public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
    if (metrics != null && executingQryScp != null) {
      metrics.endScope(executingQryScp);
    }
  }
}
