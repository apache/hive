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

package org.apache.hadoop.hive.ql.ddl;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.reflections.Reflections;

/**
 * DDLTask implementation.
**/
@SuppressWarnings("rawtypes")
public final class DDLTask extends Task<DDLWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Map<Class<? extends DDLDesc>, Class<? extends DDLOperation>> DESC_TO_OPERATION =
      new HashMap<>();

  static {
    Set<Class<? extends DDLOperation>> operationClasses =
        new Reflections("org.apache.hadoop.hive.ql.ddl").getSubTypesOf(DDLOperation.class);
    for (Class<? extends DDLOperation> operationClass : operationClasses) {
      if (Modifier.isAbstract(operationClass.getModifiers())) {
        continue;
      }

      ParameterizedType parameterizedType = (ParameterizedType) operationClass.getGenericSuperclass();
      @SuppressWarnings("unchecked")
      Class<? extends DDLDesc> descClass = (Class<? extends DDLDesc>) parameterizedType.getActualTypeArguments()[0];
      DESC_TO_OPERATION.put(descClass, operationClass);
    }
  }

  @Override
  public boolean requireLock() {
    return this.work != null && this.work.getNeedLock();
  }

  @Override
  public int execute() {
    if (context.getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }

    DDLOperation ddlOperation = null;
    try {
      DDLDesc ddlDesc = work.getDDLDesc();

      if (DESC_TO_OPERATION.containsKey(ddlDesc.getClass())) {
        DDLOperationContext ddlOperationContext = new DDLOperationContext(conf, context, this, (DDLWork)work,
            queryState, queryPlan, console);
        Class<? extends DDLOperation> ddlOperationClass = DESC_TO_OPERATION.get(ddlDesc.getClass());
        Constructor<? extends DDLOperation> constructor =
            ddlOperationClass.getConstructor(DDLOperationContext.class, ddlDesc.getClass());
        ddlOperation = constructor.newInstance(ddlOperationContext, ddlDesc);
        return ddlOperation.execute();
      } else {
        throw new IllegalArgumentException("Unknown DDL request: " + ddlDesc.getClass());
      }
    } catch (Throwable e) {
      if(work.isReplication() && ReplUtils.shouldIgnoreOnError(ddlOperation, e)) {
        LOG.warn("Error while table creation: ", e);
        return 0;
      }
      failed(e);
      if(ddlOperation != null) {
        LOG.error("DDLTask failed, DDL Operation: " + ddlOperation.getClass().toString(), e);
      }
      return ReplUtils.handleException(work.isReplication(), e, work.getDumpDirectory(),
                                       work.getMetricCollector(), getName(), conf);
    }
  }

  private void failed(Throwable e) {
    while (e.getCause() != null && e.getClass() == RuntimeException.class) {
      e = e.getCause();
    }
    setException(e);
    LOG.error("Failed", e);
  }

  @Override
  public StageType getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return "DDL";
  }

  /*
  uses the authorizer from SessionState will need some more work to get this to run in parallel,
  however this should not be a bottle neck so might not need to parallelize this.
   */
  @Override
  public boolean canExecuteInParallel() {
   return work.canExecuteInParallel();
  }
}
