/**
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

package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CreateFunctionDesc;
import org.apache.hadoop.hive.ql.plan.DropFunctionDesc;
import org.apache.hadoop.hive.ql.plan.CreateMacroDesc;
import org.apache.hadoop.hive.ql.plan.DropMacroDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * FunctionTask.
 *
 */
public class FunctionTask extends Task<FunctionWork> {
  private static final long serialVersionUID = 1L;
  private static transient final Log LOG = LogFactory.getLog(FunctionTask.class);

  transient HiveConf conf;

  public FunctionTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);
    this.conf = conf;
  }

  @Override
  public int execute(DriverContext driverContext) {
    CreateFunctionDesc createFunctionDesc = work.getCreateFunctionDesc();
    if (createFunctionDesc != null) {
      return createFunction(createFunctionDesc);
    }

    DropFunctionDesc dropFunctionDesc = work.getDropFunctionDesc();
    if (dropFunctionDesc != null) {
      return dropFunction(dropFunctionDesc);
    }

    CreateMacroDesc createMacroDesc = work.getCreateMacroDesc();
    if (createMacroDesc != null) {
      return createMacro(createMacroDesc);
    }

    DropMacroDesc dropMacroDesc = work.getDropMacroDesc();
    if (dropMacroDesc != null) {
      return dropMacro(dropMacroDesc);
    }
    return 0;
  }

  private int createFunction(CreateFunctionDesc createFunctionDesc) {
    try {
      Class<?> udfClass = getUdfClass(createFunctionDesc);
      boolean registered = FunctionRegistry.registerTemporaryFunction(
        createFunctionDesc.getFunctionName(),
        udfClass);
      if (registered) {
        return 0;
      }
      console.printError("FAILED: Class " + createFunctionDesc.getClassName()
          + " does not implement UDF, GenericUDF, or UDAF");
      return 1;
    } catch (ClassNotFoundException e) {
      console.printError("FAILED: Class " + createFunctionDesc.getClassName() + " not found");
      LOG.info("create function: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  private int createMacro(CreateMacroDesc createMacroDesc) {
    FunctionRegistry.registerTemporaryMacro(
      createMacroDesc.getMacroName(),
      createMacroDesc.getBody(),
      createMacroDesc.getColNames(),
      createMacroDesc.getColTypes());
    return 0;
  }

  private int dropMacro(DropMacroDesc dropMacroDesc) {
    try {
      FunctionRegistry.unregisterTemporaryUDF(dropMacroDesc
          .getMacroName());
      return 0;
    } catch (HiveException e) {
      LOG.info("drop macro: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  private int dropFunction(DropFunctionDesc dropFunctionDesc) {
    try {
      FunctionRegistry.unregisterTemporaryUDF(dropFunctionDesc
          .getFunctionName());
      return 0;
    } catch (HiveException e) {
      LOG.info("drop function: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  @SuppressWarnings("unchecked")
  private Class<?> getUdfClass(CreateFunctionDesc desc) throws ClassNotFoundException {
    return Class.forName(desc.getClassName(), true, JavaUtils.getClassLoader());
  }

  @Override
  public StageType getType() {
    return StageType.FUNC;
  }

  @Override
  public String getName() {
    return "FUNCTION";
  }
}
