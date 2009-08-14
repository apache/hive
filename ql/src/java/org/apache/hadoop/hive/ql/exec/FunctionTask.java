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

import java.net.URLClassLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.createFunctionDesc;
import org.apache.hadoop.hive.ql.plan.dropFunctionDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

public class FunctionTask extends Task<FunctionWork> {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog("hive.ql.exec.FunctionTask");

  transient HiveConf conf;
  
  public void initialize(HiveConf conf) {
    super.initialize(conf);
    this.conf = conf;
  }
  
  @Override
  public int execute() {
    createFunctionDesc createFunctionDesc = work.getCreateFunctionDesc();
    if (createFunctionDesc != null) {
      return createFunction(createFunctionDesc);
    }

    dropFunctionDesc dropFunctionDesc = work.getDropFunctionDesc();
    if (dropFunctionDesc != null) {
      return dropFunction(dropFunctionDesc);
    }
    return 0;
  }

  private int createFunction(createFunctionDesc createFunctionDesc) {
    try {
      Class<?> udfClass = getUdfClass(createFunctionDesc);
      if(UDF.class.isAssignableFrom(udfClass)) {
        FunctionRegistry.registerTemporaryUDF(createFunctionDesc.getFunctionName(), 
                                     (Class<? extends UDF>) udfClass, false);
        return 0;
      } else if(GenericUDF.class.isAssignableFrom(udfClass)) {
        FunctionRegistry.registerTemporaryGenericUDF(createFunctionDesc.getFunctionName(), 
                                            (Class<? extends GenericUDF>) udfClass);
        return 0;
      } else if(UDAF.class.isAssignableFrom(udfClass)) {
        FunctionRegistry.registerTemporaryUDAF(createFunctionDesc.getFunctionName(),
                                      (Class<? extends UDAF>) udfClass);
        return 0;
      } else if(GenericUDAFResolver.class.isAssignableFrom(udfClass)) {
        FunctionRegistry.registerTemporaryGenericUDAF(createFunctionDesc.getFunctionName(),
            (GenericUDAFResolver)ReflectionUtils.newInstance(udfClass, null));
        return 0;
      }
      return 1;

    } catch (ClassNotFoundException e) {
      LOG.info("create function: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  private int dropFunction(dropFunctionDesc dropFunctionDesc) {
    try {
      FunctionRegistry.unregisterTemporaryUDF(dropFunctionDesc.getFunctionName());
      return 0;
    } catch (HiveException e) {
      LOG.info("drop function: " + StringUtils.stringifyException(e));
      return 1;
    }
  }

  @SuppressWarnings("unchecked")
  private Class<?> getUdfClass(createFunctionDesc desc)
      throws ClassNotFoundException {
    return Class.forName(desc.getClassName(), true, JavaUtils.getClassLoader());
  }
}
