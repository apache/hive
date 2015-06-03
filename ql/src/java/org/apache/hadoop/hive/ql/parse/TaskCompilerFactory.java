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

package org.apache.hadoop.hive.ql.parse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.EngineSelector;
import org.apache.hadoop.hive.ql.parse.spark.SparkCompiler;
import org.apache.hive.common.util.ReflectionUtil;

/**
 * TaskCompilerFactory is a factory class to choose the appropriate
 * TaskCompiler.
 */
public class TaskCompilerFactory {

  private static final Log LOG = LogFactory.getLog(TaskCompilerFactory.class.getName());

  private TaskCompilerFactory() {
    // avoid instantiation
  }

  /**
   * Returns the appropriate compiler to translate the operator tree
   * into executable units.
   */
  public static TaskCompiler getCompiler(HiveConf conf, ParseContext parseContext) {
    EngineSelector selector = getSelector(conf);
    if (selector != null) {
      String param = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE_SELECTOR_PARAM);
      EngineSelector.Engine selected = selector.select(conf, parseContext, param);
      if (selected == EngineSelector.Engine.MR) {
        HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
        return new MapReduceCompiler();
      }
      if (selected == EngineSelector.Engine.TEZ) {
        HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
        return new TezCompiler();
      }
      if (selected == EngineSelector.Engine.SPARK) {
        HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "spark");
        return new SparkCompiler();
      }
    }

    if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      return new TezCompiler();
    } else if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      return new SparkCompiler();
    } else {
      return new MapReduceCompiler();
    }
  }

  private static EngineSelector getSelector(HiveConf conf) {
    String selector = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE_SELECTOR);
    try {
      if (selector != null) {
        return (EngineSelector)ReflectionUtil.newInstance(conf.getClassByName(selector), conf);
      }
    } catch (Exception ex) {
      LOG.warn("Failed to instantiate engine selector " + selector, ex);
    }
    return null;
  }
}
