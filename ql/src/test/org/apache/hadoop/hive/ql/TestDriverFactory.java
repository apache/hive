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

package org.apache.hadoop.hive.ql;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.reexec.IReExecutionPlugin;
import org.apache.hadoop.hive.ql.reexec.ReExecDriver;
import org.apache.hadoop.hive.ql.reexec.ReExecutionStrategyType;
import org.junit.Test;

public class TestDriverFactory {

  @Test
  public void testNormal() {
    HiveConf conf = new HiveConf();
    IDriver driver = DriverFactory.newDriver(conf);
    ReExecDriver reDriver = (ReExecDriver) driver;

    List<IReExecutionPlugin> plugins = getPlugins(conf);
    for (IReExecutionPlugin original : plugins) {
      boolean found = false;
      for (IReExecutionPlugin instance : reDriver.getPlugins()) {
        if (original.getClass().getName().equals(instance.getClass().getName())) {
          found = true;
        }
      }

      if (!found) {
        fail("The ReExecutionPlugin defined has not been instantiated");
      }
    }
  }

  @Test(expected = RuntimeException.class)
  public void testNormalFailed() {
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
        "overlay,reoptimize,reexecute_lost_am,dagsubmit,test");

    DriverFactory.newDriver(conf);
  }

  @Test
  public void testNormalAndCustom() {
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
        "overlay,reoptimize,reexecute_lost_am,dagsubmit" + 
        ",org.apache.hadoop.hive.ql.reexec.ReCompileWithoutCBOPlugin" +
        ",org.apache.hadoop.hive.ql.reexec.ReExecuteOnWriteConflictPlugin");

    IDriver driver = DriverFactory.newDriver(conf);
    ReExecDriver reDriver = (ReExecDriver) driver;

    List<IReExecutionPlugin> plugins = getPlugins(conf);
    for (IReExecutionPlugin original : plugins) {
      boolean found = false;
      for (IReExecutionPlugin instance : reDriver.getPlugins()) {
        if (original.getClass().getName().equals(instance.getClass().getName())) {
          found = true;
        }
      }

      if (!found) {
        fail("The ReExecutionPlugin defined has not been instantiated");
      }
    }
  }

  @Test(expected = RuntimeException.class)
  public void testCustomNotInstanceOfIReExecutionPlugin() {
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES,
        "overlay,reoptimize,reexecute_lost_am,dagsubmit" +
        ",org.apache.hadoop.hive.conf.HiveConf");

    DriverFactory.newDriver(conf);
  }

  private List<IReExecutionPlugin> getPlugins(HiveConf conf) {
    String strategies = conf.getVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES);
    List<IReExecutionPlugin> plugins = new ArrayList<>();
    for (String string : strategies.split(",")) {
      if (string.trim().isEmpty()) {
        continue;
      }

      IReExecutionPlugin plugin = buildReExecPlugin(string);
      if (plugin != null) {
        plugins.add(buildReExecPlugin(string));
      } else {
        plugins.add(buildCustomReExecPlugin(string));
      }
    }

    return plugins;
  }

  private IReExecutionPlugin buildReExecPlugin(String name) throws RuntimeException {
    Class<? extends IReExecutionPlugin> pluginType = ReExecutionStrategyType.getPluginClassByName(name);
    if (pluginType == null) {
      return null;
    }

    try {
      return pluginType.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(
          "Unknown re-execution plugin: " + name + " (" + ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES.varname + ")");
    }
  }

  private IReExecutionPlugin buildCustomReExecPlugin(String name) throws RuntimeException {
    try {
      Class<?> cls = Class.forName(name);
      Object o = cls.newInstance();
      if (!(o instanceof IReExecutionPlugin)) {
        throw new RuntimeException(
            "Not re-execution plugin: " + name);
      }

      return (IReExecutionPlugin) o;
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(
          "Unknown re-execution plugin: " + name + " (" + ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES.varname + ")");
    }
  }
}
