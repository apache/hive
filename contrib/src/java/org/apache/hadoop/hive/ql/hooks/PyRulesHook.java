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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Reader;
import java.io.File;
import java.io.FileReader;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * This hook executes python code to update the configuration in
 * the ContextHook using java scripting abstraction.
 *
 * fbhive.pyrules.property has the path of the python file that is to
 * be executed
 * Python code has to define a method updateConf that accepts hookContext
 * as a parameter
 *
 * Python code has to also provide a revertConf method that accepts hookContext
 * and the old configuration object and reverts the changes made to the
 * configuration in the updateConf
 */
public class PyRulesHook implements ExecuteWithHookContext {

  static final private Log LOG = LogFactory.getLog(PyRulesHook.class);
  static private HiveConf savedConf = null;
  @Override

  public void run(HookContext hookContext) throws Exception {
    HiveConf conf = hookContext.getConf();
    PyRulesHook.savedConf = new HiveConf(conf);
    ScriptEngine pythonMod = getPythonModifier(hookContext);
    if (pythonMod == null) {
      return;
    }
    conf.setBoolean("fbhive.pyrules.modified", true);
    try {
      pythonMod.put("hookContext", hookContext);
      pythonMod.eval("updateConf(hookContext)");
    } catch (Exception ex) {
      LOG.error("Error updating the conf", ex);
    }
 }

  private static ScriptEngine getPythonModifier(HookContext hookContext)
        throws Exception {
    String pyFilePath = hookContext.getConf().get("fbhive.pyrules.file");
    if (pyFilePath == null)
      return null;

    File pyFile = new File(pyFilePath);
    if (!pyFile.exists()) {
      LOG.warn("The python conf file " + pyFile + " does not exist");
      return null;
    }

    Reader reader = new FileReader(pyFile);
    try {
      ScriptEngine eng = new ScriptEngineManager().getEngineByName("python");
      if (eng == null) {
        LOG.warn("Could not initialize jython engine");
        return null;
      }
      eng.eval(reader);

      return eng;
    } catch (Exception ex) {
      LOG.warn("Error updating the conf using python hook", ex);
      return null;
    }
  }
  public static class CleanupHook implements ExecuteWithHookContext {
    public void run(HookContext hookContext) throws Exception {
      if (!hookContext.getConf().getBoolean("fbhive.pyrules.modified", false)) {
        return;
      } else {
        try {
          ScriptEngine pythonRevert = getPythonModifier(hookContext);
          pythonRevert.put("hookContext", hookContext);
          pythonRevert.put("oldConf", PyRulesHook.savedConf);
          pythonRevert.eval("revertConf(hookContext, oldConf)");
        } catch (Exception ex) {
          LOG.error("Error reverting config", ex);
        }
      }
    }
  }
}
