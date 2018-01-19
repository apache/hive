/*
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
 **/

package org.apache.hadoop.hive.ql.hooks;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;


/**
 * A loader class for {@link Hook}s. The class provides a way to create and instantiate {@link Hook} objects. The
 * methodology for how hooks are loaded is left up to the individual methods.
 */
public class HooksLoader {

  private final HiveConf conf;

  /**
   * Creates a new {@link HooksLoader} that uses the specified {@link HiveConf} to load the {@link Hook}s.
   *
   * @param conf the {@link HiveConf} to use when loading the {@link Hook}s
   */
  public HooksLoader(HiveConf conf) {
    this.conf = conf;
  }

  /**
   * Delegates to {@link #getHooks(HiveConf.ConfVars)} and prints the to the specified {@link SessionState.LogHelper} if
   * a {@link ClassNotFoundException} is thrown.
   *
   * @param hookConfVar the configuration variable specifying a comma separated list of the hook class names
   * @param console the {@link SessionState.LogHelper} to print to if a {@link ClassNotFoundException} is thrown by the
   *                {@link #getHooks(HiveConf.ConfVars)} method
   *
   * @return a list of the hooks objects, in the order they are listed in the value of hookConfVar
   *
   * @throws ClassNotFoundException if the specified class names could not be found
   * @throws IllegalAccessException if the specified class names could not be accessed
   * @throws InstantiationException if the specified class names could not be instantiated
   */
  public final <T extends Hook> List<T> getHooks(HiveConf.ConfVars hookConfVar, SessionState.LogHelper console, Class<?> clazz)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    try {
      return getHooks(hookConfVar, clazz);
    } catch (ClassNotFoundException e) {
      console.printError(hookConfVar.varname + " Class not found: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Returns the hooks specified in a configuration variable. The hooks are returned in a list in the order they were
   * specified in the configuration variable. The value of the specified conf variable should be a comma separated list
   * of class names where each class implements the {@link Hook} interface. The method uses reflection to an instance
   * of each class and then returns them in a {@link List}.
   *
   * @param hookConfVar The configuration variable specifying a comma separated list of the hook class names
   * @param class2
   * @param class1
   * @param console
   *
   * @return a list of the hooks objects, in the order they are listed in the value of hookConfVar
   *
   * @throws ClassNotFoundException if the specified class names could not be found
   * @throws IllegalAccessException if the specified class names could not be accessed
   * @throws InstantiationException if the specified class names could not be instantiated
   */
  public <T extends Hook> List<T> getHooks(HiveConf.ConfVars hookConfVar, Class<?> clazz)
          throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    String csHooks = conf.getVar(hookConfVar);
    ImmutableList.Builder<T> hooks = ImmutableList.builder();
    if (csHooks == null) {
      return ImmutableList.of();
    }

    csHooks = csHooks.trim();
    if (csHooks.isEmpty()) {
      return ImmutableList.of();
    }

    String[] hookClasses = csHooks.split(",");
    for (String hookClass : hookClasses) {
      T hook = (T) Class.forName(hookClass.trim(), true,
              Utilities.getSessionSpecifiedClassLoader()).newInstance();
      hooks.add(hook);
    }

    return hooks.build();
  }
}
