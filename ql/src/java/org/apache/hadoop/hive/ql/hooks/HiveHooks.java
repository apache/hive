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

package org.apache.hadoop.hive.ql.hooks;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Loads and stores different kinds of hooks, provides {@link HiveHooks#addHook(HookContext.HookType, Object)}} to
 *  add hook alone or {@link HiveHooks#getHooks(HookContext.HookType, Class)} to get all hooks
 *  corresponding to the specific hook type.
 */
public class HiveHooks {
  private static final Logger LOG = LoggerFactory.getLogger(HiveHooks.class);
  private final HiveConf conf;
  private final Map<HookContext.HookType, Hooks> typeHooks;
  private SessionState.LogHelper console;

  public HiveHooks(HiveConf conf) {
    this.conf = conf;
    this.typeHooks = new HashMap<>();
  }

  public HiveHooks(HiveConf conf, SessionState.LogHelper console) {
    this(conf);
    this.console = console;
  }

  /**
   * Loads the configured hooks corresponding to the specific hook type.
   * @param type hook type
   */
  @VisibleForTesting
  void loadHooksFromConf(HookContext.HookType type) {
    Hooks hooks = typeHooks.get(type);
    if (hooks == null) {
      typeHooks.put(type, hooks = new Hooks());
      List hookList = hooks.getHooks();
      HiveConf.ConfVars confVar = type.getConfVar();
      Collection<String> csHooks = conf.getStringCollection(confVar.varname);
      try {
        for (String clzName : csHooks) {
          Class hookCls = Class.forName(clzName.trim(), true, Utilities.getSessionSpecifiedClassLoader());
          if (type.getHookClass().isAssignableFrom(hookCls)) {
            Object hookObj = hookCls.newInstance();
            hookList.add(hookObj);
          } else {
            String message = "The class: " + clzName + " should be the subclass of " + type.getHookClass().getName()
                + ", as the type: " + type + " defined";
            logErrorMessage(message);
            throw new ClassCastException(clzName + " cannot be cast to " + type.getHookClass().getName());
          }
        }
      } catch(Exception e) {
        String message = "Error loading hooks(" + confVar + "): " + HiveStringUtils.stringifyException(e);
        throw new RuntimeException(message, e);
      }
    }
  }

  private void logErrorMessage(String message) {
    if (console != null) {
      console.printError(message);
    } else {
      LOG.error(message);
    }
  }

  /**
   * Add the hook corresponding to the specific hook type.
   * @param type The hook type
   * @param hook The hook that will be added
   */
  public void addHook(HookContext.HookType type, Object hook) {
    if (type.getHookClass().isAssignableFrom(hook.getClass())) {
      loadHooksFromConf(type);
      typeHooks.get(type).addHook(hook);
    } else {
      String message = "Error adding hook: " + hook.getClass().getName() + " into type: " + type +
          ", as the hook doesn't implement or extend: " + type.getHookClass().getName();
      logErrorMessage(message);
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Get all hooks corresponding to the specific hook type.
   * @param type The hook type
   * @param clazz The hook class of returning hooks
   * @param <T> The generic type of the hooks
   * @return List of hooks
   */
  public <T> List<T> getHooks(HookContext.HookType type, Class<T> clazz) {
    if (!type.getHookClass().isAssignableFrom(clazz)) {
      String message = "The arg class: " + clazz.getName() + " should be the same as, "
          + "or the subclass of " + type.getHookClass().getName()
          + ", as the type: " + type + " defined";
      logErrorMessage(message);
      throw new IllegalArgumentException(message);
    }
    loadHooksFromConf(type);
    return typeHooks.get(type).getHooks();
  }

  @VisibleForTesting
  List getHooks(HookContext.HookType type, boolean loadFromConf) {
    if (loadFromConf) {
      return getHooks(type);
    }
    return typeHooks.getOrDefault(type, new Hooks()).getHooks();
  }

  public List getHooks(HookContext.HookType type) {
    return getHooks(type, type.getHookClass());
  }

  /**
   * Inner class which keeps track of the hooks that be loaded or added,
   * should not be used outside the class
   * @param <T> the generic type of hooks
   */
  private class Hooks<T> {
    private List<T> hooks = new ArrayList<T>();
    void addHook(T hook) {
      this.hooks.add(hook);
    }

    List<T> getHooks() {
      return this.hooks;
    }
  }
  
}
