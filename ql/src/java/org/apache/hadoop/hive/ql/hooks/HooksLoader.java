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

import com.cronutils.utils.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hive.common.util.HiveStringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *  Loads and stores different kinds of hooks, provides {@link HooksLoader#addHook(HookType, Object)}} to
 *  add hook alone or {@link HooksLoader#getHooks(HookType, Class)} to get all hooks
 *  corresponding to the specific hook type.
 */
public class HooksLoader {

  private final HiveConf conf;
  // The container stores kinds of hooks and
  // keep a flag that whether the hooks has been loaded from configuration or not
  private HookContainer[] containers;

  private boolean forTest = false;

  public HooksLoader(HiveConf conf) {
    this.conf = conf;
    this.containers = new HookContainer[HookType.values().length];
    for (int i = 0; i < containers.length; i++) {
      containers[i] = new HookContainer();
    }
  }

  HooksLoader(HiveConf conf, boolean forTest) {
    this(conf);
    this.forTest = forTest;
  }

  /**
   * Loads the configured hooks corresponding to the specific hook type.
   * @param type hook type
   */
  @VisibleForTesting
  void loadHooksFromConf(HookType type) {
    int index = type.ordinal();
    HookContainer container = containers[index];

    List hooks = null;
    if (type.isGlobal()) {
      if (!type.hasLoadedFromConf()) {
        type.setLoadedFromConf(true);
        hooks = type.getHooks();
      }
    } else if (!container.loadedFromConf) {
      container.loadedFromConf = true;
      hooks = container.getHooks();
    }
    if (hooks != null) {
      HiveConf.ConfVars confVars = type.getHookConfVar();
      try {
        Collection<String> csHooks = conf.getStringCollection(confVars.varname);
        for (String clzName : csHooks) {
          Class hookCls = Class.forName(clzName.trim(), true,
              Utilities.getSessionSpecifiedClassLoader());
          if (type.getHookClass().isAssignableFrom(hookCls)) {
            Object hookObj = hookCls.newInstance();
            hooks.add(hookObj);
          }
        }
      } catch (Exception e) {
        String message = "Error loading hooks(" + confVars + "): " + HiveStringUtils.stringifyException(e);
        throw new RuntimeException(message, e);
      }
    }
  }

  /**
   * Register the hook to the specific hook type.
   * @param type hook type
   * @param hook the hook that will be added
   */
  public void addHook(HookType type, Object hook) {
    if (!forTest) {
      loadHooksFromConf(type);
    }
    if (type.getHookClass().isAssignableFrom(hook.getClass())) {
      if (type.isGlobal()) {
        type.getHooks().add(hook);
      } else {
        int index = type.ordinal();
        containers[index].addHook(hook);
      }
    }
  }

  /**
   * Get all hooks corresponding to the specific hook type.
   * @param type hook type
   * @param clz hook class
   * @param <T> the generic type of the hooks
   * @return list of hooks
   */
  public <T> List<T> getHooks(HookType type, Class<T> clz) {
    if (!forTest) {
      loadHooksFromConf(type);
    }
    if (type.isGlobal()) {
      return type.getHooks();
    }
    int index = type.ordinal();
    return containers[index].getHooks();
  }

  public List getHooks(HookType type) {
    if (!forTest) {
      loadHooksFromConf(type);
    }
    if (type.isGlobal()) {
      return type.getHooks();
    }
    int index = type.ordinal();
    return containers[index].getHooks();
  }

  /**
   * Inner class, keep tracks of the hooks that be loaded or added,
   * should not be used outside the class
   * @param <T> the generic type of hooks
   */
  private class HookContainer<T> {

    private boolean loadedFromConf = false;
    private List<T> hooks = new ArrayList<T>();
    void addHook(T hook) {
      this.hooks.add(hook);
    }

    List<T> getHooks() {
      return this.hooks;
    }

  }

}



