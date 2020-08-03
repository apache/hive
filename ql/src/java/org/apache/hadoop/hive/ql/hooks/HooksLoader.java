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

import com.cronutils.utils.Preconditions;
import com.cronutils.utils.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *  Loads and stores different kinds of hooks, provides {@link HooksLoader#addHook(HookType, Object)}} to
 *  add hook alone or {@link HooksLoader#getHooks(HookType, Class)} to get all hooks
 *  corresponding to the specific hook type.
 */
public class HooksLoader {

  private static final Logger LOG = LoggerFactory.getLogger(HooksLoader.class);

  private final HiveConf conf;
  // The containers that store different kinds of hooks
  private final HookContainer[] containers;
  // for unit test purpose, check change of the hooks after invoking loadHooksFromConf
  private boolean forTest = false;

  public HooksLoader(HiveConf conf) {
    this.conf = conf;
    this.containers = new HookContainer[HookType.values().length];
    for (int i = 0; i < containers.length; i++) {
      containers[i] = new HookContainer();
    }
  }

  @VisibleForTesting
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
    HookContainer container = containers[type.ordinal()];
    if (!container.loadedFromConf) {
      container.loadedFromConf = true;
      List hooks = container.getHooks();
      HiveConf.ConfVars confVars = type.getHookConfVar();
      try {
        Collection<String> csHooks = conf.getStringCollection(confVars.varname);
        for (String clzName : csHooks) {
          Class hookCls = Class.forName(clzName.trim(), true, Utilities.getSessionSpecifiedClassLoader());
          if (type.getHookClass().isAssignableFrom(hookCls)) {
            Object hookObj = ReflectionUtil.newInstance(hookCls, conf);
            hooks.add(hookObj);
          } else {
            LOG.warn("The clazz: {} should be the subclass of {}, as the type: {} defined, configuration key: {}",
                clzName, type.getHookClass().getName(), type, confVars.varname);
          }
        }
      } catch (Exception e) {
        String message = "Error loading hooks(" + confVars + "): " + HiveStringUtils.stringifyException(e);
        throw new RuntimeException(message, e);
      }
    }
  }

  /**
   * Add the hook corresponding to the specific hook type.
   * @param type The hook type
   * @param hook The hook that will be added
   */
  public void addHook(HookType type, Object hook) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(hook);
    if (!forTest) {
      loadHooksFromConf(type);
    }
    if (type.getHookClass().isAssignableFrom(hook.getClass())) {
      containers[type.ordinal()].addHook(hook);
    } else {
      String message = "Error adding hook: " + hook.getClass().getName() + " into type: " + type +
          ", as the hook doesn't implement or extend: " + type.getHookClass().getName();
      LOG.warn(message);
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
  public <T> List<T> getHooks(HookType type, Class<T> clazz) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(clazz);
    if (!type.getHookClass().isAssignableFrom(clazz)) {
      String message = "The arg clazz: " + clazz.getName() + " should be the same as, "
          + "or the subclass of " + type.getHookClass().getName()
          + ", as the type: " + type + " defined";
      LOG.warn(message);
      throw new IllegalArgumentException(message);
    }
    if (!forTest) {
      loadHooksFromConf(type);
    }
    return containers[type.ordinal()].getHooks();
  }

  public List getHooks(HookType type) {
    return getHooks(type, type.getHookClass());
  }

  /**
   * Inner class which keeps track of the hooks that be loaded or added,
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
