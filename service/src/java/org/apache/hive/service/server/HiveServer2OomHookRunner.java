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

package org.apache.hive.service.server;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hive.common.util.HiveStringUtils;

import java.util.ArrayList;
import java.util.List;

public class HiveServer2OomHookRunner implements Runnable {

  private OomHookContext context;
  private final List<OomHookWithContext> hooks = new ArrayList<OomHookWithContext>();
  // The default hook stops the hiveserver2 gracefully.
  private final DefaultOomHook defaultOomHook = new DefaultOomHook();

  HiveServer2OomHookRunner(HiveServer2 hiveServer2, HiveConf hiveConf) {
    context = new OomHookContext(hiveServer2);
    // The hs2 has not been initialized yet, hiveServer2.getHiveConf() would be null
    init(hiveConf);
  }

  private void init(HiveConf hiveConf) {
    try {
      List<OomHookWithContext> oomHooks = HookUtils.readHooksFromConf(hiveConf, ConfVars.HIVE_SERVER2_OOM_HOOKS);
      hooks.addAll(oomHooks);
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      String message = "Error loading hooks(" +
          ConfVars.HIVE_SERVER2_OOM_HOOKS + "): " + HiveStringUtils.stringifyException(e);
      throw new RuntimeException(message, e);
    }
  }

  @VisibleForTesting
  public HiveServer2OomHookRunner(HiveConf hiveConf) {
    init(hiveConf);
  }

  @VisibleForTesting
  public List<OomHookWithContext> getHooks() {
    return hooks;
  }

  @Override
  public synchronized void run() {
    try {
      for (OomHookWithContext hook : hooks) {
        hook.run(context);
      }
    } finally {
      // In unit test, context is null
      if (context != null) {
        defaultOomHook.run(context);
      }
    }
  }

  public static interface OomHookWithContext extends Hook {
    public void run(OomHookContext context);
  }

  public static class OomHookContext {
    private final HiveServer2 hiveServer2;

    public OomHookContext(HiveServer2 hiveServer2) {
      this.hiveServer2 = hiveServer2;
    }

    public HiveServer2 getHiveServer2() {
      return hiveServer2;
    }
  }

  /**
   * Used as default oom hook
   */
  public static class DefaultOomHook implements OomHookWithContext {
    @Override
    public void run(OomHookContext context) {
      context.getHiveServer2().stop();
    }
  }
}
