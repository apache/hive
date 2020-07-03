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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HiveServer2OOMHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(HiveServer2OOMHandler.class);
  private HookContext context;
  private final List<OOMHook> hooks = new ArrayList<OOMHook>();

  HiveServer2OOMHandler(HiveServer2 hiveServer2) {
    context = new HookContext(hiveServer2);
    init(hiveServer2.getHiveConf());
  }

  private void init(HiveConf hiveConf) {
    String csHooks = hiveConf.getVar(ConfVars.HIVE_SERVER2_OOM_HOOKS);
    if (!StringUtils.isBlank(csHooks)) {
      String[] hookClasses = csHooks.split(",");
      for (String hookClass : hookClasses) {
        try {
          OOMHook hook = (OOMHook) Class.forName(hookClass.trim(), true,
              JavaUtils.getClassLoader()).newInstance();
          hooks.add(hook);
        } catch (Exception e) {
          LOG.error("Skip adding oom hook '" + hookClass + "'", e);
        }
      }
    }
  }

  @VisibleForTesting
  HiveServer2OOMHandler(HiveConf hiveConf) {
    init(hiveConf);
  }

  @VisibleForTesting
  public List<OOMHook> getHooks() {
    return hooks;
  }

  @Override
  public void run() {
    for (OOMHook hook : hooks) {
      hook.run(context);
    }
  }

  public static interface OOMHook {
    public void run(HookContext context);
  }

  public static class HookContext {
    private final HiveServer2 hiveServer2;
    public HookContext(HiveServer2 hiveServer2) {
      this.hiveServer2 = hiveServer2;
    }
    public HiveServer2 getHiveServer2() {
      return hiveServer2;
    }
  }

  static class DefaultOOMHook implements OOMHook {
    @Override
    public void run(HookContext context) {
      context.getHiveServer2().stop();
    }
  }
}
