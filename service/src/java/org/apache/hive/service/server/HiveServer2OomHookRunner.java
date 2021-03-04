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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HiveServer2OomHookRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveServer2OomHookRunner.class);
  private final HiveServer2 hiveServer2;

  HiveServer2OomHookRunner(HiveServer2 hiveServer2) {
    this.hiveServer2 = hiveServer2;
  }

  @Override
  public synchronized void run() {
    try {
      LOG.warn("HiveServer2 is being directed to shutdown because an OOM was detected.");
      HiveConf hiveConf = hiveServer2.getHiveConf();
      List<Runnable> hooks = HookUtils.readHooksFromConf(hiveConf, HookType.HIVE_SERVER2_OOM_HOOKS);
      for (Runnable runnable : hooks) {
        runnable.run();
      }
    } finally {
      hiveServer2.stop();
    }
  }

}
