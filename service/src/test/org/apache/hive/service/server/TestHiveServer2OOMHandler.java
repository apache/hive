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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.server.HiveServer2OOMHandler.HookContext;
import org.apache.hive.service.server.HiveServer2OOMHandler.OOMHook;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

/**
 *  Test HiveServer2 oom hooks
 *
 */
public class TestHiveServer2OOMHandler {

  static class OOMHook1 implements OOMHook {
    static String MSG = "OOMHook1 throws exception";
    @Override
    public void run(HookContext context) {
      throw new RuntimeException(MSG);
    }
  }

  static class OOMHook2 implements OOMHook {
    static String MSG = "OOMHook2 throws exception";
    @Override
    public void run(HookContext context) {
      throw new RuntimeException(MSG);
    }
  }

  @Test
  public void testOOMHooks() {
    HiveConf hiveConf = new HiveConf();
    HiveServer2OOMHandler handler = new HiveServer2OOMHandler(hiveConf);
    try {
      // the default handler
      Assert.assertTrue(handler.getHooks().size() > 0);
      handler.run();
      fail("An exception should throw");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NullPointerException);
    }

    String hook1 = OOMHook1.class.getName(), hook2 = OOMHook2.class.getName();
    hiveConf.setVar(ConfVars.HIVE_SERVER2_OOM_HOOKS, hook1 + "," + hook2);
    handler = new HiveServer2OOMHandler(hiveConf);
    test(handler, OOMHook1.MSG, Arrays.asList(OOMHook1.class, OOMHook2.class));

    hiveConf.setVar(ConfVars.HIVE_SERVER2_OOM_HOOKS, hook2 + "," + hook1);
    handler = new HiveServer2OOMHandler(hiveConf);
    test(handler, OOMHook2.MSG, Arrays.asList(OOMHook2.class, OOMHook1.class));
  }

  private void test(HiveServer2OOMHandler handler,
      String expectedMsg, List<Class> expectedClass) {

    Assert.assertTrue(handler.getHooks().size() == expectedClass.size());
    for (int i = 0; i < expectedClass.size(); i++) {
      Assert.assertTrue(handler.getHooks().get(i).getClass() == expectedClass.get(i));
    }

    try {
      handler.run();
      fail("An exception should throw");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().equals(expectedMsg));
    }
  }
}
