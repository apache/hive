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

package org.apache.hive.common.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * TestShutdownHookManager.
 *
 * Originally taken from o.a.hadoop.util.TestShutdownHookManager
 */
public class TestShutdownHookManager {

  @Test
  public void shutdownHookManager() {
    Assert.assertEquals(0, ShutdownHookManager.getShutdownHooksInOrder().size());
    Runnable hook1 = new Runnable() {
      @Override
      public void run() {
      }
    };
    Runnable hook2 = new Runnable() {
      @Override
      public void run() {
      }
    };

    ShutdownHookManager.addShutdownHook(hook1, 0);
    Assert.assertTrue(ShutdownHookManager.hasShutdownHook(hook1));
    Assert.assertEquals(1, ShutdownHookManager.getShutdownHooksInOrder().size());
    Assert.assertEquals(hook1, ShutdownHookManager.getShutdownHooksInOrder().get(0));
    ShutdownHookManager.removeShutdownHook(hook1);
    Assert.assertFalse(ShutdownHookManager.hasShutdownHook(hook1));

    ShutdownHookManager.addShutdownHook(hook1, 0);
    Assert.assertTrue(ShutdownHookManager.hasShutdownHook(hook1));
    Assert.assertEquals(1, ShutdownHookManager.getShutdownHooksInOrder().size());
    Assert.assertTrue(ShutdownHookManager.hasShutdownHook(hook1));
    Assert.assertEquals(1, ShutdownHookManager.getShutdownHooksInOrder().size());

    ShutdownHookManager.addShutdownHook(hook2, 1);
    Assert.assertTrue(ShutdownHookManager.hasShutdownHook(hook1));
    Assert.assertTrue(ShutdownHookManager.hasShutdownHook(hook2));
    Assert.assertEquals(2, ShutdownHookManager.getShutdownHooksInOrder().size());
    Assert.assertEquals(hook2, ShutdownHookManager.getShutdownHooksInOrder().get(0));
    Assert.assertEquals(hook1, ShutdownHookManager.getShutdownHooksInOrder().get(1));

  }
}
