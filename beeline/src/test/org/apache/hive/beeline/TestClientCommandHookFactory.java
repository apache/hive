/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline;

import junit.framework.Assert;
import org.junit.Test;

public class TestClientCommandHookFactory {
  @Test
  public void testGetHook() {
    Assert.assertNull(ClientCommandHookFactory.get().getHook("set a;"));
    Assert.assertTrue(ClientCommandHookFactory.get()
        .getHook("set a=b;") instanceof ClientCommandHookFactory.SetCommandHook);
    Assert.assertTrue(ClientCommandHookFactory.get()
        .getHook("USE a.b") instanceof ClientCommandHookFactory.UseCommandHook);
  }
}
