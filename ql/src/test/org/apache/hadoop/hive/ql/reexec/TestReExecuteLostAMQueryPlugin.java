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
package org.apache.hadoop.hive.ql.reexec;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.tez.TezRuntimeException;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.junit.Assert;
import org.junit.Test;

public class TestReExecuteLostAMQueryPlugin {

  @Test
  public void testRetryOnUnmanagedAmFailure() throws Exception {
    testReExecuteWithExceptionMessage("AM record not found (likely died)");
  }

  @Test
  public void testRetryOnLostAmContainerMessageWithLineBreak() throws Exception {
    testReExecuteWithExceptionMessage("Application application_1728328561547_0042 failed 1 times (global limit =5; " +
        "local limit is =1) due to AM Container for appattempt_1728328561547_0042_000001 exited with  exitCode: " +
        "-100\nFailing this attempt.Diagnostics: Container released on a *lost* nodeFor more detailed output, check " +
        "the application tracking page: https://host/cluster/app/application_1728328561547_0042" +
        " Then click on links to logs of each attempt.\n" +
        ". Failing the application.");
  }

  @Test
  public void testRetryOnNoCurrentDAGException() throws Exception {
    testReExecuteWithExceptionMessage("No running DAG at present");
  }

  private void testReExecuteWithExceptionMessage(String message) throws Exception {
    ReExecuteLostAMQueryPlugin plugin = new ReExecuteLostAMQueryPlugin();
    ReExecuteLostAMQueryPlugin.LocalHook hook = plugin.new LocalHook();

    HookContext context = new HookContext(null, QueryState.getNewQueryState(new HiveConf(), null), null, null, null,
        null, null, null, null, false, null, null);
    context.setHookType(HookContext.HookType.ON_FAILURE_HOOK);
    context.setException(new TezRuntimeException("dag_0_0", message));

    hook.run(context);

    Assert.assertTrue(plugin.shouldReExecute(1));
  }
}
