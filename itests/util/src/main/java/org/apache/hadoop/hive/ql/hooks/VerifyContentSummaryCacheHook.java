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

import java.util.Map;

import org.junit.Assert;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;

public class VerifyContentSummaryCacheHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    Map<String, ContentSummary> inputToCS = hookContext.getInputPathToContentSummary();
    if (hookContext.getHookType().equals(HookType.PRE_EXEC_HOOK)) {
      Assert.assertEquals(0, inputToCS.size());
    } else {
      Assert.assertEquals(1, inputToCS.size());
      String tmp_dir = System.getProperty("test.tmp.dir");
      for (String key : inputToCS.keySet()) {
        if (!key.equals(tmp_dir + "/VerifyContentSummaryCacheHook") &&
            !key.equals("pfile:" + tmp_dir + "/VerifyContentSummaryCacheHook")) {
          Assert.fail("VerifyContentSummaryCacheHook fails the input path check");
        }
      }
    }
  }
}
