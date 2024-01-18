/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestHiveHooks {

  public static class PostExecHook implements ExecuteWithHookContext {

    @Override public void run(HookContext hookContext) {
      // no op
    }
  }

  public static class PreExecHook implements ExecuteWithHookContext {

    @Override public void run(HookContext hookContext) {
      // no op
    }
  }

  public static class SemanticAnalysisHook implements HiveSemanticAnalyzerHook {

    @Override public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
        ASTNode ast) throws SemanticException {
      return null;
    }

    @Override public void postAnalyze(HiveSemanticAnalyzerHookContext context,
        List<Task<?>> rootTasks) throws SemanticException {
      // no op
    }
  }

  @Test
  public void testLoadHooksFromConf() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS,
        PreExecHook.class.getName() + "," + PreExecHook.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.POST_EXEC_HOOKS,
        PostExecHook.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
        SemanticAnalysisHook.class.getName());

    HiveHooks loader = new HiveHooks(hiveConf);
    verify(HookContext.HookType.PRE_EXEC_HOOK, loader, PreExecHook.class, 0,2);
    verify(HookContext.HookType.POST_EXEC_HOOK, loader, PostExecHook.class, 0, 1);
    verify(HookContext.HookType.SEMANTIC_ANALYZER_HOOK, loader, SemanticAnalysisHook.class, 0,1);
    // load again
    verify(HookContext.HookType.PRE_EXEC_HOOK, loader, PreExecHook.class, 2, 2);
    verify(HookContext.HookType.POST_EXEC_HOOK, loader, PostExecHook.class, 1,1);
    verify(HookContext.HookType.SEMANTIC_ANALYZER_HOOK, loader, SemanticAnalysisHook.class, 1,1);
  }

  private <T> void verify(HookContext.HookType type, HiveHooks loader, Class<T> expectedCls,
      int beforeSize, int afterSize) throws Exception {
    List loadedHooks = loader.getHooks(type, false);
    Assert.assertTrue(loadedHooks.size() == beforeSize);
    for (int i = 0; i < loadedHooks.size(); i++) {
      Assert.assertTrue(expectedCls.isAssignableFrom(loadedHooks.get(i).getClass()));
    }

    loader.loadHooksFromConf(type);

    loadedHooks = loader.getHooks(type, expectedCls);
    Assert.assertTrue(loadedHooks.size() == afterSize);
    for (int i = 0; i < loadedHooks.size(); i++) {
      Assert.assertTrue(expectedCls.isAssignableFrom(loadedHooks.get(i).getClass()));
    }
  }

  @Test
  public void testAddHooks() throws Exception {
    HiveConf hiveConf = new HiveConf();
    HiveHooks loader = new HiveHooks(hiveConf);
    verify(HookContext.HookType.PRE_EXEC_HOOK, loader, ExecuteWithHookContext.class, PreExecHook.class);
    verify(HookContext.HookType.POST_EXEC_HOOK, loader, ExecuteWithHookContext.class, PostExecHook.class);
    verify(HookContext.HookType.SEMANTIC_ANALYZER_HOOK, loader, HiveSemanticAnalyzerHook.class, SemanticAnalysisHook.class);
  }

  private<T> void verify(HookContext.HookType type, HiveHooks loader, Class<T> superCls, Class realCls)
      throws Exception {
    List<T> origHooks = loader.getHooks(type, superCls);
    int size = origHooks.size();
    verify(type, loader, superCls, size, size);
    loader.addHook(type, realCls.newInstance());
    verify(type, loader, superCls, size + 1, size + 1);

    List<T> loadedHooks = loader.getHooks(type, superCls);
    T hook = loadedHooks.get(loadedHooks.size() - 1);
    Assert.assertTrue(hook.getClass() == realCls);
  }
  
}
