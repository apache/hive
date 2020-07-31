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

import java.util.ArrayList;
import java.util.List;

public class TestHooksLoader {

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

  public static class OomRunner implements Runnable {
    @Override public void run() {
      // no op
    }
  }

  @Test
  public void testLoadHooksFromConf() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS,
        PreExecHook.class.getName() + "," + PreExecHook.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS,
        PostExecHook.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
        SemanticAnalysisHook.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_OOM_HOOKS,
        OomRunner.class.getName() + "," + OomRunner.class.getName() + "," + OomRunner.class.getName());
    HookType.OOM.reset();

    HooksLoader loader = new HooksLoader(hiveConf, true);
    verify(HookType.OOM, loader, OomRunner.class, 0, 3);
    verify(HookType.PRE_EXEC_HOOK, loader, PreExecHook.class, 0,2);
    verify(HookType.POST_EXEC_HOOK, loader, PostExecHook.class, 0, 1);
    verify(HookType.SEMANTIC_ANALYZER_HOOK, loader, SemanticAnalysisHook.class, 0,1);

    // load again
    verify(HookType.OOM, loader, OomRunner.class, 3, 3);
    verify(HookType.PRE_EXEC_HOOK, loader, PreExecHook.class, 2, 2);
    verify(HookType.POST_EXEC_HOOK, loader, PostExecHook.class, 1,1);
    verify(HookType.SEMANTIC_ANALYZER_HOOK, loader, SemanticAnalysisHook.class, 1,1);

    // recreate a loader
    loader = new HooksLoader(hiveConf, true);
    // the oom hooks are permanent
    verify(HookType.OOM, loader, OomRunner.class, 3, 3);
    verify(HookType.PRE_EXEC_HOOK, loader, PreExecHook.class, 0,2);
    verify(HookType.POST_EXEC_HOOK, loader, PostExecHook.class, 0, 1);
    verify(HookType.SEMANTIC_ANALYZER_HOOK, loader, SemanticAnalysisHook.class, 0,1);
  }

  private <T> void verify(HookType type, HooksLoader loader, Class<T> expectedCls,
      int beforeSize, int afterSize) throws Exception {
    List<T> loadedHooks = loader.getHooks(type, expectedCls);
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
    HooksLoader loader = new HooksLoader(hiveConf);
    HookType.OOM.reset();

    verify(HookType.OOM, loader, Runnable.class, OomRunner.class);

    List<ExecuteWithHookContext> origPreExecHooks = new ArrayList<>(
        loader.getHooks(HookType.PRE_EXEC_HOOK, ExecuteWithHookContext.class));
    verify(HookType.PRE_EXEC_HOOK, loader, ExecuteWithHookContext.class, PreExecHook.class);

    List<ExecuteWithHookContext> origPostExecHooks = new ArrayList<>(
        loader.getHooks(HookType.POST_EXEC_HOOK, ExecuteWithHookContext.class));
    verify(HookType.POST_EXEC_HOOK, loader, ExecuteWithHookContext.class, PostExecHook.class);

    List<HiveSemanticAnalyzerHook> origSemHooks = new ArrayList<>(
        loader.getHooks(HookType.SEMANTIC_ANALYZER_HOOK, HiveSemanticAnalyzerHook.class));
    verify(HookType.SEMANTIC_ANALYZER_HOOK, loader, HiveSemanticAnalyzerHook.class, SemanticAnalysisHook.class);

    // recreate a loader
    loader = new HooksLoader(hiveConf);
    verify(HookType.OOM, loader, OomRunner.class, 1, 1);
    // the above added hook will be disappear
    verify(HookType.PRE_EXEC_HOOK, loader, origPreExecHooks);
    verify(HookType.POST_EXEC_HOOK, loader, origPostExecHooks);
    verify(HookType.SEMANTIC_ANALYZER_HOOK, loader, origSemHooks);
  }

  private void verify(HookType type, HooksLoader loader, List origHooks) {
    List hooks = loader.getHooks(type);
    Assert.assertTrue(hooks.size() == origHooks.size());
    for (int i = 0; i < hooks.size(); i++) {
      Assert.assertTrue(hooks.get(i) != origHooks.get(i));
      Assert.assertTrue(hooks.get(i).getClass() == origHooks.get(i).getClass());
    }
  }

  private<T> void verify(HookType type, HooksLoader loader, Class<T> superCls, Class realCls)
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
