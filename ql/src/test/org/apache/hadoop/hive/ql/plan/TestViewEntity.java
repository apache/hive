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
package org.apache.hadoop.hive.ql.plan;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.TestReadEntityDirect.CheckInputReadEntityDirect;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestViewEntity {
  /**
   * Hook used in the test to capture the set of ReadEntities
   */
  public static class CheckInputReadEntity extends
      AbstractSemanticAnalyzerHook {
    public static ReadEntity[] readEntities;

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
        List<Task<? extends Serializable>> rootTasks) throws SemanticException {
      readEntities = context.getInputs().toArray(new ReadEntity[0]);
    }

  }

  private static Driver driver;

  @BeforeClass
  public static void onetimeSetup() throws Exception {
    HiveConf conf = new HiveConf(Driver.class);
    conf.setVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
        CheckInputReadEntity.class.getName());
    HiveConf
        .setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    SessionState.start(conf);
    driver = new Driver(conf);
    driver.init();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    driver.close();
    driver.destroy();
  }

  /**
   * Verify that the parent entities are captured correctly for union views
   * @throws Exception
   */
  @Test
  public void testUnionView() throws Exception {
    int ret = driver.run("create table t1(id int)").getResponseCode();
    assertEquals("Checking command success", 0, ret);
    ret = driver.run("create table t2(id int)").getResponseCode();
    assertEquals("Checking command success", 0, ret);
    ret = driver.run("create view v1 as select t.id from "
            + "(select t1.id from t1 union all select t2.id from t2) as t")
        .getResponseCode();
    assertEquals("Checking command success", 0, ret);

    driver.compile("select * from v1");
    // view entity
    assertEquals("default@v1", CheckInputReadEntity.readEntities[0].getName());

    // first table in union query with view as parent
    assertEquals("default@t1", CheckInputReadEntity.readEntities[1].getName());
    assertEquals("default@v1", CheckInputReadEntity.readEntities[1]
        .getParents()
        .iterator().next().getName());
    // second table in union query with view as parent
    assertEquals("default@t2", CheckInputReadEntity.readEntities[2].getName());
    assertEquals("default@v1", CheckInputReadEntity.readEntities[2]
        .getParents()
        .iterator().next().getName());

  }

}
