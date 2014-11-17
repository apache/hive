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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Test if ReadEntity isDirect flag is set correctly to indicate if
 * the entity represents a direct or indirect dependency. See documentation
 * of flag in ReadEntity.
 */
public class TestReadEntityDirect {

  @BeforeClass
  public static void onetimeSetup() throws CommandNeedRetryException {
    Driver driver = createDriver();
    int ret = driver.run("create table t1(i int)").getResponseCode();
    assertEquals("Checking command success", 0, ret);
    ret = driver.run("create view v1 as select * from t1").getResponseCode();
    assertEquals("Checking command success", 0, ret);
  }

  @AfterClass
  public static void onetimeTeardown() throws Exception {
    Driver driver = createDriver();
    driver.run("drop table t1");
    driver.run("drop view v1");
  }

  @Before
  public void setup() {
    CheckInputReadEntityDirect.readEntities = null;
  }

  /**
   * No views in the query so it should be a direct entity
   *
   * @throws ParseException
   */
  @Test
  public void testSelectEntityDirect() throws ParseException {
    Driver driver = createDriver();
    int ret = driver.compile("select * from t1");
    assertEquals("Checking command success", 0, ret);
    assertEquals(1, CheckInputReadEntityDirect.readEntities.size());
    assertTrue("isDirect", CheckInputReadEntityDirect.readEntities.iterator().next().isDirect());
  }

  /**
   * Underlying table of view should be marked as indirect
   *
   * @throws ParseException
   */
  @Test
  public void testSelectEntityInDirect() throws ParseException {
    Driver driver = createDriver();
    int ret = driver.compile("select * from v1");
    assertEquals("Checking command success", 0, ret);
    assertEquals(2, CheckInputReadEntityDirect.readEntities.size());
    for (ReadEntity readEntity : CheckInputReadEntityDirect.readEntities) {
      if (readEntity.getName().equals("default@t1")) {
        assertFalse("not direct", readEntity.isDirect());
      } else if (readEntity.getName().equals("default@v1")) {
        assertTrue("direct", readEntity.isDirect());
      } else {
        fail("unexpected entity name " + readEntity.getName());
      }
    }
  }

  /**
   * Underlying table of view should be marked as direct, as it is also accessed
   * directly in the join query
   *
   * @throws ParseException
   */
  @Test
  public void testSelectEntityViewDirectJoin() throws ParseException {
    Driver driver = createDriver();
    int ret = driver.compile("select * from v1 join t1 on (v1.i = t1.i)");
    assertEquals("Checking command success", 0, ret);
    assertEquals(2, CheckInputReadEntityDirect.readEntities.size());
    for (ReadEntity readEntity : CheckInputReadEntityDirect.readEntities) {
      if (readEntity.getName().equals("default@t1")) {
        assertTrue("direct", readEntity.isDirect());
      } else if (readEntity.getName().equals("default@v1")) {
        assertTrue("direct", readEntity.isDirect());
      } else {
        fail("unexpected entity name " + readEntity.getName());
      }
    }
  }

  /**
   * Underlying table of view should be marked as direct, as it is also accessed
   * directly in the union-all query
   *
   * @throws ParseException
   */
  @Test
  public void testSelectEntityViewDirectUnion() throws ParseException {
    Driver driver = createDriver();
    int ret = driver.compile("select * from ( select * from v1 union all select * from t1) uv1t1");
    assertEquals("Checking command success", 0, ret);
    assertEquals(2, CheckInputReadEntityDirect.readEntities.size());
    for (ReadEntity readEntity : CheckInputReadEntityDirect.readEntities) {
      if (readEntity.getName().equals("default@t1")) {
        assertTrue("direct", readEntity.isDirect());
      } else if (readEntity.getName().equals("default@v1")) {
        assertTrue("direct", readEntity.isDirect());
      } else {
        fail("unexpected entity name " + readEntity.getName());
      }
    }
  }

  /**
   * Underlying table of view should be marked as indirect. Query with join of views and aliases
   *
   * @throws ParseException
   */
  @Test
  public void testSelectEntityInDirectJoinAlias() throws ParseException {
    Driver driver = createDriver();
    int ret = driver.compile("select * from v1 as a join v1 as b on (a.i = b.i)");
    assertEquals("Checking command success", 0, ret);
    assertEquals(2, CheckInputReadEntityDirect.readEntities.size());
    for (ReadEntity readEntity : CheckInputReadEntityDirect.readEntities) {
      if (readEntity.getName().equals("default@t1")) {
        assertFalse("not direct", readEntity.isDirect());
      } else if (readEntity.getName().equals("default@v1")) {
        assertTrue("direct", readEntity.isDirect());
      } else {
        fail("unexpected entity name " + readEntity.getName());
      }
    }
  }

  /**
   * Create driver with the test hook set in config
   */
  private static Driver createDriver() {
    HiveConf conf = new HiveConf(Driver.class);
    conf.setVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
        CheckInputReadEntityDirect.class.getName());
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    driver.init();
    return driver;
  }

  /**
   * Hook used in the test to capture the set of ReadEntities
   */
  public static class CheckInputReadEntityDirect extends AbstractSemanticAnalyzerHook {
    public static Set<ReadEntity> readEntities;

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
        List<Task<? extends Serializable>> rootTasks) throws SemanticException {
      readEntities = context.getInputs();
    }

  }

}
