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

package org.apache.hadoop.hive.ql.metadata;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestSemanticAnalyzerHookLoading.
 */
public class TestSemanticAnalyzerHookLoading {

  @Test
  public void testHookLoading() throws Exception {

    HiveConf conf = new HiveConf(this.getClass());
    conf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, DummySemanticAnalyzerHook.class.getName());
    conf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(conf);

    run("drop table testDL", conf);
    try {
      run("create table testDL (a int) as select * from tbl2", conf);
      assert false;
    } catch (CommandProcessorException e) {
      assertEquals(40000, e.getResponseCode());
    }

    run("create table testDL (a int)", conf);

    Map<String,String> params = Hive.get(conf).getTable(Warehouse.DEFAULT_DATABASE_NAME, "testDL").getParameters();

    assertEquals(DummyCreateTableHook.class.getName(),params.get("createdBy"));
    assertEquals("Open Source rocks!!", params.get("Message"));

    run("drop table testDL", conf);
  }

  private void run(String command, HiveConf conf) throws CommandProcessorException {
    try (IDriver driver = DriverFactory.newDriver(conf)) {
      driver.run(command);
    }
  }
}
