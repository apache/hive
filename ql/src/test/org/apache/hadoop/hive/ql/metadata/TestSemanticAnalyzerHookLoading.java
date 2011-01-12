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

package org.apache.hadoop.hive.ql.metadata;

import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

public class TestSemanticAnalyzerHookLoading extends TestCase {

  public void testHookLoading() throws Exception{

    HiveConf conf = new HiveConf(this.getClass());
    conf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, DummySemanticAnalyzerHook.class.getName());
    conf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(conf);
    Driver driver = new Driver(conf);

    driver.run("drop table testDL");
    CommandProcessorResponse resp = driver.run("create table testDL (a int) as select * from tbl2");
    assertEquals(10, resp.getResponseCode());
    assertTrue(resp.getErrorMessage().contains("CTAS not supported."));

    resp = driver.run("create table testDL (a int)");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    Map<String,String> params = Hive.get(conf).getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, "testDL").getParameters();

    assertEquals(DummyCreateTableHook.class.getName(),params.get("createdBy"));
    assertEquals("Open Source rocks!!", params.get("Message"));

    driver.run("drop table testDL");
  }
}
