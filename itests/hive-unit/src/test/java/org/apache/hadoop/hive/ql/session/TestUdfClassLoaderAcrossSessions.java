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
package org.apache.hadoop.hive.ql.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestUdfClassLoaderAcrossSessions {

  @Test
  public void testDropDatabaseCascadeDoesNotThrow() throws CommandProcessorException, IOException {
    HiveConf conf = new HiveConfForTest(this.getClass());
    conf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(conf);
    IDriver driver = DriverFactory.newDriver(conf);

    driver.run("CREATE DATABASE udfacross1");
    driver.run("USE udfacross1");
    driver.run("CREATE FUNCTION dummyFunc AS 'DummyUDF' USING JAR 'testUdf/DummyUDF.jar'");
    SessionState.get().close();
    SessionState.start(conf);
    driver.run("DROP DATABASE udfacross1 CASCADE");
    assertFunctionRemoved(driver, "udfacross1.dummyFunc");
  }

  @Test
  public void testDropFunctionDoesNotThrow() throws CommandProcessorException, IOException {
    HiveConf conf = new HiveConfForTest(this.getClass());
    conf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(conf);
    IDriver driver = DriverFactory.newDriver(conf);

    driver.run("CREATE DATABASE udfacross2");
    driver.run("USE udfacross2");
    driver.run("CREATE FUNCTION dummyFunc AS 'DummyUDF' USING JAR 'testUdf/DummyUDF.jar'");
    SessionState.get().close();
    SessionState.start(conf);
    driver.run("DROP FUNCTION udfacross2.dummyFunc");
    driver.run("DROP DATABASE udfacross2");
    assertFunctionRemoved(driver, "udfacross2.dummyFunc");
  }

  @Test
  public void testUseBeforeDropDatabaseCascadeDoesNotThrow() throws CommandProcessorException, IOException {
    HiveConf conf = new HiveConfForTest(this.getClass());
    conf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(conf);
    IDriver driver = DriverFactory.newDriver(conf);

    driver.run("CREATE DATABASE udfacross3");
    driver.run("USE udfacross3");
    driver.run("CREATE FUNCTION dummyFunc AS 'DummyUDF' USING JAR 'testUdf/DummyUDF.jar'");
    SessionState.get().close();
    SessionState.start(conf);
    driver.run("SELECT udfacross3.dummyFunc(true)");
    driver.run("DROP DATABASE udfacross3 CASCADE");
    assertFunctionRemoved(driver, "udfacross3.dummyFunc");
  }

  private void assertFunctionRemoved(IDriver driver, String name) throws CommandProcessorException, IOException {
    driver.run("DESCRIBE FUNCTION " + name);
    List<String> result = new ArrayList<>();
    driver.getResults(result);
    if (result.size() > 1) {
      Assert.fail("DESCRIBE FUNCTION " + name + ":\n" + result.toArray().toString());
    }
    Assert.assertEquals("Function '" + name +"' does not exist." ,result.get(0));
  }
}
