/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.reexec.ReExecDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCreateUdfEntities {
  private IDriver driver;
  private String funcName = "print_test";

  @Before
  public void setUp() throws Exception {

    HiveConf conf = new HiveConf(IDriver.class);
    SessionState.start(conf);
    driver = DriverFactory.newDriver(conf);
  }

  @After
  public void tearDown() throws Exception {
    driver.run("drop function " + funcName);
    driver.close();
    SessionState.get().close();
  }

  @Test
  public void testUdfWithLocalResource() throws Exception {
    int rc = ((ReExecDriver)driver).compile("CREATE FUNCTION " + funcName + " AS " +
        "'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'  using file '" + "file:///tmp/udf1.jar'", true);
    assertEquals(0, rc);
    WriteEntity outputEntities[] = driver.getPlan().getOutputs().toArray(new WriteEntity[] {});
    assertEquals(outputEntities.length, 2);

    assertEquals(Entity.Type.FUNCTION, outputEntities[0].getType());
    assertEquals(funcName, outputEntities[0].getFunction().getFunctionName());

    assertEquals(Entity.Type.LOCAL_DIR, outputEntities[1].getType());
    assertEquals("file:///tmp/udf1.jar", outputEntities[1].getLocation().toString());
  }

  @Test
  public void testUdfWithDfsResource() throws Exception {
    int rc = ((ReExecDriver)driver).compile("CREATE FUNCTION default." + funcName + " AS " +
        "'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'  using file '" + "hdfs:///tmp/udf1.jar'", true);
    assertEquals(0, rc);
    WriteEntity outputEntities[] = driver.getPlan().getOutputs().toArray(new WriteEntity[] {});
    assertEquals(outputEntities.length, 2);

    assertEquals(Entity.Type.FUNCTION, outputEntities[0].getType());
    assertEquals(funcName, outputEntities[0].getFunction().getFunctionName());

    assertEquals(Entity.Type.DFS_DIR, outputEntities[1].getType());
    assertEquals("hdfs:///tmp/udf1.jar", outputEntities[1].getLocation().toString());
  }

}
