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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.pig;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hcatalog.HcatTestUtils;
import org.apache.hcatalog.mapreduce.HCatBaseTest;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test checks the {@link HCatConstants#HCAT_PIG_STORER_EXTERNAL_LOCATION} that we can set in the
 * UDFContext of {@link HCatStorer} so that it writes to the specified external location.
 *
 * Since {@link HCatStorer} does not allow extra parameters in the constructor, we use {@link HCatStorerWrapper}
 * that always treats the last parameter as the external path.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.pig.TestHCatStorerWrapper} instead
 */
public class TestHCatStorerWrapper extends HCatBaseTest {

  private static final String INPUT_FILE_NAME = TEST_DATA_DIR + "/input.data";

  @Test
  public void testStoreExternalTableWithExternalDir() throws IOException, CommandNeedRetryException{

    File tmpExternalDir = new File(TEST_DATA_DIR, UUID.randomUUID().toString());
    tmpExternalDir.deleteOnExit();

    String part_val = "100";

    driver.run("drop table junit_external");
    String createTable = "create external table junit_external(a int, b string) partitioned by (c string) stored as RCFILE";
    Assert.assertEquals(0, driver.run(createTable).getResponseCode());

    int LOOP_SIZE = 3;
    String[] inputData = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        inputData[k++] = si + "\t"+j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    logAndRegister(server, "A = load '"+INPUT_FILE_NAME+"' as (a:int, b:chararray);");
    logAndRegister(server, "store A into 'default.junit_external' using " + HCatStorerWrapper.class.getName()
        + "('c=" + part_val + "','" + tmpExternalDir.getPath().replaceAll("\\\\", "/") + "');");
    server.executeBatch();

    Assert.assertTrue(tmpExternalDir.exists());
    Assert.assertTrue(new File(tmpExternalDir.getPath().replaceAll("\\\\", "/") + "/" + "part-m-00000").exists());

    driver.run("select * from junit_external");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_external");
    Iterator<String> itr = res.iterator();
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        Assert.assertEquals( si + "\t" + j + "\t" + part_val,itr.next());
      }
    }
    Assert.assertFalse(itr.hasNext());

  }
}
