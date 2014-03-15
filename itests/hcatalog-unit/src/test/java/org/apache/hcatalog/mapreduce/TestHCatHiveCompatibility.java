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

package org.apache.hcatalog.mapreduce;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.Assert;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.TestHCatHiveCompatibility} instead
 */
public class TestHCatHiveCompatibility extends HCatBaseTest {
  private static final String INPUT_FILE_NAME = TEST_DATA_DIR + "/input.data";

  @BeforeClass
  public static void createInputData() throws Exception {
    int LOOP_SIZE = 11;
    File file = new File(INPUT_FILE_NAME);
    file.deleteOnExit();
    FileWriter writer = new FileWriter(file);
    for (int i = 0; i < LOOP_SIZE; i++) {
      writer.write(i + "\t1\n");
    }
    writer.close();
  }

  @Test
  public void testUnpartedReadWrite() throws Exception {

    driver.run("drop table if exists junit_unparted_noisd");
    String createTable = "create table junit_unparted_noisd(a int) stored as RCFILE";
    Assert.assertEquals(0, driver.run(createTable).getResponseCode());

    // assert that the table created has no hcat instrumentation, and that we're still able to read it.
    Table table = client.getTable("default", "junit_unparted_noisd");
    Assert.assertTrue(table.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));

    PigServer server = new PigServer(ExecType.LOCAL);
    logAndRegister(server, "A = load '" + INPUT_FILE_NAME + "' as (a:int);");
    logAndRegister(server, "store A into 'default.junit_unparted_noisd' using org.apache.hcatalog.pig.HCatStorer();");
    logAndRegister(server, "B = load 'default.junit_unparted_noisd' using org.apache.hcatalog.pig.HCatLoader();");
    Iterator<Tuple> itr = server.openIterator("B");

    int i = 0;

    while (itr.hasNext()) {
      Tuple t = itr.next();
      Assert.assertEquals(1, t.size());
      Assert.assertEquals(t.get(0), i);
      i++;
    }

    Assert.assertFalse(itr.hasNext());
    Assert.assertEquals(11, i);

    // assert that the table created still has no hcat instrumentation
    Table table2 = client.getTable("default", "junit_unparted_noisd");
    Assert.assertTrue(table2.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));

    driver.run("drop table junit_unparted_noisd");
  }

  @Test
  public void testPartedRead() throws Exception {

    driver.run("drop table if exists junit_parted_noisd");
    String createTable = "create table junit_parted_noisd(a int) partitioned by (b string) stored as RCFILE";
    Assert.assertEquals(0, driver.run(createTable).getResponseCode());

    // assert that the table created has no hcat instrumentation, and that we're still able to read it.
    Table table = client.getTable("default", "junit_parted_noisd");
    Assert.assertTrue(table.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));

    PigServer server = new PigServer(ExecType.LOCAL);
    logAndRegister(server, "A = load '" + INPUT_FILE_NAME + "' as (a:int);");
    logAndRegister(server, "store A into 'default.junit_parted_noisd' using org.apache.hcatalog.pig.HCatStorer('b=42');");
    logAndRegister(server, "B = load 'default.junit_parted_noisd' using org.apache.hcatalog.pig.HCatLoader();");
    Iterator<Tuple> itr = server.openIterator("B");

    int i = 0;

    while (itr.hasNext()) {
      Tuple t = itr.next();
      Assert.assertEquals(2, t.size()); // Contains explicit field "a" and partition "b".
      Assert.assertEquals(t.get(0), i);
      Assert.assertEquals(t.get(1), "42");
      i++;
    }

    Assert.assertFalse(itr.hasNext());
    Assert.assertEquals(11, i);

    // assert that the table created still has no hcat instrumentation
    Table table2 = client.getTable("default", "junit_parted_noisd");
    Assert.assertTrue(table2.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));

    // assert that there is one partition present, and it had hcat instrumentation inserted when it was created.
    Partition ptn = client.getPartition("default", "junit_parted_noisd", Arrays.asList("42"));

    Assert.assertNotNull(ptn);
    Assert.assertTrue(ptn.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));
    driver.run("drop table junit_unparted_noisd");
  }
}
