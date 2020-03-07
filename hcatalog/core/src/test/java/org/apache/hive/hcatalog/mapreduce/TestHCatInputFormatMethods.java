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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Before;
import org.junit.Test;

public class TestHCatInputFormatMethods extends HCatBaseTest {

  private boolean setUpComplete = false;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (setUpComplete) {
      return;
    }

    Assert.assertEquals(0, driver.run("drop table if exists testHCIFMethods").getResponseCode());
    Assert.assertEquals(0, driver.run(
      "create table testHCIFMethods (a string, b int) partitioned by (x string, y string)")
      .getResponseCode());

    setUpComplete = true;
  }


  @Test
  public void testGetPartitionAndDataColumns() throws Exception {

    Configuration conf = new Configuration();
    Job myJob = new Job(conf, "hcatTest");

    HCatInputFormat.setInput(myJob, "default", "testHCIFMethods");
    HCatSchema cols = HCatInputFormat.getDataColumns(myJob.getConfiguration());

    Assert.assertTrue(cols.getFields() != null);
    Assert.assertEquals(cols.getFields().size(), 2);
    Assert.assertTrue(cols.getFields().get(0).getName().equals("a"));
    Assert.assertTrue(cols.getFields().get(1).getName().equals("b"));
    Assert.assertTrue(cols.getFields().get(0).getType().equals(HCatFieldSchema.Type.STRING));
    Assert.assertTrue(cols.getFields().get(1).getType().equals(HCatFieldSchema.Type.INT));

    HCatSchema pcols = HCatInputFormat.getPartitionColumns(myJob.getConfiguration());

    Assert.assertTrue(pcols.getFields() != null);
    Assert.assertEquals(pcols.getFields().size(), 2);
    Assert.assertTrue(pcols.getFields().get(0).getName().equals("x"));
    Assert.assertTrue(pcols.getFields().get(1).getName().equals("y"));
    Assert.assertTrue(pcols.getFields().get(0).getType().equals(HCatFieldSchema.Type.STRING));
    Assert.assertTrue(pcols.getFields().get(1).getType().equals(HCatFieldSchema.Type.STRING));

  }

}

