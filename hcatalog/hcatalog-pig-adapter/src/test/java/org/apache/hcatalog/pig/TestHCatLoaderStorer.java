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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hcatalog.HcatTestUtils;
import org.apache.hcatalog.mapreduce.HCatBaseTest;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * Test that require both HCatLoader and HCatStorer. For read or write only functionality,
 * please consider @{link TestHCatLoader} or @{link TestHCatStorer}.
 */
public class TestHCatLoaderStorer extends HCatBaseTest {

    /**
     * Ensure Pig can read/write tinyint/smallint columns.
     */
    @Test
    public void testSmallTinyInt() throws Exception {

        String readTblName = "test_small_tiny_int";
        File dataDir = new File(TEST_DATA_DIR + "/testSmallTinyIntData");
        File dataFile = new File(dataDir, "testSmallTinyInt.tsv");

        String writeTblName = "test_small_tiny_int_write";
        File writeDataFile = new File(TEST_DATA_DIR, writeTblName + ".tsv");

        FileUtil.fullyDelete(dataDir); // Might not exist
        Assert.assertTrue(dataDir.mkdir());

        HcatTestUtils.createTestDataFile(dataFile.getAbsolutePath(), new String[]{
            String.format("%d\t%d", Short.MIN_VALUE, Byte.MIN_VALUE),
            String.format("%d\t%d", Short.MAX_VALUE, Byte.MAX_VALUE)
        });

        // Create a table with smallint/tinyint columns, load data, and query from Hive.
        Assert.assertEquals(0, driver.run("drop table if exists " + readTblName).getResponseCode());
        Assert.assertEquals(0, driver.run("create external table " + readTblName +
            " (my_small_int smallint, my_tiny_int tinyint)" +
            " row format delimited fields terminated by '\t' stored as textfile").getResponseCode());
        Assert.assertEquals(0, driver.run("load data local inpath '" +
            dataDir.getAbsolutePath() + "' into table " + readTblName).getResponseCode());

        PigServer server = new PigServer(ExecType.LOCAL);
        server.registerQuery(
            "data = load '" + readTblName + "' using org.apache.hcatalog.pig.HCatLoader();");

        // Ensure Pig schema is correct.
        Schema schema = server.dumpSchema("data");
        Assert.assertEquals(2, schema.getFields().size());
        Assert.assertEquals("my_small_int", schema.getField(0).alias);
        Assert.assertEquals(DataType.INTEGER, schema.getField(0).type);
        Assert.assertEquals("my_tiny_int", schema.getField(1).alias);
        Assert.assertEquals(DataType.INTEGER, schema.getField(1).type);

        // Ensure Pig can read data correctly.
        Iterator<Tuple> it = server.openIterator("data");
        Tuple t = it.next();
        Assert.assertEquals(new Integer(Short.MIN_VALUE), t.get(0));
        Assert.assertEquals(new Integer(Byte.MIN_VALUE), t.get(1));
        t = it.next();
        Assert.assertEquals(new Integer(Short.MAX_VALUE), t.get(0));
        Assert.assertEquals(new Integer(Byte.MAX_VALUE), t.get(1));
        Assert.assertFalse(it.hasNext());

        // Ensure Pig can write correctly to smallint/tinyint columns. This means values within the
        // bounds of the column type are written, and values outside throw an exception.
        Assert.assertEquals(0, driver.run("drop table if exists " + writeTblName).getResponseCode());
        Assert.assertEquals(0, driver.run("create table " + writeTblName +
            " (my_small_int smallint, my_tiny_int tinyint) stored as rcfile").getResponseCode());

        // Values within the column type bounds.
        HcatTestUtils.createTestDataFile(writeDataFile.getAbsolutePath(), new String[]{
            String.format("%d\t%d", Short.MIN_VALUE, Byte.MIN_VALUE),
            String.format("%d\t%d", Short.MAX_VALUE, Byte.MAX_VALUE)
        });
        smallTinyIntBoundsCheckHelper(writeDataFile.getAbsolutePath(), ExecJob.JOB_STATUS.COMPLETED);

        // Values outside the column type bounds will fail at runtime.
        HcatTestUtils.createTestDataFile(TEST_DATA_DIR + "/shortTooSmall.tsv", new String[]{
            String.format("%d\t%d", Short.MIN_VALUE - 1, 0)});
        smallTinyIntBoundsCheckHelper(TEST_DATA_DIR + "/shortTooSmall.tsv", ExecJob.JOB_STATUS.FAILED);

        HcatTestUtils.createTestDataFile(TEST_DATA_DIR + "/shortTooBig.tsv", new String[]{
            String.format("%d\t%d", Short.MAX_VALUE + 1, 0)});
        smallTinyIntBoundsCheckHelper(TEST_DATA_DIR + "/shortTooBig.tsv", ExecJob.JOB_STATUS.FAILED);

        HcatTestUtils.createTestDataFile(TEST_DATA_DIR + "/byteTooSmall.tsv", new String[]{
            String.format("%d\t%d", 0, Byte.MIN_VALUE - 1)});
        smallTinyIntBoundsCheckHelper(TEST_DATA_DIR + "/byteTooSmall.tsv", ExecJob.JOB_STATUS.FAILED);

        HcatTestUtils.createTestDataFile(TEST_DATA_DIR + "/byteTooBig.tsv", new String[]{
            String.format("%d\t%d", 0, Byte.MAX_VALUE + 1)});
        smallTinyIntBoundsCheckHelper(TEST_DATA_DIR + "/byteTooBig.tsv", ExecJob.JOB_STATUS.FAILED);
    }

    private void smallTinyIntBoundsCheckHelper(String data, ExecJob.JOB_STATUS expectedStatus)
        throws Exception {
        Assert.assertEquals(0, driver.run("drop table if exists test_tbl").getResponseCode());
        Assert.assertEquals(0, driver.run("create table test_tbl" +
            " (my_small_int smallint, my_tiny_int tinyint) stored as rcfile").getResponseCode());

        PigServer server = new PigServer(ExecType.LOCAL);
        server.setBatchOn();
        server.registerQuery("data = load '" + data +
            "' using PigStorage('\t') as (my_small_int:int, my_tiny_int:int);");
        server.registerQuery(
            "store data into 'test_tbl' using org.apache.hcatalog.pig.HCatStorer();");
        List<ExecJob> jobs = server.executeBatch();
        Assert.assertEquals(expectedStatus, jobs.get(0).getStatus());
    }
}
