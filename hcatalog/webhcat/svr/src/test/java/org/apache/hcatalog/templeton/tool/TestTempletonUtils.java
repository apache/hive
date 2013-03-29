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
package org.apache.hcatalog.templeton.tool;

import org.junit.Assert;

import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestTempletonUtils {
    public static final String[] CONTROLLER_LINES = {
        "2011-12-15 18:12:21,758 [main] INFO  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher - More information at: http://localhost:50030/jobdetails.jsp?jobid=job_201112140012_0047",
        "2011-12-15 18:12:46,907 [main] INFO  org.apache.pig.tools.pigstats.SimplePigStats - Script Statistics: "
    };

    @Test
    public void testIssetString() {
        Assert.assertFalse(TempletonUtils.isset((String)null));
        Assert.assertFalse(TempletonUtils.isset(""));
        Assert.assertTrue(TempletonUtils.isset("hello"));
    }

    @Test
    public void testIssetTArray() {
        Assert.assertFalse(TempletonUtils.isset((Long[]) null));
        Assert.assertFalse(TempletonUtils.isset(new String[0]));
        String[] parts = new String("hello.world").split("\\.");
        Assert.assertTrue(TempletonUtils.isset(parts));
    }

    @Test
    public void testPrintTaggedJobID() {
        //JobID job = new JobID();
        // TODO -- capture System.out?
    }


    @Test
    public void testExtractPercentComplete() {
        Assert.assertNull(TempletonUtils.extractPercentComplete("fred"));
        for (String line : CONTROLLER_LINES)
            Assert.assertNull(TempletonUtils.extractPercentComplete(line));

        String fifty = "2011-12-15 18:12:36,333 [main] INFO  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher - 50% complete";
        Assert.assertEquals("50% complete", TempletonUtils.extractPercentComplete(fifty));
    }

    @Test
    public void testEncodeArray() {
        Assert.assertEquals(null, TempletonUtils.encodeArray((String []) null));
        String[] tmp = new String[0];
        Assert.assertTrue(TempletonUtils.encodeArray(new String[0]).length() == 0);
        tmp = new String[3];
        tmp[0] = "fred";
        tmp[1] = null;
        tmp[2] = "peter,lisa,, barney";
        Assert.assertEquals("fred,,peter" +
                     StringUtils.ESCAPE_CHAR + ",lisa" + StringUtils.ESCAPE_CHAR + "," +
                     StringUtils.ESCAPE_CHAR + ", barney",
                     TempletonUtils.encodeArray(tmp));
    }

    @Test
    public void testDecodeArray() {
        Assert.assertTrue(TempletonUtils.encodeArray((String[]) null) == null);
        String[] tmp = new String[3];
        tmp[0] = "fred";
        tmp[1] = null;
        tmp[2] = "peter,lisa,, barney";
        String[] tmp2 = TempletonUtils.decodeArray(TempletonUtils.encodeArray(tmp));
        try {
            for (int i=0; i< tmp.length; i++) {
                Assert.assertEquals((String) tmp[i], (String)tmp2[i]);
            }
        } catch (Exception e) {
            Assert.fail("Arrays were not equal" + e.getMessage());
        }
    }

    @Test
    public void testHadoopFsPath() {
        try {
            TempletonUtils.hadoopFsPath(null, null, null);
            TempletonUtils.hadoopFsPath("/tmp", null, null);
            TempletonUtils.hadoopFsPath("/tmp", new Configuration(), null);
        } catch (FileNotFoundException e) {
            Assert.fail("Couldn't find /tmp");
        } catch (Exception e) {
            // This is our problem -- it means the configuration was wrong.
            e.printStackTrace();
        }
        try {
            TempletonUtils.hadoopFsPath("/scoobydoo/teddybear",
                                        new Configuration(), null);
            Assert.fail("Should not have found /scoobydoo/teddybear");
        } catch (FileNotFoundException e) {
            // Should go here.
        } catch (Exception e) {
            // This is our problem -- it means the configuration was wrong.
            e.printStackTrace();
        }
    }

    @Test
    public void testHadoopFsFilename() {
        try {
            Assert.assertEquals(null, TempletonUtils.hadoopFsFilename(null, null, null));
            Assert.assertEquals(null, TempletonUtils.hadoopFsFilename("/tmp", null, null));
            Assert.assertEquals("file:/tmp",
                         TempletonUtils.hadoopFsFilename("/tmp",
                                                         new Configuration(),
                                                         null));
        } catch (FileNotFoundException e) {
            Assert.fail("Couldn't find name for /tmp");
        } catch (Exception e) {
            // Something else is wrong
            e.printStackTrace();
        }
        try {
            TempletonUtils.hadoopFsFilename("/scoobydoo/teddybear",
                                            new Configuration(), null);
            Assert.fail("Should not have found /scoobydoo/teddybear");
        } catch (FileNotFoundException e) {
            // Should go here.
        } catch (Exception e) {
            // Something else is wrong.
            e.printStackTrace();
        }
    }

    @Test
    public void testHadoopFsListAsArray() {
        try {
            Assert.assertTrue(TempletonUtils.hadoopFsListAsArray(null, null, null) == null);
            Assert.assertTrue(TempletonUtils.hadoopFsListAsArray("/tmp, /usr",
                                                          null, null) == null);
            String[] tmp2
                = TempletonUtils.hadoopFsListAsArray("/tmp,/usr",
                                                     new Configuration(), null);
            Assert.assertEquals("file:/tmp", tmp2[0]);
            Assert.assertEquals("file:/usr", tmp2[1]);
        } catch (FileNotFoundException e) {
            Assert.fail("Couldn't find name for /tmp");
        } catch (Exception e) {
            // Something else is wrong
            e.printStackTrace();
        }
        try {
            TempletonUtils.hadoopFsListAsArray("/scoobydoo/teddybear,joe",
                                               new Configuration(),
                                               null);
            Assert.fail("Should not have found /scoobydoo/teddybear");
        } catch (FileNotFoundException e) {
            // Should go here.
        } catch (Exception e) {
            // Something else is wrong.
            e.printStackTrace();
        }
    }

    @Test
    public void testHadoopFsListAsString() {
        try {
            Assert.assertTrue(TempletonUtils.hadoopFsListAsString(null, null, null) == null);
            Assert.assertTrue(TempletonUtils.hadoopFsListAsString("/tmp,/usr",
                                                           null, null) == null);
            Assert.assertEquals("file:/tmp,file:/usr", TempletonUtils.hadoopFsListAsString
                ("/tmp,/usr", new Configuration(), null));
        } catch (FileNotFoundException e) {
            Assert.fail("Couldn't find name for /tmp");
        } catch (Exception e) {
            // Something else is wrong
            e.printStackTrace();
        }
        try {
            TempletonUtils.hadoopFsListAsString("/scoobydoo/teddybear,joe",
                                                new Configuration(),
                                                null);
            Assert.fail("Should not have found /scoobydoo/teddybear");
        } catch (FileNotFoundException e) {
            // Should go here.
        } catch (Exception e) {
            // Something else is wrong.
            e.printStackTrace();
        }
    }

}
