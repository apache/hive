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
package org.apache.hadoop.hive.metastore.tools.metatool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;


/* Unit tests for MetaToolTaskListExtTblLocs. */
@Category(MetastoreUnitTest.class)
public class TestMetaToolTaskListExtTblLocs {

  /*
   * Test grouping of locations. No extra data assumed.
   */
  @Test
  public void testGroupLocations() {
    Set<String> inputLocations = new TreeSet<>();
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetaToolTaskListExtTblLocs.msConf = conf;
    MetaToolTaskListExtTblLocs task = new MetaToolTaskListExtTblLocs();

    //Case 1: Multiple unpartitioned external tables, expected o/p: 1 location
    inputLocations.add("/warehouse/customLocation/t1");
    inputLocations.add("/warehouse/customLocation/t2");
    inputLocations.add("/warehouse/customLocation/t3");
    Map<String, HashSet<String>> output = task.runTest(inputLocations, null);
    Assert.assertEquals(1, output.size());
    String expectedOutput = "/warehouse/customLocation";
    Assert.assertTrue(output.containsKey(expectedOutput));
    HashSet<String> coveredLocs = output.get(expectedOutput);
    Assert.assertEquals(3, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/t1"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/t2"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/t3"));

    //Case 2 : inputs at multiple depths
    // inputs   ../ext/b0 - contains 1 location
    //          ../ext/p=0 - contains 1 location
    //          ../ext/b1/b2/b3 - contains 3 locations (p1, p2, p3)
    // expected output : [../ext/b1/b2/b3 containing 3 elements, t1, p0]
    inputLocations.clear();
    inputLocations.add("/warehouse/customLocation/ext/b0");
    inputLocations.add("/warehouse/customLocation/ext/p=0");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/b3/p=1");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/b3/p=2");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/b3/p=3");
    output = task.runTest(inputLocations, null);
    Assert.assertEquals(3, output.size());
    String expectedOutput1 = "/warehouse/customLocation/ext/b0";
    Assert.assertTrue(output.containsKey(expectedOutput1));
    coveredLocs = output.get(expectedOutput1);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b0"));
    String expectedOutput2 = "/warehouse/customLocation/ext/p=0";
    Assert.assertTrue(output.containsKey(expectedOutput2));
    coveredLocs = output.get(expectedOutput2);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/p=0"));
    String expectedOutput3 = "/warehouse/customLocation/ext/b1/b2/b3";
    Assert.assertTrue(output.containsKey(expectedOutput3));
    coveredLocs = output.get(expectedOutput3);
    Assert.assertEquals(3, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/b3/p=1"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/b3/p=2"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/b3/p=3"));

    //Case 3 : root with a lot of leaves 
    // inputs   ../ext/ - contains 4 locations
    //          ../ext/b1 - contains 3 locations
    // expected output : [../ext covering all locations] since root (ext) has more than half of locations
    inputLocations.clear();
    inputLocations.add("/warehouse/customLocation/ext/p=0");
    inputLocations.add("/warehouse/customLocation/ext/p=1");
    inputLocations.add("/warehouse/customLocation/ext/p=2");
    inputLocations.add("/warehouse/customLocation/ext/p=3");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=4");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=5");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=6");
    output = task.runTest(inputLocations, null);
    Assert.assertEquals(1, output.size());
    expectedOutput = "/warehouse/customLocation/ext";
    Assert.assertTrue(output.containsKey(expectedOutput));
    coveredLocs = output.get(expectedOutput);
    Assert.assertEquals(7, coveredLocs.size());
    Assert.assertTrue(coveredLocs.containsAll(inputLocations));

    //Case 4 : root with a lot of trivial locations (non leaf)
    // inputs   ../ext/ - contains 4 trivial locations
    //          ../ext/b1 - contains 3 locations
    // expected output : [../ext covering all locations] since non trivial (grouped) locations under ext is less than half 
    inputLocations.clear();
    inputLocations.add("/warehouse/customLocation/ext/dir01/dir02/p=0");
    inputLocations.add("/warehouse/customLocation/ext/dir11/dir12/p=1");
    inputLocations.add("/warehouse/customLocation/ext/dir21/dir22/p=2");
    inputLocations.add("/warehouse/customLocation/ext/dir31/dir32/p=3");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=4");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=5");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=6");
    output = task.runTest(inputLocations, null);
    Assert.assertEquals(1, output.size());
    expectedOutput = "/warehouse/customLocation/ext";
    Assert.assertTrue(output.containsKey(expectedOutput));
    coveredLocs = output.get(expectedOutput);
    Assert.assertEquals(7, coveredLocs.size());
    Assert.assertTrue(coveredLocs.containsAll(inputLocations));

    //Case 5 : several grouped locations and 1 outlier at root
    // inputs   ../ext/b0 - contains 4 locations
    //          ../ext/b1 - contains 3 locations
    // expected output : [../ext/b0, ../ext/b1, p=7 ]
    inputLocations.clear();
    inputLocations.add("/warehouse/customLocation/ext/b0/p=0");
    inputLocations.add("/warehouse/customLocation/ext/b0/p=1");
    inputLocations.add("/warehouse/customLocation/ext/b0/p=2");
    inputLocations.add("/warehouse/customLocation/ext/b0/p=3");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=4");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=5");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=6");
    inputLocations.add("/warehouse/customLocation/ext/p=7");
    output = task.runTest(inputLocations, null);
    Assert.assertEquals(3, output.size());
    expectedOutput1 = "/warehouse/customLocation/ext/b0";
    Assert.assertTrue(output.containsKey(expectedOutput1));
    coveredLocs = output.get(expectedOutput1);
    Assert.assertEquals(4, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b0/p=0"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b0/p=1"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b0/p=2"));
    expectedOutput2 = "/warehouse/customLocation/ext/b1";
    Assert.assertTrue(output.containsKey(expectedOutput2));
    coveredLocs = output.get(expectedOutput2);
    Assert.assertEquals(3, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/p=4"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/p=5"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/p=6"));
    expectedOutput3 = "/warehouse/customLocation/ext/p=7";
    Assert.assertTrue(output.containsKey(expectedOutput3));
    coveredLocs = output.get(expectedOutput3);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/p=7"));

    //Case 6 : inputs with nested structure
    // inputs   ../ext/b0 - contains 4 locations
    //          ../ext/b1 
    //          ../ext/b1/b2 - contains 4 locations
    // expected output : [../ext/b0, ../ext/b1 ] : (no extra location for b2 since covered by b1 itself)
    inputLocations.clear();
    inputLocations.add("/warehouse/customLocation/ext/b0/p=0");
    inputLocations.add("/warehouse/customLocation/ext/b0/p=1");
    inputLocations.add("/warehouse/customLocation/ext/b0/p=2");
    inputLocations.add("/warehouse/customLocation/ext/b0/p=3");
    inputLocations.add("/warehouse/customLocation/ext/b1");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/p=7");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/p=8");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/p=9");
    output = task.runTest(inputLocations, null);
    Assert.assertEquals(2, output.size());
    expectedOutput1 = "/warehouse/customLocation/ext/b0";
    Assert.assertTrue(output.containsKey(expectedOutput1));
    coveredLocs = output.get(expectedOutput1);
    Assert.assertEquals(4, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b0/p=0"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b0/p=1"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b0/p=2"));
    expectedOutput2 = "/warehouse/customLocation/ext/b1";
    Assert.assertTrue(output.containsKey(expectedOutput2));
    coveredLocs = output.get(expectedOutput2);
    Assert.assertEquals(4, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/p=7"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/p=8"));
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/p=9"));
  }

  @Test
  public void testGroupLocationsDummyDataSizes() {
    Set<String> inputLocations = new TreeSet<>();
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetaToolTaskListExtTblLocs.msConf = conf;
    MetaToolTaskListExtTblLocs task = new MetaToolTaskListExtTblLocs();

    //Case 1: Multiple unpartitioned external tables, expected o/p without extra data: 1 location (tested in testGroupLocations#1)
    //        But say there is some data at ../customLocation, then we list all the 3 paths
    inputLocations.add("/warehouse/customLocation/t1");
    inputLocations.add("/warehouse/customLocation/t2");
    inputLocations.add("/warehouse/customLocation/t3");
    Map<String, Long> dataSizes = new HashMap<>();
    dataSizes.put("/warehouse/customLocation", Long.valueOf(100)); //Simulate 100 bytes extra data at customLocation
    Map<String, HashSet<String>> output = task.runTest(inputLocations, dataSizes);
    Assert.assertEquals(3, output.size());
    String expectedOutput1 = "/warehouse/customLocation/t1";
    Assert.assertTrue(output.containsKey(expectedOutput1));
    HashSet<String> coveredLocs = output.get(expectedOutput1);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/t1"));

    //Case 2 : inputs at multiple depths
    // inputs   ../ext/b0 - contains 1 location
    //          ../ext/p=0 - contains 1 location
    //          ../ext/b1/b2/b3 - contains 3 locations (p1, p2, p3)
    // expected output without extra data  : [../ext/b1/b2/b3 containing 3 elements, t1, p0]  (tested in testGroupLocations#2)
    // expected output with extra data at ../ext/b1/b2/b3 : [p1, p2, p3, t1, p0]
    inputLocations.clear();
    dataSizes.clear();
    inputLocations.add("/warehouse/customLocation/ext/b0");
    inputLocations.add("/warehouse/customLocation/ext/p=0");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/b3/p=1");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/b3/p=2");
    inputLocations.add("/warehouse/customLocation/ext/b1/b2/b3/p=3");
    dataSizes.put("/warehouse/customLocation/ext/b1/b2/b3", Long.valueOf(100));  // simulate 100 bytes of extra data at ../b3
    output = task.runTest(inputLocations, dataSizes);
    Assert.assertEquals(5, output.size());
    expectedOutput1 = "/warehouse/customLocation/ext/b0";
    Assert.assertTrue(output.containsKey(expectedOutput1));
    coveredLocs = output.get(expectedOutput1);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b0"));
    String expectedOutput2 = "/warehouse/customLocation/ext/p=0";
    Assert.assertTrue(output.containsKey(expectedOutput2));
    coveredLocs = output.get(expectedOutput2);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/p=0"));
    String expectedOutput3 = "/warehouse/customLocation/ext/b1/b2/b3/p=1";
    Assert.assertTrue(output.containsKey(expectedOutput3));
    coveredLocs = output.get(expectedOutput3);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/b3/p=1"));
    String expectedOutput4 = "/warehouse/customLocation/ext/b1/b2/b3/p=2";
    Assert.assertTrue(output.containsKey(expectedOutput4));
    coveredLocs = output.get(expectedOutput4);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/b3/p=2"));
    String expectedOutput5 = "/warehouse/customLocation/ext/b1/b2/b3/p=3";
    Assert.assertTrue(output.containsKey(expectedOutput5));
    coveredLocs = output.get(expectedOutput5);
    Assert.assertEquals(1, coveredLocs.size());
    Assert.assertTrue(coveredLocs.contains("/warehouse/customLocation/ext/b1/b2/b3/p=3"));

    //Case 3 : intermediate directory has extra data
    // inputs   ../ext/ - contains 4 locations
    //          ../ext/b1 - contains 3 locations
    // expected output without extra data : [../ext covering all locations] (tested in testGroupLocations#3)
    // We simulate extra data at ../ext/b1. So, expected output is the list of all locations.
    inputLocations.clear();
    dataSizes.clear();
    inputLocations.add("/warehouse/customLocation/ext/p=0");
    inputLocations.add("/warehouse/customLocation/ext/p=1");
    inputLocations.add("/warehouse/customLocation/ext/p=2");
    inputLocations.add("/warehouse/customLocation/ext/p=3");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=4");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=5");
    inputLocations.add("/warehouse/customLocation/ext/b1/p=6");
    dataSizes.put("/warehouse/customLocation/ext/b1", Long.valueOf(100));  // simulate 100 bytes of extra data at ..ext/b1
    dataSizes.put("/warehouse/customLocation/ext", Long.valueOf(100));// since ext/b1 contains 100 bytes, ../ext also has 100 bytes
    output = task.runTest(inputLocations, dataSizes);
    Assert.assertEquals(7, output.size());
    Assert.assertTrue(output.keySet().containsAll(inputLocations));
    for(String outLoc : output.keySet()) {
      Assert.assertTrue(output.get(outLoc).contains(outLoc));
    }
  }
}

