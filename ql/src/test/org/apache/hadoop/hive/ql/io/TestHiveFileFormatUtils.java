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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;



import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestHiveFileFormatUtils.
 */
public class TestHiveFileFormatUtils {

  @Test
  public void testGetPartitionDescFromPathRecursively() throws IOException {

    PartitionDesc partDesc_3 = new PartitionDesc();
    PartitionDesc partDesc_4 = new PartitionDesc();
    PartitionDesc partDesc_5 = new PartitionDesc();
    PartitionDesc partDesc_6 = new PartitionDesc();

    Map<Path, PartitionDesc> pathToPartitionInfo = new HashMap<>();

    pathToPartitionInfo.put(new Path("file:///tbl/par1/part2/part3"), partDesc_3);
    pathToPartitionInfo.put(new Path("/tbl/par1/part2/part4"), partDesc_4);
    pathToPartitionInfo.put(new Path("/tbl/par1/part2/part5/"), partDesc_5);
    pathToPartitionInfo.put(new Path("hdfs:///tbl/par1/part2/part6/"), partDesc_6);

    // first group
    PartitionDesc ret = null;
    
    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("file:///tbl/par1/part2/part3"),
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("file:///tbl/par1/part2/part3 not found.", partDesc_3, ret);

    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("/tbl/par1/part2/part3"), 
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("/tbl/par1/part2/part3 not found.", partDesc_3, ret);

    boolean exception = false;
    try {
      ret = HiveFileFormatUtils.getFromPathRecursively(
          pathToPartitionInfo, new Path("hdfs:///tbl/par1/part2/part3"),
          IOPrepareCache.get().allocatePartitionDescMap());
    } catch (IOException e) {
      exception = true;
    }
    assertEquals("hdfs:///tbl/par1/part2/part3 should return null", true,
        exception);
    exception = false;

    // second group
    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("file:///tbl/par1/part2/part4"),
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("file:///tbl/par1/part2/part4 not found.", partDesc_4, ret);

    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("/tbl/par1/part2/part4"), 
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("/tbl/par1/part2/part4 not found.", partDesc_4, ret);

    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("hdfs:///tbl/par1/part2/part4"),
        IOPrepareCache.get().allocatePartitionDescMap());

    assertEquals("hdfs:///tbl/par1/part2/part4 should  not found", partDesc_4,
        ret);

    // third group
    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("file:///tbl/par1/part2/part5"),
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("file:///tbl/par1/part2/part5 not found.", partDesc_5, ret);

    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("/tbl/par1/part2/part5"), 
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("/tbl/par1/part2/part5 not found.", partDesc_5, ret);

    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("hdfs:///tbl/par1/part2/part5"),
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("hdfs:///tbl/par1/part2/part5 not found", partDesc_5, ret);

    // fourth group
    try {
      ret = HiveFileFormatUtils.getFromPathRecursively(
          pathToPartitionInfo, new Path("file:///tbl/par1/part2/part6"),
          IOPrepareCache.get().allocatePartitionDescMap());
    } catch (IOException e) {
      exception = true;
    }
    assertEquals("file:///tbl/par1/part2/part6 should return null", true,
        exception);
    exception = false;

    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("/tbl/par1/part2/part6"), 
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("/tbl/par1/part2/part6 not found.", partDesc_6, ret);

    ret = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, new Path("hdfs:///tbl/par1/part2/part6"),
        IOPrepareCache.get().allocatePartitionDescMap());
    assertEquals("hdfs:///tbl/par1/part2/part6 not found.", partDesc_6, ret);

  }

}
