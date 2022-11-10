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
package org.apache.hadoop.hive.ql.io.esriJson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class TestUnenclosedGeoJsonRecordReader {  // MRv2; for MRv1, name Mrv1

  private TaskAttemptContext createTaskAttemptContext(Configuration conf, TaskAttemptID taid)
      throws Exception {       //shim
    try {                     // Hadoop-1
      return TaskAttemptContext.class.
          getConstructor(Configuration.class, TaskAttemptID.class).
          newInstance(conf, taid);
    } catch (Exception e) {   // Hadoop-2
      Class<?> clazz = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
      return (TaskAttemptContext) clazz.getConstructor(Configuration.class, TaskAttemptID.class).
          newInstance(conf, taid);
    }
  }

  private UnenclosedGeoJsonRecordReader getReader() throws IOException {
    return new UnenclosedGeoJsonRecordReader();
  }

  int[] getRecordIndexesInFile(String resource, int start, int end) throws Exception {
    return getRecordIndexesInFile(getReader(), resource, start, end, false);
  }

  int[] getRecordIndexesInFile(String resource, int start, int end, boolean flag) throws Exception {
    return getRecordIndexesInFile(getReader(), resource, start, end, flag);
  }

  int[] getRecordIndexesInFile(UnenclosedGeoJsonRecordReader reader, String resource, int start, int end, boolean flag)
      throws Exception {
    Path path = new Path(this.getClass().getResource("/json/" + resource).getFile());
    FileSplit split = new FileSplit(path, start, end - start, new String[0]);
    try {
      TaskAttemptContext tac = createTaskAttemptContext(new Configuration(), new TaskAttemptID());
      reader.initialize(split, tac);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    List<Integer> linesList = new LinkedList<Integer>();

    LongWritable key = null;
    Text value = null;

    try {
      while (reader.nextKeyValue()) {
        key = reader.getCurrentKey();
        value = reader.getCurrentValue();
        int line = flag ? (int) (key.get()) : value.toString().charAt(40) - '0';
        linesList.add(line);
        // System.out.println(key.get() + " - " + value.toString());
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }

    int[] lines = new int[linesList.size()];
    for (int i = 0; i < linesList.size(); i++) {
      lines[i] = linesList.get(i);
    }
    return lines;
  }

  @Test
  public void TestArbitrarySplitLocations() throws Exception {
    //int totalSize = 415;
    //int [] recordBreaks = new int[] { 0, 57, 114, 171, ... };

    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 0, 57));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 0, 58));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 0, 59));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 56, 191));

    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 37, 191));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 57, 191));
    Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 58, 191));
    Assert.assertArrayEquals(new int[] { 6, 7, 8 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 342, 493));
    Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 506, 585));
    Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 507, 585));
    Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInFile("unenclosed-geojs-simple.json", 508, 585));
  }

  @Test
  public void TestTypeAttribute() throws Exception {
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-geojs-type.json", 0, 71, true));
    Assert.assertArrayEquals(new int[] { 74 }, getRecordIndexesInFile("unenclosed-geojs-type.json", 71, 148, true));
    Assert.assertArrayEquals(new int[] { 148, 222 },
        getRecordIndexesInFile("unenclosed-geojs-type.json", 148, 223, true));
    Assert.assertArrayEquals(new int[] { 296 }, getRecordIndexesInFile("unenclosed-geojs-type.json", 223, 298, true));
    Assert.assertArrayEquals(new int[] { 370 }, getRecordIndexesInFile("unenclosed-geojs-type.json", 298, 401, true));
    Assert.assertArrayEquals(new int[] { 444 }, getRecordIndexesInFile("unenclosed-geojs-type.json", 401, 518, true));
    Assert.assertArrayEquals(new int[] { 518 }, getRecordIndexesInFile("unenclosed-geojs-type.json", 518, 593, true));
  }

  @Test
  public void TestEscape() throws Exception {
    //int [] recordBreaks = new int[] { 0, 61, 122, 188, 249, 314, 372, 432, 492 };  //length 562
    Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 0, 61));
    Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 0, 62));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 60, 200));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 35, 200));
    Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 61, 200));
    Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 62, 200));
    Assert.assertArrayEquals(new int[] { 4, 5, 6 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 249, 407));
    Assert.assertArrayEquals(new int[] { 7, 8 },
        getRecordIndexesInFile("unenclosed-geojs-escape.json", 414, 565));  // 6|{}"
    Assert
        .assertArrayEquals(new int[] { 8 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 474, 565));  // 7|}{"
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 31, 62));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 39, 62));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 40, 62));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 41, 62));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 42, 62));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 61, 102));
    Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInFile("unenclosed-geojs-escape.json", 61, 103));
  }

}
