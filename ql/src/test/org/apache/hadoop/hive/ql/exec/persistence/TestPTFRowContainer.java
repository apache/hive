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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestPTFRowContainer {

  private static final String COL_NAMES = "x,y,z,a,b";
  private static final String COL_TYPES = "int,string,double,int,string";

  static SerDe serDe;
  static Configuration cfg;

  @BeforeClass
  public static void setupClass()  throws SerDeException {
    cfg = new Configuration();
    serDe = new LazyBinarySerDe();
    Properties p = new Properties();
    p.setProperty(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS,
        COL_NAMES);
    p.setProperty(
        org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES,
        COL_TYPES);
    SerDeUtils.initializeSerDe(serDe, cfg, p, null);
  }

  private PTFRowContainer<List<Object>> rowContainer(int blockSize)
      throws SerDeException, HiveException {

    PTFRowContainer<List<Object>> rc = new PTFRowContainer<List<Object>>(blockSize, cfg, null);
    rc.setSerDe(serDe,
        ObjectInspectorUtils.getStandardObjectInspector(serDe.getObjectInspector()));
    rc.setTableDesc(
        PTFRowContainer.createTableDesc((StructObjectInspector) serDe.getObjectInspector()));
    return rc;
  }

  private void runTest(int sz, int blockSize) throws SerDeException, HiveException {
    List<Object> row;

    PTFRowContainer<List<Object>> rc = rowContainer(blockSize);
    int i;
    for(i =0; i < sz; i++) {
      row = new ArrayList<Object>();
      row.add(new IntWritable(i));
      row.add(new Text("abc " + i));
      row.add(new DoubleWritable(i));
      row.add(new IntWritable(i));
      row.add(new Text("def " + i));
      rc.addRow(row);
    }

    // test forward scan
    assert(rc.rowCount() == sz);
    i = 0;
    row = new ArrayList<Object>();
    row = rc.first();
    while(row != null ) {
      assert(row.get(1).toString().equals("abc " + i));
      i++;
      row = rc.next();
    }

    // test backward scan
    row = rc.first();
    for(i = sz - 1; i >= 0; i-- ) {
      row = rc.getAt(i);
      assert(row.get(1).toString().equals("abc " + i));
    }

    Random r = new Random(1000L);

    //test random scan
    for(i=0; i < 100; i++) {
      int j = r.nextInt(sz);
      row = rc.getAt(j);
      assert(row.get(1).toString().equals("abc " + j));
    }

    // intersperse getAt and next calls
    for(i=0; i < 100; i++) {
      int j = r.nextInt(sz);
      row = rc.getAt(j);
      assert(row.get(1).toString().equals("abc " + j));
      for(int k = j + 1; k < j + (blockSize/4) && k < sz; k++) {
        row = rc.next();
        assert(row.get(4).toString().equals("def " + k));
      }
    }

  }

  @Test
  public void testLargeBlockSize() throws SerDeException, HiveException {
    runTest(100 * 1000, 25 * 1000);
  }

  @Test
  public void testSmallBlockSize() throws SerDeException, HiveException {
    runTest(10 * 1000, 5);
  }
}
