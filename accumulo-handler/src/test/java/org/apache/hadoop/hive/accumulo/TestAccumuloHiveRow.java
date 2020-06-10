/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Test basic operations on AccumuloHiveRow
 */
public class TestAccumuloHiveRow {

  @Test
  public void testHasFamilyAndQualifier() {
    AccumuloHiveRow row = new AccumuloHiveRow("row1");

    // Add some columns
    for (int i = 1; i <= 5; i++) {
      row.add("cf1", "cq" + i, Integer.toString(i).getBytes());
    }

    // Check that we don't find unexpected columns
    assertFalse(row.hasFamAndQual(new Text(""), new Text("")));
    assertFalse(row.hasFamAndQual(new Text("cf0"), new Text("cq1")));
    assertFalse(row.hasFamAndQual(new Text("cf1"), new Text("cq0")));

    // Check that we do find all expected columns
    for (int i = 1; i <= 5; i++) {
      assertTrue(row.hasFamAndQual(new Text("cf1"), new Text("cq" + i)));
    }
  }

  @Test
  public void testGetValueFromColumn() {
    AccumuloHiveRow row = new AccumuloHiveRow("row1");

    // Should return null when there is no column
    assertNull(row.getValue(new Text(""), new Text("")));

    for (int i = 1; i <= 5; i++) {
      row.add("cf", "cq" + i, Integer.toString(i).getBytes());
    }

    assertNull(row.getValue(new Text("cf"), new Text("cq0")));

    for (int i = 1; i <= 5; i++) {
      assertArrayEquals(Integer.toString(i).getBytes(),
          row.getValue(new Text("cf"), new Text("cq" + i)));
    }
  }

  @Test
  public void testWritableEmptyRow() throws IOException {
    AccumuloHiveRow emptyRow = new AccumuloHiveRow();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    emptyRow.write(out);
    out.close();

    AccumuloHiveRow emptyCopy = new AccumuloHiveRow();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream in = new DataInputStream(bais);
    emptyCopy.readFields(in);

    assertEquals(emptyRow, emptyCopy);
  }

  @Test
  public void testWritableWithColumns() throws IOException {
    AccumuloHiveRow rowWithColumns = new AccumuloHiveRow("row");
    rowWithColumns.add("cf", "cq1", "1".getBytes());
    rowWithColumns.add("cf", "cq2", "2".getBytes());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    rowWithColumns.write(out);
    out.close();

    AccumuloHiveRow copy = new AccumuloHiveRow();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream in = new DataInputStream(bais);
    copy.readFields(in);

    assertEquals(rowWithColumns, copy);
  }
}
