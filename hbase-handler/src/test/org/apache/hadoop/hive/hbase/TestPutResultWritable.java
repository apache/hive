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

package org.apache.hadoop.hive.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

public class TestPutResultWritable {

  @Test
  public void testResult() throws Exception {
    // Initialize a result
    KeyValue[] kvs = new KeyValue[] {
      new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfa"),
        Bytes.toBytes("col1"), Bytes.toBytes("cfacol1")),
      new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfa"),
        Bytes.toBytes("col2"), Bytes.toBytes("cfacol2"))
    };
    Result expected = new Result(kvs);
    ResultWritable actual = copy(new ResultWritable(expected), new ResultWritable());
    Assert.assertArrayEquals(expected.raw(), actual.getResult().raw());

  }

  @Test
  public void testPut() throws Exception {
    byte[] row = Bytes.toBytes("test-row");
    // Initialize a result
    KeyValue[] kvs = new KeyValue[] {
      new KeyValue(row, Bytes.toBytes("cfa"),
        Bytes.toBytes("col1"), Bytes.toBytes("cfacol1")),
      new KeyValue(row, Bytes.toBytes("cfa"),
        Bytes.toBytes("col2"), Bytes.toBytes("cfacol2"))
    };
    Put expected = new Put(row);
    for (int i = 0; i < kvs.length; i++) {
      expected.add(kvs[i]);
    }
    PutWritable actual = copy(new PutWritable(expected), new PutWritable());
    Assert.assertArrayEquals(expected.getRow(), actual.getPut().getRow());
    Assert.assertEquals(expected.getFamilyMap(), actual.getPut().getFamilyMap());
  }

  private <T extends Writable> T copy(T oldWritable, T newWritable) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    oldWritable.write(out);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream in = new DataInputStream(bais);
    newWritable.readFields(in);
    return newWritable;
  }

}
