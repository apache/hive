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

package org.apache.hadoop.hive.ql.parse;

import junit.framework.TestCase;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TestEximUtil.
 *
 */
public class TestEximUtil extends TestCase {

  private JSONParser parser = new JSONParser();

  private class FakeSeekableInputStream extends DataInputStream
          implements Seekable, PositionedReadable {

    public FakeSeekableInputStream(InputStream in) {
      super(in);
    }

    @Override
    public void seek(long l) throws IOException {

    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      return false;
    }

    @Override
    public int read(long l, byte[] bytes, int i, int i1) throws IOException {
      return 0;
    }

    @Override
    public void readFully(long l, byte[] bytes, int i, int i1) throws IOException {

    }

    @Override
    public void readFully(long l, byte[] bytes) throws IOException {

    }
  }

  @Override
  protected void setUp() {
  }

  @Override
  protected void tearDown() {
  }

  @Test
  public void testReadMetaData() throws Exception {

    // serialize table
    TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());

    Table table = new Table();
    table.setDbName("test-db-name-table");
    String tableJson = serializer.toString(table, "UTF-8");

    Partition partition1 = new Partition();
    partition1.setTableName("test-table-name-p1");
    String partition1Json = serializer.toString(partition1, "UTF-8");

    Partition partition2 = new Partition();
    partition2.setTableName("test-table-name-p2");
    String partition2Json = serializer.toString(partition2, "UTF-8");

    String json = "{" +
            "\"version\": \"0.1\"," +
            "\"fcversion\": \"0.1\"," +
            "\"table\": " + tableJson + "," +
            "\"partitions\": [" + partition1Json + ", " + partition2Json + "]" +
            "}";
    DataInputStream is = new FakeSeekableInputStream(
            new ByteArrayInputStream(json.getBytes("UTF-8")));

    FSDataInputStream fsis = new FSDataInputStream(is);

    FileSystem fs = mock(FileSystem.class);
    Path pathMock = mock(Path.class);
    when(fs.open(pathMock)).thenReturn(fsis);
    EximUtil.ReadMetaData result = EximUtil.readMetaData(fs, pathMock);

    assertEquals("test-db-name-table", result.getTable().getDbName());
    Iterator<Partition> iterator = result.getPartitions().iterator();
    assertEquals("test-table-name-p1", iterator.next().getTableName());
    assertEquals("test-table-name-p2", iterator.next().getTableName());
  }

  @Test
  public void testGetJSONStringEntry() throws Exception {
    String jsonString = "{\"string-key\":\"string-value\",\"non-string-key\":1}";
    JSONObject jsonObject = (JSONObject) parser.parse(jsonString);
    assertEquals("string-value", EximUtil.getJSONStringEntry(jsonObject, "string-key"));
    assertEquals("1", EximUtil.getJSONStringEntry(jsonObject, "non-string-key"));
    assertNull(EximUtil.getJSONStringEntry(jsonObject, "no-such-key"));
  }

  public void testCheckCompatibility() throws SemanticException {

    // backward/forward compatible
    EximUtil.doCheckCompatibility(
        "10.3", // current code version
        "10.4", // data's version
        null // data's FC version
        ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "10.4", // current code version
        "10.4", // data's version
        null // data's FC version
        ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "10.5", // current code version
        "10.4", // data's version
        null // data's FC version
        ); // No exceptions expected

    // not backward compatible
    try {
      EximUtil.doCheckCompatibility(
          "11.0", // current code version
          "10.4", // data's version
          null // data's FC version
          ); // No exceptions expected
      fail();
    } catch (SemanticException e) {
    }

    // not forward compatible
    try {
      EximUtil.doCheckCompatibility(
          "9.9", // current code version
          "10.4", // data's version
          null // data's FC version
          ); // No exceptions expected
      fail();
    } catch (SemanticException e) {
    }

    // forward compatible
    EximUtil.doCheckCompatibility(
          "9.9", // current code version
        "10.4", // data's version
        "9.9" // data's FC version
    ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "9.9", // current code version
        "10.4", // data's version
        "9.8" // data's FC version
    ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "9.9", // current code version
        "10.4", // data's version
        "8.8" // data's FC version
    ); // No exceptions expected
    EximUtil.doCheckCompatibility(
        "10.3", // current code version
        "10.4", // data's version
        "10.3" // data's FC version
    ); // No exceptions expected

    // not forward compatible
    try {
      EximUtil.doCheckCompatibility(
          "10.2", // current code version
          "10.4", // data's version
          "10.3" // data's FC version
      ); // No exceptions expected
      fail();
    } catch (SemanticException e) {
    }

  }
}
