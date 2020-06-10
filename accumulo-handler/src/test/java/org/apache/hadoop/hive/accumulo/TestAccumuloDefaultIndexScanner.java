/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.serde.AccumuloIndexParameters;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestAccumuloDefaultIndexScanner {
  private static final Logger LOG = LoggerFactory.getLogger(TestAccumuloDefaultIndexScanner.class);
  private static final Value EMPTY_VALUE = new Value();

  private static void addRow(BatchWriter writer, String rowId, String cf, String cq) throws MutationsRejectedException {
    Mutation mut = new Mutation(rowId);
    mut.put(new Text(cf), new Text(cq), EMPTY_VALUE);
    writer.addMutation(mut);
  }

  private static void addRow(BatchWriter writer, Integer rowId, String cf, String cq) throws MutationsRejectedException {
    Mutation mut = new Mutation(AccumuloIndexLexicoder.encodeValue(String.valueOf(rowId).getBytes(), "int", true));
    mut.put(new Text(cf), new Text(cq), EMPTY_VALUE);
    writer.addMutation(mut);
  }

  private static void addRow(BatchWriter writer, boolean rowId, String cf, String cq) throws MutationsRejectedException {
    Mutation mut = new Mutation(String.valueOf(rowId));
    mut.put(new Text(cf), new Text(cq), EMPTY_VALUE);
    writer.addMutation(mut);
  }

  public static AccumuloDefaultIndexScanner buildMockHandler(int maxMatches) {
    try {
      String table = "table";
      Text emptyText = new Text("");
      Configuration conf = new Configuration();
      conf.set(AccumuloIndexParameters.INDEXTABLE_NAME, table);
      conf.setInt(AccumuloIndexParameters.MAX_INDEX_ROWS, maxMatches);
      conf.set(AccumuloIndexParameters.INDEXED_COLUMNS, "*");
      conf.set(serdeConstants.LIST_COLUMNS, "rid,name,age,cars,mgr");
      conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowId,name:name,age:age,cars:cars,mgr:mgr");
      AccumuloDefaultIndexScanner handler = new AccumuloDefaultIndexScanner();
      handler.init(conf);

      MockInstance inst = new MockInstance("test_instance");
      Connector conn = inst.getConnector("root", new PasswordToken(""));
      if (!conn.tableOperations().exists(table)) {
        conn.tableOperations().create(table);
        BatchWriterConfig batchConfig = new BatchWriterConfig();
        BatchWriter writer = conn.createBatchWriter(table, batchConfig);
        addRow(writer, "fred", "name_name", "row1");
        addRow(writer, "25", "age_age", "row1");
        addRow(writer, 5, "cars_cars", "row1");
        addRow(writer, true, "mgr_mgr", "row1");
        addRow(writer, "bill", "name_name", "row2");
        addRow(writer, "20", "age_age", "row2");
        addRow(writer, 2, "cars_cars", "row2");
        addRow(writer, false, "mgr_mgr", "row2");
        addRow(writer, "sally", "name_name", "row3");
        addRow(writer, "23", "age_age", "row3");
        addRow(writer, 6, "cars_cars", "row3");
        addRow(writer, true, "mgr_mgr", "row3");
        addRow(writer, "rob", "name_name", "row4");
        addRow(writer, "60", "age_age", "row4");
        addRow(writer, 1, "cars_cars", "row4");
        addRow(writer, false, "mgr_mgr", "row4");
        writer.close();
      }
      AccumuloConnectionParameters connectionParams = Mockito
          .mock(AccumuloConnectionParameters.class);
      AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);

      Mockito.when(connectionParams.getConnector()).thenReturn(conn);
      handler.setConnectParams(connectionParams);
      return handler;
    } catch (AccumuloSecurityException | AccumuloException | TableExistsException | TableNotFoundException e) {
      LOG.error(e.getLocalizedMessage(), e);
    }
    return null;
  }

  @Test
  public void testMatchNone() {
    AccumuloDefaultIndexScanner handler = buildMockHandler(10);
    List<Range> ranges = handler.getIndexRowRanges("name", new Range("mike"));
    assertEquals(0, ranges.size());
  }

  @Test
  public void testMatchRange() {
    AccumuloDefaultIndexScanner handler = buildMockHandler(10);
    List<Range> ranges = handler.getIndexRowRanges("age", new Range("10", "50"));
    assertEquals(3, ranges.size());
    assertTrue("does not contain row1", ranges.contains(new Range("row1")));
    assertTrue("does not contain row2", ranges.contains(new Range("row2")));
    assertTrue("does not contain row3", ranges.contains(new Range("row3")));
  }

  @Test
  public void testTooManyMatches() {
    AccumuloDefaultIndexScanner handler = buildMockHandler(2);
    List<Range> ranges = handler.getIndexRowRanges("age", new Range("10", "50"));
    assertNull("ranges should be null", ranges);
  }

  @Test
  public void testMatchExact() {
    AccumuloDefaultIndexScanner handler = buildMockHandler(10);
    List<Range> ranges = handler.getIndexRowRanges("age", new Range("20"));
    assertEquals(1, ranges.size());
    assertTrue("does not contain row2", ranges.contains(new Range("row2")));
  }

  @Test
  public void testValidIndex() {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEXED_COLUMNS, "name,age,phone,email");
    conf.set(AccumuloIndexParameters.INDEXTABLE_NAME, "contact");
    AccumuloDefaultIndexScanner handler = new AccumuloDefaultIndexScanner();
    handler.init(conf);
    assertTrue("name is not identified as an index", handler.isIndexed("name"));
    assertTrue("age is not identified as an index", handler.isIndexed("age"));
    assertTrue("phone is not identified as an index", handler.isIndexed("phone"));
    assertTrue("email is not identified as an index", handler.isIndexed("email"));
  }

  @Test
  public void testInvalidIndex() {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEXED_COLUMNS, "name,age,phone,email");
    conf.set(AccumuloIndexParameters.INDEXTABLE_NAME, "contact");
    AccumuloDefaultIndexScanner handler = new AccumuloDefaultIndexScanner();
    handler.init(conf);
    assertFalse("mobile is identified as an index", handler.isIndexed("mobile"));
    assertFalse("mail is identified as an index", handler.isIndexed("mail"));
  }


  @Test
  public void testMissingTable() {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEXED_COLUMNS, "name,age,phone,email");
    AccumuloDefaultIndexScanner handler = new AccumuloDefaultIndexScanner();
    handler.init(conf);
    assertFalse("name is identified as an index", handler.isIndexed("name"));
    assertFalse("age is identified as an index", handler.isIndexed("age"));
  }

  @Test
  public void testWildcardIndex() {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEXED_COLUMNS, "*");
    conf.set(AccumuloIndexParameters.INDEXTABLE_NAME, "contact");
    AccumuloDefaultIndexScanner handler = new AccumuloDefaultIndexScanner();
    handler.init(conf);
    assertTrue("name is not identified as an index", handler.isIndexed("name"));
    assertTrue("age is not identified as an index", handler.isIndexed("age"));
  }

  @Test
  public void testNullIndex() {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEXTABLE_NAME, "contact");
    AccumuloDefaultIndexScanner handler = new AccumuloDefaultIndexScanner();
    handler.init(conf);
    assertTrue("name is not identified as an index", handler.isIndexed("name"));
  }

  @Test
  public void testEmptyIndex() {
    Configuration conf = new Configuration();
    conf.set(AccumuloIndexParameters.INDEXED_COLUMNS, "");
    conf.set(AccumuloIndexParameters.INDEXTABLE_NAME, "contact");
    AccumuloDefaultIndexScanner handler = new AccumuloDefaultIndexScanner();
    handler.init(conf);
    assertFalse("name is identified as an index", handler.isIndexed("name"));
  }
}
