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
package org.apache.hadoop.hive.accumulo.mr;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.accumulo.AccumuloConnectionParameters;
import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.HiveAccumuloHelper;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloRowIdColumnMapping;
import org.apache.hadoop.hive.accumulo.predicate.AccumuloPredicateHandler;
import org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter;
import org.apache.hadoop.hive.accumulo.predicate.compare.DoubleCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.Equal;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.GreaterThanOrEqual;
import org.apache.hadoop.hive.accumulo.predicate.compare.IntCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.LessThan;
import org.apache.hadoop.hive.accumulo.predicate.compare.LongCompare;
import org.apache.hadoop.hive.accumulo.predicate.compare.StringCompare;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.accumulo.serde.TooManyAccumuloColumnsException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

public class TestHiveAccumuloTableInputFormat {
  public static final String USER = "user";
  public static final String PASS = "password";
  public static final String TEST_TABLE = "table1";
  public static final Text COLUMN_FAMILY = new Text("cf");

  private static final Text NAME = new Text("name");
  private static final Text SID = new Text("sid");
  private static final Text DEGREES = new Text("dgrs");
  private static final Text MILLIS = new Text("mills");

  private Instance mockInstance;
  private Connector con;
  private HiveAccumuloTableInputFormat inputformat;
  private JobConf conf;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  @Rule
  public TestName test = new TestName();

  @Before
  public void createMockKeyValues() throws Exception {
    // Make a MockInstance here, by setting the instance name to be the same as this mock instance
    // we can "trick" the InputFormat into using a MockInstance
    mockInstance = new MockInstance(test.getMethodName());
    inputformat = new HiveAccumuloTableInputFormat();
    conf = new JobConf();
    conf.set(AccumuloSerDeParameters.TABLE_NAME, TEST_TABLE);
    conf.set(AccumuloSerDeParameters.USE_MOCK_INSTANCE, "true");
    conf.set(AccumuloSerDeParameters.INSTANCE_NAME, test.getMethodName());
    conf.set(AccumuloSerDeParameters.USER_NAME, USER);
    conf.set(AccumuloSerDeParameters.USER_PASS, PASS);
    conf.set(AccumuloSerDeParameters.ZOOKEEPERS, "localhost:2181"); // not used for mock, but
                                                                    // required by input format.

    columnNames = Arrays.asList("name", "sid", "dgrs", "mills");
    columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo, TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.longTypeInfo);
    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:name,cf:sid,cf:dgrs,cf:mills");
    conf.set(serdeConstants.LIST_COLUMNS, "name,sid,dgrs,mills");
    conf.set(serdeConstants.LIST_COLUMN_TYPES, "string,int,double,bigint");

    con = mockInstance.getConnector(USER, new PasswordToken(PASS.getBytes()));
    con.tableOperations().create(TEST_TABLE);
    con.securityOperations().changeUserAuthorizations(USER, new Authorizations("blah"));
    BatchWriterConfig writerConf = new BatchWriterConfig();
    BatchWriter writer = con.createBatchWriter(TEST_TABLE, writerConf);

    Mutation m1 = new Mutation(new Text("r1"));
    m1.put(COLUMN_FAMILY, NAME, new Value("brian".getBytes()));
    m1.put(COLUMN_FAMILY, SID, new Value(parseIntBytes("1")));
    m1.put(COLUMN_FAMILY, DEGREES, new Value(parseDoubleBytes("44.5")));
    m1.put(COLUMN_FAMILY, MILLIS, new Value(parseLongBytes("555")));

    Mutation m2 = new Mutation(new Text("r2"));
    m2.put(COLUMN_FAMILY, NAME, new Value("mark".getBytes()));
    m2.put(COLUMN_FAMILY, SID, new Value(parseIntBytes("2")));
    m2.put(COLUMN_FAMILY, DEGREES, new Value(parseDoubleBytes("55.5")));
    m2.put(COLUMN_FAMILY, MILLIS, new Value(parseLongBytes("666")));

    Mutation m3 = new Mutation(new Text("r3"));
    m3.put(COLUMN_FAMILY, NAME, new Value("dennis".getBytes()));
    m3.put(COLUMN_FAMILY, SID, new Value(parseIntBytes("3")));
    m3.put(COLUMN_FAMILY, DEGREES, new Value(parseDoubleBytes("65.5")));
    m3.put(COLUMN_FAMILY, MILLIS, new Value(parseLongBytes("777")));

    writer.addMutation(m1);
    writer.addMutation(m2);
    writer.addMutation(m3);

    writer.close();
  }

  private byte[] parseIntBytes(String s) throws IOException {
    int val = Integer.parseInt(s);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4);
    DataOutputStream out = new DataOutputStream(baos);
    out.writeInt(val);
    out.close();
    return baos.toByteArray();
  }

  private byte[] parseLongBytes(String s) throws IOException {
    long val = Long.parseLong(s);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(8);
    DataOutputStream out = new DataOutputStream(baos);
    out.writeLong(val);
    out.close();
    return baos.toByteArray();
  }

  private byte[] parseDoubleBytes(String s) throws IOException {
    double val = Double.parseDouble(s);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(8);
    DataOutputStream out = new DataOutputStream(baos);
    out.writeDouble(val);
    out.close();
    return baos.toByteArray();
  }

  @Test
  public void testHiveAccumuloRecord() throws Exception {
    FileInputFormat.addInputPath(conf, new Path("unused"));
    InputSplit[] splits = inputformat.getSplits(conf, 0);
    assertEquals(splits.length, 1);
    RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);
    Text rowId = new Text("r1");
    AccumuloHiveRow row = new AccumuloHiveRow();
    row.add(COLUMN_FAMILY.toString(), NAME.toString(), "brian".getBytes());
    row.add(COLUMN_FAMILY.toString(), SID.toString(), parseIntBytes("1"));
    row.add(COLUMN_FAMILY.toString(), DEGREES.toString(), parseDoubleBytes("44.5"));
    row.add(COLUMN_FAMILY.toString(), MILLIS.toString(), parseLongBytes("555"));
    assertTrue(reader.next(rowId, row));
    assertEquals(rowId.toString(), row.getRowId());
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, NAME));
    assertArrayEquals("brian".getBytes(), row.getValue(COLUMN_FAMILY, NAME));
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, SID));
    assertArrayEquals(parseIntBytes("1"), row.getValue(COLUMN_FAMILY, SID));
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, DEGREES));
    assertArrayEquals(parseDoubleBytes("44.5"), row.getValue(COLUMN_FAMILY, DEGREES));
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, MILLIS));
    assertArrayEquals(parseLongBytes("555"), row.getValue(COLUMN_FAMILY, MILLIS));
  }

  @Test
  public void testGetOnlyName() throws Exception {
    FileInputFormat.addInputPath(conf, new Path("unused"));

    InputSplit[] splits = inputformat.getSplits(conf, 0);
    assertEquals(splits.length, 1);
    RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);
    Text rowId = new Text("r1");
    AccumuloHiveRow row = new AccumuloHiveRow();
    assertTrue(reader.next(rowId, row));
    assertEquals(row.getRowId(), rowId.toString());
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, NAME));
    assertArrayEquals(row.getValue(COLUMN_FAMILY, NAME), "brian".getBytes());

    rowId = new Text("r2");
    assertTrue(reader.next(rowId, row));
    assertEquals(row.getRowId(), rowId.toString());
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, NAME));
    assertArrayEquals(row.getValue(COLUMN_FAMILY, NAME), "mark".getBytes());

    rowId = new Text("r3");
    assertTrue(reader.next(rowId, row));
    assertEquals(row.getRowId(), rowId.toString());
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, NAME));
    assertArrayEquals(row.getValue(COLUMN_FAMILY, NAME), "dennis".getBytes());

    assertFalse(reader.next(rowId, row));
  }

  @Test
  public void testDegreesAndMillis() throws Exception {
    Connector con = mockInstance.getConnector(USER, new PasswordToken(PASS.getBytes()));
    Scanner scan = con.createScanner(TEST_TABLE, new Authorizations("blah"));
    IteratorSetting is = new IteratorSetting(1, PrimitiveComparisonFilter.FILTER_PREFIX + 1,
        PrimitiveComparisonFilter.class);

    is.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, DoubleCompare.class.getName());
    is.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, GreaterThanOrEqual.class.getName());
    is.addOption(PrimitiveComparisonFilter.CONST_VAL, Base64.getEncoder().encodeToString(parseDoubleBytes("55.6")));
    is.addOption(PrimitiveComparisonFilter.COLUMN, "cf:dgrs");
    scan.addScanIterator(is);

    IteratorSetting is2 = new IteratorSetting(2, PrimitiveComparisonFilter.FILTER_PREFIX + 2,
        PrimitiveComparisonFilter.class);

    is2.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, LongCompare.class.getName());
    is2.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, LessThan.class.getName());
    is2.addOption(PrimitiveComparisonFilter.CONST_VAL, Base64.getEncoder().encodeToString(parseLongBytes("778")));
    is2.addOption(PrimitiveComparisonFilter.COLUMN, "cf:mills");

    scan.addScanIterator(is2);

    boolean foundDennis = false;
    int totalCount = 0;
    for (Map.Entry<Key,Value> kv : scan) {
      boolean foundName = false;
      boolean foundSid = false;
      boolean foundDegrees = false;
      boolean foundMillis = false;
      SortedMap<Key,Value> items = PrimitiveComparisonFilter.decodeRow(kv.getKey(), kv.getValue());
      for (Map.Entry<Key,Value> item : items.entrySet()) {
        SortedMap<Key,Value> nestedItems = PrimitiveComparisonFilter.decodeRow(item.getKey(),
            item.getValue());
        for (Map.Entry<Key,Value> nested : nestedItems.entrySet()) {
          if (nested.getKey().getRow().toString().equals("r3")) {
            foundDennis = true;
          }
          if (nested.getKey().getColumnQualifier().equals(NAME)) {
            foundName = true;
          } else if (nested.getKey().getColumnQualifier().equals(SID)) {
            foundSid = true;
          } else if (nested.getKey().getColumnQualifier().equals(DEGREES)) {
            foundDegrees = true;
          } else if (nested.getKey().getColumnQualifier().equals(MILLIS)) {
            foundMillis = true;
          }
        }
      }
      totalCount++;
      assertTrue(foundDegrees & foundMillis & foundName & foundSid);
    }
    assertTrue(foundDennis);
    assertEquals(totalCount, 1);
  }

  @Test
  public void testGreaterThan1Sid() throws Exception {
    Connector con = mockInstance.getConnector(USER, new PasswordToken(PASS.getBytes()));
    Scanner scan = con.createScanner(TEST_TABLE, new Authorizations("blah"));
    IteratorSetting is = new IteratorSetting(1, PrimitiveComparisonFilter.FILTER_PREFIX + 1,
        PrimitiveComparisonFilter.class);

    is.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, IntCompare.class.getName());
    is.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, GreaterThan.class.getName());
    is.addOption(PrimitiveComparisonFilter.CONST_VAL, Base64.getEncoder().encodeToString(parseIntBytes("1")));
    is.addOption(PrimitiveComparisonFilter.COLUMN, "cf:sid");
    scan.addScanIterator(is);
    boolean foundMark = false;
    boolean foundDennis = false;
    int totalCount = 0;
    for (Map.Entry<Key,Value> kv : scan) {
      boolean foundName = false;
      boolean foundSid = false;
      boolean foundDegrees = false;
      boolean foundMillis = false;
      SortedMap<Key,Value> items = PrimitiveComparisonFilter.decodeRow(kv.getKey(), kv.getValue());
      for (Map.Entry<Key,Value> item : items.entrySet()) {
        if (item.getKey().getRow().toString().equals("r2")) {
          foundMark = true;
        } else if (item.getKey().getRow().toString().equals("r3")) {
          foundDennis = true;
        }
        if (item.getKey().getColumnQualifier().equals(NAME)) {
          foundName = true;
        } else if (item.getKey().getColumnQualifier().equals(SID)) {
          foundSid = true;
        } else if (item.getKey().getColumnQualifier().equals(DEGREES)) {
          foundDegrees = true;
        } else if (item.getKey().getColumnQualifier().equals(MILLIS)) {
          foundMillis = true;
        }
      }
      totalCount++;
      assertTrue(foundDegrees & foundMillis & foundName & foundSid);
    }
    assertTrue(foundDennis & foundMark);
    assertEquals(totalCount, 2);
  }

  @Test
  public void testNameEqualBrian() throws Exception {
    Connector con = mockInstance.getConnector(USER, new PasswordToken(PASS.getBytes()));
    Scanner scan = con.createScanner(TEST_TABLE, new Authorizations("blah"));
    IteratorSetting is = new IteratorSetting(1, PrimitiveComparisonFilter.FILTER_PREFIX + 1,
        PrimitiveComparisonFilter.class);

    is.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, StringCompare.class.getName());
    is.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, Equal.class.getName());
    is.addOption(PrimitiveComparisonFilter.CONST_VAL, Base64.getEncoder().encodeToString("brian".getBytes()));
    is.addOption(PrimitiveComparisonFilter.COLUMN, "cf:name");
    scan.addScanIterator(is);
    boolean foundName = false;
    boolean foundSid = false;
    boolean foundDegrees = false;
    boolean foundMillis = false;
    for (Map.Entry<Key,Value> kv : scan) {
      SortedMap<Key,Value> items = PrimitiveComparisonFilter.decodeRow(kv.getKey(), kv.getValue());
      for (Map.Entry<Key,Value> item : items.entrySet()) {
        assertEquals(item.getKey().getRow().toString(), "r1");
        if (item.getKey().getColumnQualifier().equals(NAME)) {
          foundName = true;
          assertArrayEquals(item.getValue().get(), "brian".getBytes());
        } else if (item.getKey().getColumnQualifier().equals(SID)) {
          foundSid = true;
          assertArrayEquals(item.getValue().get(), parseIntBytes("1"));
        } else if (item.getKey().getColumnQualifier().equals(DEGREES)) {
          foundDegrees = true;
          assertArrayEquals(item.getValue().get(), parseDoubleBytes("44.5"));
        } else if (item.getKey().getColumnQualifier().equals(MILLIS)) {
          foundMillis = true;
          assertArrayEquals(item.getValue().get(), parseLongBytes("555"));
        }
      }
    }
    assertTrue(foundDegrees & foundMillis & foundName & foundSid);
  }

  @Test
  public void testGetNone() throws Exception {
    FileInputFormat.addInputPath(conf, new Path("unused"));
    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:f1");
    InputSplit[] splits = inputformat.getSplits(conf, 0);
    assertEquals(splits.length, 1);
    RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);
    Text rowId = new Text("r1");
    AccumuloHiveRow row = new AccumuloHiveRow();
    row.setRowId("r1");
    assertFalse(reader.next(rowId, row));
  }

  @Test
  public void testIteratorNotInSplitsCompensation() throws Exception {
    FileInputFormat.addInputPath(conf, new Path("unused"));
    InputSplit[] splits = inputformat.getSplits(conf, 0);

    assertEquals(1, splits.length);
    InputSplit split = splits[0];

    IteratorSetting is = new IteratorSetting(1, PrimitiveComparisonFilter.FILTER_PREFIX + 1,
        PrimitiveComparisonFilter.class);

    is.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, StringCompare.class.getName());
    is.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, Equal.class.getName());
    is.addOption(PrimitiveComparisonFilter.CONST_VAL, Base64.getEncoder().encodeToString(new byte[] {'0'}));
    is.addOption(PrimitiveComparisonFilter.COLUMN, "cf:cq");

    // Mock out the predicate handler because it's just easier
    AccumuloPredicateHandler predicateHandler = Mockito.mock(AccumuloPredicateHandler.class);
    Mockito.when(
        predicateHandler.getIterators(Mockito.any(JobConf.class), Mockito.any(ColumnMapper.class)))
        .thenReturn(Arrays.asList(is));

    // Set it on our inputformat
    inputformat.predicateHandler = predicateHandler;

    inputformat.getRecordReader(split, conf, null);

    // The code should account for the bug and update the iterators on the split
    List<IteratorSetting> settingsOnSplit = ((HiveAccumuloSplit) split).getSplit().getIterators();
    assertEquals(1, settingsOnSplit.size());
    assertEquals(is, settingsOnSplit.get(0));
  }

  @Test
  public void testColumnMappingsToPairs() {
    List<ColumnMapping> mappings = new ArrayList<ColumnMapping>();
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();

    // Row ID
    mappings.add(new HiveAccumuloRowIdColumnMapping(AccumuloHiveConstants.ROWID,
        ColumnEncoding.STRING, "row", TypeInfoFactory.stringTypeInfo.toString()));

    // Some cf:cq
    mappings.add(new HiveAccumuloColumnMapping("person", "name", ColumnEncoding.STRING, "col1",
        TypeInfoFactory.stringTypeInfo.toString()));
    mappings.add(new HiveAccumuloColumnMapping("person", "age", ColumnEncoding.STRING, "col2",
        TypeInfoFactory.stringTypeInfo.toString()));
    mappings.add(new HiveAccumuloColumnMapping("person", "height", ColumnEncoding.STRING, "col3",
        TypeInfoFactory.stringTypeInfo.toString()));

    // Bare cf
    mappings.add(new HiveAccumuloColumnMapping("city", "name", ColumnEncoding.STRING, "col4",
        TypeInfoFactory.stringTypeInfo.toString()));

    columns.add(new Pair<Text,Text>(new Text("person"), new Text("name")));
    columns.add(new Pair<Text,Text>(new Text("person"), new Text("age")));
    columns.add(new Pair<Text,Text>(new Text("person"), new Text("height")));
    // Null qualifier would mean all qualifiers in that family, want an empty qualifier
    columns.add(new Pair<Text,Text>(new Text("city"), new Text("name")));

    assertEquals(columns, inputformat.getPairCollection(mappings));
  }

  @Test
  public void testConfigureMockAccumuloInputFormat() throws Exception {
    AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(conf);
    ColumnMapper columnMapper = new ColumnMapper(conf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS),
        conf.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE), columnNames, columnTypes);
    Set<Pair<Text,Text>> cfCqPairs = inputformat
        .getPairCollection(columnMapper.getColumnMappings());
    List<IteratorSetting> iterators = Collections.emptyList();
    Set<Range> ranges = Collections.singleton(new Range());

    HiveAccumuloTableInputFormat mockInputFormat = Mockito.mock(HiveAccumuloTableInputFormat.class);
    HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);

    // Stub out a mocked Helper instance
    Mockito.when(mockInputFormat.getHelper()).thenReturn(helper);

    // Call out to the real configure method
    Mockito.doCallRealMethod().when(mockInputFormat)
        .configure(conf, mockInstance, con, accumuloParams, columnMapper, iterators, ranges);

    // Also compute the correct cf:cq pairs so we can assert the right argument was passed
    Mockito.doCallRealMethod().when(mockInputFormat)
        .getPairCollection(columnMapper.getColumnMappings());

    mockInputFormat.configure(conf, mockInstance, con, accumuloParams, columnMapper, iterators,
        ranges);

    // Verify that the correct methods are invoked on AccumuloInputFormat
    Mockito.verify(helper).setInputFormatMockInstance(conf, mockInstance.getInstanceName());
    Mockito.verify(helper).setInputFormatConnectorInfo(conf, USER, new PasswordToken(PASS));
    Mockito.verify(mockInputFormat).setInputTableName(conf, TEST_TABLE);
    Mockito.verify(mockInputFormat).setScanAuthorizations(conf,
        con.securityOperations().getUserAuthorizations(USER));
    Mockito.verify(mockInputFormat).addIterators(conf, iterators);
    Mockito.verify(mockInputFormat).setRanges(conf, ranges);
    Mockito.verify(mockInputFormat).fetchColumns(conf, cfCqPairs);
  }

  @Test
  public void testConfigureAccumuloInputFormat() throws Exception {
    AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(conf);
    ColumnMapper columnMapper = new ColumnMapper(conf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS),
        conf.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE), columnNames, columnTypes);
    Set<Pair<Text,Text>> cfCqPairs = inputformat
        .getPairCollection(columnMapper.getColumnMappings());
    List<IteratorSetting> iterators = Collections.emptyList();
    Set<Range> ranges = Collections.singleton(new Range());
    String instanceName = "realInstance";
    String zookeepers = "host1:2181,host2:2181,host3:2181";

    ZooKeeperInstance zkInstance = Mockito.mock(ZooKeeperInstance.class);
    HiveAccumuloTableInputFormat mockInputFormat = Mockito.mock(HiveAccumuloTableInputFormat.class);
    HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);

    // Stub out the ZKI mock
    Mockito.when(zkInstance.getInstanceName()).thenReturn(instanceName);
    Mockito.when(zkInstance.getZooKeepers()).thenReturn(zookeepers);
    // Stub out a mocked Helper instance
    Mockito.when(mockInputFormat.getHelper()).thenReturn(helper);

    // Call out to the real configure method
    Mockito.doCallRealMethod().when(mockInputFormat)
        .configure(conf, zkInstance, con, accumuloParams, columnMapper, iterators, ranges);

    // Also compute the correct cf:cq pairs so we can assert the right argument was passed
    Mockito.doCallRealMethod().when(mockInputFormat)
        .getPairCollection(columnMapper.getColumnMappings());

    mockInputFormat.configure(conf, zkInstance, con, accumuloParams, columnMapper, iterators,
        ranges);

    // Verify that the correct methods are invoked on AccumuloInputFormat
    Mockito.verify(helper).setInputFormatZooKeeperInstance(conf, instanceName, zookeepers, false);
    Mockito.verify(helper).setInputFormatConnectorInfo(conf, USER, new PasswordToken(PASS));
    Mockito.verify(mockInputFormat).setInputTableName(conf, TEST_TABLE);
    Mockito.verify(mockInputFormat).setScanAuthorizations(conf,
        con.securityOperations().getUserAuthorizations(USER));
    Mockito.verify(mockInputFormat).addIterators(conf, iterators);
    Mockito.verify(mockInputFormat).setRanges(conf, ranges);
    Mockito.verify(mockInputFormat).fetchColumns(conf, cfCqPairs);
  }

  @Test
  public void testConfigureAccumuloInputFormatWithAuthorizations() throws Exception {
    AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(conf);
    conf.set(AccumuloSerDeParameters.AUTHORIZATIONS_KEY, "foo,bar");
    ColumnMapper columnMapper = new ColumnMapper(conf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS),
        conf.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE), columnNames, columnTypes);
    Set<Pair<Text,Text>> cfCqPairs = inputformat
        .getPairCollection(columnMapper.getColumnMappings());
    List<IteratorSetting> iterators = Collections.emptyList();
    Set<Range> ranges = Collections.singleton(new Range());
    String instanceName = "realInstance";
    String zookeepers = "host1:2181,host2:2181,host3:2181";

    ZooKeeperInstance zkInstance = Mockito.mock(ZooKeeperInstance.class);
    HiveAccumuloTableInputFormat mockInputFormat = Mockito.mock(HiveAccumuloTableInputFormat.class);
    HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);

    // Stub out the ZKI mock
    Mockito.when(zkInstance.getInstanceName()).thenReturn(instanceName);
    Mockito.when(zkInstance.getZooKeepers()).thenReturn(zookeepers);
    // Stub out a mocked Helper instance
    Mockito.when(mockInputFormat.getHelper()).thenReturn(helper);

    // Call out to the real configure method
    Mockito.doCallRealMethod().when(mockInputFormat)
        .configure(conf, zkInstance, con, accumuloParams, columnMapper, iterators, ranges);

    // Also compute the correct cf:cq pairs so we can assert the right argument was passed
    Mockito.doCallRealMethod().when(mockInputFormat)
        .getPairCollection(columnMapper.getColumnMappings());

    mockInputFormat.configure(conf, zkInstance, con, accumuloParams, columnMapper, iterators,
        ranges);

    // Verify that the correct methods are invoked on AccumuloInputFormat
    Mockito.verify(helper).setInputFormatZooKeeperInstance(conf, instanceName, zookeepers, false);
    Mockito.verify(helper).setInputFormatConnectorInfo(conf, USER, new PasswordToken(PASS));
    Mockito.verify(mockInputFormat).setInputTableName(conf, TEST_TABLE);
    Mockito.verify(mockInputFormat).setScanAuthorizations(conf, new Authorizations("foo,bar"));
    Mockito.verify(mockInputFormat).addIterators(conf, iterators);
    Mockito.verify(mockInputFormat).setRanges(conf, ranges);
    Mockito.verify(mockInputFormat).fetchColumns(conf, cfCqPairs);
  }

  @Test
  public void testConfigureAccumuloInputFormatWithIterators() throws Exception {
    AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(conf);
    ColumnMapper columnMapper = new ColumnMapper(conf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS),
        conf.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE), columnNames, columnTypes);
    Set<Pair<Text,Text>> cfCqPairs = inputformat
        .getPairCollection(columnMapper.getColumnMappings());
    List<IteratorSetting> iterators = new ArrayList<IteratorSetting>();
    Set<Range> ranges = Collections.singleton(new Range());
    String instanceName = "realInstance";
    String zookeepers = "host1:2181,host2:2181,host3:2181";

    IteratorSetting cfg = new IteratorSetting(50, PrimitiveComparisonFilter.class);
    cfg.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, StringCompare.class.getName());
    cfg.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, Equal.class.getName());
    cfg.addOption(PrimitiveComparisonFilter.CONST_VAL, "dave");
    cfg.addOption(PrimitiveComparisonFilter.COLUMN, "person:name");
    iterators.add(cfg);

    cfg = new IteratorSetting(50, PrimitiveComparisonFilter.class);
    cfg.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, IntCompare.class.getName());
    cfg.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, Equal.class.getName());
    cfg.addOption(PrimitiveComparisonFilter.CONST_VAL, "50");
    cfg.addOption(PrimitiveComparisonFilter.COLUMN, "person:age");
    iterators.add(cfg);

    ZooKeeperInstance zkInstance = Mockito.mock(ZooKeeperInstance.class);
    HiveAccumuloTableInputFormat mockInputFormat = Mockito.mock(HiveAccumuloTableInputFormat.class);
    HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);

    // Stub out the ZKI mock
    Mockito.when(zkInstance.getInstanceName()).thenReturn(instanceName);
    Mockito.when(zkInstance.getZooKeepers()).thenReturn(zookeepers);
    // Stub out a mocked Helper instance
    Mockito.when(mockInputFormat.getHelper()).thenReturn(helper);

    // Call out to the real configure method
    Mockito.doCallRealMethod().when(mockInputFormat)
        .configure(conf, zkInstance, con, accumuloParams, columnMapper, iterators, ranges);

    // Also compute the correct cf:cq pairs so we can assert the right argument was passed
    Mockito.doCallRealMethod().when(mockInputFormat)
        .getPairCollection(columnMapper.getColumnMappings());

    mockInputFormat.configure(conf, zkInstance, con, accumuloParams, columnMapper, iterators,
        ranges);

    // Verify that the correct methods are invoked on AccumuloInputFormat
    Mockito.verify(helper).setInputFormatZooKeeperInstance(conf, instanceName, zookeepers, false);
    Mockito.verify(helper).setInputFormatConnectorInfo(conf, USER, new PasswordToken(PASS));
    Mockito.verify(mockInputFormat).setInputTableName(conf, TEST_TABLE);
    Mockito.verify(mockInputFormat).setScanAuthorizations(conf,
        con.securityOperations().getUserAuthorizations(USER));
    Mockito.verify(mockInputFormat).addIterators(conf, iterators);
    Mockito.verify(mockInputFormat).setRanges(conf, ranges);
    Mockito.verify(mockInputFormat).fetchColumns(conf, cfCqPairs);
  }

  @Test
  public void testConfigureAccumuloInputFormatWithEmptyColumns() throws Exception {
    AccumuloConnectionParameters accumuloParams = new AccumuloConnectionParameters(conf);
    ColumnMapper columnMapper = new ColumnMapper(conf.get(AccumuloSerDeParameters.COLUMN_MAPPINGS),
        conf.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE), columnNames, columnTypes);
    HashSet<Pair<Text,Text>> cfCqPairs = Sets.newHashSet();
    List<IteratorSetting> iterators = new ArrayList<IteratorSetting>();
    Set<Range> ranges = Collections.singleton(new Range());
    String instanceName = "realInstance";
    String zookeepers = "host1:2181,host2:2181,host3:2181";

    IteratorSetting cfg = new IteratorSetting(50, PrimitiveComparisonFilter.class);
    cfg.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, StringCompare.class.getName());
    cfg.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, Equal.class.getName());
    cfg.addOption(PrimitiveComparisonFilter.CONST_VAL, "dave");
    cfg.addOption(PrimitiveComparisonFilter.COLUMN, "person:name");
    iterators.add(cfg);

    cfg = new IteratorSetting(50, PrimitiveComparisonFilter.class);
    cfg.addOption(PrimitiveComparisonFilter.P_COMPARE_CLASS, IntCompare.class.getName());
    cfg.addOption(PrimitiveComparisonFilter.COMPARE_OPT_CLASS, Equal.class.getName());
    cfg.addOption(PrimitiveComparisonFilter.CONST_VAL, "50");
    cfg.addOption(PrimitiveComparisonFilter.COLUMN, "person:age");
    iterators.add(cfg);

    ZooKeeperInstance zkInstance = Mockito.mock(ZooKeeperInstance.class);
    HiveAccumuloTableInputFormat mockInputFormat = Mockito.mock(HiveAccumuloTableInputFormat.class);
    HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);

    // Stub out the ZKI mock
    Mockito.when(zkInstance.getInstanceName()).thenReturn(instanceName);
    Mockito.when(zkInstance.getZooKeepers()).thenReturn(zookeepers);
    Mockito.when(mockInputFormat.getPairCollection(columnMapper.getColumnMappings())).thenReturn(
        cfCqPairs);
    // Stub out a mocked Helper instance
    Mockito.when(mockInputFormat.getHelper()).thenReturn(helper);

    // Call out to the real configure method
    Mockito.doCallRealMethod().when(mockInputFormat)
        .configure(conf, zkInstance, con, accumuloParams, columnMapper, iterators, ranges);

    // Also compute the correct cf:cq pairs so we can assert the right argument was passed
    Mockito.doCallRealMethod().when(mockInputFormat)
        .getPairCollection(columnMapper.getColumnMappings());

    mockInputFormat.configure(conf, zkInstance, con, accumuloParams, columnMapper, iterators,
        ranges);

    // Verify that the correct methods are invoked on AccumuloInputFormat
    Mockito.verify(helper).setInputFormatZooKeeperInstance(conf, instanceName, zookeepers, false);
    Mockito.verify(helper).setInputFormatConnectorInfo(conf, USER, new PasswordToken(PASS));
    Mockito.verify(mockInputFormat).setInputTableName(conf, TEST_TABLE);
    Mockito.verify(mockInputFormat).setScanAuthorizations(conf,
        con.securityOperations().getUserAuthorizations(USER));
    Mockito.verify(mockInputFormat).addIterators(conf, iterators);
    Mockito.verify(mockInputFormat).setRanges(conf, ranges);

    // fetchColumns is not called because we had no columns to fetch
  }

  @Test
  public void testGetProtectedField() throws Exception {
    FileInputFormat.addInputPath(conf, new Path("unused"));

    BatchWriterConfig writerConf = new BatchWriterConfig();
    BatchWriter writer = con.createBatchWriter(TEST_TABLE, writerConf);

    Authorizations origAuths = con.securityOperations().getUserAuthorizations(USER);
    con.securityOperations().changeUserAuthorizations(USER,
        new Authorizations(origAuths.toString() + ",foo"));

    Mutation m = new Mutation("r4");
    m.put(COLUMN_FAMILY, NAME, new ColumnVisibility("foo"), new Value("frank".getBytes()));
    m.put(COLUMN_FAMILY, SID, new ColumnVisibility("foo"), new Value(parseIntBytes("4")));
    m.put(COLUMN_FAMILY, DEGREES, new ColumnVisibility("foo"), new Value(parseDoubleBytes("60.6")));
    m.put(COLUMN_FAMILY, MILLIS, new ColumnVisibility("foo"), new Value(parseLongBytes("777")));

    writer.addMutation(m);
    writer.close();

    conf.set(AccumuloSerDeParameters.AUTHORIZATIONS_KEY, "foo");

    InputSplit[] splits = inputformat.getSplits(conf, 0);
    assertEquals(splits.length, 1);
    RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);
    Text rowId = new Text("r1");
    AccumuloHiveRow row = new AccumuloHiveRow();
    assertTrue(reader.next(rowId, row));
    assertEquals(row.getRowId(), rowId.toString());
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, NAME));
    assertArrayEquals(row.getValue(COLUMN_FAMILY, NAME), "brian".getBytes());

    rowId = new Text("r2");
    assertTrue(reader.next(rowId, row));
    assertEquals(row.getRowId(), rowId.toString());
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, NAME));
    assertArrayEquals(row.getValue(COLUMN_FAMILY, NAME), "mark".getBytes());

    rowId = new Text("r3");
    assertTrue(reader.next(rowId, row));
    assertEquals(row.getRowId(), rowId.toString());
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, NAME));
    assertArrayEquals(row.getValue(COLUMN_FAMILY, NAME), "dennis".getBytes());

    rowId = new Text("r4");
    assertTrue(reader.next(rowId, row));
    assertEquals(row.getRowId(), rowId.toString());
    assertTrue(row.hasFamAndQual(COLUMN_FAMILY, NAME));
    assertArrayEquals(row.getValue(COLUMN_FAMILY, NAME), "frank".getBytes());

    assertFalse(reader.next(rowId, row));
  }

  @Test
  public void testMapColumnPairs() throws TooManyAccumuloColumnsException {
    ColumnMapper columnMapper = new ColumnMapper(":rowID,cf:*",
        conf.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE), Arrays.asList("row", "col"),
        Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.getMapTypeInfo(
            TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo)));
    Set<Pair<Text,Text>> pairs = inputformat.getPairCollection(columnMapper.getColumnMappings());

    Assert.assertEquals(1, pairs.size());

    Pair<Text,Text> cfCq = pairs.iterator().next();
    Assert.assertEquals("cf", cfCq.getFirst().toString());
    Assert.assertNull(cfCq.getSecond());
  }
}
