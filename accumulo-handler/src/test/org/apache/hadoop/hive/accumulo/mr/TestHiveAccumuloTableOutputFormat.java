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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.accumulo.AccumuloConnectionParameters;
import org.apache.hadoop.hive.accumulo.HiveAccumuloHelper;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.serde.AccumuloRowSerializer;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDe;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import com.google.common.base.Joiner;

/**
 *
 */
public class TestHiveAccumuloTableOutputFormat {

  protected JobConf conf;
  protected String user = "root";
  protected String password = "password";
  protected String instanceName = "instance";
  protected String zookeepers = "host1:2181,host2:2181,host3:2181";
  protected String outputTable = "output";

  @Rule
  public TestName test = new TestName();

  @Before
  public void setup() throws IOException {
    conf = new JobConf();

    conf.set(AccumuloConnectionParameters.USER_NAME, user);
    conf.set(AccumuloConnectionParameters.USER_PASS, password);
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, instanceName);
    conf.set(AccumuloConnectionParameters.ZOOKEEPERS, zookeepers);
    conf.set(AccumuloConnectionParameters.TABLE_NAME, outputTable);

    System.setProperty("java.security.krb5.realm", "accumulo");
    System.setProperty("java.security.krb5.kdc", "fake");
  }

  @After
  public void cleanup() {
    System.setProperty("java.security.krb5.realm", "");
    System.setProperty("java.security.krb5.kdc", "");
  }

  @Test
  public void testBasicConfiguration() throws IOException, AccumuloSecurityException {
    HiveAccumuloTableOutputFormat outputFormat = Mockito.mock(HiveAccumuloTableOutputFormat.class);

    Mockito.doCallRealMethod().when(outputFormat).configureAccumuloOutputFormat(conf);
    Mockito.doCallRealMethod().when(outputFormat).getConnectionParams(conf);

    outputFormat.configureAccumuloOutputFormat(conf);

    Mockito.verify(outputFormat).setConnectorInfoWithErrorChecking(conf, user, new PasswordToken(password));
    Mockito.verify(outputFormat).setZooKeeperInstanceWithErrorChecking(conf, instanceName, zookeepers, false);
    Mockito.verify(outputFormat).setDefaultAccumuloTableName(conf, outputTable);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSaslConfiguration() throws IOException, AccumuloException, AccumuloSecurityException {
    final HiveAccumuloTableOutputFormat outputFormat = Mockito.mock(HiveAccumuloTableOutputFormat.class);
    final AuthenticationToken authToken = Mockito.mock(AuthenticationToken.class);
    final Token hadoopToken = Mockito.mock(Token.class);
    final HiveAccumuloHelper helper = Mockito.mock(HiveAccumuloHelper.class);
    final AccumuloConnectionParameters cnxnParams = Mockito.mock(AccumuloConnectionParameters.class);
    final Connector connector = Mockito.mock(Connector.class);

    // Set UGI to use Kerberos
    // Have to use the string constant to support hadoop 1
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);

    // Set the current UGI to a fake user
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting(user, new String[0]);
    // Use that as the "current user"
    Mockito.when(outputFormat.getCurrentUser()).thenReturn(user1);

    // Turn off passwords, enable sasl and set a keytab
    conf.unset(AccumuloConnectionParameters.USER_PASS);

    // Call the real method instead of the mock
    Mockito.doCallRealMethod().when(outputFormat).configureAccumuloOutputFormat(conf);

    // Return our mocked objects
    Mockito.when(outputFormat.getHelper()).thenReturn(helper);
    Mockito.when(outputFormat.getConnectionParams(conf)).thenReturn(cnxnParams);
    Mockito.when(cnxnParams.getConnector()).thenReturn(connector);
    Mockito.when(helper.getDelegationToken(connector)).thenReturn(authToken);
    Mockito.when(helper.getHadoopToken(authToken)).thenReturn(hadoopToken);

    // Stub AccumuloConnectionParameters actions
    Mockito.when(cnxnParams.useSasl()).thenReturn(true);
    Mockito.when(cnxnParams.getAccumuloUserName()).thenReturn(user);
    Mockito.when(cnxnParams.getAccumuloInstanceName()).thenReturn(instanceName);
    Mockito.when(cnxnParams.getZooKeepers()).thenReturn(zookeepers);

    // Stub OutputFormat actions
    Mockito.when(outputFormat.hasKerberosCredentials(user1)).thenReturn(true);

    // Invoke the method
    outputFormat.configureAccumuloOutputFormat(conf);

    // The AccumuloInputFormat methods
    Mockito.verify(outputFormat).setZooKeeperInstanceWithErrorChecking(conf, instanceName, zookeepers, true);
    Mockito.verify(outputFormat).setConnectorInfoWithErrorChecking(conf, user, authToken);
    Mockito.verify(outputFormat).setDefaultAccumuloTableName(conf, outputTable);

    // Other methods we expect
    Mockito.verify(helper).mergeTokenIntoJobConf(conf, hadoopToken);

    // Make sure the token made it into the UGI
    Collection<Token<? extends TokenIdentifier>> tokens = user1.getTokens();
    Assert.assertEquals(1, tokens.size());
    Assert.assertEquals(hadoopToken, tokens.iterator().next());
  }

  @Test
  public void testMockInstance() throws IOException, AccumuloSecurityException {
    HiveAccumuloTableOutputFormat outputFormat = Mockito.mock(HiveAccumuloTableOutputFormat.class);
    conf.setBoolean(AccumuloConnectionParameters.USE_MOCK_INSTANCE, true);
    conf.unset(AccumuloConnectionParameters.ZOOKEEPERS);

    Mockito.doCallRealMethod().when(outputFormat).configureAccumuloOutputFormat(conf);
    Mockito.doCallRealMethod().when(outputFormat).getConnectionParams(conf);

    outputFormat.configureAccumuloOutputFormat(conf);

    Mockito.verify(outputFormat).setConnectorInfoWithErrorChecking(conf, user, new PasswordToken(password));
    Mockito.verify(outputFormat).setMockInstanceWithErrorChecking(conf, instanceName);
    Mockito.verify(outputFormat).setDefaultAccumuloTableName(conf, outputTable);
  }

  @Test
  public void testWriteToMockInstance() throws Exception {
    Instance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));

    HiveAccumuloTableOutputFormat outputFormat = new HiveAccumuloTableOutputFormat();
    String table = test.getMethodName();
    conn.tableOperations().create(table);

    JobConf conf = new JobConf();
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, inst.getInstanceName());
    conf.set(AccumuloConnectionParameters.USER_NAME, "root");
    conf.set(AccumuloConnectionParameters.USER_PASS, "");
    conf.setBoolean(AccumuloConnectionParameters.USE_MOCK_INSTANCE, true);
    conf.set(AccumuloConnectionParameters.TABLE_NAME, test.getMethodName());

    FileSystem local = FileSystem.getLocal(conf);
    outputFormat.checkOutputSpecs(local, conf);

    RecordWriter<Text,Mutation> recordWriter = outputFormat
        .getRecordWriter(local, conf, null, null);

    List<String> names = Arrays.asList("row", "col1", "col2");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);

    Properties tableProperties = new Properties();
    tableProperties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:cq1,cf:cq2");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, " ");
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(names));
    tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(',').join(types));
    AccumuloSerDeParameters accumuloSerDeParams = new AccumuloSerDeParameters(new Configuration(),
        tableProperties, AccumuloSerDe.class.getSimpleName());
    LazySerDeParameters serDeParams = accumuloSerDeParams.getSerDeParameters();

    AccumuloRowSerializer serializer = new AccumuloRowSerializer(0, serDeParams,
        accumuloSerDeParams.getColumnMappings(), AccumuloSerDeParameters.DEFAULT_VISIBILITY_LABEL,
        accumuloSerDeParams.getRowIdFactory());

    TypeInfo stringTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);

    LazySimpleStructObjectInspector structOI = (LazySimpleStructObjectInspector) LazyFactory
        .createLazyStructInspector(Arrays.asList("row", "cq1", "cq2"),
            Arrays.asList(stringTypeInfo, stringTypeInfo, stringTypeInfo),
            serDeParams.getSeparators(), serDeParams.getNullSequence(),
            serDeParams.isLastColumnTakesRest(), serDeParams.isEscaped(),
            serDeParams.getEscapeChar());

    LazyStruct struct = (LazyStruct) LazyFactory.createLazyObject(structOI);

    ByteArrayRef bytes = new ByteArrayRef();
    bytes.setData("row value1 value2".getBytes());
    struct.init(bytes, 0, bytes.getData().length);

    // Serialize the struct into a mutation
    Mutation m = serializer.serialize(struct, structOI);

    // Write the mutation
    recordWriter.write(new Text(table), m);

    // Close the writer
    recordWriter.close(null);

    Iterator<Entry<Key,Value>> iter = conn.createScanner(table, new Authorizations()).iterator();
    Assert.assertTrue("Iterator did not have an element as expected", iter.hasNext());

    Entry<Key,Value> entry = iter.next();
    Key k = entry.getKey();
    Value v = entry.getValue();

    Assert.assertEquals("row", k.getRow().toString());
    Assert.assertEquals("cf", k.getColumnFamily().toString());
    Assert.assertEquals("cq1", k.getColumnQualifier().toString());
    Assert.assertEquals("", k.getColumnVisibility().toString());
    Assert.assertEquals("value1", new String(v.get()));

    Assert.assertTrue("Iterator did not have an element as expected", iter.hasNext());

    entry = iter.next();
    k = entry.getKey();
    v = entry.getValue();

    Assert.assertEquals("row", k.getRow().toString());
    Assert.assertEquals("cf", k.getColumnFamily().toString());
    Assert.assertEquals("cq2", k.getColumnQualifier().toString());
    Assert.assertEquals("", k.getColumnVisibility().toString());
    Assert.assertEquals("value2", new String(v.get()));

    Assert.assertFalse("Iterator unexpectedly had more data", iter.hasNext());
  }

  @Test
  public void testWriteToMockInstanceWithVisibility() throws Exception {
    Instance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    Authorizations auths = new Authorizations("foo");
    conn.securityOperations().changeUserAuthorizations("root", auths);

    HiveAccumuloTableOutputFormat outputFormat = new HiveAccumuloTableOutputFormat();
    String table = test.getMethodName();
    conn.tableOperations().create(table);

    JobConf conf = new JobConf();
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, inst.getInstanceName());
    conf.set(AccumuloConnectionParameters.USER_NAME, "root");
    conf.set(AccumuloConnectionParameters.USER_PASS, "");
    conf.setBoolean(AccumuloConnectionParameters.USE_MOCK_INSTANCE, true);
    conf.set(AccumuloConnectionParameters.TABLE_NAME, test.getMethodName());

    FileSystem local = FileSystem.getLocal(conf);
    outputFormat.checkOutputSpecs(local, conf);

    RecordWriter<Text,Mutation> recordWriter = outputFormat
        .getRecordWriter(local, conf, null, null);

    List<String> names = Arrays.asList("row", "col1", "col2");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);

    Properties tableProperties = new Properties();
    tableProperties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:cq1,cf:cq2");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, " ");
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(names));
    tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(',').join(types));
    AccumuloSerDeParameters accumuloSerDeParams = new AccumuloSerDeParameters(new Configuration(),
        tableProperties, AccumuloSerDe.class.getSimpleName());
    LazySerDeParameters serDeParams = accumuloSerDeParams.getSerDeParameters();

    AccumuloRowSerializer serializer = new AccumuloRowSerializer(0, serDeParams,
        accumuloSerDeParams.getColumnMappings(), new ColumnVisibility("foo"),
        accumuloSerDeParams.getRowIdFactory());

    LazySimpleStructObjectInspector structOI = (LazySimpleStructObjectInspector) LazyFactory
        .createLazyStructInspector(Arrays.asList("row", "cq1", "cq2"), Arrays.<TypeInfo> asList(
            TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo), serDeParams.getSeparators(), serDeParams
            .getNullSequence(), serDeParams.isLastColumnTakesRest(), serDeParams.isEscaped(),
            serDeParams.getEscapeChar());

    LazyStruct struct = (LazyStruct) LazyFactory.createLazyObject(structOI);

    ByteArrayRef bytes = new ByteArrayRef();
    bytes.setData("row value1 value2".getBytes());
    struct.init(bytes, 0, bytes.getData().length);

    // Serialize the struct into a mutation
    Mutation m = serializer.serialize(struct, structOI);

    // Write the mutation
    recordWriter.write(new Text(table), m);

    // Close the writer
    recordWriter.close(null);

    Iterator<Entry<Key,Value>> iter = conn.createScanner(table, auths).iterator();
    Assert.assertTrue("Iterator did not have an element as expected", iter.hasNext());

    Entry<Key,Value> entry = iter.next();
    Key k = entry.getKey();
    Value v = entry.getValue();

    Assert.assertEquals("row", k.getRow().toString());
    Assert.assertEquals("cf", k.getColumnFamily().toString());
    Assert.assertEquals("cq1", k.getColumnQualifier().toString());
    Assert.assertEquals("foo", k.getColumnVisibility().toString());
    Assert.assertEquals("value1", new String(v.get()));

    Assert.assertTrue("Iterator did not have an element as expected", iter.hasNext());

    entry = iter.next();
    k = entry.getKey();
    v = entry.getValue();

    Assert.assertEquals("row", k.getRow().toString());
    Assert.assertEquals("cf", k.getColumnFamily().toString());
    Assert.assertEquals("cq2", k.getColumnQualifier().toString());
    Assert.assertEquals("foo", k.getColumnVisibility().toString());
    Assert.assertEquals("value2", new String(v.get()));

    Assert.assertFalse("Iterator unexpectedly had more data", iter.hasNext());
  }

  @Test
  public void testWriteMap() throws Exception {
    Instance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));

    HiveAccumuloTableOutputFormat outputFormat = new HiveAccumuloTableOutputFormat();
    String table = test.getMethodName();
    conn.tableOperations().create(table);

    JobConf conf = new JobConf();
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, inst.getInstanceName());
    conf.set(AccumuloConnectionParameters.USER_NAME, "root");
    conf.set(AccumuloConnectionParameters.USER_PASS, "");
    conf.setBoolean(AccumuloConnectionParameters.USE_MOCK_INSTANCE, true);
    conf.set(AccumuloConnectionParameters.TABLE_NAME, test.getMethodName());

    FileSystem local = FileSystem.getLocal(conf);
    outputFormat.checkOutputSpecs(local, conf);

    RecordWriter<Text,Mutation> recordWriter = outputFormat
        .getRecordWriter(local, conf, null, null);

    List<String> names = Arrays.asList("row", "col1");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);

    Properties tableProperties = new Properties();
    tableProperties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:*");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, " ");
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(names));
    tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(',').join(types));
    AccumuloSerDeParameters accumuloSerDeParams = new AccumuloSerDeParameters(new Configuration(),
        tableProperties, AccumuloSerDe.class.getSimpleName());
    LazySerDeParameters serDeParams = accumuloSerDeParams.getSerDeParameters();

    AccumuloRowSerializer serializer = new AccumuloRowSerializer(0, serDeParams,
        accumuloSerDeParams.getColumnMappings(), AccumuloSerDeParameters.DEFAULT_VISIBILITY_LABEL,
        accumuloSerDeParams.getRowIdFactory());

    TypeInfo stringTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
    LazyStringObjectInspector stringOI = (LazyStringObjectInspector) LazyFactory
        .createLazyObjectInspector(stringTypeInfo, new byte[] {0}, 0,
            serDeParams.getNullSequence(), serDeParams.isEscaped(), serDeParams.getEscapeChar());

    LazyMapObjectInspector mapOI = LazyObjectInspectorFactory.getLazySimpleMapObjectInspector(
        stringOI, stringOI, (byte) ',', (byte) ':', serDeParams.getNullSequence(),
        serDeParams.isEscaped(), serDeParams.getEscapeChar());

    LazySimpleStructObjectInspector structOI = (LazySimpleStructObjectInspector) LazyObjectInspectorFactory
        .getLazySimpleStructObjectInspector(Arrays.asList("row", "data"),
            Arrays.asList(stringOI, mapOI), (byte) ' ', serDeParams.getNullSequence(),
            serDeParams.isLastColumnTakesRest(), serDeParams.isEscaped(),
            serDeParams.getEscapeChar());

    LazyStruct struct = (LazyStruct) LazyFactory.createLazyObject(structOI);

    ByteArrayRef bytes = new ByteArrayRef();
    bytes.setData("row cq1:value1,cq2:value2".getBytes());
    struct.init(bytes, 0, bytes.getData().length);

    // Serialize the struct into a mutation
    Mutation m = serializer.serialize(struct, structOI);

    // Write the mutation
    recordWriter.write(new Text(table), m);

    // Close the writer
    recordWriter.close(null);

    Iterator<Entry<Key,Value>> iter = conn.createScanner(table, new Authorizations()).iterator();
    Assert.assertTrue("Iterator did not have an element as expected", iter.hasNext());

    Entry<Key,Value> entry = iter.next();
    Key k = entry.getKey();
    Value v = entry.getValue();

    Assert.assertEquals("row", k.getRow().toString());
    Assert.assertEquals("cf", k.getColumnFamily().toString());
    Assert.assertEquals("cq1", k.getColumnQualifier().toString());
    Assert.assertEquals(AccumuloSerDeParameters.DEFAULT_VISIBILITY_LABEL,
        k.getColumnVisibilityParsed());
    Assert.assertEquals("value1", new String(v.get()));

    Assert.assertTrue("Iterator did not have an element as expected", iter.hasNext());

    entry = iter.next();
    k = entry.getKey();
    v = entry.getValue();

    Assert.assertEquals("row", k.getRow().toString());
    Assert.assertEquals("cf", k.getColumnFamily().toString());
    Assert.assertEquals("cq2", k.getColumnQualifier().toString());
    Assert.assertEquals(AccumuloSerDeParameters.DEFAULT_VISIBILITY_LABEL,
        k.getColumnVisibilityParsed());
    Assert.assertEquals("value2", new String(v.get()));

    Assert.assertFalse("Iterator unexpectedly had more data", iter.hasNext());
  }

  @Test
  public void testBinarySerializationOnStringFallsBackToUtf8() throws Exception {
    Instance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));

    HiveAccumuloTableOutputFormat outputFormat = new HiveAccumuloTableOutputFormat();
    String table = test.getMethodName();
    conn.tableOperations().create(table);

    JobConf conf = new JobConf();
    conf.set(AccumuloConnectionParameters.INSTANCE_NAME, inst.getInstanceName());
    conf.set(AccumuloConnectionParameters.USER_NAME, "root");
    conf.set(AccumuloConnectionParameters.USER_PASS, "");
    conf.setBoolean(AccumuloConnectionParameters.USE_MOCK_INSTANCE, true);
    conf.set(AccumuloConnectionParameters.TABLE_NAME, test.getMethodName());

    FileSystem local = FileSystem.getLocal(conf);
    outputFormat.checkOutputSpecs(local, conf);

    RecordWriter<Text,Mutation> recordWriter = outputFormat
        .getRecordWriter(local, conf, null, null);

    List<String> names = Arrays.asList("row", "col1", "col2");
    List<TypeInfo> types = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);

    Properties tableProperties = new Properties();
    tableProperties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowID,cf:cq1,cf:cq2");
    tableProperties.setProperty(serdeConstants.FIELD_DELIM, " ");
    tableProperties.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(',').join(names));
    tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(',').join(types));
    tableProperties.setProperty(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE, ColumnEncoding.BINARY.getName());
    AccumuloSerDeParameters accumuloSerDeParams = new AccumuloSerDeParameters(new Configuration(),
        tableProperties, AccumuloSerDe.class.getSimpleName());
    LazySerDeParameters serDeParams = accumuloSerDeParams.getSerDeParameters();

    AccumuloRowSerializer serializer = new AccumuloRowSerializer(0, serDeParams,
        accumuloSerDeParams.getColumnMappings(), AccumuloSerDeParameters.DEFAULT_VISIBILITY_LABEL,
        accumuloSerDeParams.getRowIdFactory());

    TypeInfo stringTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);

    LazySimpleStructObjectInspector structOI = (LazySimpleStructObjectInspector) LazyFactory
        .createLazyStructInspector(Arrays.asList("row", "cq1", "cq2"),
            Arrays.asList(stringTypeInfo, stringTypeInfo, stringTypeInfo),
            serDeParams.getSeparators(), serDeParams.getNullSequence(),
            serDeParams.isLastColumnTakesRest(), serDeParams.isEscaped(),
            serDeParams.getEscapeChar());

    LazyStruct struct = (LazyStruct) LazyFactory.createLazyObject(structOI);

    ByteArrayRef bytes = new ByteArrayRef();
    bytes.setData("row value1 value2".getBytes());
    struct.init(bytes, 0, bytes.getData().length);

    // Serialize the struct into a mutation
    Mutation m = serializer.serialize(struct, structOI);

    // Write the mutation
    recordWriter.write(new Text(table), m);

    // Close the writer
    recordWriter.close(null);

    Iterator<Entry<Key,Value>> iter = conn.createScanner(table, new Authorizations()).iterator();
    Assert.assertTrue("Iterator did not have an element as expected", iter.hasNext());

    Entry<Key,Value> entry = iter.next();
    Key k = entry.getKey();
    Value v = entry.getValue();

    Assert.assertEquals("row", k.getRow().toString());
    Assert.assertEquals("cf", k.getColumnFamily().toString());
    Assert.assertEquals("cq1", k.getColumnQualifier().toString());
    Assert.assertEquals("", k.getColumnVisibility().toString());
    Assert.assertEquals("value1", new String(v.get()));

    Assert.assertTrue("Iterator did not have an element as expected", iter.hasNext());

    entry = iter.next();
    k = entry.getKey();
    v = entry.getValue();

    Assert.assertEquals("row", k.getRow().toString());
    Assert.assertEquals("cf", k.getColumnFamily().toString());
    Assert.assertEquals("cq2", k.getColumnQualifier().toString());
    Assert.assertEquals("", k.getColumnVisibility().toString());
    Assert.assertEquals("value2", new String(v.get()));

    Assert.assertFalse("Iterator unexpectedly had more data", iter.hasNext());
  }
}
