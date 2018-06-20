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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.LocalDateTime;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveChar;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveDecimal;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveVarchar;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDateObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyShortObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 *
 */
public class TestHiveAccumuloTypes {

  @Rule
  public TestName test = new TestName();

  @Test
  public void testBinaryTypes() throws Exception {
    final String tableName = test.getMethodName(), user = "root", pass = "";

    MockInstance mockInstance = new MockInstance(test.getMethodName());
    Connector conn = mockInstance.getConnector(user, new PasswordToken(pass));
    HiveAccumuloTableInputFormat inputformat = new HiveAccumuloTableInputFormat();
    JobConf conf = new JobConf();

    conf.set(AccumuloSerDeParameters.TABLE_NAME, tableName);
    conf.set(AccumuloSerDeParameters.USE_MOCK_INSTANCE, "true");
    conf.set(AccumuloSerDeParameters.INSTANCE_NAME, test.getMethodName());
    conf.set(AccumuloSerDeParameters.USER_NAME, user);
    conf.set(AccumuloSerDeParameters.USER_PASS, pass);
    conf.set(AccumuloSerDeParameters.ZOOKEEPERS, "localhost:2181"); // not used for mock, but
                                                                    // required by input format.

    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, AccumuloHiveConstants.ROWID
        + ",cf:string,cf:boolean,cf:tinyint,cf:smallint,cf:int,cf:bigint"
        + ",cf:float,cf:double,cf:decimal,cf:date,cf:timestamp,cf:char,cf:varchar");
    conf.set(
        serdeConstants.LIST_COLUMNS,
        "string,string,boolean,tinyint,smallint,int,bigint,float,double,decimal,date,timestamp,char(4),varchar(7)");
    conf.set(
        serdeConstants.LIST_COLUMN_TYPES,
        "string,string,boolean,tinyint,smallint,int,bigint,float,double,decimal,date,timestamp,char(4),varchar(7)");
    conf.set(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE, "binary");

    conn.tableOperations().create(tableName);
    BatchWriterConfig writerConf = new BatchWriterConfig();
    BatchWriter writer = conn.createBatchWriter(tableName, writerConf);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    String cf = "cf";
    byte[] cfBytes = cf.getBytes();

    Mutation m = new Mutation("row1");

    // string
    String stringValue = "string";
    JavaStringObjectInspector stringOI = (JavaStringObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, stringOI.create(stringValue), stringOI, false, (byte) 0,
        null);
    m.put(cfBytes, "string".getBytes(), baos.toByteArray());

    // boolean
    boolean booleanValue = true;
    baos.reset();
    JavaBooleanObjectInspector booleanOI = (JavaBooleanObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME));
    LazyUtils.writePrimitive(baos, booleanOI.create(booleanValue), booleanOI);
    m.put(cfBytes, "boolean".getBytes(), baos.toByteArray());

    // tinyint
    byte tinyintValue = -127;
    baos.reset();
    JavaByteObjectInspector byteOI = (JavaByteObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME));
    LazyUtils.writePrimitive(baos, tinyintValue, byteOI);
    m.put(cfBytes, "tinyint".getBytes(), baos.toByteArray());

    // smallint
    short smallintValue = Short.MAX_VALUE;
    baos.reset();
    JavaShortObjectInspector shortOI = (JavaShortObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME));
    LazyUtils.writePrimitive(baos, smallintValue, shortOI);
    m.put(cfBytes, "smallint".getBytes(), baos.toByteArray());

    // int
    int intValue = Integer.MAX_VALUE;
    baos.reset();
    JavaIntObjectInspector intOI = (JavaIntObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME));
    LazyUtils.writePrimitive(baos, intValue, intOI);
    m.put(cfBytes, "int".getBytes(), baos.toByteArray());

    // bigint
    long bigintValue = Long.MAX_VALUE;
    baos.reset();
    JavaLongObjectInspector longOI = (JavaLongObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME));
    LazyUtils.writePrimitive(baos, bigintValue, longOI);
    m.put(cfBytes, "bigint".getBytes(), baos.toByteArray());

    // float
    float floatValue = Float.MAX_VALUE;
    baos.reset();
    JavaFloatObjectInspector floatOI = (JavaFloatObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME));
    LazyUtils.writePrimitive(baos, floatValue, floatOI);
    m.put(cfBytes, "float".getBytes(), baos.toByteArray());

    // double
    double doubleValue = Double.MAX_VALUE;
    baos.reset();
    JavaDoubleObjectInspector doubleOI = (JavaDoubleObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME));
    LazyUtils.writePrimitive(baos, doubleValue, doubleOI);
    m.put(cfBytes, "double".getBytes(), baos.toByteArray());

    // decimal
    baos.reset();
    HiveDecimal decimalValue = HiveDecimal.create(65536l);
    HiveDecimalWritable decimalWritable = new HiveDecimalWritable(decimalValue);
    decimalWritable.write(out);
    m.put(cfBytes, "decimal".getBytes(), baos.toByteArray());

    // date
    baos.reset();
    Date now = Date.ofEpochMilli(System.currentTimeMillis());
    DateWritableV2 dateWritable = new DateWritableV2(now);
    Date dateValue = dateWritable.get();
    dateWritable.write(out);
    m.put(cfBytes, "date".getBytes(), baos.toByteArray());

    // tiemestamp
    baos.reset();
    Timestamp timestampValue = Timestamp.valueOf(LocalDateTime.now().toString());
    ByteStream.Output output = new ByteStream.Output();
    TimestampWritableV2 timestampWritable = new TimestampWritableV2(Timestamp.valueOf(LocalDateTime.now().toString()));
    timestampWritable.write(new DataOutputStream(output));
    output.close();
    m.put(cfBytes, "timestamp".getBytes(), output.toByteArray());

    // char
    baos.reset();
    HiveChar charValue = new HiveChar("char", 4);
    JavaHiveCharObjectInspector charOI = (JavaHiveCharObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(new CharTypeInfo(4));
    LazyUtils.writePrimitiveUTF8(baos, charOI.create(charValue), charOI, false, (byte) 0, null);
    m.put(cfBytes, "char".getBytes(), baos.toByteArray());

    baos.reset();
    HiveVarchar varcharValue = new HiveVarchar("varchar", 7);
    JavaHiveVarcharObjectInspector varcharOI = (JavaHiveVarcharObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(new VarcharTypeInfo(7));
    LazyUtils.writePrimitiveUTF8(baos, varcharOI.create(varcharValue), varcharOI, false, (byte) 0,
        null);
    m.put(cfBytes, "varchar".getBytes(), baos.toByteArray());

    writer.addMutation(m);

    writer.close();

    for (Entry<Key,Value> e : conn.createScanner(tableName, new Authorizations())) {
      System.out.println(e);
    }

    // Create the RecordReader
    FileInputFormat.addInputPath(conf, new Path("unused"));
    InputSplit[] splits = inputformat.getSplits(conf, 0);
    assertEquals(splits.length, 1);
    RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);

    Text key = reader.createKey();
    AccumuloHiveRow value = reader.createValue();

    reader.next(key, value);

    Assert.assertEquals(13, value.getTuples().size());

    ByteArrayRef byteRef = new ByteArrayRef();

    // string
    Text cfText = new Text(cf), cqHolder = new Text();
    cqHolder.set("string");
    byte[] valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyStringObjectInspector lazyStringOI = LazyPrimitiveObjectInspectorFactory
        .getLazyStringObjectInspector(false, (byte) 0);
    LazyString lazyString = (LazyString) LazyFactory.createLazyObject(lazyStringOI);
    lazyString.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(stringValue, lazyString.getWritableObject().toString());

    // boolean
    cqHolder.set("boolean");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyBooleanObjectInspector lazyBooleanOI = (LazyBooleanObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME));
    LazyBoolean lazyBoolean = (LazyBoolean) LazyFactory
        .createLazyPrimitiveBinaryClass(lazyBooleanOI);
    lazyBoolean.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(booleanValue, lazyBoolean.getWritableObject().get());

    // tinyint
    cqHolder.set("tinyint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyByteObjectInspector lazyByteOI = (LazyByteObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME));
    LazyByte lazyByte = (LazyByte) LazyFactory.createLazyPrimitiveBinaryClass(lazyByteOI);
    lazyByte.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(tinyintValue, lazyByte.getWritableObject().get());

    // smallint
    cqHolder.set("smallint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyShortObjectInspector lazyShortOI = (LazyShortObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME));
    LazyShort lazyShort = (LazyShort) LazyFactory.createLazyPrimitiveBinaryClass(lazyShortOI);
    lazyShort.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(smallintValue, lazyShort.getWritableObject().get());

    // int
    cqHolder.set("int");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyIntObjectInspector lazyIntOI = (LazyIntObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME));
    LazyInteger lazyInt = (LazyInteger) LazyFactory.createLazyPrimitiveBinaryClass(lazyIntOI);
    lazyInt.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(intValue, lazyInt.getWritableObject().get());

    // bigint
    cqHolder.set("bigint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyLongObjectInspector lazyLongOI = (LazyLongObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME));
    LazyLong lazyLong = (LazyLong) LazyFactory.createLazyPrimitiveBinaryClass(lazyLongOI);
    lazyLong.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(bigintValue, lazyLong.getWritableObject().get());

    // float
    cqHolder.set("float");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyFloatObjectInspector lazyFloatOI = (LazyFloatObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME));
    LazyFloat lazyFloat = (LazyFloat) LazyFactory.createLazyPrimitiveBinaryClass(lazyFloatOI);
    lazyFloat.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(floatValue, lazyFloat.getWritableObject().get(), 0);

    // double
    cqHolder.set("double");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyDoubleObjectInspector lazyDoubleOI = (LazyDoubleObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME));
    LazyDouble lazyDouble = (LazyDouble) LazyFactory.createLazyPrimitiveBinaryClass(lazyDoubleOI);
    lazyDouble.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(doubleValue, lazyDouble.getWritableObject().get(), 0);

    // decimal
    cqHolder.set("decimal");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    ByteArrayInputStream bais = new ByteArrayInputStream(valueBytes);
    DataInputStream in = new DataInputStream(bais);
    decimalWritable.readFields(in);

    Assert.assertEquals(decimalValue, decimalWritable.getHiveDecimal());

    // date
    cqHolder.set("date");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);
    dateWritable.readFields(in);

    Assert.assertEquals(dateValue, dateWritable.get());

    // timestamp
    cqHolder.set("timestamp");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    bais = new ByteArrayInputStream(valueBytes);
    in = new DataInputStream(bais);
    timestampWritable.readFields(in);

    Assert.assertEquals(timestampValue, timestampWritable.getTimestamp());

    // char
    cqHolder.set("char");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyHiveCharObjectInspector lazyCharOI = (LazyHiveCharObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(new CharTypeInfo(4));
    LazyHiveChar lazyChar = (LazyHiveChar) LazyFactory.createLazyObject(lazyCharOI);
    lazyChar.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(charValue, lazyChar.getWritableObject().getHiveChar());

    // varchar
    cqHolder.set("varchar");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyHiveVarcharObjectInspector lazyVarcharOI = (LazyHiveVarcharObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(new VarcharTypeInfo(7));
    LazyHiveVarchar lazyVarchar = (LazyHiveVarchar) LazyFactory.createLazyObject(lazyVarcharOI);
    lazyVarchar.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(varcharValue.toString(), lazyVarchar.getWritableObject().getHiveVarchar()
        .toString());
  }

  @Test
  public void testUtf8Types() throws Exception {
    final String tableName = test.getMethodName(), user = "root", pass = "";

    MockInstance mockInstance = new MockInstance(test.getMethodName());
    Connector conn = mockInstance.getConnector(user, new PasswordToken(pass));
    HiveAccumuloTableInputFormat inputformat = new HiveAccumuloTableInputFormat();
    JobConf conf = new JobConf();

    conf.set(AccumuloSerDeParameters.TABLE_NAME, tableName);
    conf.set(AccumuloSerDeParameters.USE_MOCK_INSTANCE, "true");
    conf.set(AccumuloSerDeParameters.INSTANCE_NAME, test.getMethodName());
    conf.set(AccumuloSerDeParameters.USER_NAME, user);
    conf.set(AccumuloSerDeParameters.USER_PASS, pass);
    conf.set(AccumuloSerDeParameters.ZOOKEEPERS, "localhost:2181"); // not used for mock, but
                                                                    // required by input format.

    conf.set(AccumuloSerDeParameters.COLUMN_MAPPINGS, AccumuloHiveConstants.ROWID
        + ",cf:string,cf:boolean,cf:tinyint,cf:smallint,cf:int,cf:bigint"
        + ",cf:float,cf:double,cf:decimal,cf:date,cf:timestamp,cf:char,cf:varchar");
    conf.set(
        serdeConstants.LIST_COLUMNS,
        "string,string,boolean,tinyint,smallint,int,bigint,float,double,decimal,date,timestamp,char(4),varchar(7)");
    conf.set(
        serdeConstants.LIST_COLUMN_TYPES,
        "string,string,boolean,tinyint,smallint,int,bigint,float,double,decimal,date,timestamp,char(4),varchar(7)");

    conn.tableOperations().create(tableName);
    BatchWriterConfig writerConf = new BatchWriterConfig();
    BatchWriter writer = conn.createBatchWriter(tableName, writerConf);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    String cf = "cf";
    byte[] cfBytes = cf.getBytes();
    ByteArrayRef byteRef = new ByteArrayRef();

    Mutation m = new Mutation("row1");

    // string
    String stringValue = "string";
    baos.reset();
    JavaStringObjectInspector stringOI = (JavaStringObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, stringOI.create(stringValue), stringOI, false, (byte) 0,
        null);
    m.put(cfBytes, "string".getBytes(), baos.toByteArray());

    // boolean
    boolean booleanValue = true;
    baos.reset();
    JavaBooleanObjectInspector booleanOI = (JavaBooleanObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, booleanOI.create(booleanValue), booleanOI, false, (byte) 0,
        null);
    m.put(cfBytes, "boolean".getBytes(), baos.toByteArray());

    // tinyint
    byte tinyintValue = -127;
    baos.reset();
    JavaByteObjectInspector byteOI = (JavaByteObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, tinyintValue, byteOI, false, (byte) 0, null);
    m.put(cfBytes, "tinyint".getBytes(), baos.toByteArray());

    // smallint
    short smallintValue = Short.MAX_VALUE;
    baos.reset();
    JavaShortObjectInspector shortOI = (JavaShortObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, smallintValue, shortOI, false, (byte) 0, null);
    m.put(cfBytes, "smallint".getBytes(), baos.toByteArray());

    // int
    int intValue = Integer.MAX_VALUE;
    baos.reset();
    JavaIntObjectInspector intOI = (JavaIntObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, intValue, intOI, false, (byte) 0, null);
    m.put(cfBytes, "int".getBytes(), baos.toByteArray());

    // bigint
    long bigintValue = Long.MAX_VALUE;
    baos.reset();
    JavaLongObjectInspector longOI = (JavaLongObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, bigintValue, longOI, false, (byte) 0, null);
    m.put(cfBytes, "bigint".getBytes(), baos.toByteArray());

    // float
    float floatValue = Float.MAX_VALUE;
    baos.reset();
    JavaFloatObjectInspector floatOI = (JavaFloatObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, floatValue, floatOI, false, (byte) 0, null);
    m.put(cfBytes, "float".getBytes(), baos.toByteArray());

    // double
    double doubleValue = Double.MAX_VALUE;
    baos.reset();
    JavaDoubleObjectInspector doubleOI = (JavaDoubleObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, doubleValue, doubleOI, false, (byte) 0, null);
    m.put(cfBytes, "double".getBytes(), baos.toByteArray());

    // decimal
    HiveDecimal decimalValue = HiveDecimal.create("1.23");
    baos.reset();
    JavaHiveDecimalObjectInspector decimalOI = (JavaHiveDecimalObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(new DecimalTypeInfo(5, 2));
    LazyUtils.writePrimitiveUTF8(baos, decimalOI.create(decimalValue), decimalOI, false, (byte) 0,
        null);
    m.put(cfBytes, "decimal".getBytes(), baos.toByteArray());

    // date
    Date now = Date.ofEpochMilli(System.currentTimeMillis());
    DateWritableV2 dateWritable = new DateWritableV2(now);
    Date dateValue = dateWritable.get();
    baos.reset();
    JavaDateObjectInspector dateOI = (JavaDateObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, dateOI.create(dateValue), dateOI, false, (byte) 0, null);
    m.put(cfBytes, "date".getBytes(), baos.toByteArray());

    // timestamp
    Timestamp timestampValue = Timestamp.valueOf(LocalDateTime.now().toString());
    baos.reset();
    JavaTimestampObjectInspector timestampOI = (JavaTimestampObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.TIMESTAMP_TYPE_NAME));
    LazyUtils.writePrimitiveUTF8(baos, timestampOI.create(timestampValue), timestampOI, false,
        (byte) 0, null);
    m.put(cfBytes, "timestamp".getBytes(), baos.toByteArray());

    // char
    baos.reset();
    HiveChar charValue = new HiveChar("char", 4);
    JavaHiveCharObjectInspector charOI = (JavaHiveCharObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(new CharTypeInfo(4));
    LazyUtils.writePrimitiveUTF8(baos, charOI.create(charValue), charOI, false, (byte) 0, null);
    m.put(cfBytes, "char".getBytes(), baos.toByteArray());

    // varchar
    baos.reset();
    HiveVarchar varcharValue = new HiveVarchar("varchar", 7);
    JavaHiveVarcharObjectInspector varcharOI = (JavaHiveVarcharObjectInspector) PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(new VarcharTypeInfo(7));
    LazyUtils.writePrimitiveUTF8(baos, varcharOI.create(varcharValue), varcharOI, false, (byte) 0,
        null);
    m.put(cfBytes, "varchar".getBytes(), baos.toByteArray());

    writer.addMutation(m);

    writer.close();

    for (Entry<Key,Value> e : conn.createScanner(tableName, new Authorizations())) {
      System.out.println(e);
    }

    // Create the RecordReader
    FileInputFormat.addInputPath(conf, new Path("unused"));
    InputSplit[] splits = inputformat.getSplits(conf, 0);
    assertEquals(splits.length, 1);
    RecordReader<Text,AccumuloHiveRow> reader = inputformat.getRecordReader(splits[0], conf, null);

    Text key = reader.createKey();
    AccumuloHiveRow value = reader.createValue();

    reader.next(key, value);

    Assert.assertEquals(13, value.getTuples().size());

    // string
    Text cfText = new Text(cf), cqHolder = new Text();
    cqHolder.set("string");
    byte[] valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyStringObjectInspector lazyStringOI = LazyPrimitiveObjectInspectorFactory
        .getLazyStringObjectInspector(false, (byte) 0);
    LazyString lazyString = (LazyString) LazyFactory.createLazyObject(lazyStringOI);
    lazyString.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(new Text(stringValue), lazyString.getWritableObject());

    // boolean
    cqHolder.set("boolean");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyBooleanObjectInspector lazyBooleanOI = (LazyBooleanObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME));
    LazyBoolean lazyBoolean = (LazyBoolean) LazyFactory.createLazyObject(lazyBooleanOI);
    lazyBoolean.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(booleanValue, lazyBoolean.getWritableObject().get());

    // tinyint
    cqHolder.set("tinyint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyByteObjectInspector lazyByteOI = (LazyByteObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME));
    LazyByte lazyByte = (LazyByte) LazyFactory.createLazyObject(lazyByteOI);
    lazyByte.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(tinyintValue, lazyByte.getWritableObject().get());

    // smallint
    cqHolder.set("smallint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyShortObjectInspector lazyShortOI = (LazyShortObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME));
    LazyShort lazyShort = (LazyShort) LazyFactory.createLazyObject(lazyShortOI);
    lazyShort.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(smallintValue, lazyShort.getWritableObject().get());

    // int
    cqHolder.set("int");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyIntObjectInspector lazyIntOI = (LazyIntObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME));
    LazyInteger lazyInt = (LazyInteger) LazyFactory.createLazyObject(lazyIntOI);
    lazyInt.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(intValue, lazyInt.getWritableObject().get());

    // bigint
    cqHolder.set("bigint");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyLongObjectInspector lazyLongOI = (LazyLongObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME));
    LazyLong lazyLong = (LazyLong) LazyFactory.createLazyObject(lazyLongOI);
    lazyLong.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(bigintValue, lazyLong.getWritableObject().get());

    // float
    cqHolder.set("float");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyFloatObjectInspector lazyFloatOI = (LazyFloatObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME));
    LazyFloat lazyFloat = (LazyFloat) LazyFactory.createLazyObject(lazyFloatOI);
    lazyFloat.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(floatValue, lazyFloat.getWritableObject().get(), 0);

    // double
    cqHolder.set("double");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyDoubleObjectInspector lazyDoubleOI = (LazyDoubleObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME));
    LazyDouble lazyDouble = (LazyDouble) LazyFactory.createLazyObject(lazyDoubleOI);
    lazyDouble.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(doubleValue, lazyDouble.getWritableObject().get(), 0);

    // decimal
    cqHolder.set("decimal");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyHiveDecimalObjectInspector lazyDecimalOI = (LazyHiveDecimalObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(new DecimalTypeInfo(5, 2));
    LazyHiveDecimal lazyDecimal = (LazyHiveDecimal) LazyFactory.createLazyObject(lazyDecimalOI);
    lazyDecimal.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(decimalValue, lazyDecimal.getWritableObject().getHiveDecimal());

    // date
    cqHolder.set("date");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyDateObjectInspector lazyDateOI = (LazyDateObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME));
    LazyDate lazyDate = (LazyDate) LazyFactory.createLazyObject(lazyDateOI);
    lazyDate.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(dateValue, lazyDate.getWritableObject().get());

    // timestamp
    cqHolder.set("timestamp");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyTimestampObjectInspector lazyTimestampOI = (LazyTimestampObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(serdeConstants.TIMESTAMP_TYPE_NAME));
    LazyTimestamp lazyTimestamp = (LazyTimestamp) LazyFactory.createLazyObject(lazyTimestampOI);
    lazyTimestamp.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(timestampValue, lazyTimestamp.getWritableObject().getTimestamp());

    // char
    cqHolder.set("char");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyHiveCharObjectInspector lazyCharOI = (LazyHiveCharObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(new CharTypeInfo(4));
    LazyHiveChar lazyChar = (LazyHiveChar) LazyFactory.createLazyObject(lazyCharOI);
    lazyChar.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(charValue, lazyChar.getWritableObject().getHiveChar());

    // varchar
    cqHolder.set("varchar");
    valueBytes = value.getValue(cfText, cqHolder);
    Assert.assertNotNull(valueBytes);

    byteRef.setData(valueBytes);
    LazyHiveVarcharObjectInspector lazyVarcharOI = (LazyHiveVarcharObjectInspector) LazyPrimitiveObjectInspectorFactory
        .getLazyObjectInspector(new VarcharTypeInfo(7));
    LazyHiveVarchar lazyVarchar = (LazyHiveVarchar) LazyFactory.createLazyObject(lazyVarcharOI);
    lazyVarchar.init(byteRef, 0, valueBytes.length);

    Assert.assertEquals(varcharValue.toString(), lazyVarchar.getWritableObject().getHiveVarchar()
        .toString());
  }
}
