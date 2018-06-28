/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.arrow;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestArrowColumnarBatchSerDe {
  private Configuration conf;

  private final static Object[][] INTEGER_ROWS = {
      {byteW(0), shortW(0), intW(0), longW(0)},
      {byteW(1), shortW(1), intW(1), longW(1)},
      {byteW(-1), shortW(-1), intW(-1), longW(-1)},
      {byteW(Byte.MIN_VALUE), shortW(Short.MIN_VALUE), intW(Integer.MIN_VALUE),
          longW(Long.MIN_VALUE)},
      {byteW(Byte.MAX_VALUE), shortW(Short.MAX_VALUE), intW(Integer.MAX_VALUE),
          longW(Long.MAX_VALUE)},
      {null, null, null, null},
  };

  private final static Object[][] FLOAT_ROWS = {
      {floatW(0f), doubleW(0d)},
      {floatW(1f), doubleW(1d)},
      {floatW(-1f), doubleW(-1d)},
      {floatW(Float.MIN_VALUE), doubleW(Double.MIN_VALUE)},
      {floatW(-Float.MIN_VALUE), doubleW(-Double.MIN_VALUE)},
      {floatW(Float.MAX_VALUE), doubleW(Double.MAX_VALUE)},
      {floatW(-Float.MAX_VALUE), doubleW(-Double.MAX_VALUE)},
      {floatW(Float.POSITIVE_INFINITY), doubleW(Double.POSITIVE_INFINITY)},
      {floatW(Float.NEGATIVE_INFINITY), doubleW(Double.NEGATIVE_INFINITY)},
      {null, null},
  };

  private final static Object[][] STRING_ROWS = {
      {text(""), charW("", 10), varcharW("", 10)},
      {text("Hello"), charW("Hello", 10), varcharW("Hello", 10)},
      {text("world!"), charW("world!", 10), varcharW("world!", 10)},
      {null, null, null},
  };

  private final static long TIME_IN_MILLIS = TimeUnit.DAYS.toMillis(365 + 31 + 3);
  private final static long NEGATIVE_TIME_IN_MILLIS = TimeUnit.DAYS.toMillis(-9 * 365 + 31 + 3);
  private final static Timestamp TIMESTAMP;
  private final static Timestamp NEGATIVE_TIMESTAMP_WITHOUT_NANOS;

  static {
    TIMESTAMP = Timestamp.ofEpochMilli(TIME_IN_MILLIS);
    NEGATIVE_TIMESTAMP_WITHOUT_NANOS = Timestamp.ofEpochMilli(NEGATIVE_TIME_IN_MILLIS);
  }

  private final static Object[][] DTI_ROWS = {
      {
          new DateWritableV2(DateWritableV2.millisToDays(TIME_IN_MILLIS)),
          new TimestampWritableV2(TIMESTAMP),
          new HiveIntervalYearMonthWritable(new HiveIntervalYearMonth(1, 2)),
          new HiveIntervalDayTimeWritable(new HiveIntervalDayTime(1, 2, 3, 4, 5_000_000))
      },
      {
          new DateWritableV2(DateWritableV2.millisToDays(NEGATIVE_TIME_IN_MILLIS)),
          new TimestampWritableV2(NEGATIVE_TIMESTAMP_WITHOUT_NANOS),
          null,
          null
      },
      {null, null, null, null},
  };

  private final static Object[][] DECIMAL_ROWS = {
      {decimalW(HiveDecimal.ZERO)},
      {decimalW(HiveDecimal.ONE)},
      {decimalW(HiveDecimal.ONE.negate())},
      {decimalW(HiveDecimal.create("0.000001"))},
      {decimalW(HiveDecimal.create("100000"))},
      {null},
  };

  private final static Object[][] BOOLEAN_ROWS = {
      {new BooleanWritable(true)},
      {new BooleanWritable(false)},
      {null},
  };

  private final static Object[][] BINARY_ROWS = {
      {new BytesWritable("".getBytes())},
      {new BytesWritable("Hello".getBytes())},
      {new BytesWritable("world!".getBytes())},
      {null},
  };

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  private static ByteWritable byteW(int value) {
    return new ByteWritable((byte) value);
  }

  private static ShortWritable shortW(int value) {
    return new ShortWritable((short) value);
  }

  private static IntWritable intW(int value) {
    return new IntWritable(value);
  }

  private static LongWritable longW(long value) {
    return new LongWritable(value);
  }

  private static FloatWritable floatW(float value) {
    return new FloatWritable(value);
  }

  private static DoubleWritable doubleW(double value) {
    return new DoubleWritable(value);
  }

  private static Text text(String value) {
    return new Text(value);
  }

  private static HiveCharWritable charW(String value, int length) {
    return new HiveCharWritable(new HiveChar(value, length));
  }

  private static HiveVarcharWritable varcharW(String value, int length) {
    return new HiveVarcharWritable(new HiveVarchar(value, length));
  }

  private static HiveDecimalWritable decimalW(HiveDecimal value) {
    return new HiveDecimalWritable(value);
  }

  private void initAndSerializeAndDeserialize(String[][] schema, Object[][] rows) throws SerDeException {
    ArrowColumnarBatchSerDe serDe = new ArrowColumnarBatchSerDe();
    StructObjectInspector rowOI = initSerDe(serDe, schema);
    serializeAndDeserialize(serDe, rows, rowOI);
  }

  private StructObjectInspector initSerDe(AbstractSerDe serDe, String[][] schema)
      throws SerDeException {
    List<String> fieldNameList = newArrayList();
    List<String> fieldTypeList = newArrayList();
    List<TypeInfo> typeInfoList = newArrayList();

    for (String[] nameAndType : schema) {
      String name = nameAndType[0];
      String type = nameAndType[1];
      fieldNameList.add(name);
      fieldTypeList.add(type);
      typeInfoList.add(TypeInfoUtils.getTypeInfoFromTypeString(type));
    }

    String fieldNames = Joiner.on(',').join(fieldNameList);
    String fieldTypes = Joiner.on(',').join(fieldTypeList);

    Properties schemaProperties = new Properties();
    schemaProperties.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
    schemaProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);
    SerDeUtils.initializeSerDe(serDe, conf, schemaProperties, null);
    return (StructObjectInspector) TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
        TypeInfoFactory.getStructTypeInfo(fieldNameList, typeInfoList));
  }

  private void serializeAndDeserialize(ArrowColumnarBatchSerDe serDe, Object[][] rows,
      StructObjectInspector rowOI) {
    ArrowWrapperWritable serialized = null;
    for (Object[] row : rows) {
      serialized = serDe.serialize(row, rowOI);
    }
    // Pass null to complete a batch
    if (serialized == null) {
      serialized = serDe.serialize(null, rowOI);
    }
    String s = serialized.getVectorSchemaRoot().contentToTSVString();
    final Object[][] deserializedRows = (Object[][]) serDe.deserialize(serialized);

    for (int rowIndex = 0; rowIndex < Math.min(deserializedRows.length, rows.length); rowIndex++) {
      final Object[] row = rows[rowIndex];
      final Object[] deserializedRow = deserializedRows[rowIndex];
      assertEquals(row.length, deserializedRow.length);

      final List<? extends StructField> fields = rowOI.getAllStructFieldRefs();
      for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
        final StructField field = fields.get(fieldIndex);
        final ObjectInspector fieldObjInspector = field.getFieldObjectInspector();
        switch (fieldObjInspector.getCategory()) {
          case PRIMITIVE:
            final PrimitiveObjectInspector primitiveObjInspector =
                (PrimitiveObjectInspector) fieldObjInspector;
            switch (primitiveObjInspector.getPrimitiveCategory()) {
              case STRING:
              case VARCHAR:
              case CHAR:
                assertEquals(Objects.toString(row[fieldIndex]),
                    Objects.toString(deserializedRow[fieldIndex]));
                break;
              default:
                assertEquals(row[fieldIndex], deserializedRow[fieldIndex]);
                break;
            }
            break;
          case STRUCT:
            final Object[] rowStruct = (Object[]) row[fieldIndex];
            final List deserializedRowStruct = (List) deserializedRow[fieldIndex];
            if (rowStruct == null) {
              assertNull(deserializedRowStruct);
            } else {
              assertArrayEquals(rowStruct, deserializedRowStruct.toArray());
            }
            break;
          case LIST:
          case UNION:
            assertEquals(row[fieldIndex], deserializedRow[fieldIndex]);
            break;
          case MAP:
            final Map rowMap = (Map) row[fieldIndex];
            final Map deserializedRowMap = (Map) deserializedRow[fieldIndex];
            if (rowMap == null) {
              assertNull(deserializedRowMap);
            } else {
              final Set rowMapKeySet = rowMap.keySet();
              final Set deserializedRowMapKeySet = deserializedRowMap.keySet();
              assertEquals(rowMapKeySet, deserializedRowMapKeySet);
              for (Object key : rowMapKeySet) {
                assertEquals(rowMap.get(key), deserializedRowMap.get(key));
              }
            }
            break;
        }
      }
    }
  }

  @Test
  public void testComprehensive() throws SerDeException {
    String[][] schema = {
        {"datatypes.c1", "int"},
        {"datatypes.c2", "boolean"},
        {"datatypes.c3", "double"},
        {"datatypes.c4", "string"},
        {"datatypes.c5", "array<int>"},
        {"datatypes.c6", "map<int,string>"},
        {"datatypes.c7", "map<string,string>"},
        {"datatypes.c8", "struct<r:string,s:int,t:double>"},
        {"datatypes.c9", "tinyint"},
        {"datatypes.c10", "smallint"},
        {"datatypes.c11", "float"},
        {"datatypes.c12", "bigint"},
        {"datatypes.c13", "array<array<string>>"},
        {"datatypes.c14", "map<int,map<int,int>>"},
        {"datatypes.c15", "struct<r:int,s:struct<a:int,b:string>>"},
        {"datatypes.c16", "array<struct<m:map<string,string>,n:int>>"},
        {"datatypes.c17", "timestamp"},
        {"datatypes.c18", "decimal(16,7)"},
        {"datatypes.c19", "binary"},
        {"datatypes.c20", "date"},
        {"datatypes.c21", "varchar(20)"},
        {"datatypes.c22", "char(15)"},
        {"datatypes.c23", "binary"},
    };

    Object[][] comprehensiveRows = {
        {
          intW(0), // c1:int
            new BooleanWritable(false), // c2:boolean
            doubleW(0), // c3:double
            text("Hello"), // c4:string
            newArrayList(intW(0), intW(1), intW(2)), // c5:array<int>
            Maps.toMap(
                newArrayList(intW(0), intW(1), intW(2)),
                input -> text("Number " + input)), // c6:map<int,string>
            Maps.toMap(
                newArrayList(text("apple"), text("banana"), text("carrot")),
                input -> text(input.toString().toUpperCase())), // c7:map<string,string>
            new Object[] {text("0"), intW(1), doubleW(2)}, // c8:struct<r:string,s:int,t:double>
            byteW(0), // c9:tinyint
            shortW(0), // c10:smallint
            floatW(0), // c11:float
            longW(0), // c12:bigint
            newArrayList(
                newArrayList(text("a"), text("b"), text("c")),
                newArrayList(text("A"), text("B"), text("C"))), // c13:array<array<string>>
            Maps.toMap(
                newArrayList(intW(0), intW(1), intW(2)),
                x -> Maps.toMap(
                    newArrayList(x, intW(x.get() * 2)),
                    y -> y)), // c14:map<int,map<int,int>>
            new Object[] {
                intW(0),
                newArrayList(
                    intW(1),
                    text("Hello"))}, // c15:struct<r:int,s:struct<a:int,b:string>>
            Collections.singletonList(
                newArrayList(
                    Maps.toMap(
                        newArrayList(text("hello")),
                        input -> text(input.toString().toUpperCase())),
                    intW(0))), // c16:array<struct<m:map<string,string>,n:int>>
            new TimestampWritableV2(TIMESTAMP), // c17:timestamp
            decimalW(HiveDecimal.create(0, 0)), // c18:decimal(16,7)
            new BytesWritable("Hello".getBytes()), // c19:binary
            new DateWritableV2(123), // c20:date
            varcharW("x", 20), // c21:varchar(20)
            charW("y", 15), // c22:char(15)
            new BytesWritable("world!".getBytes()), // c23:binary
        }, {
            null, null, null, null, null, null, null, null, null, null, // c1-c10
            null, null, null, null, null, null, null, null, null, null, // c11-c20
            null, null, null, // c21-c23
        }
    };

    initAndSerializeAndDeserialize(schema, comprehensiveRows);
  }

  private <E> List<E> newArrayList(E ... elements) {
    return Lists.newArrayList(elements);
  }

  @Test
  public void testPrimitiveInteger() throws SerDeException {
    String[][] schema = {
        {"tinyint1", "tinyint"},
        {"smallint1", "smallint"},
        {"int1", "int"},
        {"bigint1", "bigint"}
    };

    initAndSerializeAndDeserialize(schema, INTEGER_ROWS);
  }

  @Test
  public void testPrimitiveBigInt10000() throws SerDeException {
    String[][] schema = {
        {"bigint1", "bigint"}
    };

    final int batchSize = 1000;
    final Object[][] integerRows = new Object[batchSize][];
    final ArrowColumnarBatchSerDe serDe = new ArrowColumnarBatchSerDe();
    StructObjectInspector rowOI = initSerDe(serDe, schema);

    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < batchSize; i++) {
        integerRows[i] = new Object[] {longW(i + j * batchSize)};
      }

      serializeAndDeserialize(serDe, integerRows, rowOI);
    }
  }

  @Test
  public void testPrimitiveBigIntRandom() {
    try {
      String[][] schema = {
          {"bigint1", "bigint"}
      };

      final ArrowColumnarBatchSerDe serDe = new ArrowColumnarBatchSerDe();
      StructObjectInspector rowOI = initSerDe(serDe, schema);

      final Random random = new Random();
      for (int j = 0; j < 1000; j++) {
        final int batchSize = random.nextInt(1000);
        final Object[][] integerRows = new Object[batchSize][];
        for (int i = 0; i < batchSize; i++) {
          integerRows[i] = new Object[] {longW(random.nextLong())};
        }

        serializeAndDeserialize(serDe, integerRows, rowOI);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testPrimitiveFloat() throws SerDeException {
    String[][] schema = {
        {"float1", "float"},
        {"double1", "double"},
    };

    initAndSerializeAndDeserialize(schema, FLOAT_ROWS);
  }

  @Test(expected = AssertionError.class)
  public void testPrimitiveFloatNaN() throws SerDeException {
    String[][] schema = {
        {"float1", "float"},
    };

    Object[][] rows = {{new FloatWritable(Float.NaN)}};

    initAndSerializeAndDeserialize(schema, rows);
  }

  @Test(expected = AssertionError.class)
  public void testPrimitiveDoubleNaN() throws SerDeException {
    String[][] schema = {
        {"double1", "double"},
    };

    Object[][] rows = {{new DoubleWritable(Double.NaN)}};

    initAndSerializeAndDeserialize(schema, rows);
  }

  @Test
  public void testPrimitiveString() throws SerDeException {
    String[][] schema = {
        {"string1", "string"},
        {"char1", "char(10)"},
        {"varchar1", "varchar(10)"},
    };

    initAndSerializeAndDeserialize(schema, STRING_ROWS);
  }

  @Test
  public void testPrimitiveDTI() throws SerDeException {
    String[][] schema = {
        {"date1", "date"},
        {"timestamp1", "timestamp"},
        {"interval_year_month1", "interval_year_month"},
        {"interval_day_time1", "interval_day_time"},
    };

    initAndSerializeAndDeserialize(schema, DTI_ROWS);
  }

  @Test
  public void testPrimitiveRandomTimestamp() throws SerDeException {
    String[][] schema = {
        {"timestamp1", "timestamp"},
    };

    int size = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_ARROW_BATCH_SIZE);
    Random rand = new Random(294722773L);
    Object[][] rows = new Object[size][];
    for (int i = 0; i < size; i++) {
      long millis = ((long) rand.nextInt(Integer.MAX_VALUE)) * 1000;
      Timestamp timestamp = Timestamp.ofEpochMilli(rand.nextBoolean() ? millis : -millis);
      timestamp.setNanos(rand.nextInt(1000) * 1000);
      rows[i] = new Object[] {new TimestampWritableV2(timestamp)};
    }

    initAndSerializeAndDeserialize(schema, rows);
  }

  @Test
  public void testPrimitiveDecimal() throws SerDeException {
    String[][] schema = {
        {"decimal1", "decimal(38,10)"},
    };

    initAndSerializeAndDeserialize(schema, DECIMAL_ROWS);
  }

  @Test
  public void testPrimitiveBoolean() throws SerDeException {
    String[][] schema = {
        {"boolean1", "boolean"},
    };

    initAndSerializeAndDeserialize(schema, BOOLEAN_ROWS);
  }

  @Test
  public void testPrimitiveBinary() throws SerDeException {
    String[][] schema = {
        {"binary1", "binary"},
    };

    initAndSerializeAndDeserialize(schema, BINARY_ROWS);
  }

  private List[][] toList(Object[][] rows) {
    List[][] array = new List[rows.length][];
    for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
      Object[] row = rows[rowIndex];
      array[rowIndex] = new List[row.length];
      for (int fieldIndex = 0; fieldIndex < row.length; fieldIndex++) {
        array[rowIndex][fieldIndex] = newArrayList(row[fieldIndex]);
      }
    }
    return array;
  }

  @Test
  public void testListInteger() throws SerDeException {
    String[][] schema = {
        {"tinyint_list", "array<tinyint>"},
        {"smallint_list", "array<smallint>"},
        {"int_list", "array<int>"},
        {"bigint_list", "array<bigint>"},
    };

    initAndSerializeAndDeserialize(schema, toList(INTEGER_ROWS));
  }

  @Test
  public void testListFloat() throws SerDeException {
    String[][] schema = {
        {"float_list", "array<float>"},
        {"double_list", "array<double>"},
    };

    initAndSerializeAndDeserialize(schema, toList(FLOAT_ROWS));
  }

  @Test
  public void testListString() throws SerDeException {
    String[][] schema = {
        {"string_list", "array<string>"},
        {"char_list", "array<char(10)>"},
        {"varchar_list", "array<varchar(10)>"},
    };

    initAndSerializeAndDeserialize(schema, toList(STRING_ROWS));
  }

  @Test
  public void testListDTI() throws SerDeException {
    String[][] schema = {
        {"date_list", "array<date>"},
        {"timestamp_list", "array<timestamp>"},
        {"interval_year_month_list", "array<interval_year_month>"},
        {"interval_day_time_list", "array<interval_day_time>"},
    };

    initAndSerializeAndDeserialize(schema, toList(DTI_ROWS));
  }

  @Test
  public void testListBoolean() throws SerDeException {
    String[][] schema = {
        {"boolean_list", "array<boolean>"},
    };

    initAndSerializeAndDeserialize(schema, toList(BOOLEAN_ROWS));
  }

  @Test
  public void testListBinary() throws SerDeException {
    String[][] schema = {
        {"binary_list", "array<binary>"},
    };

    initAndSerializeAndDeserialize(schema, toList(BINARY_ROWS));
  }

  private Object[][][] toStruct(Object[][] rows) {
    Object[][][] struct = new Object[rows.length][][];
    for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
      Object[] row = rows[rowIndex];
      struct[rowIndex] = new Object[][] {row};
    }
    return struct;
  }

  @Test
  public void testStructInteger() throws SerDeException {
    String[][] schema = {
        {"int_struct", "struct<tinyint1:tinyint,smallint1:smallint,int1:int,bigint1:bigint>"},
    };

    initAndSerializeAndDeserialize(schema, toStruct(INTEGER_ROWS));
  }

  @Test
  public void testStructFloat() throws SerDeException {
    String[][] schema = {
        {"float_struct", "struct<float1:float,double1:double>"},
    };

    initAndSerializeAndDeserialize(schema, toStruct(FLOAT_ROWS));
  }

  @Test
  public void testStructString() throws SerDeException {
    String[][] schema = {
        {"string_struct", "struct<string1:string,char1:char(10),varchar1:varchar(10)>"},
    };

    initAndSerializeAndDeserialize(schema, toStruct(STRING_ROWS));
  }

  @Test
  public void testStructDTI() throws SerDeException {
    String[][] schema = {
        {"date_struct", "struct<date1:date,timestamp1:timestamp," +
            "interval_year_month1:interval_year_month,interval_day_time1:interval_day_time>"},
    };

    initAndSerializeAndDeserialize(schema, toStruct(DTI_ROWS));
  }

  @Test
  public void testStructDecimal() throws SerDeException {
    String[][] schema = {
        {"decimal_struct", "struct<decimal1:decimal(38,10)>"},
    };

    initAndSerializeAndDeserialize(schema, toStruct(DECIMAL_ROWS));
  }

  @Test
  public void testStructBoolean() throws SerDeException {
    String[][] schema = {
        {"boolean_struct", "struct<boolean1:boolean>"},
    };

    initAndSerializeAndDeserialize(schema, toStruct(BOOLEAN_ROWS));
  }

  @Test
  public void testStructBinary() throws SerDeException {
    String[][] schema = {
        {"binary_struct", "struct<binary1:binary>"},
    };

    initAndSerializeAndDeserialize(schema, toStruct(BINARY_ROWS));
  }

  private Object[][] toMap(Object[][] rows) {
    Map[][] array = new Map[rows.length][];
    for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
      Object[] row = rows[rowIndex];
      array[rowIndex] = new Map[row.length];
      for (int fieldIndex = 0; fieldIndex < row.length; fieldIndex++) {
        Map map = Maps.newHashMap();
        map.put(new Text(String.valueOf(row[fieldIndex])), row[fieldIndex]);
        array[rowIndex][fieldIndex] = map;
      }
    }
    return array;
  }

  @Test
  public void testMapInteger() throws SerDeException {
    String[][] schema = {
        {"tinyint_map", "map<string,tinyint>"},
        {"smallint_map", "map<string,smallint>"},
        {"int_map", "map<string,int>"},
        {"bigint_map", "map<string,bigint>"},
    };

    initAndSerializeAndDeserialize(schema, toMap(INTEGER_ROWS));
  }

  @Test
  public void testMapFloat() throws SerDeException {
    String[][] schema = {
        {"float_map", "map<string,float>"},
        {"double_map", "map<string,double>"},
    };

    initAndSerializeAndDeserialize(schema, toMap(FLOAT_ROWS));
  }

  @Test
  public void testMapString() throws SerDeException {
    String[][] schema = {
        {"string_map", "map<string,string>"},
        {"char_map", "map<string,char(10)>"},
        {"varchar_map", "map<string,varchar(10)>"},
    };

    initAndSerializeAndDeserialize(schema, toMap(STRING_ROWS));
  }

  @Test
  public void testMapDTI() throws SerDeException {
    String[][] schema = {
        {"date_map", "map<string,date>"},
        {"timestamp_map", "map<string,timestamp>"},
        {"interval_year_month_map", "map<string,interval_year_month>"},
        {"interval_day_time_map", "map<string,interval_day_time>"},
    };

    initAndSerializeAndDeserialize(schema, toMap(DTI_ROWS));
  }

  @Test
  public void testMapBoolean() throws SerDeException {
    String[][] schema = {
        {"boolean_map", "map<string,boolean>"},
    };

    initAndSerializeAndDeserialize(schema, toMap(BOOLEAN_ROWS));
  }

  @Test
  public void testMapBinary() throws SerDeException {
    String[][] schema = {
        {"binary_map", "map<string,binary>"},
    };

    initAndSerializeAndDeserialize(schema, toMap(BINARY_ROWS));
  }

  public void testMapDecimal() throws SerDeException {
    String[][] schema = {
        {"decimal_map", "map<string,decimal(38,10)>"},
    };

    initAndSerializeAndDeserialize(schema, toMap(DECIMAL_ROWS));
  }

  public void testListDecimal() throws SerDeException {
    String[][] schema = {
        {"decimal_list", "array<decimal(38,10)>"},
    };

    initAndSerializeAndDeserialize(schema, toList(DECIMAL_ROWS));
  }

}
