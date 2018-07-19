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
package org.apache.hadoop.hive.serde2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * TestBinarySortableSerDe.
 *
 */
public class VerifyFast {

  public static void verifyDeserializeRead(DeserializeRead deserializeRead,
      TypeInfo typeInfo, Object object) throws IOException {

    boolean isNull;

    isNull = !deserializeRead.readNextField();
    doVerifyDeserializeRead(deserializeRead, typeInfo, object, isNull);
  }

  public static void doVerifyDeserializeRead(DeserializeRead deserializeRead,
        TypeInfo typeInfo, Object object, boolean isNull) throws IOException {
    if (isNull) {
      if (object != null) {
        TestCase.fail("Field reports null but object is not null (class " + object.getClass().getName() + ", " + object.toString() + ")");
      }
      return;
    } else if (object == null) {
      TestCase.fail("Field report not null but object is null");
    }
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case BOOLEAN:
          {
            boolean value = deserializeRead.currentBoolean;
            if (!(object instanceof BooleanWritable)) {
              TestCase.fail("Boolean expected writable not Boolean");
            }
            boolean expected = ((BooleanWritable) object).get();
            if (value != expected) {
              TestCase.fail("Boolean field mismatch (expected " + expected + " found " + value + ")");
            }
          }
          break;
        case BYTE:
          {
            byte value = deserializeRead.currentByte;
            if (!(object instanceof ByteWritable)) {
              TestCase.fail("Byte expected writable not Byte");
            }
            byte expected = ((ByteWritable) object).get();
            if (value != expected) {
              TestCase.fail("Byte field mismatch (expected " + (int) expected + " found " + (int) value + ")");
            }
          }
          break;
        case SHORT:
          {
            short value = deserializeRead.currentShort;
            if (!(object instanceof ShortWritable)) {
              TestCase.fail("Short expected writable not Short");
            }
            short expected = ((ShortWritable) object).get();
            if (value != expected) {
              TestCase.fail("Short field mismatch (expected " + expected + " found " + value + ")");
            }
          }
          break;
        case INT:
          {
            int value = deserializeRead.currentInt;
            if (!(object instanceof IntWritable)) {
              TestCase.fail("Integer expected writable not Integer");
            }
            int expected = ((IntWritable) object).get();
            if (value != expected) {
              TestCase.fail("Int field mismatch (expected " + expected + " found " + value + ")");
            }
          }
          break;
        case LONG:
          {
            long value = deserializeRead.currentLong;
            if (!(object instanceof LongWritable)) {
              TestCase.fail("Long expected writable not Long");
            }
            Long expected = ((LongWritable) object).get();
            if (value != expected) {
              TestCase.fail("Long field mismatch (expected " + expected + " found " + value + ")");
            }
          }
          break;
        case FLOAT:
          {
            float value = deserializeRead.currentFloat;
            if (!(object instanceof FloatWritable)) {
              TestCase.fail("Float expected writable not Float");
            }
            float expected = ((FloatWritable) object).get();
            if (value != expected) {
              TestCase.fail("Float field mismatch (expected " + expected + " found " + value + ")");
            }
          }
          break;
        case DOUBLE:
          {
            double value = deserializeRead.currentDouble;
            if (!(object instanceof DoubleWritable)) {
              TestCase.fail("Double expected writable not Double");
            }
            double expected = ((DoubleWritable) object).get();
            if (value != expected) {
              TestCase.fail("Double field mismatch (expected " + expected + " found " + value + ")");
            }
          }
          break;
        case STRING:
          {
            byte[] stringBytes = Arrays.copyOfRange(
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);
            Text text = new Text(stringBytes);
            String string = text.toString();
            String expected = ((Text) object).toString();
            if (!string.equals(expected)) {
              TestCase.fail("String field mismatch (expected '" + expected + "' found '" + string + "')");
            }
          }
          break;
        case CHAR:
          {
            byte[] stringBytes = Arrays.copyOfRange(
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);
            Text text = new Text(stringBytes);
            String string = text.toString();
    
            HiveChar hiveChar = new HiveChar(string, ((CharTypeInfo) primitiveTypeInfo).getLength());
    
            HiveChar expected = ((HiveCharWritable) object).getHiveChar();
            if (!hiveChar.equals(expected)) {
              TestCase.fail("Char field mismatch (expected '" + expected + "' found '" + hiveChar + "')");
            }
          }
          break;
        case VARCHAR:
          {
            byte[] stringBytes = Arrays.copyOfRange(
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);
            Text text = new Text(stringBytes);
            String string = text.toString();
    
            HiveVarchar hiveVarchar = new HiveVarchar(string, ((VarcharTypeInfo) primitiveTypeInfo).getLength());
    
            HiveVarchar expected = ((HiveVarcharWritable) object).getHiveVarchar();
            if (!hiveVarchar.equals(expected)) {
              TestCase.fail("Varchar field mismatch (expected '" + expected + "' found '" + hiveVarchar + "')");
            }
          }
          break;
        case DECIMAL:
          {
            HiveDecimal value = deserializeRead.currentHiveDecimalWritable.getHiveDecimal();
            if (value == null) {
              TestCase.fail("Decimal field evaluated to NULL");
            }
            HiveDecimal expected = ((HiveDecimalWritable) object).getHiveDecimal();
            if (!value.equals(expected)) {
              DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
              int precision = decimalTypeInfo.getPrecision();
              int scale = decimalTypeInfo.getScale();
              TestCase.fail("Decimal field mismatch (expected " + expected.toString() + " found " + value.toString() + ") precision " + precision + ", scale " + scale);
            }
          }
          break;
        case DATE:
          {
            Date value = deserializeRead.currentDateWritable.get();
            Date expected = ((DateWritableV2) object).get();
            if (!value.equals(expected)) {
              TestCase.fail("Date field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
            }
          }
          break;
        case TIMESTAMP:
          {
            Timestamp value = deserializeRead.currentTimestampWritable.getTimestamp();
            Timestamp expected = ((TimestampWritableV2) object).getTimestamp();
            if (!value.equals(expected)) {
              TestCase.fail("Timestamp field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
            }
          }
          break;
        case INTERVAL_YEAR_MONTH:
          {
            HiveIntervalYearMonth value = deserializeRead.currentHiveIntervalYearMonthWritable.getHiveIntervalYearMonth();
            HiveIntervalYearMonth expected = ((HiveIntervalYearMonthWritable) object).getHiveIntervalYearMonth();
            if (!value.equals(expected)) {
              TestCase.fail("HiveIntervalYearMonth field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
            }
          }
          break;
        case INTERVAL_DAY_TIME:
          {
            HiveIntervalDayTime value = deserializeRead.currentHiveIntervalDayTimeWritable.getHiveIntervalDayTime();
            HiveIntervalDayTime expected = ((HiveIntervalDayTimeWritable) object).getHiveIntervalDayTime();
            if (!value.equals(expected)) {
              TestCase.fail("HiveIntervalDayTime field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
            }
          }
          break;
        case BINARY:
          {
            byte[] byteArray = Arrays.copyOfRange(
                deserializeRead.currentBytes,
                deserializeRead.currentBytesStart,
                deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);
            BytesWritable bytesWritable = (BytesWritable) object;
            byte[] expected = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());
            if (byteArray.length != expected.length){
              TestCase.fail("Byte Array field mismatch (expected " + Arrays.toString(expected)
                  + " found " + Arrays.toString(byteArray) + ")");
            }
            for (int b = 0; b < byteArray.length; b++) {
              if (byteArray[b] != expected[b]) {
                TestCase.fail("Byte Array field mismatch (expected " + Arrays.toString(expected)
                  + " found " + Arrays.toString(byteArray) + ")");
              }
            }
          }
          break;
        default:
          throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory());
        }
      }
      break;
    case LIST:
    case MAP:
    case STRUCT:
    case UNION:
      throw new Error("Complex types need to be handled separately");
    default:
      throw new Error("Unknown category " + typeInfo.getCategory());
    }
  }

  public static void serializeWrite(SerializeWrite serializeWrite,
      TypeInfo typeInfo, Object object) throws IOException {
    if (object == null) {
      serializeWrite.writeNull();
      return;
    }
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case BOOLEAN:
          {
            boolean value = ((BooleanWritable) object).get();
            serializeWrite.writeBoolean(value);
          }
          break;
        case BYTE:
          {
            byte value = ((ByteWritable) object).get();
            serializeWrite.writeByte(value);
          }
          break;
        case SHORT:
          {
            short value = ((ShortWritable) object).get();
            serializeWrite.writeShort(value);
          }
          break;
        case INT:
          {
            int value = ((IntWritable) object).get();
            serializeWrite.writeInt(value);
          }
          break;
        case LONG:
          {
            long value = ((LongWritable) object).get();
            serializeWrite.writeLong(value);
          }
          break;
        case FLOAT:
          {
            float value = ((FloatWritable) object).get();
            serializeWrite.writeFloat(value);
          }
          break;
        case DOUBLE:
          {
            double value = ((DoubleWritable) object).get();
            serializeWrite.writeDouble(value);
          }
          break;
        case STRING:
          {
            Text value = (Text) object;
            byte[] stringBytes = value.getBytes();
            int stringLength = stringBytes.length;
            serializeWrite.writeString(stringBytes, 0, stringLength);
          }
          break;
        case CHAR:
          {
            HiveChar value = ((HiveCharWritable) object).getHiveChar();
            serializeWrite.writeHiveChar(value);
          }
          break;
        case VARCHAR:
          {
            HiveVarchar value = ((HiveVarcharWritable) object).getHiveVarchar();
            serializeWrite.writeHiveVarchar(value);
          }
          break;
        case DECIMAL:
          {
            HiveDecimal value = ((HiveDecimalWritable) object).getHiveDecimal();
            DecimalTypeInfo decTypeInfo = (DecimalTypeInfo)primitiveTypeInfo;
            serializeWrite.writeHiveDecimal(value, decTypeInfo.scale());
          }
          break;
        case DATE:
          {
            Date value = ((DateWritableV2) object).get();
            serializeWrite.writeDate(value);
          }
          break;
        case TIMESTAMP:
          {
            Timestamp value = ((TimestampWritableV2) object).getTimestamp();
            serializeWrite.writeTimestamp(value);
          }
          break;
        case INTERVAL_YEAR_MONTH:
          {
            HiveIntervalYearMonth value = ((HiveIntervalYearMonthWritable) object).getHiveIntervalYearMonth();
            serializeWrite.writeHiveIntervalYearMonth(value);
          }
          break;
        case INTERVAL_DAY_TIME:
          {
            HiveIntervalDayTime value = ((HiveIntervalDayTimeWritable) object).getHiveIntervalDayTime();
            serializeWrite.writeHiveIntervalDayTime(value);
          }
          break;
        case BINARY:
          {
            BytesWritable byteWritable = (BytesWritable) object;
            byte[] binaryBytes = byteWritable.getBytes();
            int length = byteWritable.getLength();
            serializeWrite.writeBinary(binaryBytes, 0, length);
          }
          break;
        default:
          throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory().name());
        }
      }
      break;
    case LIST:
      {
        ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
        ArrayList<Object> elements = (ArrayList<Object>) object;
        serializeWrite.beginList(elements);
        boolean isFirst = true;
        for (Object elementObject : elements) {
          if (isFirst) {
            isFirst = false;
          } else {
            serializeWrite.separateList();
          }
          if (elementObject == null) {
            serializeWrite.writeNull();
          } else {
            serializeWrite(serializeWrite, elementTypeInfo, elementObject);
          }
        }
        serializeWrite.finishList();
      }
      break;
    case MAP:
      {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
        HashMap<Object, Object> hashMap = (HashMap<Object, Object>) object;
        serializeWrite.beginMap(hashMap);
        boolean isFirst = true;
        for (Entry<Object, Object> entry : hashMap.entrySet()) {
          if (isFirst) {
            isFirst = false;
          } else {
            serializeWrite.separateKeyValuePair();
          }
          if (entry.getKey() == null) {
            serializeWrite.writeNull();
          } else {
            serializeWrite(serializeWrite, keyTypeInfo, entry.getKey());
          }
          serializeWrite.separateKey();
          if (entry.getValue() == null) {
            serializeWrite.writeNull();
          } else {
            serializeWrite(serializeWrite, valueTypeInfo, entry.getValue());
          }
        }
        serializeWrite.finishMap();
      }
      break;
    case STRUCT:
      {
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        ArrayList<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        ArrayList<Object> fieldValues = (ArrayList<Object>) object;
        final int size = fieldValues.size();
        serializeWrite.beginStruct(fieldValues);
        boolean isFirst = true;
        for (int i = 0; i < size; i++) {
          if (isFirst) {
            isFirst = false;
          } else {
            serializeWrite.separateStruct();
          }
          serializeWrite(serializeWrite, fieldTypeInfos.get(i), fieldValues.get(i));
        }
        serializeWrite.finishStruct();
      }
      break;
    case UNION:
      {
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> fieldTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
        final int size = fieldTypeInfos.size();
        StandardUnion standardUnion = (StandardUnion) object;
        byte tag = standardUnion.getTag();
        serializeWrite.beginUnion(tag);
        serializeWrite(serializeWrite, fieldTypeInfos.get(tag), standardUnion.getObject());
        serializeWrite.finishUnion();
      }
      break;
    default:
      throw new Error("Unknown category " + typeInfo.getCategory().name());
    }
  }

  public Object readComplexPrimitiveField(DeserializeRead deserializeRead,
      PrimitiveTypeInfo primitiveTypeInfo) throws IOException {
    boolean isNull = !deserializeRead.readComplexField();
    if (isNull) {
      return null;
    } else {
      return doReadComplexPrimitiveField(deserializeRead, primitiveTypeInfo);
    }
  }

  private static Object doReadComplexPrimitiveField(DeserializeRead deserializeRead,
      PrimitiveTypeInfo primitiveTypeInfo) throws IOException {
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
    case BOOLEAN:
      return new BooleanWritable(deserializeRead.currentBoolean);
    case BYTE:
      return new ByteWritable(deserializeRead.currentByte);
    case SHORT:
      return new ShortWritable(deserializeRead.currentShort);
    case INT:
      return new IntWritable(deserializeRead.currentInt);
    case LONG:
      return new LongWritable(deserializeRead.currentLong);
    case FLOAT:
      return new FloatWritable(deserializeRead.currentFloat);
    case DOUBLE:
      return new DoubleWritable(deserializeRead.currentDouble);
    case STRING:
      return new Text(new String(
          deserializeRead.currentBytes,
          deserializeRead.currentBytesStart,
          deserializeRead.currentBytesLength,
          StandardCharsets.UTF_8));
    case CHAR:
      return new HiveCharWritable(new HiveChar(
          new String(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesLength,
            StandardCharsets.UTF_8),
            ((CharTypeInfo) primitiveTypeInfo).getLength()));
    case VARCHAR:
      if (deserializeRead.currentBytes == null) {
        throw new RuntimeException();
      }
      return new HiveVarcharWritable(new HiveVarchar(
          new String(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesLength,
            StandardCharsets.UTF_8),
            ((VarcharTypeInfo) primitiveTypeInfo).getLength()));
    case DECIMAL:
      return new HiveDecimalWritable(deserializeRead.currentHiveDecimalWritable);
    case DATE:
      return new DateWritableV2(deserializeRead.currentDateWritable);
    case TIMESTAMP:
      return new TimestampWritableV2(deserializeRead.currentTimestampWritable);
    case INTERVAL_YEAR_MONTH:
      return new HiveIntervalYearMonthWritable(deserializeRead.currentHiveIntervalYearMonthWritable);
    case INTERVAL_DAY_TIME:
      return new HiveIntervalDayTimeWritable(deserializeRead.currentHiveIntervalDayTimeWritable);
    case BINARY:
      return new BytesWritable(
          Arrays.copyOfRange(
              deserializeRead.currentBytes,
              deserializeRead.currentBytesStart,
              deserializeRead.currentBytesLength + deserializeRead.currentBytesStart));
    default:
      throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory());
    }
  }

  public static Object deserializeReadComplexType(DeserializeRead deserializeRead,
      TypeInfo typeInfo) throws IOException {

    boolean isNull = !deserializeRead.readNextField();
    if (isNull) {
      return null;
    }
    return getComplexField(deserializeRead, typeInfo);
  }

  static int fake = 0;

  private static Object getComplexField(DeserializeRead deserializeRead,
      TypeInfo typeInfo) throws IOException {
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      return doReadComplexPrimitiveField(deserializeRead, (PrimitiveTypeInfo) typeInfo);
    case LIST:
      {
        ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
        ArrayList<Object> list = new ArrayList<Object>();
        Object eleObj;
        boolean isNull;
        while (deserializeRead.isNextComplexMultiValue()) {
          isNull = !deserializeRead.readComplexField();
          if (isNull) {
            eleObj = null;
          } else {
            eleObj = getComplexField(deserializeRead, elementTypeInfo);
          }
          list.add(eleObj);
        }
        return list;
      }
    case MAP:
      {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
        HashMap<Object, Object> hashMap = new HashMap<Object, Object>();
        Object keyObj;
        Object valueObj;
        boolean isNull;
        while (deserializeRead.isNextComplexMultiValue()) {
          isNull = !deserializeRead.readComplexField();
          if (isNull) {
            keyObj = null;
          } else {
            keyObj = getComplexField(deserializeRead, keyTypeInfo);
          }
          isNull = !deserializeRead.readComplexField();
          if (isNull) {
            valueObj = null;
          } else {
            valueObj = getComplexField(deserializeRead, valueTypeInfo);
          }
          hashMap.put(keyObj, valueObj);
        }
        return hashMap;
      }
    case STRUCT:
      {
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        ArrayList<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        final int size = fieldTypeInfos.size();
        ArrayList<Object> fieldValues = new ArrayList<Object>();
        Object fieldObj;
        boolean isNull;
        for (int i = 0; i < size; i++) {
          isNull = !deserializeRead.readComplexField();
          if (isNull) {
            fieldObj = null;
          } else {
            fieldObj = getComplexField(deserializeRead, fieldTypeInfos.get(i));
          }
          fieldValues.add(fieldObj);
        }
        deserializeRead.finishComplexVariableFieldsType();
        return fieldValues;
      }
    case UNION:
      {
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> unionTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
        final int size = unionTypeInfos.size();
        Object tagObj;
        int tag;
        Object unionObj;
        boolean isNull = !deserializeRead.readComplexField();
        if (isNull) {
          unionObj = null;
        } else {
          // Get the tag value.
          tagObj = getComplexField(deserializeRead, TypeInfoFactory.intTypeInfo);
          tag = ((IntWritable) tagObj).get();

          isNull = !deserializeRead.readComplexField();
          if (isNull) {
            unionObj = null;
          } else {
            // Get the union value.
            unionObj = new StandardUnion((byte) tag, getComplexField(deserializeRead, unionTypeInfos.get(tag)));
          }
        }

        deserializeRead.finishComplexVariableFieldsType();
        return unionObj;
      }
    default:
      throw new Error("Unexpected category " + typeInfo.getCategory());
    }
  }
}