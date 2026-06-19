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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
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
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * TestBinarySortableSerDe.
 *
 */
public class VerifyLazy {

  public static boolean lazyCompareList(ListTypeInfo listTypeInfo, List<Object> list, List<Object> expectedList) {
    TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
    final int size = list.size();
    for (int i = 0; i < size; i++) {
      Object lazyEleObj = list.get(i);
      Object expectedEleObj = expectedList.get(i);
      if (!lazyCompare(elementTypeInfo, lazyEleObj, expectedEleObj)) {
        throw new RuntimeException("List element deserialized value does not match elementTypeInfo " + elementTypeInfo.toString());
      }
    }
    return true;
  }

  public static boolean lazyCompareMap(MapTypeInfo mapTypeInfo, Map<Object, Object> map, Map<Object, Object> expectedMap) {
    TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
    TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
    if (map.size() != expectedMap.size()) {
      throw new RuntimeException("Map key/value deserialized map.size() " + map.size() + " map " + map.toString() +
          " expectedMap.size() " + expectedMap.size() + " expectedMap " + expectedMap.toString() +
          " does not match keyTypeInfo " + keyTypeInfo.toString() + " valueTypeInfo " + valueTypeInfo.toString());
    }
    return true;
  }

  public static boolean lazyCompareStruct(StructTypeInfo structTypeInfo, List<Object> fields, List<Object> expectedFields) {
    List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
    final int size = fieldTypeInfos.size();
    for (int i = 0; i < size; i++) {
      Object lazyEleObj = fields.get(i);
      Object expectedEleObj = expectedFields.get(i);
      if (!lazyCompare(fieldTypeInfos.get(i), lazyEleObj, expectedEleObj)) {
        throw new RuntimeException("SerDe deserialized value does not match");
      }
    }
    return true;
  }

  public static boolean lazyCompareUnion(UnionTypeInfo unionTypeInfo, LazyBinaryUnion union, UnionObject expectedUnion) {
    byte tag = union.getTag();
    byte expectedTag = expectedUnion.getTag();
    if (tag != expectedTag) {
      throw new RuntimeException("Union tag does not match union.getTag() " + tag + " expectedUnion.getTag() " + expectedTag);
    }
    return lazyCompare(unionTypeInfo.getAllUnionObjectTypeInfos().get(tag),
        union.getField(), expectedUnion.getObject());
  }

  public static boolean lazyCompareUnion(UnionTypeInfo unionTypeInfo, LazyUnion union, UnionObject expectedUnion) {
    byte tag = union.getTag();
    byte expectedTag = expectedUnion.getTag();
    if (tag != expectedTag) {
      throw new RuntimeException("Union tag does not match union.getTag() " + tag + " expectedUnion.getTag() " + expectedTag);
    }
    return lazyCompare(unionTypeInfo.getAllUnionObjectTypeInfos().get(tag),
        union.getField(), expectedUnion.getObject());
  }

  public static boolean lazyCompareUnion(UnionTypeInfo unionTypeInfo, UnionObject union, UnionObject expectedUnion) {
    byte tag = union.getTag();
    byte expectedTag = expectedUnion.getTag();
    if (tag != expectedTag) {
      throw new RuntimeException("Union tag does not match union.getTag() " + tag +
          " expectedUnion.getTag() " + expectedTag);
    }
    return lazyCompare(unionTypeInfo.getAllUnionObjectTypeInfos().get(tag),
        union.getObject(), expectedUnion.getObject());
  }

  public static boolean lazyCompare(TypeInfo typeInfo, Object lazyObject, Object expectedObject) {
    if (expectedObject == null) {
      if (lazyObject != null) {
        throw new RuntimeException("Expected object is null but object is not null " + lazyObject.toString() +
            " typeInfo " + typeInfo.toString());
      }
      return true;
    } else if (lazyObject == null) {
      throw new RuntimeException("Expected object is not null \"" + expectedObject.toString() +
          "\" typeInfo " + typeInfo.toString() + " but object is null");
    }
    if (lazyObject instanceof Writable) {
      if (!lazyObject.equals(expectedObject)) {
        throw new RuntimeException("Expected object " + expectedObject.toString() +
            " and actual object " + lazyObject.toString() + " is not equal typeInfo " + typeInfo.toString());
      }
      return true;
    }
    if (lazyObject instanceof LazyPrimitive) {
      Object primitiveObject = ((LazyPrimitive) lazyObject).getObject();
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
      switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case BOOLEAN:
        {
          if (!(primitiveObject instanceof LazyBoolean)) {
            throw new RuntimeException("Expected LazyBoolean");
          }
          boolean value = ((LazyBoolean) primitiveObject).getWritableObject().get();
          boolean expected = ((BooleanWritable) expectedObject).get();
          if (value != expected) {
            throw new RuntimeException("Boolean field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case BYTE:
        {
          if (!(primitiveObject instanceof LazyByte)) {
            throw new RuntimeException("Expected LazyByte");
          }
          byte value = ((LazyByte) primitiveObject).getWritableObject().get();
          byte expected = ((ByteWritable) expectedObject).get();
          if (value != expected) {
            throw new RuntimeException("Byte field mismatch (expected " + (int) expected + " found " + (int) value + ")");
          }
        }
        break;
      case SHORT:
        {
          if (!(primitiveObject instanceof LazyShort)) {
            throw new RuntimeException("Expected LazyShort");
          }
          short value = ((LazyShort) primitiveObject).getWritableObject().get();
          short expected = ((ShortWritable) expectedObject).get();
          if (value != expected) {
            throw new RuntimeException("Short field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case INT:
        {
          if (!(primitiveObject instanceof LazyInteger)) {
            throw new RuntimeException("Expected LazyInteger");
          }
          int value = ((LazyInteger) primitiveObject).getWritableObject().get();
          int expected = ((IntWritable) expectedObject).get();
          if (value != expected) {
            throw new RuntimeException("Int field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case LONG:
        {
          if (!(primitiveObject instanceof LazyLong)) {
            throw new RuntimeException("Expected LazyLong");
          }
          long value = ((LazyLong) primitiveObject).getWritableObject().get();
          long expected = ((LongWritable) expectedObject).get();
          if (value != expected) {
            throw new RuntimeException("Long field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case FLOAT:
        {
          if (!(primitiveObject instanceof LazyFloat)) {
            throw new RuntimeException("Expected LazyFloat");
          }
          float value = ((LazyFloat) primitiveObject).getWritableObject().get();
          float expected = ((FloatWritable) expectedObject).get();
          if (value != expected) {
            throw new RuntimeException("Float field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case DOUBLE:
        {
          if (!(primitiveObject instanceof LazyDouble)) {
            throw new RuntimeException("Expected LazyDouble");
          }
          double value = ((LazyDouble) primitiveObject).getWritableObject().get();
          double expected = ((DoubleWritable) expectedObject).get();
          if (value != expected) {
            throw new RuntimeException("Double field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case STRING:
        {
          if (!(primitiveObject instanceof LazyString)) {
            throw new RuntimeException("Text expected writable not Text");
          }
          Text value = ((LazyString) primitiveObject).getWritableObject();
          Text expected = ((Text) expectedObject);
          if (!value.equals(expected)) {
            throw new RuntimeException("String field mismatch (expected '" + expected + "' found '" + value + "')");
          }
        }
        break;
      case CHAR:
        {
          if (!(primitiveObject instanceof LazyHiveChar)) {
            throw new RuntimeException("Expected LazyHiveChar");
          }
          HiveChar value = ((LazyHiveChar) primitiveObject).getWritableObject().getHiveChar();
          HiveChar expected = ((HiveCharWritable) expectedObject).getHiveChar();
  
          if (!value.equals(expected)) {
            throw new RuntimeException("HiveChar field mismatch (expected '" + expected + "' found '" + value + "')");
          }
        }
        break;
      case VARCHAR:
        {
          if (!(primitiveObject instanceof LazyHiveVarchar)) {
            throw new RuntimeException("Expected LazyHiveVarchar");
          }
          HiveVarchar value = ((LazyHiveVarchar) primitiveObject).getWritableObject().getHiveVarchar();
          HiveVarchar expected = ((HiveVarcharWritable) expectedObject).getHiveVarchar();
  
          if (!value.equals(expected)) {
            throw new RuntimeException("HiveVarchar field mismatch (expected '" + expected + "' found '" + value + "')");
          }
        }
        break;
      case DECIMAL:
        {
          if (!(primitiveObject instanceof LazyHiveDecimal)) {
            throw new RuntimeException("Expected LazyDecimal");
          }
          HiveDecimal value = ((LazyHiveDecimal) primitiveObject).getWritableObject().getHiveDecimal();
          HiveDecimal expected = ((HiveDecimalWritable) expectedObject).getHiveDecimal();
  
          if (!value.equals(expected)) {
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
            int precision = decimalTypeInfo.getPrecision();
            int scale = decimalTypeInfo.getScale();
            throw new RuntimeException("Decimal field mismatch (expected " + expected.toString() +
                " found " + value.toString() + ") precision " + precision + ", scale " + scale);
          }
        }
        break;
      case DATE:
        {
          if (!(primitiveObject instanceof LazyDate)) {
            throw new RuntimeException("Expected LazyDate");
          }
          Date value = ((LazyDate) primitiveObject).getWritableObject().get();
          Date expected = ((DateWritableV2) expectedObject).get();
          if (!value.equals(expected)) {
            throw new RuntimeException("Date field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case TIMESTAMP:
        {
          if (!(primitiveObject instanceof LazyTimestamp)) {
            throw new RuntimeException("TimestampWritableV2 expected writable not TimestampWritableV2");
          }
          Timestamp value = ((LazyTimestamp) primitiveObject).getWritableObject().getTimestamp();
          Timestamp expected = ((TimestampWritableV2) expectedObject).getTimestamp();
          if (!value.equals(expected)) {
            throw new RuntimeException("Timestamp field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case INTERVAL_YEAR_MONTH:
        {
          if (!(primitiveObject instanceof LazyHiveIntervalYearMonth)) {
            throw new RuntimeException("Expected LazyHiveIntervalYearMonth");
          }
          HiveIntervalYearMonth value = ((LazyHiveIntervalYearMonth) primitiveObject).getWritableObject().getHiveIntervalYearMonth();
          HiveIntervalYearMonth expected = ((HiveIntervalYearMonthWritable) expectedObject).getHiveIntervalYearMonth();
          if (!value.equals(expected)) {
            throw new RuntimeException("HiveIntervalYearMonth field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case INTERVAL_DAY_TIME:
        {
          if (!(primitiveObject instanceof LazyHiveIntervalDayTime)) {
            throw new RuntimeException("Expected writable LazyHiveIntervalDayTime");
          }
          HiveIntervalDayTime value = ((LazyHiveIntervalDayTime) primitiveObject).getWritableObject().getHiveIntervalDayTime();
          HiveIntervalDayTime expected = ((HiveIntervalDayTimeWritable) expectedObject).getHiveIntervalDayTime();
          if (!value.equals(expected)) {
            throw new RuntimeException("HiveIntervalDayTime field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case BINARY:
        {
          if (!(primitiveObject instanceof LazyBinary)) {
            throw new RuntimeException("Expected LazyBinary");
          }
          BytesWritable bytesWritable = ((LazyBinary) primitiveObject).getWritableObject();
          byte[] value = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());
          BytesWritable bytesWritableExpected = (BytesWritable) expectedObject;
          byte[] expected = Arrays.copyOfRange(bytesWritableExpected.getBytes(), 0, bytesWritableExpected.getLength());
          if (value.length != expected.length){
            throw new RuntimeException("Byte Array field mismatch (expected " + Arrays.toString(expected)
                + " found " + Arrays.toString(value) + ")");
          }
          for (int b = 0; b < value.length; b++) {
            if (value[b] != expected[b]) {
              throw new RuntimeException("Byte Array field mismatch (expected " + Arrays.toString(expected)
                + " found " + Arrays.toString(value) + ")");
            }
          }
        }
        break;
      default:
        throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory());
      }
    } else if (lazyObject instanceof LazyArray) {
      LazyArray lazyArray = (LazyArray) lazyObject;
      List<Object> list = lazyArray.getList();
      List<Object> expectedList = (List<Object>) expectedObject;
      ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
      if (list.size() != expectedList.size()) {
        throw new RuntimeException("SerDe deserialized list length does not match (list " +
            list.toString() + " list.size() " + list.size() + " expectedList " + expectedList.toString() +
            " expectedList.size() " + expectedList.size() + ")" +
            " elementTypeInfo " + listTypeInfo.getListElementTypeInfo().toString());
      }
      return lazyCompareList((ListTypeInfo) typeInfo, list, expectedList);
    } else if (typeInfo instanceof ListTypeInfo) {
      List<Object> list;
      if (lazyObject instanceof LazyBinaryArray) {
        list = ((LazyBinaryArray) lazyObject).getList();
      } else {
        list = (List<Object>) lazyObject;
      }
      List<Object> expectedList = (List<Object>) expectedObject;
      if (list.size() != expectedList.size()) {
        throw new RuntimeException("SerDe deserialized list length does not match (list " +
            list.toString() + " list.size() " + list.size() + " expectedList " + expectedList.toString() +
            " expectedList.size() " + expectedList.size() + ")");
      }
      return lazyCompareList((ListTypeInfo) typeInfo, list, expectedList);
    } else if (lazyObject instanceof LazyMap) {
      LazyMap lazyMap = (LazyMap) lazyObject;
      Map<Object, Object> map = lazyMap.getMap();
      Map<Object, Object> expectedMap = (Map<Object, Object>) expectedObject;
      return lazyCompareMap((MapTypeInfo) typeInfo, map, expectedMap);
    } else if (typeInfo instanceof MapTypeInfo) {
      Map<Object, Object> map;
      Map<Object, Object> expectedMap = (Map<Object, Object>) expectedObject;
      if (lazyObject instanceof LazyBinaryMap) {
        map = ((LazyBinaryMap) lazyObject).getMap();
      } else {
        map = (Map<Object, Object>) lazyObject;
      }
      return lazyCompareMap((MapTypeInfo) typeInfo, map, expectedMap);
    } else if (lazyObject instanceof LazyStruct) {
      LazyStruct lazyStruct = (LazyStruct) lazyObject;
      List<Object> fields = lazyStruct.getFieldsAsList();
      List<Object> expectedFields = (List<Object>) expectedObject;
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      return lazyCompareStruct(structTypeInfo, fields, expectedFields);
    } else if (typeInfo instanceof StructTypeInfo) {
      ArrayList<Object> fields;
      if (lazyObject instanceof LazyBinaryStruct) {
        fields = ((LazyBinaryStruct) lazyObject).getFieldsAsList();
      } else {
        fields = (ArrayList<Object>) lazyObject;
      }
      List<Object> expectedFields = (List<Object>) expectedObject;
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      return lazyCompareStruct(structTypeInfo, fields, expectedFields);
    } else if (lazyObject instanceof LazyUnion) {
      LazyUnion union = (LazyUnion) lazyObject;
      StandardUnionObjectInspector.StandardUnion expectedUnion = (StandardUnionObjectInspector.StandardUnion) expectedObject;
      UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
      return lazyCompareUnion(unionTypeInfo, union, expectedUnion);
    } else if (typeInfo instanceof UnionTypeInfo) {
      StandardUnionObjectInspector.StandardUnion expectedUnion = (StandardUnionObjectInspector.StandardUnion) expectedObject;
      UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
      if (lazyObject instanceof LazyBinaryUnion) {
        return lazyCompareUnion(unionTypeInfo, (LazyBinaryUnion) lazyObject, expectedUnion);
      } else {
        return lazyCompareUnion(unionTypeInfo, (UnionObject) lazyObject, expectedUnion);
      }
    } else {
      System.err.println("Not implemented " + typeInfo.getClass().getName());
    }
    return true;
  }
}