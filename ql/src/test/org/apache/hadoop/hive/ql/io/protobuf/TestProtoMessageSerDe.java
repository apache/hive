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

package org.apache.hadoop.hive.ql.io.protobuf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.protobuf.SampleProtos.AllTypes;
import org.apache.hadoop.hive.ql.io.protobuf.SampleProtos.AllTypes.Enum1;
import org.apache.hadoop.hive.ql.io.protobuf.ProtobufBytesWritableSerDe;
import org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe;
import org.apache.hadoop.hive.ql.io.protobuf.ProtobufSerDe;
import org.apache.hadoop.hive.ql.io.protobuf.SampleProtos.MapFieldEntry;
import org.apache.hadoop.hive.ql.io.protobuf.SampleProtos.Mesg1;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWritable;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * Test class for ProtobufSerDe.
 */
public class TestProtoMessageSerDe {
  private static ObjectInspector stroi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  private static ObjectInspector intoi = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  private static ObjectInspector strmapoi = ObjectInspectorFactory.getStandardMapObjectInspector(
      stroi, stroi);
  private static ObjectInspector mfoi = structoi(list("key", "value"), list(stroi, stroi));
  private static ObjectInspector m1oi = structoi(list("anotherMap", "noMap", "intList"),
      list(strmapoi, mfoi, listoi(intoi)));

  private ProtobufSerDe serde;

  private Configuration conf = new Configuration(false);
  @SuppressWarnings("unchecked")
  private <T extends Message> ProtoMessageWritable<T> init(Class<T> clazz, String mapTypes)
      throws Exception {
    serde = new ProtobufMessageSerDe();
    Properties tbl = new Properties();
    tbl.setProperty(ProtobufSerDe.PROTO_CLASS, clazz.getName());
    tbl.setProperty(ProtobufSerDe.MAP_TYPES, mapTypes);
    serde.initialize(conf, tbl, null);

    @SuppressWarnings("rawtypes")
    Constructor<ProtoMessageWritable> cons = ProtoMessageWritable.class.getDeclaredConstructor(
        Parser.class);
    cons.setAccessible(true);
    return cons.newInstance((Parser<T>)clazz.getField("PARSER").get(null));
  }

  private MapFieldEntry makeMap(int i) {
    return MapFieldEntry.newBuilder().setKey("key" + i).setValue("val" + i).build();
  }

  private Mesg1 makeMesg1(int start) {
    return Mesg1.newBuilder()
        .addAnotherMap(makeMap(start + 1)).addAnotherMap(makeMap(start + 2))
        .setNoMap(makeMap(start + 3))
        .addIntList(start + 4).addIntList(start + 5).build();
  }

  @Test
  public void testSimpleMessage() throws Exception {
    ProtoMessageWritable<MapFieldEntry> writable = init(MapFieldEntry.class,
        " MapFieldEntry     , Invalid  ");
    assertEquals(mfoi, serde.getObjectInspector());

    writable.setMessage(MapFieldEntry.getDefaultInstance());
    assertArrayEquals(arr(null, null), (Object[])serde.deserialize(writable));

    MapFieldEntry proto = makeMap(1);
    writable.setMessage(proto);
    Object obj = serde.deserialize(writable);
    assertTrue(obj instanceof Object[]);
    assertArrayEquals(arr(proto.getKey(), proto.getValue()), (Object[])obj);
  }

  @Test
  public void testMapAndList() throws Exception {
    ProtoMessageWritable<Mesg1> writable = init(Mesg1.class, "MapFieldEntry,Invalid");
    assertEquals(m1oi, serde.getObjectInspector());

    writable.setMessage(Mesg1.getDefaultInstance());
    assertArrayEquals(arr(null, null, null), (Object[])serde.deserialize(writable));

    Mesg1 proto = makeMesg1(0);
    writable.setMessage(proto);
    assertArrayEquals(arr(map("key1", "val1", "key2", "val2"), arr("key3", "val3"), arr(4, 5)),
        (Object[])serde.deserialize(writable));
  }

  @Test
  public void testMapAndListNoMapConfigured() throws Exception {
    ProtoMessageWritable<Mesg1> writable = init(Mesg1.class, "");
    assertEquals(structoi(list("anotherMap", "noMap", "intList"),
        list(listoi(mfoi), mfoi, listoi(intoi))), serde.getObjectInspector());

    writable.setMessage(Mesg1.getDefaultInstance());
    assertArrayEquals(arr(null, null, null), (Object[])serde.deserialize(writable));

    Mesg1 proto = makeMesg1(0);
    writable.setMessage(proto);
    assertArrayEquals(arr(arr(arr("key1", "val1"), arr("key2", "val2")), arr("key3", "val3"),
        arr(4, 5)), (Object[])serde.deserialize(writable));
  }

  @Test
  public void testAll() throws Exception {
    ProtoMessageWritable<AllTypes> writable = init(AllTypes.class, "MapFieldEntry");
    ObjectInspector oi = structoi(
        list("doubleType", "floatType", "int32Type", "int64Type", "uint32Type", "uint64Type",
             "sint32Type", "sint64Type", "fixed32Type", "fixed64Type", "sfixed32Type",
             "sfixed64Type", "boolType", "stringType", "bytesType", "mapType", "stringListType",
             "messageType", "messageListType", "enumType"),
        list(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
            strmapoi,
            listoi(stroi),
            m1oi,
            listoi(m1oi),
            stroi)
        );
    assertEquals(oi, serde.getObjectInspector());

    writable.setMessage(AllTypes.getDefaultInstance());
    assertArrayEquals(arr(null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null), (Object[])serde.deserialize(writable));

    AllTypes proto = AllTypes.newBuilder()
        .setDoubleType(1.0)
        .setFloatType(2.0f)
        .setInt32Type(3)
        .setInt64Type(4)
        .setUint32Type(5)
        .setUint64Type(6)
        .setSint32Type(7)
        .setSint64Type(8)
        .setFixed32Type(9)
        .setFixed64Type(10)
        .setSfixed32Type(11)
        .setSfixed64Type(12)
        .setBoolType(true)
        .setStringType("val13")
        .setBytesType(ByteString.copyFrom(new byte[] {14, 15}))
        .addMapType(makeMap(16))
        .addMapType(makeMap(17))
        .addStringListType("val18")
        .addStringListType("val19")
        .setMessageType(makeMesg1(19))
        .addMessageListType(makeMesg1(24))
        .addMessageListType(makeMesg1(29))
        .setEnumType(Enum1.VAL1)
        .build();
    writable.setMessage(proto);
    assertArrayEquals(arr(1.0d, 2.0f, 3, 4L, 5, 6L, 7, 8L, 9, 10L, 11, 12L, true, "val13",
        new byte[] {14, 15}, map("key16", "val16", "key17", "val17"), arr("val18", "val19"),
        arr(map("key20", "val20", "key21", "val21"), arr("key22", "val22"), arr(23, 24)),
        arr(arr(map("key25", "val25", "key26", "val26"), arr("key27", "val27"), arr(28, 29)),
        arr(map("key30", "val30", "key31", "val31"), arr("key32", "val32"), arr(33, 34))), "VAL1"),
        (Object[])serde.deserialize(writable));
  }

  @Test
  public void testBytesWritable() throws Exception {
    serde = new ProtobufBytesWritableSerDe();
    Properties tbl = new Properties();
    tbl.setProperty(ProtobufSerDe.PROTO_CLASS, MapFieldEntry.class.getName());
    tbl.setProperty(ProtobufSerDe.MAP_TYPES, "MapFieldEntry");
    serde.initialize(conf, tbl, null);
    assertEquals(mfoi, serde.getObjectInspector());

    BytesWritable writable = new BytesWritable(MapFieldEntry.getDefaultInstance().toByteArray());
    assertArrayEquals(arr(null, null), (Object[])serde.deserialize(writable));

    MapFieldEntry proto = makeMap(1);
    writable = new BytesWritable(proto.toByteArray());
    Object obj = serde.deserialize(writable);
    assertTrue(obj instanceof Object[]);
    assertArrayEquals(arr(proto.getKey(), proto.getValue()), (Object[])obj);
  }

  private static ObjectInspector structoi(List<String> names, List<ObjectInspector> ois) {
    return ObjectInspectorFactory.getStandardStructObjectInspector(names, ois);
  }

  private static ObjectInspector listoi(ObjectInspector oi) {
    return ObjectInspectorFactory.getStandardListObjectInspector(oi);
  }

  @SafeVarargs
  private static <T> List<T> list(T ... ts) {
    return Arrays.asList(ts);
  }

  private static Map<String, String> map(String ... s) {
    Map<String, String> ret = new HashMap<>();
    for (int i = 0; i < s.length; i += 2) {
      ret.put(s[i], s[i + 1]);
    }
    return ret;
  }

  private static Object[] arr(Object ... objs) {
    return objs;
  }
}
