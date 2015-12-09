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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto.PartitionKeyComparator.Operator.Type;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;

import com.google.protobuf.InvalidProtocolBufferException;

public class PartitionKeyComparator extends ByteArrayComparable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionKeyComparator.class);
  static class Mark {
    Mark(String value, boolean inclusive) {
      this.value = value;
      this.inclusive = inclusive;
    }
    String value;
    boolean inclusive;
    public String toString() {
      return value + (inclusive?"_":"");
    }
  }
  static class Range {
    Range(String keyName, Mark start, Mark end) {
      this.keyName = keyName;
      this.start = start;
      this.end = end;
    }
    String keyName;
    Mark start;
    Mark end;
    public String toString() {
      return "" + keyName + ":" + (start!=null?start.toString():"") + (end!=null?end.toString():"");
    }
  }
  // Cache the information derived from ranges for performance, including
  // range in native datatype
  static class NativeRange {
    int pos;
    Comparable start;
    Comparable end;
  }
  static class Operator {
    public Operator(Type type, String keyName, String val) {
      this.type = type;
      this.keyName = keyName;
      this.val = val;
    }
    enum Type {
      LIKE, NOTEQUALS
    };
    Type type;
    String keyName;
    String val;
  }
  static class NativeOperator {
    int pos;
    Comparable val;
  }
  String names;
  String types;
  List<Range> ranges;
  List<NativeRange> nativeRanges;
  List<Operator> ops;
  List<NativeOperator> nativeOps;
  Properties serdeProps;
  public PartitionKeyComparator(String names, String types, List<Range> ranges, List<Operator> ops) {
    super(null);
    this.names = names;
    this.types = types;
    this.ranges = ranges;
    this.ops = ops;
    serdeProps = new Properties();
    serdeProps.setProperty(serdeConstants.LIST_COLUMNS, "dbName,tableName," + names);
    serdeProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string," + types);

    this.nativeRanges = new ArrayList<NativeRange>(this.ranges.size());
    for (int i=0;i<ranges.size();i++) {
      Range range = ranges.get(i);
      NativeRange nativeRange = new NativeRange();;
      nativeRanges.add(i, nativeRange);
      nativeRange.pos = Arrays.asList(names.split(",")).indexOf(range.keyName);
      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(types.split(",")[nativeRange.pos]);
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(expectedType);
      nativeRange.start = null;
      if (range.start != null) {
        Converter converter = ObjectInspectorConverters.getConverter(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);
        nativeRange.start = (Comparable)converter.convert(range.start.value);
      }
      nativeRange.end = null;
      if (range.end != null) {
        Converter converter = ObjectInspectorConverters.getConverter(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);
        nativeRange.end = (Comparable)converter.convert(range.end.value);
      }
    }

    this.nativeOps = new ArrayList<NativeOperator>(this.ops.size());
    for (int i=0;i<ops.size();i++) {
      Operator op = ops.get(i);
      NativeOperator nativeOp = new NativeOperator();
      nativeOps.add(i, nativeOp);
      nativeOp.pos = ArrayUtils.indexOf(names.split(","), op.keyName);
      TypeInfo expectedType =
          TypeInfoUtils.getTypeInfoFromTypeString(types.split(",")[nativeOp.pos]);
      ObjectInspector outputOI =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(expectedType);
      Converter converter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, outputOI);
      nativeOp.val = (Comparable)converter.convert(op.val);
    }
  }

  public static PartitionKeyComparator parseFrom(final byte [] bytes) {
    HbaseMetastoreProto.PartitionKeyComparator proto;
    try {
      proto = HbaseMetastoreProto.PartitionKeyComparator.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    List<Range> ranges = new ArrayList<Range>();
    for (HbaseMetastoreProto.PartitionKeyComparator.Range range : proto.getRangeList()) {
      Mark start = null;
      if (range.hasStart()) {
        start = new Mark(range.getStart().getValue(), range.getStart().getInclusive());
      }
      Mark end = null;
      if (range.hasEnd()) {
        end = new Mark(range.getEnd().getValue(), range.getEnd().getInclusive());
      }
      ranges.add(new Range(range.getKey(), start, end));
    }
    List<Operator> ops = new ArrayList<Operator>();
    for (HbaseMetastoreProto.PartitionKeyComparator.Operator op : proto.getOpList()) {
      ops.add(new Operator(Operator.Type.valueOf(op.getType().name()), op.getKey(),
          op.getVal()));
    }
    return new PartitionKeyComparator(proto.getNames(), proto.getTypes(), ranges, ops);
  }

  @Override
  public byte[] toByteArray() {
    HbaseMetastoreProto.PartitionKeyComparator.Builder builder = 
        HbaseMetastoreProto.PartitionKeyComparator.newBuilder();
    builder.setNames(names);
    builder.setTypes(types);
    for (int i=0;i<ranges.size();i++) {
      Range range = ranges.get(i);
      HbaseMetastoreProto.PartitionKeyComparator.Mark startMark = null;
      if (range.start != null) {
        startMark = HbaseMetastoreProto.PartitionKeyComparator.Mark.newBuilder()
          .setValue(range.start.value)
          .setInclusive(range.start.inclusive)
          .build();
      }
      HbaseMetastoreProto.PartitionKeyComparator.Mark endMark = null;
      if (range.end != null) {
        endMark = HbaseMetastoreProto.PartitionKeyComparator.Mark.newBuilder()
          .setValue(range.end.value)
          .setInclusive(range.end.inclusive)
          .build();
      }
        
      HbaseMetastoreProto.PartitionKeyComparator.Range.Builder rangeBuilder = 
        HbaseMetastoreProto.PartitionKeyComparator.Range.newBuilder();
      rangeBuilder.setKey(range.keyName);
      if (startMark != null) {
        rangeBuilder.setStart(startMark);
      }
      if (endMark != null) {
        rangeBuilder.setEnd(endMark);
      }
      builder.addRange(rangeBuilder.build());
    }
    for (int i=0;i<ops.size();i++) {
      Operator op = ops.get(i);
      builder.addOp(HbaseMetastoreProto.PartitionKeyComparator.Operator.newBuilder()
        .setKey(op.keyName)
        .setType(Type.valueOf(op.type.toString()))
        .setVal(op.val).build());
    }
    return builder.build().toByteArray();
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    byte[] bytes = Arrays.copyOfRange(value, offset, offset + length);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Get key " + new String(bytes));
    }
    BinarySortableSerDe serDe = new BinarySortableSerDe();
    List deserializedkeys = null;
    try {
      serDe.initialize(new Configuration(), serdeProps);
      deserializedkeys = ((List)serDe.deserialize(new BytesWritable(bytes))).subList(2, 2 + names.split(",").length);
    } catch (SerDeException e) {
      // don't bother with failed deserialization, continue with next key
      return 1;
    }
    for (int i=0;i<ranges.size();i++) {
      Range range = ranges.get(i);
      NativeRange nativeRange = nativeRanges.get(i);

      Comparable partVal = (Comparable)deserializedkeys.get(nativeRange.pos);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Try to match range " + partVal + ", start " + nativeRange.start + ", end "
            + nativeRange.end);
      }
      if (range.start == null || range.start.inclusive && partVal.compareTo(nativeRange.start)>=0 ||
          !range.start.inclusive && partVal.compareTo(nativeRange.start)>0) {
        if (range.end == null || range.end.inclusive && partVal.compareTo(nativeRange.end)<=0 ||
            !range.end.inclusive && partVal.compareTo(nativeRange.end)<0) {
          continue;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fail to match range " + range.keyName + "-" + partVal + "[" + nativeRange.start
            + "," + nativeRange.end + "]");
      }
      return 1;
    }

    for (int i=0;i<ops.size();i++) {
      Operator op = ops.get(i);
      NativeOperator nativeOp = nativeOps.get(i);
      switch (op.type) {
      case LIKE:
        if (!deserializedkeys.get(nativeOp.pos).toString().matches(op.val)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Fail to match operator " + op.keyName + "(" + deserializedkeys.get(nativeOp.pos)
                + ") LIKE " + nativeOp.val);
          }
          return 1;
        }
        break;
      case NOTEQUALS:
        if (nativeOp.val.equals(deserializedkeys.get(nativeOp.pos))) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Fail to match operator " + op.keyName + "(" + deserializedkeys.get(nativeOp.pos)
                + ")!=" + nativeOp.val);
          }
          return 1;
        }
        break;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("All conditions satisfied:" + deserializedkeys);
    }
    return 0;
  }

}
