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

package org.apache.hadoop.hive.contrib.util.typedbytes;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.io.NonSyncDataOutputBuffer;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * TypedBytesRecordReader.
 *
 */
public class TypedBytesRecordReader implements RecordReader {

  private DataInputStream din;
  private TypedBytesWritableInput tbIn;

  private final NonSyncDataOutputBuffer barrStr = new NonSyncDataOutputBuffer();
  private TypedBytesWritableOutput tbOut;

  private final ArrayList<Writable> row = new ArrayList<Writable>(0);
  private final ArrayList<String> rowTypeName = new ArrayList<String>(0);
  private List<String> columnTypes;

  private final ArrayList<ObjectInspector> srcOIns = new ArrayList<ObjectInspector>();
  private final ArrayList<ObjectInspector> dstOIns = new ArrayList<ObjectInspector>();
  private final ArrayList<Converter> converters = new ArrayList<Converter>();

  private static Map<Type, String> typedBytesToTypeName = new HashMap<Type, String>();
  static {
    typedBytesToTypeName.put(getType(1), serdeConstants.TINYINT_TYPE_NAME);
    typedBytesToTypeName.put(getType(2), serdeConstants.BOOLEAN_TYPE_NAME);
    typedBytesToTypeName.put(getType(3), serdeConstants.INT_TYPE_NAME);
    typedBytesToTypeName.put(getType(4), serdeConstants.BIGINT_TYPE_NAME);
    typedBytesToTypeName.put(getType(5), serdeConstants.FLOAT_TYPE_NAME);
    typedBytesToTypeName.put(getType(6), serdeConstants.DOUBLE_TYPE_NAME);
    typedBytesToTypeName.put(getType(7), serdeConstants.STRING_TYPE_NAME);
    typedBytesToTypeName.put(getType(11), serdeConstants.SMALLINT_TYPE_NAME);
  }

  public void initialize(InputStream in, Configuration conf, Properties tbl) throws IOException {
    din = new DataInputStream(in);
    tbIn = new TypedBytesWritableInput(din);
    tbOut = new TypedBytesWritableOutput(barrStr);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    columnTypes = Arrays.asList(columnTypeProperty.split(","));
    for (String columnType : columnTypes) {
      PrimitiveTypeInfo dstTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(columnType);
      dstOIns.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          dstTypeInfo));
    }
  }

  public Writable createRow() throws IOException {
    BytesWritable retWrit = new BytesWritable();
    return retWrit;
  }

  private Writable allocateWritable(Type type) {
    switch (type) {
    case BYTE:
      return new ByteWritable();
    case BOOL:
      return new BooleanWritable();
    case INT:
      return new IntWritable();
    case SHORT:
      return new ShortWritable();
    case LONG:
      return new LongWritable();
    case FLOAT:
      return new FloatWritable();
    case DOUBLE:
      return new DoubleWritable();
    case STRING:
      return new Text();
    default:
      assert false; // not supported
    }
    return null;
  }

  public int next(Writable data) throws IOException {
    int pos = 0;
    barrStr.reset();

    while (true) {
      Type type = tbIn.readTypeCode();

      // it was a empty stream
      if (type == null) {
        return -1;
      }

      if (type == Type.ENDOFRECORD) {
        tbOut.writeEndOfRecord();
        if (barrStr.getLength() > 0) {
          ((BytesWritable) data).set(barrStr.getData(), 0, barrStr.getLength());
        }
        return barrStr.getLength();
      }

      if (pos >= row.size()) {
        Writable wrt = allocateWritable(type);
        assert pos == row.size();
        assert pos == rowTypeName.size();
        row.add(wrt);
        rowTypeName.add(type.name());
        String typeName = typedBytesToTypeName.get(type);
        PrimitiveTypeInfo srcTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(typeName);
        srcOIns
            .add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                srcTypeInfo));
        converters.add(ObjectInspectorConverters.getConverter(srcOIns.get(pos),
            dstOIns.get(pos)));
      } else {
        if (!rowTypeName.get(pos).equals(type.name())) {
          throw new RuntimeException("datatype of row changed from "
              + rowTypeName.get(pos) + " to " + type.name());
        }
      }

      Writable w = row.get(pos);
      switch (type) {
      case BYTE:
        tbIn.readByte((ByteWritable) w);
        break;
      case BOOL:
        tbIn.readBoolean((BooleanWritable) w);
        break;
      case INT:
        tbIn.readInt((IntWritable) w);
        break;
      case SHORT:
        tbIn.readShort((ShortWritable) w);
        break;
      case LONG:
        tbIn.readLong((LongWritable) w);
        break;
      case FLOAT:
        tbIn.readFloat((FloatWritable) w);
        break;
      case DOUBLE:
        tbIn.readDouble((DoubleWritable) w);
        break;
      case STRING:
        tbIn.readText((Text) w);
        break;
      default:
        assert false; // should never come here
      }

      write(pos, w);
      pos++;
    }
  }

  private void write(int pos, Writable inpw) throws IOException {
    String typ = columnTypes.get(pos);

    Writable w = (Writable) converters.get(pos).convert(inpw);

    if (typ.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
      tbOut.writeBoolean((BooleanWritable) w);
    } else if (typ.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)) {
      tbOut.writeByte((ByteWritable) w);
    } else if (typ.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)) {
      tbOut.writeShort((ShortWritable) w);
    } else if (typ.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
      tbOut.writeInt((IntWritable) w);
    } else if (typ.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
      tbOut.writeLong((LongWritable) w);
    } else if (typ.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
      tbOut.writeFloat((FloatWritable) w);
    } else if (typ.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
      tbOut.writeDouble((DoubleWritable) w);
    } else if (typ.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {
      tbOut.writeText((Text) w);
    } else {
      assert false;
    }
  }

  public void close() throws IOException {
    if (din != null) {
      din.close();
    }
  }

  public static Type getType(int code) {
    for (Type type : Type.values()) {
      if (type.code == code) {
        return type;
      }
    }
    return null;
  }
}
