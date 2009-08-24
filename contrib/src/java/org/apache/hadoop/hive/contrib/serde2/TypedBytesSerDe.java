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
package org.apache.hadoop.hive.contrib.serde2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.util.typedbytes.Type;
import org.apache.hadoop.hive.ql.util.typedbytes.TypedBytesWritableInput;
import org.apache.hadoop.hive.ql.util.typedbytes.TypedBytesWritableOutput;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.ql.io.NonSyncDataOutputBuffer;
import org.apache.hadoop.hive.ql.io.NonSyncDataInputBuffer;

/**
 * TypedBytesSerDe uses typed bytes to serialize/deserialize.
 * 
 * More info on the typedbytes stuff that Dumbo uses.
 * http://issues.apache.org/jira/browse/HADOOP-1722 
 * A fast python decoder for this, which is apparently 25% faster than the python version is available at
 * http://github.com/klbostee/ctypedbytes/tree/master 
 */
public class TypedBytesSerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(TypedBytesSerDe.class.getName());
  
  int numColumns;
  StructObjectInspector rowOI;
  ArrayList<Object> row;
 
  BytesWritable serializeBytesWritable;
  NonSyncDataOutputBuffer barrStr;
  TypedBytesWritableOutput tbOut;
  
  NonSyncDataInputBuffer inBarrStr;
  TypedBytesWritableInput tbIn;
  
  List<String>   columnNames;
  List<TypeInfo> columnTypes;
  
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // We can get the table definition from tbl.
    serializeBytesWritable = new BytesWritable();
    barrStr = new NonSyncDataOutputBuffer();
    tbOut = new TypedBytesWritableOutput(barrStr);
    
    inBarrStr = new NonSyncDataInputBuffer();
    tbIn = new TypedBytesWritableInput(inBarrStr);
    
    // Read the configuration parameters
    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);

    columnNames = Arrays.asList(columnNameProperty.split(","));
    columnTypes = new ArrayList<TypeInfo>();
    List<String> columnTypeProps = Arrays.asList(columnTypeProperty.split(","));

    for (String colType : columnTypeProps) {
      columnTypes.add(TypeInfoUtils
                      .getTypeInfoFromTypeString(colType));
    }

    assert columnNames.size() == columnTypes.size();
    numColumns = columnNames.size(); 
    
    // All columns have to be primitive.
    for (int c = 0; c < numColumns; c++) {
      if (columnTypes.get(c).getCategory() != Category.PRIMITIVE) {
        throw new SerDeException(getClass().getName() 
            + " only accepts primitive columns, but column[" + c 
            + "] named " + columnNames.get(c) + " has category "
            + columnTypes.get(c).getCategory());
      }
    }
    
    // Constructing the row ObjectInspector:
    // The row consists of some string columns, each column will be a java 
    // String object.
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      columnOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(columnTypes.get(c)));
    }
    
    // StandardStruct uses ArrayList to store the row. 
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
    
    // Constructing the row object, etc, which will be reused for all rows.
    row = new ArrayList<Object>(numColumns);
    for (int c = 0; c < numColumns; c++) {
      row.add(null);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }
  
  @Override
  public Object deserialize(Writable blob) throws SerDeException {

    BytesWritable data = (BytesWritable)blob;
    inBarrStr.reset(data.get(), 0, data.getSize()-1);   
    
    try {

      for (int i=0; i<columnNames.size(); i++) {
        row.set(i, deserializeField(tbIn, columnTypes.get(i), row.get(i)));
      }

      // The next byte should be the marker
      assert tbIn.readTypeCode() == Type.ENDOFRECORD;
      
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    
    return row;
  }

  static Object deserializeField(TypedBytesWritableInput in, TypeInfo type, Object reuse) throws IOException {

    // read the type
    in.readType();

    switch (type.getCategory()) {
      case PRIMITIVE: {
        PrimitiveTypeInfo ptype = (PrimitiveTypeInfo)type;
        switch (ptype.getPrimitiveCategory()) {

          case VOID: {
            return null;
          }

          case BOOLEAN: {
            BooleanWritable r = reuse == null ? new BooleanWritable() : (BooleanWritable)reuse;
            r = (BooleanWritable)in.readBoolean(r);
            return r;
          }
          case BYTE: {
            ByteWritable r = reuse == null ? new ByteWritable() : (ByteWritable)reuse;
            r = (ByteWritable)in.readByte(r);
            return r;
          }
          case SHORT: {
            ShortWritable r = reuse == null ? new ShortWritable() : (ShortWritable)reuse;
            r = (ShortWritable)in.readShort(r);
            return r;
          }
          case INT: {
            IntWritable r = reuse == null ? new IntWritable() : (IntWritable)reuse;
            r = (IntWritable)in.readInt(r);
            return r;
          }
          case LONG: {
            LongWritable r = reuse == null ? new LongWritable() : (LongWritable)reuse;
            r = (LongWritable)in.readLong(r);
            return r;
          }
          case FLOAT: {
            FloatWritable r = reuse == null ? new FloatWritable() : (FloatWritable)reuse;
            r = (FloatWritable)in.readFloat(r);
            return r;
          }
          case DOUBLE: {
            DoubleWritable r = reuse == null ? new DoubleWritable() : (DoubleWritable)reuse;
            r = (DoubleWritable)in.readDouble(r);
            return r;
          }
          case STRING: {
            Text r = reuse == null ? new Text() : (Text)reuse;
            r = (Text)in.readText(r);
            return r;
          }
          default: {
            throw new RuntimeException("Unrecognized type: " + ptype.getPrimitiveCategory());
          }
        }
      }
      // Currently, deserialization of complex types is not supported
      case LIST: 
      case MAP:
      case STRUCT: 
      default: {
        throw new RuntimeException("Unsupported category: " + type.getCategory());
      }
    }
  }


  
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    try {
      barrStr.reset();
      StructObjectInspector soi = (StructObjectInspector)objInspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
    
      for (int i = 0; i < numColumns; i++) {
        Object o = soi.getStructFieldData(obj, fields.get(i));
        ObjectInspector oi = fields.get(i).getFieldObjectInspector(); 
        serializeField(o, oi, row.get(i));
      }
    
      // End of the record is part of the data
      tbOut.writeEndOfRecord();
      
      serializeBytesWritable.set(barrStr.getData(), 0, barrStr.getLength());
    } catch (IOException e) {
      throw new SerDeException(e.getMessage());
    }
    return serializeBytesWritable;
  }
   
  private byte[] tmpByteArr = new byte[1];
  
  private void serializeField(Object o, ObjectInspector oi, Object reuse) throws IOException {
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        switch (poi.getPrimitiveCategory()) {
          case VOID: {
            return;
          }
          case BOOLEAN: {
            BooleanObjectInspector boi = (BooleanObjectInspector)poi;
            BooleanWritable r = reuse == null ? new BooleanWritable() : (BooleanWritable)reuse;
            r.set(boi.get(o));
            tbOut.write(r);
            return;
          }
          case BYTE: {
            ByteObjectInspector boi = (ByteObjectInspector)poi;
            BytesWritable r = reuse == null ? new BytesWritable() : (BytesWritable)reuse;
            tmpByteArr[0] = boi.get(o);
            r.set(tmpByteArr, 0, 1);
            tbOut.write(r);
            return;
          }
          case SHORT: {
            ShortObjectInspector spoi = (ShortObjectInspector)poi;
            ShortWritable r = reuse == null ? new ShortWritable() : (ShortWritable)reuse;
            r.set(spoi.get(o));
            tbOut.write(r);
            return;
          }
          case INT: {
            IntObjectInspector ioi = (IntObjectInspector)poi;
            IntWritable r = reuse == null ? new IntWritable() : (IntWritable)reuse;
            r.set(ioi.get(o));
            tbOut.write(r);
            return;
          }
          case LONG: {
            LongObjectInspector loi = (LongObjectInspector)poi;
            LongWritable r = reuse == null ? new LongWritable() : (LongWritable)reuse;
            r.set(loi.get(o));
            tbOut.write(r);
            return;
          }
          case FLOAT: {
            FloatObjectInspector foi = (FloatObjectInspector)poi;
            FloatWritable r = reuse == null ? new FloatWritable() : (FloatWritable)reuse;
            r.set(foi.get(o));
            tbOut.write(r);
            return;
          }
          case DOUBLE: {
            DoubleObjectInspector doi = (DoubleObjectInspector)poi;
            DoubleWritable r = reuse == null ? new DoubleWritable() : (DoubleWritable)reuse;
            r.set(doi.get(o));
            tbOut.write(r);
            return;
          }
          case STRING: {
            StringObjectInspector soi = (StringObjectInspector)poi;
            Text t = soi.getPrimitiveWritableObject(o);
            tbOut.write(t);
            return;
          }
          default: {
            throw new RuntimeException("Unrecognized type: " + poi.getPrimitiveCategory());
          }
        }
      }
      case LIST: 
      case MAP:
      case STRUCT: {
        // For complex object, serialize to JSON format
        String s = SerDeUtils.getJSONString(o, oi);
        Text t = reuse == null ? new Text() : (Text)reuse;
        
        // convert to Text and write it
        t.set(s);
        tbOut.write(t);
      }
      default: {
        throw new RuntimeException("Unrecognized type: " + oi.getCategory());
      }
    }
  }
}
