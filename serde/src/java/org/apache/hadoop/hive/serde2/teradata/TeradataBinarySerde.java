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

package org.apache.hadoop.hive.serde2.teradata;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.common.type.Date;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;

/**
 * https://cwiki.apache.org/confluence/display/Hive/TeradataBinarySerde.
 * TeradataBinarySerde handles the serialization and deserialization of Teradata Binary Record
 * passed from TeradataBinaryRecordReader.
 *
 * The Teradata Binary Record uses little-endian to handle the SHORT, INT, LONG, DOUBLE...
 * We extend SwappedDataInputStream to handle these types and extend to handle the Teradata
 * specific types like VARCHAR, CHAR, TIMESTAMP, DATE...
 *
 * Currently we support 11 Teradata data types: VARCHAR ,INTEGER, TIMESTAMP, FLOAT, DATE,
 * BYTEINT, BIGINT, CHARACTER, DECIMAL, SMALLINT, VARBYTE.
 * The mapping between Teradata data type and Hive data type is
 * Teradata Data Type: Hive Data Type
 * VARCHAR: VARCHAR,
 * INTEGER: INT,
 * TIMESTAMP: TIMESTAMP,
 * FLOAT: DOUBLE,
 * DATE: DATE,
 * BYTEINT: TINYINT ,
 * BIGINT: BIGINT,
 * CHAR: CHAR,
 * DECIMAL: DECIMAL,
 * SMALLINT: SMALLINT,
 * VARBYTE: BINARY.
 *
 * TeradataBinarySerde currently doesn't support complex types like MAP, ARRAY and STRUCT.
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS,
    serdeConstants.LIST_COLUMN_TYPES }) public class TeradataBinarySerde extends AbstractSerDe {
  private static final Log LOG = LogFactory.getLog(TeradataBinarySerde.class);

  public static final String TD_SCHEMA_LITERAL = "teradata.schema.literal";

  private StructObjectInspector rowOI;
  private ArrayList<Object> row;
  private byte[] inForNull;

  private int numCols;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  private TeradataBinaryDataOutputStream out;
  private BytesWritable serializeBytesWritable;
  private byte[] outForNull;

  public static final String TD_TIMESTAMP_PRECISION = "teradata.timestamp.precision";
  private int timestampPrecision;
  private static final int DEFAULT_TIMESTAMP_BYTE_NUM = 19;
  private static final String DEFAULT_TIMESTAMP_PRECISION = "6";

  public static final String TD_CHAR_SET = "teradata.char.charset";
  private String charCharset;
  private static final String DEFAULT_CHAR_CHARSET = "UNICODE";
  private static final Map<String, Integer> CHARSET_TO_BYTE_NUM = ImmutableMap.of("LATIN", 2, "UNICODE", 3);

  /**
   * Initialize the HiveSerializer.
   *
   * @param conf
   *          System properties. Can be null in compile time
   * @param tbl
   *          table properties
   * @throws SerDeException
   */
  @Override public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {
    columnNames = Arrays.asList(tbl.getProperty(serdeConstants.LIST_COLUMNS).split(","));

    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    LOG.debug(serdeConstants.LIST_COLUMN_TYPES + ": " + columnTypeProperty);
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    assert columnNames.size() == columnTypes.size();
    numCols = columnNames.size();

    // get the configured teradata timestamp precision
    // you can configure to generate timestamp of different precision in the binary file generated by TPT/BTEQ
    timestampPrecision = Integer.parseInt(tbl.getProperty(TD_TIMESTAMP_PRECISION, DEFAULT_TIMESTAMP_PRECISION));

    // get the configured teradata char charset
    // in TD, latin charset will have 2 bytes per char and unicode will have 3 bytes per char
    charCharset = tbl.getProperty(TD_CHAR_SET, DEFAULT_CHAR_CHARSET);
    if (!CHARSET_TO_BYTE_NUM.containsKey(charCharset)) {
      throw new SerDeException(
          format("%s isn't supported in Teradata Char Charset %s", charCharset, CHARSET_TO_BYTE_NUM.keySet()));
    }

    // All columns have to be primitive.
    // Constructing the row ObjectInspector:
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numCols);
    for (int i = 0; i < numCols; i++) {
      if (columnTypes.get(i).getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new SerDeException(
            getClass().getName() + " only accepts primitive columns, but column[" + i + "] named " + columnNames.get(i)
                + " has category " + columnTypes.get(i).getCategory());
      }
      columnOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(columnTypes.get(i)));
    }

    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

    // Constructing the row object and will be reused for all rows
    row = new ArrayList<Object>(numCols);
    for (int i = 0; i < numCols; i++) {
      row.add(null);
    }

    // Initialize vars related to Null Array which represents the null bitmap
    int byteNumForNullArray = (numCols / 8) + ((numCols % 8 == 0) ? 0 : 1);
    LOG.debug(format("The Null Bytes for each record will have %s bytes", byteNumForNullArray));
    inForNull = new byte[byteNumForNullArray];

    out = new TeradataBinaryDataOutputStream();
    serializeBytesWritable = new BytesWritable();
    outForNull = new byte[byteNumForNullArray];
  }

  /**
   * Returns the Writable class that would be returned by the serialize method.
   * This is used to initialize SequenceFile header.
   */
  @Override public Class<? extends Writable> getSerializedClass() {
    return ByteWritable.class;
  }

  /**
   * Serialize an object by navigating inside the Object with the
   * ObjectInspector. In most cases, the return value of this function will be
   * constant since the function will reuse the Writable object. If the client
   * wants to keep a copy of the Writable, the client needs to clone the
   * returned value.

   * @param obj
   * @param objInspector
   */
  @Override public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    try {
      out.reset();
      final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
      final List<? extends StructField> fieldRefs = outputRowOI.getAllStructFieldRefs();

      if (fieldRefs.size() != numCols) {
        throw new SerDeException(
            "Cannot serialize the object because there are " + fieldRefs.size() + " fieldRefs but the table defined "
                + numCols + " columns.");
      }

      // Fully refresh the Null Array to write into the out
      for (int i = 0; i < numCols; i++) {
        Object objectForField = outputRowOI.getStructFieldData(obj, fieldRefs.get(i));
        if (objectForField == null) {
          outForNull[i / 8] = (byte) (outForNull[i / 8] | (0x01 << (7 - (i % 8))));
        } else {
          outForNull[i / 8] = (byte) (outForNull[i / 8] & ~(0x01 << (7 - (i % 8))));
        }
      }
      out.write(outForNull);

      // serialize each field using FieldObjectInspector
      for (int i = 0; i < numCols; i++) {
        Object objectForField = outputRowOI.getStructFieldData(obj, fieldRefs.get(i));
        serializeField(objectForField, fieldRefs.get(i).getFieldObjectInspector(), columnTypes.get(i));
      }

      serializeBytesWritable.set(out.toByteArray(), 0, out.size());
      return serializeBytesWritable;
    } catch (IOException e) {
      throw new SerDeException(e);
    }
  }

  private void serializeField(Object objectForField, ObjectInspector oi, TypeInfo ti)
      throws IOException, SerDeException {
    switch (oi.getCategory()) {
    case PRIMITIVE:
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      switch (poi.getPrimitiveCategory()) {
      // Teradata Type: BYTEINT
      case BYTE:
        ByteObjectInspector boi = (ByteObjectInspector) poi;
        byte b = 0;
        if (objectForField != null) {
          b = boi.get(objectForField);
        }
        out.write(b);
        return;
      // Teradata Type: SMALLINT
      case SHORT:
        ShortObjectInspector spoi = (ShortObjectInspector) poi;
        short s = 0;
        if (objectForField != null) {
          s = spoi.get(objectForField);
        }
        out.writeShort(s);
        return;
      // Teradata Type: INT
      case INT:
        IntObjectInspector ioi = (IntObjectInspector) poi;
        int i = 0;
        if (objectForField != null) {
          i = ioi.get(objectForField);
        }
        out.writeInt(i);
        return;
      // Teradata Type: BIGINT
      case LONG:
        LongObjectInspector loi = (LongObjectInspector) poi;
        long l = 0;
        if (objectForField != null) {
          l = loi.get(objectForField);
        }
        out.writeLong(l);
        return;
      // Teradata Type: FLOAT
      case DOUBLE:
        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
        double d = 0;
        if (objectForField != null) {
          d = doi.get(objectForField);
        }
        out.writeDouble(d);
        return;
      // Teradata Type: VARCHAR
      case VARCHAR:
        HiveVarcharObjectInspector hvoi = (HiveVarcharObjectInspector) poi;
        HiveVarcharWritable hv = hvoi.getPrimitiveWritableObject(objectForField);
        // assert the length of varchar record fits into the table definition
        if (hv != null) {
          assert ((VarcharTypeInfo) ti).getLength() >= hv.getHiveVarchar().getCharacterLength();
        }
        out.writeVarChar(hv);
        return;
      // Teradata Type: TIMESTAMP
      case TIMESTAMP:
        TimestampObjectInspector tsoi = (TimestampObjectInspector) poi;
        TimestampWritableV2 ts = tsoi.getPrimitiveWritableObject(objectForField);
        out.writeTimestamp(ts, getTimeStampByteNum(timestampPrecision));
        return;
      // Teradata Type: DATE
      case DATE:
        DateObjectInspector dtoi = (DateObjectInspector) poi;
        DateWritableV2 dw = dtoi.getPrimitiveWritableObject(objectForField);
        out.writeDate(dw);
        return;
      // Teradata Type: CHAR
      case CHAR:
        HiveCharObjectInspector coi = (HiveCharObjectInspector) poi;
        HiveCharWritable hc = coi.getPrimitiveWritableObject(objectForField);
        // assert the length of char record fits into the table definition
        if (hc != null) {
          assert ((CharTypeInfo) ti).getLength() >= hc.getHiveChar().getCharacterLength();
        }
        out.writeChar(hc, getCharByteNum(charCharset) * ((CharTypeInfo) ti).getLength());
        return;
      // Teradata Type: DECIMAL
      case DECIMAL:
        DecimalTypeInfo dtype = (DecimalTypeInfo) ti;
        int precision = dtype.precision();
        int scale = dtype.scale();
        HiveDecimalObjectInspector hdoi = (HiveDecimalObjectInspector) poi;
        HiveDecimalWritable hd = hdoi.getPrimitiveWritableObject(objectForField);
        // assert the precision of decimal record fits into the table definition
        if (hd != null) {
          assert (dtype.getPrecision() >= hd.precision());
        }
        out.writeDecimal(hd, getDecimalByteNum(precision), scale);
        return;
      // Teradata Type: VARBYTE
      case BINARY:
        BinaryObjectInspector bnoi = (BinaryObjectInspector) poi;
        BytesWritable byw = bnoi.getPrimitiveWritableObject(objectForField);
        out.writeVarByte(byw);
        return;
      default:
        throw new SerDeException("Unrecognized type: " + poi.getPrimitiveCategory());
      }
      // Currently, serialization of complex types is not supported
    case LIST:
    case MAP:
    case STRUCT:
    default:
      throw new SerDeException("Unrecognized type: " + oi.getCategory());
    }
  }

  @Override public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  /**
   * Deserialize an object out of a Writable blob. In most cases, the return
   * value of this function will be constant since the function will reuse the
   * returned object. If the client wants to keep a copy of the object, the
   * client needs to clone the returned value by calling
   * ObjectInspectorUtils.getStandardObject().
   *
   * @param blob
   *          The Writable object containing a serialized object
   * @return A Java object representing the contents in the blob.
   */
  @Override public Object deserialize(Writable blob) throws SerDeException {
    try {
      BytesWritable data = (BytesWritable) blob;

      // initialize the data to be the input stream
      TeradataBinaryDataInputStream in =
          new TeradataBinaryDataInputStream(new ByteArrayInputStream(data.getBytes(), 0, data.getLength()));

      int numOfByteRead = in.read(inForNull);

      if (inForNull.length != 0 && numOfByteRead != inForNull.length) {
        throw new EOFException("not enough bytes for one object");
      }

      boolean isNull;
      for (int i = 0; i < numCols; i++) {
        // get if the ith field is null or not
        isNull = ((inForNull[i / 8] & (128 >> (i % 8))) != 0);
        row.set(i, deserializeField(in, columnTypes.get(i), row.get(i), isNull));
      }

      //After deserializing all the fields, the input should be over in which case in.read will return -1
      if (in.read() != -1) {
        throw new EOFException("The inputstream has more after we deserialize all the fields - this is unexpected");
      }
    } catch (EOFException e) {
      LOG.warn("Catch thrown exception", e);
      LOG.warn("This record has been polluted. We have reset all the row fields to be null");
      for (int i = 0; i < numCols; i++) {
        row.set(i, null);
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    } catch (ParseException e) {
      throw new SerDeException(e);
    }
    return row;
  }

  private Object deserializeField(TeradataBinaryDataInputStream in, TypeInfo type, Object reuse, boolean isNull)
      throws IOException, ParseException, SerDeException {
    // isNull:
    // In the Teradata Binary file, even the field is null (isNull=true),
    // thd data still has some default values to pad the record.
    // In this case, you cannot avoid reading the bytes even it is not used.
    switch (type.getCategory()) {
    case PRIMITIVE:
      PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
      switch (ptype.getPrimitiveCategory()) {
      case VARCHAR: // Teradata Type: VARCHAR
        String st = in.readVarchar();
        if (isNull) {
          return null;
        } else {
          HiveVarcharWritable r = reuse == null ? new HiveVarcharWritable() : (HiveVarcharWritable) reuse;
          r.set(st, ((VarcharTypeInfo) type).getLength());
          return r;
        }
      case INT: // Teradata Type: INT
        int i = in.readInt();
        if (isNull) {
          return null;
        } else {
          IntWritable r = reuse == null ? new IntWritable() : (IntWritable) reuse;
          r.set(i);
          return r;
        }
      case TIMESTAMP: // Teradata Type: TIMESTAMP
        Timestamp ts = in.readTimestamp(getTimeStampByteNum(timestampPrecision));
        if (isNull) {
          return null;
        } else {
          TimestampWritableV2 r = reuse == null ? new TimestampWritableV2() : (TimestampWritableV2) reuse;
          r.set(ts);
          return r;
        }
      case DOUBLE: // Teradata Type: FLOAT
        double d = in.readDouble();
        if (isNull) {
          return null;
        } else {
          DoubleWritable r = reuse == null ? new DoubleWritable() : (DoubleWritable) reuse;
          r.set(d);
          return r;
        }
      case DATE: // Teradata Type: DATE
        Date dt = in.readDate();
        if (isNull) {
          return null;
        } else {
          DateWritableV2 r = reuse == null ? new DateWritableV2() : (DateWritableV2) reuse;
          r.set(dt);
          return r;
        }
      case BYTE: // Teradata Type: BYTEINT
        byte bt = in.readByte();
        if (isNull) {
          return null;
        } else {
          ByteWritable r = reuse == null ? new ByteWritable() : (ByteWritable) reuse;
          r.set(bt);
          return r;
        }
      case LONG: // Teradata Type: BIGINT
        long l = in.readLong();
        if (isNull) {
          return null;
        } else {
          LongWritable r = reuse == null ? new LongWritable() : (LongWritable) reuse;
          r.set(l);
          return r;
        }
      case CHAR: // Teradata Type: CHAR
        CharTypeInfo ctype = (CharTypeInfo) type;
        int length = ctype.getLength();
        String c = in.readChar(length * getCharByteNum(charCharset));
        if (isNull) {
          return null;
        } else {
          HiveCharWritable r = reuse == null ? new HiveCharWritable() : (HiveCharWritable) reuse;
          r.set(c, length);
          return r;
        }
      case DECIMAL: // Teradata Type: DECIMAL
        DecimalTypeInfo dtype = (DecimalTypeInfo) type;
        int precision = dtype.precision();
        int scale = dtype.scale();
        HiveDecimal hd = in.readDecimal(scale, getDecimalByteNum(precision));
        if (isNull) {
          return null;
        } else {
          HiveDecimalWritable r = (reuse == null ? new HiveDecimalWritable() : (HiveDecimalWritable) reuse);
          r.set(hd);
          return r;
        }
      case SHORT: // Teradata Type: SMALLINT
        short s = in.readShort();
        if (isNull) {
          return null;
        } else {
          ShortWritable r = reuse == null ? new ShortWritable() : (ShortWritable) reuse;
          r.set(s);
          return r;
        }
      case BINARY: // Teradata Type: VARBYTE
        byte[] content = in.readVarbyte();
        if (isNull) {
          return null;
        } else {
          BytesWritable r = new BytesWritable();
          r.set(content, 0, content.length);
          return r;
        }
      default:
        throw new SerDeException("Unrecognized type: " + ptype.getPrimitiveCategory());
      }
      // Currently, deserialization of complex types is not supported
    case LIST:
    case MAP:
    case STRUCT:
    default:
      throw new SerDeException("Unsupported category: " + type.getCategory());
    }
  }

  /**
   * Get the object inspector that can be used to navigate through the internal
   * structure of the Object returned from deserialize(...).
   */
  @Override public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  private int getTimeStampByteNum(int precision) {
    if (precision == 0) {
      return DEFAULT_TIMESTAMP_BYTE_NUM;
    } else {
      return precision + 1 + DEFAULT_TIMESTAMP_BYTE_NUM;
    }
  }

  private int getCharByteNum(String charset) throws SerDeException {
    if (!CHARSET_TO_BYTE_NUM.containsKey(charCharset)) {
      throw new SerDeException(
          format("%s isn't supported in Teradata Char Charset %s", charCharset, CHARSET_TO_BYTE_NUM.keySet()));
    } else {
      return CHARSET_TO_BYTE_NUM.get(charset);
    }
  }

  private int getDecimalByteNum(int precision) throws SerDeException {
    if (precision <= 0) {
      throw new SerDeException(format("the precision of Decimal should be bigger than 0. %d is illegal", precision));
    }
    if (precision <= 2) {
      return 1;
    }
    if (precision <= 4) {
      return 2;
    }
    if (precision <= 9) {
      return 4;
    }
    if (precision <= 18) {
      return 8;
    }
    if (precision <= 38) {
      return 16;
    }
    throw new IllegalArgumentException(
        format("the precision of Decimal should be smaller than 39. %d is illegal", precision));
  }
}
