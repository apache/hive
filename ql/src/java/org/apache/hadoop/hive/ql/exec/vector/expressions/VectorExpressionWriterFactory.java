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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * VectorExpressionWritableFactory helper class for generating VectorExpressionWritable objects.
 */
public final class VectorExpressionWriterFactory {

  /**
   * VectorExpressionWriter base implementation, to be specialized for Long/Double/Bytes columns
   */
  private static abstract class VectorExpressionWriterBase implements VectorExpressionWriter {

    protected ObjectInspector objectInspector;

    /**
     * The object inspector associated with this expression. This is created from the expression
     * NodeDesc (compile metadata) not from the VectorColumn info and thus preserves the type info
     * lost by the vectorization process.
     */
    public ObjectInspector getObjectInspector() {
      return objectInspector;
    }

    public VectorExpressionWriter init(ObjectInspector objectInspector) throws HiveException {
      this.objectInspector = objectInspector;
      return this;
    }
    
    /**
     * The base implementation must be overridden by the Long specialization
     */
    @Override
    public Object writeValue(long value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }
    
    /**
     * The base implementation must be overridden by the Long specialization
     */
    public Object setValue(Object field, long value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Double specialization
     */
    @Override
    public Object writeValue(double value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Double specialization
     */
    public Object setValue(Object field, double value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }
    
    /**
     * The base implementation must be overridden by the Bytes specialization
     */
    @Override
    public Object writeValue(byte[] value, int start, int length) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }
    
    /**
     * The base implementation must be overridden by the Bytes specialization
     */
    public Object setValue(Object field, byte[] value, int start, int length) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }
  }

  /**
   * Specialized writer for LongVectorColumn expressions. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterLong
    extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      LongColumnVector lcv = (LongColumnVector) column;
      if (lcv.noNulls && !lcv.isRepeating) {
        return writeValue(lcv.vector[row]);
      } else if (lcv.noNulls && lcv.isRepeating) {
        return writeValue(lcv.vector[0]);
      } else if (!lcv.noNulls && !lcv.isRepeating && !lcv.isNull[row]) {
        return writeValue(lcv.vector[row]);
      } else if (!lcv.noNulls && !lcv.isRepeating && lcv.isNull[row]) {
        return null;
      } else if (!lcv.noNulls && lcv.isRepeating && !lcv.isNull[0]) {
        return writeValue(lcv.vector[0]);
      } else if (!lcv.noNulls && lcv.isRepeating && lcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, lcv.noNulls, lcv.isRepeating, lcv.isNull[row], lcv.isNull[0]));
    }
    
    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      LongColumnVector lcv = (LongColumnVector) column;
      if (lcv.noNulls && !lcv.isRepeating) {
        return setValue(field, lcv.vector[row]);
      } else if (lcv.noNulls && lcv.isRepeating) {
        return setValue(field, lcv.vector[0]);
      } else if (!lcv.noNulls && !lcv.isRepeating && !lcv.isNull[row]) {
        return setValue(field, lcv.vector[row]);
      } else if (!lcv.noNulls && !lcv.isRepeating && lcv.isNull[row]) {
        return null;
      } else if (!lcv.noNulls && lcv.isRepeating && !lcv.isNull[0]) {
        return setValue(field, lcv.vector[0]);
      } else if (!lcv.noNulls && lcv.isRepeating && lcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, lcv.noNulls, lcv.isRepeating, lcv.isNull[row], lcv.isNull[0]));
    }    
  }

  /**
   * Specialized writer for DoubleColumnVector. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterDouble extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      DoubleColumnVector dcv = (DoubleColumnVector) column;
      if (dcv.noNulls && !dcv.isRepeating) {
        return writeValue(dcv.vector[row]);
      } else if (dcv.noNulls && dcv.isRepeating) {
        return writeValue(dcv.vector[0]);
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return writeValue(dcv.vector[row]);
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return writeValue(dcv.vector[0]);
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }
    
    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      DoubleColumnVector dcv = (DoubleColumnVector) column;
      if (dcv.noNulls && !dcv.isRepeating) {
        return setValue(field, dcv.vector[row]);
      } else if (dcv.noNulls && dcv.isRepeating) {
        return setValue(field, dcv.vector[0]);
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return setValue(field, dcv.vector[row]);
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return setValue(field, dcv.vector[0]);
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }    
   }

  /**
   * Specialized writer for BytesColumnVector. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterBytes extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      BytesColumnVector bcv = (BytesColumnVector) column;
      if (bcv.noNulls && !bcv.isRepeating) {
        return writeValue(bcv.vector[row], bcv.start[row], bcv.length[row]);
      } else if (bcv.noNulls && bcv.isRepeating) {
        return writeValue(bcv.vector[0], bcv.start[0], bcv.length[0]);
      } else if (!bcv.noNulls && !bcv.isRepeating && !bcv.isNull[row]) {
        return writeValue(bcv.vector[row], bcv.start[row], bcv.length[row]);
      } else if (!bcv.noNulls && !bcv.isRepeating && bcv.isNull[row]) {
        return null;
      } else if (!bcv.noNulls && bcv.isRepeating && !bcv.isNull[0]) {
        return writeValue(bcv.vector[0], bcv.start[0], bcv.length[0]);
      } else if (!bcv.noNulls && bcv.isRepeating && bcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, bcv.noNulls, bcv.isRepeating, bcv.isNull[row], bcv.isNull[0]));
    }
    
    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      BytesColumnVector bcv = (BytesColumnVector) column;
      if (bcv.noNulls && !bcv.isRepeating) {
        return setValue(field, bcv.vector[row], bcv.start[row], bcv.length[row]);
      } else if (bcv.noNulls && bcv.isRepeating) {
        return setValue(field, bcv.vector[0], bcv.start[0], bcv.length[0]);
      } else if (!bcv.noNulls && !bcv.isRepeating && !bcv.isNull[row]) {
        return setValue(field, bcv.vector[row], bcv.start[row], bcv.length[row]);
      } else if (!bcv.noNulls && !bcv.isRepeating && bcv.isNull[row]) {
        return null;
      } else if (!bcv.noNulls && bcv.isRepeating && !bcv.isNull[0]) {
        return setValue(field, bcv.vector[0], bcv.start[0], bcv.length[0]);
      } else if (!bcv.noNulls && bcv.isRepeating && bcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, bcv.noNulls, bcv.isRepeating, bcv.isNull[row], bcv.isNull[0]));
    }    
   }

    /**
     * Compiles the appropriate vector expression writer based on an expression info (ExprNodeDesc)
     */
    public static VectorExpressionWriter genVectorExpressionWritable(ExprNodeDesc nodeDesc)
      throws HiveException {
      String nodeType = nodeDesc.getTypeString();
      ObjectInspector objectInspector = nodeDesc.getWritableObjectInspector();
      if (null == objectInspector) {
        objectInspector = TypeInfoUtils
            .getStandardWritableObjectInspectorFromTypeInfo(nodeDesc.getTypeInfo());
      }
      if (null == objectInspector) {
        throw new HiveException(String.format(
            "Failed to initialize VectorExpressionWriter for expr: %s",
            nodeDesc.getExprString()));
      }
      return genVectorExpressionWritable(objectInspector);
    }

    /**
     * Compiles the appropriate vector expression writer based on an expression info (ExprNodeDesc)
     */
    public static VectorExpressionWriter genVectorExpressionWritable(
        ObjectInspector fieldObjInspector) throws HiveException {

      switch (fieldObjInspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) fieldObjInspector).getPrimitiveCategory()) {
          case FLOAT:
            return genVectorExpressionWritableFloat(
                (SettableFloatObjectInspector) fieldObjInspector);
          case DOUBLE:
            return genVectorExpressionWritableDouble(
                (SettableDoubleObjectInspector) fieldObjInspector);
          case BOOLEAN:
            return genVectorExpressionWritableBoolean(
                (SettableBooleanObjectInspector) fieldObjInspector);
          case BYTE:
            return genVectorExpressionWritableByte(
                (SettableByteObjectInspector) fieldObjInspector);
          case SHORT:
            return genVectorExpressionWritableShort(
                (SettableShortObjectInspector) fieldObjInspector);
          case INT:
            return genVectorExpressionWritableInt(
                (SettableIntObjectInspector) fieldObjInspector);
          case LONG:
            return genVectorExpressionWritableLong(
                (SettableLongObjectInspector) fieldObjInspector);
          case BINARY:
            return genVectorExpressionWritableBinary(
                (SettableBinaryObjectInspector) fieldObjInspector);
          case STRING:
            return genVectorExpressionWritableString(
                (SettableStringObjectInspector) fieldObjInspector);
          case VARCHAR:
            return genVectorExpressionWritableVarchar(
                (SettableHiveVarcharObjectInspector) fieldObjInspector);
          case TIMESTAMP:
            return genVectorExpressionWritableTimestamp(
                (SettableTimestampObjectInspector) fieldObjInspector);
          case DATE:
            return genVectorExpressionWritableDate(
                (SettableDateObjectInspector) fieldObjInspector);
          case DECIMAL:
            return genVectorExpressionWritableDecimal(
                (SettableHiveDecimalObjectInspector) fieldObjInspector);
          default:
            throw new IllegalArgumentException("Unknown primitive type: " +
              ((PrimitiveObjectInspector) fieldObjInspector).getPrimitiveCategory());
        }
        
      case STRUCT:
      case UNION:
      case MAP:
      case LIST:
        throw new IllegalArgumentException("Unsupported complex type: " +
            fieldObjInspector.getCategory());
      default:
        throw new IllegalArgumentException("Unknown type " +
            fieldObjInspector.getCategory());      
      }
  }

  private static VectorExpressionWriter genVectorExpressionWritableDecimal(
        SettableHiveDecimalObjectInspector fieldObjInspector) throws HiveException {
    
      // We should never reach this, the compile validation should guard us
      throw new HiveException("DECIMAL primitive type not supported in vectorization.");
    }

  private static VectorExpressionWriter genVectorExpressionWritableDate(
        SettableDateObjectInspector fieldObjInspector) throws HiveException {
    // We should never reach this, the compile validation should guard us
    throw new HiveException("DATE primitive type not supported in vectorization.");
    }

  private static VectorExpressionWriter genVectorExpressionWritableTimestamp(
        SettableTimestampObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;
      private Timestamp ts;

      public VectorExpressionWriter init(SettableTimestampObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        ts = new Timestamp(0);
        obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) {
        TimestampUtils.assignTimeInNanoSec(value, ts);
        ((SettableTimestampObjectInspector) this.objectInspector).set(obj, ts);
        return obj;
      }

      @Override
      public Object setValue(Object field, long value) {
        if (null == field) {
          field = initValue(null);
        }
        TimestampUtils.assignTimeInNanoSec(value, ts);
        ((SettableTimestampObjectInspector) this.objectInspector).set(field, ts);
        return field;
      }
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableTimestampObjectInspector) this.objectInspector).create(new Timestamp(0));
      }
   }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableVarchar(
        SettableHiveVarcharObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterBytes() {
      private Object obj;
      private Text text;
      
      public VectorExpressionWriter init(SettableHiveVarcharObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.text = new Text();
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(byte[] value, int start, int length) throws HiveException {
        text.set(value, start, length);
        ((SettableHiveVarcharObjectInspector) this.objectInspector).set(this.obj, text.toString());
        return this.obj;
      }

      @Override
      public Object setValue(Object field, byte[] value, int start, int length) 
          throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        text.set(value, start, length);
        ((SettableHiveVarcharObjectInspector) this.objectInspector).set(field, text.toString());
        return field;
      }
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableHiveVarcharObjectInspector) this.objectInspector)
            .create(new HiveVarchar(StringUtils.EMPTY, -1));
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableString(
      SettableStringObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterBytes() {
      private Object obj;
      private Text text;
      
      public VectorExpressionWriter init(SettableStringObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.text = new Text();
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(byte[] value, int start, int length) throws HiveException {
        this.text.set(value, start, length);
        ((SettableStringObjectInspector) this.objectInspector).set(this.obj, this.text.toString());
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, byte[] value, int start, int length) 
          throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        this.text.set(value, start, length);
        ((SettableStringObjectInspector) this.objectInspector).set(field, this.text.toString());
        return field;
      }      
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableStringObjectInspector) this.objectInspector).create(StringUtils.EMPTY);
      }      
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableBinary(
      SettableBinaryObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterBytes() {
      private Object obj;
      private byte[] bytes;
      
      public VectorExpressionWriter init(SettableBinaryObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.bytes = ArrayUtils.EMPTY_BYTE_ARRAY;
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(byte[] value, int start, int length) throws HiveException {
        bytes = Arrays.copyOfRange(value, start, start + length);
        ((SettableBinaryObjectInspector) this.objectInspector).set(this.obj, bytes);
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, byte[] value, int start, int length) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        bytes = Arrays.copyOfRange(value, start, start + length);
        ((SettableBinaryObjectInspector) this.objectInspector).set(field, bytes);
        return field;
      }      

      @Override
      public Object initValue(Object ignored) {
        return ((SettableBinaryObjectInspector) this.objectInspector)
            .create(ArrayUtils.EMPTY_BYTE_ARRAY);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableLong(
      SettableLongObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;
      
      public VectorExpressionWriter init(SettableLongObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableLongObjectInspector) this.objectInspector).set(this.obj, value);
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableLongObjectInspector) this.objectInspector).set(field, value);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableLongObjectInspector) this.objectInspector)
            .create(0L);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableInt(
      SettableIntObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;
      
      public VectorExpressionWriter init(SettableIntObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableIntObjectInspector) this.objectInspector).set(this.obj, (int) value);
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableIntObjectInspector) this.objectInspector).set(field, (int) value);
        return field;
      }
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableIntObjectInspector) this.objectInspector)
            .create(0);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableShort(
      SettableShortObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;
      
      public VectorExpressionWriter init(SettableShortObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableShortObjectInspector) this.objectInspector).set(this.obj, (short) value);
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableShortObjectInspector) this.objectInspector).set(field, (short) value);
        return field;
      }
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableShortObjectInspector) this.objectInspector)
            .create((short) 0);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableByte(
      SettableByteObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;
      
      public VectorExpressionWriter init(SettableByteObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableByteObjectInspector) this.objectInspector).set(this.obj, (byte) value);
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableByteObjectInspector) this.objectInspector).set(field, (byte) value);
        return field;
      }
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableByteObjectInspector) this.objectInspector)
            .create((byte) 0);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableBoolean(
      SettableBooleanObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;
      
      public VectorExpressionWriter init(SettableBooleanObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableBooleanObjectInspector) this.objectInspector).set(this.obj, 
            value == 0 ? false : true);
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableBooleanObjectInspector) this.objectInspector).set(field, 
            value == 0 ? false : true);
        return field;
      }
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableBooleanObjectInspector) this.objectInspector)
            .create(false);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableDouble(
      SettableDoubleObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterDouble() {
      private Object obj;
      
      public VectorExpressionWriter init(SettableDoubleObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(double value) throws HiveException {
        ((SettableDoubleObjectInspector) this.objectInspector).set(this.obj, value);
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, double value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableDoubleObjectInspector) this.objectInspector).set(field, value);
        return field;
      }      
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableDoubleObjectInspector) this.objectInspector)
            .create(0f);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableFloat(
      SettableFloatObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterDouble() {
      private Object obj;
      
      public VectorExpressionWriter init(SettableFloatObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(double value) throws HiveException {
        ((SettableFloatObjectInspector) this.objectInspector).set(this.obj, (float) value);
        return this.obj;
      }
      
      @Override
      public Object setValue(Object field, double value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableFloatObjectInspector) this.objectInspector).set(field, (float) value);
        return field;
      }
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableFloatObjectInspector) this.objectInspector)
            .create(0f);
      }
    }.init(fieldObjInspector);
  }

  /**
   * Helper function to create an array of writers from a list of expression descriptors.
   */
  public static VectorExpressionWriter[] getExpressionWriters(List<ExprNodeDesc> nodesDesc)
      throws HiveException {
    VectorExpressionWriter[] writers = new VectorExpressionWriter[nodesDesc.size()];
    for(int i=0; i<writers.length; ++i) {
      ExprNodeDesc nodeDesc = nodesDesc.get(i);
      writers[i] = genVectorExpressionWritable(nodeDesc);
    }
    return writers;
  }

  /**
   * A poor man Java closure. Works around the problem of having to return multiple objects
   * from one function call.
   */
  public static interface SingleOIDClosure {
    void assign(VectorExpressionWriter[] writers, ObjectInspector objectInspector);
  }

  public static interface ListOIDClosure {
    void assign(VectorExpressionWriter[] writers, List<ObjectInspector> oids);
  }

  /**
   * Creates the value writers for a column vector expression list.
   * Creates an appropriate output object inspector.
   */
  public static void processVectorExpressions(
      List<ExprNodeDesc> nodesDesc,
      List<String> columnNames,
      SingleOIDClosure closure)
      throws HiveException {
    VectorExpressionWriter[] writers = getExpressionWriters(nodesDesc);
    List<ObjectInspector> oids = new ArrayList<ObjectInspector>(writers.length);
    for(int i=0; i<writers.length; ++i) {
      oids.add(writers[i].getObjectInspector());
    }
    ObjectInspector objectInspector = ObjectInspectorFactory.
        getStandardStructObjectInspector(columnNames,oids);
    closure.assign(writers, objectInspector);
  }

  /**
   * Creates the value writers for a column vector expression list.
   * Creates an appropriate output object inspector.
   */
  public static void processVectorExpressions(
      List<ExprNodeDesc> nodesDesc,
      ListOIDClosure closure)
      throws HiveException {
    VectorExpressionWriter[] writers = getExpressionWriters(nodesDesc);
    List<ObjectInspector> oids = new ArrayList<ObjectInspector>(writers.length);
    for(int i=0; i<writers.length; ++i) {
      oids.add(writers[i].getObjectInspector());
    }
    closure.assign(writers, oids);
  }


  /**
   * Returns {@link VectorExpressionWriter} objects for the fields in the given
   * object inspector.
   *
   * @param objInspector
   * @return
   * @throws HiveException
   */
  public static VectorExpressionWriter[] getExpressionWriters(StructObjectInspector objInspector)
      throws HiveException {
    
    if (objInspector.isSettable()) {
      return getSettableExpressionWriters((SettableStructObjectInspector) objInspector);
    }
    
    List<? extends StructField> allFieldRefs = objInspector.getAllStructFieldRefs();
    
    VectorExpressionWriter[] expressionWriters = new VectorExpressionWriter[allFieldRefs.size()];
    
    for(int i=0; i<expressionWriters.length; ++i) {
      expressionWriters[i] = genVectorExpressionWritable(allFieldRefs.get(i).getFieldObjectInspector());
    }
    
    return expressionWriters;
  }

  public static VectorExpressionWriter[] getSettableExpressionWriters(
      SettableStructObjectInspector objInspector) throws HiveException {
    List<? extends StructField> fieldsRef = objInspector.getAllStructFieldRefs(); 
    VectorExpressionWriter[] writers = new VectorExpressionWriter[fieldsRef.size()];
    for(int i=0; i<writers.length; ++i) {
      StructField fieldRef = fieldsRef.get(i);
      VectorExpressionWriter baseWriter = genVectorExpressionWritable(
          fieldRef.getFieldObjectInspector());
      writers[i] = genVectorExpressionWritable(objInspector, fieldRef, baseWriter);
    }
    return writers;
    
  }
  
  /**
   * VectorExpressionWriterSetter helper for vector expression writers that use 
   * settable ObjectInspector fields to assign the values.
   * This is used by the OrcStruct serialization (eg. CREATE TABLE ... AS ...) 
   */
  private static class VectorExpressionWriterSetter extends VectorExpressionWriterBase {
    private SettableStructObjectInspector settableObjInspector;
    private StructField fieldRef;
    private VectorExpressionWriter baseWriter;
    
    public VectorExpressionWriterSetter init(
        SettableStructObjectInspector objInspector,
        StructField fieldRef,
        VectorExpressionWriter baseWriter) {
      this.fieldRef = fieldRef;
      this.settableObjInspector = objInspector;
      this.objectInspector = fieldRef.getFieldObjectInspector();
      this.baseWriter = baseWriter;
      return this;
    }

    @Override
    public Object writeValue(ColumnVector column, int row)
        throws HiveException {
      throw new HiveException("Should never reach here");
    }

    @Override
    public Object setValue(Object row, ColumnVector column, int columnRow)
        throws HiveException {
      
      // NULLs are handled by each individual base writer setter
      // We could handle NULLs centrally here but that would result in spurious allocs
      
      Object fieldValue = this.settableObjInspector.getStructFieldData(row, fieldRef);
      fieldValue = baseWriter.setValue(fieldValue, column, columnRow);
      return this.settableObjInspector.setStructFieldData(row, fieldRef, fieldValue);
    }
    
    @Override
    public Object initValue(Object struct) throws HiveException {
      Object initValue = this.baseWriter.initValue(null);
      this.settableObjInspector.setStructFieldData(struct, fieldRef, initValue);
      return struct;
    }
  }
  
  private static VectorExpressionWriter genVectorExpressionWritable(
      SettableStructObjectInspector objInspector,
      StructField fieldRef,
      VectorExpressionWriter baseWriter) throws HiveException {
    return new VectorExpressionWriterSetter().init(objInspector, fieldRef, baseWriter);
  }
}
