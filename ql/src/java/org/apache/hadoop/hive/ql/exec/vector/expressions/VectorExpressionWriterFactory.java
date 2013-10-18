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
import java.util.List;

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
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

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

    public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
      objectInspector = nodeDesc.getWritableObjectInspector();
      if (null == objectInspector) {
        objectInspector = TypeInfoUtils
            .getStandardWritableObjectInspectorFromTypeInfo(nodeDesc.getTypeInfo());
      }
      if (null == objectInspector) {
        throw new HiveException(String.format(
            "Failed to initialize VectorExpressionWriter for expr: %s",
            nodeDesc.getExprString()));
      }
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
     * The base implementation must be overridden by the Double specialization
     */
    @Override
    public Object writeValue(double value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Bytes specialization
     */
    @Override
    public Object writeValue(byte[] value, int start, int length) throws HiveException {
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
   }

  /**
   * Compiles the appropriate vector expression writer based on an expression info (ExprNodeDesc)
   */
  public static VectorExpressionWriter genVectorExpressionWritable(ExprNodeDesc nodeDesc)
    throws HiveException {
    String nodeType = nodeDesc.getTypeString();
    if (nodeType.equalsIgnoreCase("tinyint")) {
      return new VectorExpressionWriterLong()
      {
        private ByteWritable writable;

        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new ByteWritable();
          return this;
        }

        @Override
        public Object writeValue(long value) {
          writable.set((byte) value);
          return writable;
        }
      }.init(nodeDesc);
    } else if (nodeType.equalsIgnoreCase("smallint")) {
      return new VectorExpressionWriterLong()
      {
        private ShortWritable writable;
        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new ShortWritable();
          return this;
        }

        @Override
        public Object writeValue(long value) {
          writable.set((short) value);
          return writable;
        }
      }.init(nodeDesc);
    } else if (nodeType.equalsIgnoreCase("int")) {
      return new VectorExpressionWriterLong()
      {
        private IntWritable writable;
        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new IntWritable();
          return this;
        }

        @Override
        public Object writeValue(long value) {
          writable.set((int) value);
          return writable;
        }
      }.init(nodeDesc);
    } else if (nodeType.equalsIgnoreCase("bigint")) {
      return new VectorExpressionWriterLong()
      {
        private LongWritable writable;
        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new LongWritable();
          return this;
        }

        @Override
        public Object writeValue(long value) {
          writable.set(value);
          return writable;
        }
      }.init(nodeDesc);
    } else if (nodeType.equalsIgnoreCase("boolean")) {
      return new VectorExpressionWriterLong()
      {
        private BooleanWritable writable;
        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new BooleanWritable();
          return this;
        }

        @Override
        public Object writeValue(long value) {
          writable.set(value != 0 ? true : false);
          return writable;
        }
      }.init(nodeDesc);
    } else if (nodeType.equalsIgnoreCase("timestamp")) {
      return new VectorExpressionWriterLong()
      {
        private TimestampWritable writable;
        private Timestamp timestamp;
        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new TimestampWritable();
          timestamp = new Timestamp(0);
          return this;
        }

        @Override
        public Object writeValue(long value) {
          TimestampUtils.assignTimeInNanoSec(value, timestamp);
          writable.set(timestamp);
          return writable;
        }
      }.init(nodeDesc);
    } else if (nodeType.equalsIgnoreCase("string")) {
      return new VectorExpressionWriterBytes()
      {
        private Text writable;
        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new Text();
          return this;
        }

        @Override
        public Object writeValue(byte[] value, int start, int length) throws HiveException {
          writable.set(value, start, length);
          return writable;
        }
      }.init(nodeDesc);
    } else if (nodeType.equalsIgnoreCase("float")) {
      return new VectorExpressionWriterDouble()
      {
        private FloatWritable writable;
        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new FloatWritable();
          return this;
        }

        @Override
        public Object writeValue(double value) {
          writable.set((float)value);
          return writable;
        }
      }.init(nodeDesc);
    } else if (nodeType.equalsIgnoreCase("double")) {
      return new VectorExpressionWriterDouble()
      {
        private DoubleWritable writable;
        @Override
        public VectorExpressionWriter init(ExprNodeDesc nodeDesc) throws HiveException {
          super.init(nodeDesc);
          writable = new DoubleWritable();
          return this;
        }

        @Override
        public Object writeValue(double value) {
          writable.set(value);
          return writable;
        }
      }.init(nodeDesc);
    }

    throw new HiveException(String.format(
        "Unimplemented genVectorExpressionWritable type: %s for expression: %s",
        nodeType, nodeDesc));
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
    List<ExprNodeDesc> outputNodeDescs = new ArrayList<ExprNodeDesc>();
    for (StructField fieldRef : (objInspector)
        .getAllStructFieldRefs()) {
      String typeName = fieldRef.getFieldObjectInspector().getTypeName();
      TypeInfo ti = TypeInfoFactory.getPrimitiveTypeInfo(typeName);
      outputNodeDescs.add(new ExprNodeDesc(ti) {
        private static final long serialVersionUID = 1L;

        @Override
        public ExprNodeDesc clone() { /* Not needed */
          return null;
        }

        @Override
        public boolean isSame(Object o) { /* Not needed */
          return false;
        }
      });
    }
    return VectorExpressionWriterFactory.getExpressionWriters(outputNodeDescs);
  }
}
