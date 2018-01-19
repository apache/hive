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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.google.common.annotations.VisibleForTesting;
import org.apache.orc.PhysicalWriter;

/**
 * An ORC file writer. The file is divided into stripes, which is the natural
 * unit of work when reading. Each stripe is buffered in memory until the
 * memory reaches the stripe size and then it is written out broken down by
 * columns. Each column is written by a TreeWriter that is specific to that
 * type of column. TreeWriters may have children TreeWriters that handle the
 * sub-types. Each of the TreeWriters writes the column's data as a set of
 * streams.
 *
 * This class is unsynchronized like most Stream objects, so from the creation of an OrcFile and all
 * access to a single instance has to be from a single thread.
 * 
 * There are no known cases where these happen between different threads today.
 * 
 * Caveat: the MemoryManager is created during WriterOptions create, that has to be confined to a single
 * thread as well.
 * 
 */
public class WriterImpl extends org.apache.orc.impl.WriterImpl implements Writer {

  private final ObjectInspector inspector;
  private final VectorizedRowBatch internalBatch;
  private final StructField[] fields;

  WriterImpl(FileSystem fs,
             Path path,
             OrcFile.WriterOptions opts) throws IOException {
    super(fs, path, opts);
    this.inspector = opts.getInspector();
    this.internalBatch = opts.getSchema().createRowBatch(opts.getBatchSize());
    this.fields = initializeFieldsFromOi(inspector);
  }

  private static StructField[] initializeFieldsFromOi(ObjectInspector inspector) {
    if (inspector instanceof StructObjectInspector) {
      List<? extends StructField> fieldList =
          ((StructObjectInspector) inspector).getAllStructFieldRefs();
      StructField[] fields = new StructField[fieldList.size()];
      fieldList.toArray(fields);
      return fields;
    } else {
      return null;
    }
  }

  /**
   * Set the value for a given column value within a batch.
   * @param rowId the row to set
   * @param column the column to set
   * @param inspector the object inspector to interpret the obj
   * @param obj the value to use
   */
  static void setColumn(int rowId, ColumnVector column,
                        ObjectInspector inspector, Object obj) {
    if (obj == null) {
      column.noNulls = false;
      column.isNull[rowId] = true;
    } else {
      switch (inspector.getCategory()) {
        case PRIMITIVE:
          switch (((PrimitiveObjectInspector) inspector)
              .getPrimitiveCategory()) {
            case BOOLEAN: {
              LongColumnVector vector = (LongColumnVector) column;
              vector.vector[rowId] =
                  ((BooleanObjectInspector) inspector).get(obj) ? 1 : 0;
              break;
            }
            case BYTE: {
              LongColumnVector vector = (LongColumnVector) column;
              vector.vector[rowId] = ((ByteObjectInspector) inspector).get(obj);
              break;
            }
            case SHORT: {
              LongColumnVector vector = (LongColumnVector) column;
              vector.vector[rowId] =
                  ((ShortObjectInspector) inspector).get(obj);
              break;
            }
            case INT: {
              LongColumnVector vector = (LongColumnVector) column;
              vector.vector[rowId] = ((IntObjectInspector) inspector).get(obj);
              break;
            }
            case LONG: {
              LongColumnVector vector = (LongColumnVector) column;
              vector.vector[rowId] = ((LongObjectInspector) inspector).get(obj);
              break;
            }
            case FLOAT: {
              DoubleColumnVector vector = (DoubleColumnVector) column;
              vector.vector[rowId] =
                  ((FloatObjectInspector) inspector).get(obj);
              break;
            }
            case DOUBLE: {
              DoubleColumnVector vector = (DoubleColumnVector) column;
              vector.vector[rowId] =
                  ((DoubleObjectInspector) inspector).get(obj);
              break;
            }
            case BINARY: {
              BytesColumnVector vector = (BytesColumnVector) column;
              BytesWritable blob = ((BinaryObjectInspector) inspector)
                  .getPrimitiveWritableObject(obj);
              vector.setVal(rowId, blob.getBytes(), 0, blob.getLength());
              break;
            }
            case STRING: {
              BytesColumnVector vector = (BytesColumnVector) column;
              Text blob = ((StringObjectInspector) inspector)
                  .getPrimitiveWritableObject(obj);
              vector.setVal(rowId, blob.getBytes(), 0, blob.getLength());
              break;
            }
            case VARCHAR: {
              BytesColumnVector vector = (BytesColumnVector) column;
              Text blob = ((HiveVarcharObjectInspector) inspector)
                  .getPrimitiveWritableObject(obj).getTextValue();
              vector.setVal(rowId, blob.getBytes(), 0, blob.getLength());
              break;
            }
            case CHAR: {
              BytesColumnVector vector = (BytesColumnVector) column;
              Text blob = ((HiveCharObjectInspector) inspector)
                  .getPrimitiveWritableObject(obj).getTextValue();
              vector.setVal(rowId, blob.getBytes(), 0, blob.getLength());
              break;
            }
            case TIMESTAMP: {
              TimestampColumnVector vector = (TimestampColumnVector) column;
              Timestamp ts = ((TimestampObjectInspector) inspector)
                  .getPrimitiveJavaObject(obj);
              vector.set(rowId, ts);
              break;
            }
            case DATE: {
              LongColumnVector vector = (LongColumnVector) column;
              vector.vector[rowId] = ((DateObjectInspector) inspector)
                  .getPrimitiveWritableObject(obj).getDays();
              break;
            }
            case DECIMAL: {
              DecimalColumnVector vector = (DecimalColumnVector) column;
              vector.set(rowId, ((HiveDecimalObjectInspector) inspector)
                  .getPrimitiveWritableObject(obj));
              break;
            }
          }
          break;
        case STRUCT: {
          StructColumnVector vector = (StructColumnVector) column;
          StructObjectInspector oi = (StructObjectInspector) inspector;
          List<? extends StructField> fields = oi.getAllStructFieldRefs();
          for (int c = 0; c < vector.fields.length; ++c) {
            StructField field = fields.get(c);
            setColumn(rowId, vector.fields[c], field.getFieldObjectInspector(),
                oi.getStructFieldData(obj, field));
          }
          break;
        }
        case UNION: {
          UnionColumnVector vector = (UnionColumnVector) column;
          UnionObjectInspector oi = (UnionObjectInspector) inspector;
          int tag = oi.getTag(obj);
          vector.tags[rowId] = tag;
          setColumn(rowId, vector.fields[tag],
              oi.getObjectInspectors().get(tag), oi.getField(obj));
          break;
        }
        case LIST: {
          ListColumnVector vector = (ListColumnVector) column;
          ListObjectInspector oi = (ListObjectInspector) inspector;
          int offset = vector.childCount;
          int length = oi.getListLength(obj);
          vector.offsets[rowId] = offset;
          vector.lengths[rowId] = length;
          vector.child.ensureSize(offset + length, true);
          vector.childCount += length;
          for (int c = 0; c < length; ++c) {
            setColumn(offset + c, vector.child,
                oi.getListElementObjectInspector(),
                oi.getListElement(obj, c));
          }
          break;
        }
        case MAP: {
          MapColumnVector vector = (MapColumnVector) column;
          MapObjectInspector oi = (MapObjectInspector) inspector;
          int offset = vector.childCount;
          Set map = oi.getMap(obj).entrySet();
          int length = map.size();
          vector.offsets[rowId] = offset;
          vector.lengths[rowId] = length;
          vector.keys.ensureSize(offset + length, true);
          vector.values.ensureSize(offset + length, true);
          vector.childCount += length;
          for (Object item: map) {
            Map.Entry pair = (Map.Entry) item;
            setColumn(offset, vector.keys, oi.getMapKeyObjectInspector(),
                pair.getKey());
            setColumn(offset, vector.values, oi.getMapValueObjectInspector(),
                pair.getValue());
            offset += 1;
          }
          break;
        }
        default:
          throw new IllegalArgumentException("Unknown ObjectInspector kind " +
              inspector.getCategory());
      }
    }
  }

  void flushInternalBatch() throws IOException {
    if (internalBatch.size != 0) {
      super.addRowBatch(internalBatch);
      internalBatch.reset();
    }
  }

  @Override
  public void addRow(Object row) throws IOException {
    int rowId = internalBatch.size++;
    if (fields != null) {
      StructObjectInspector soi = (StructObjectInspector) inspector;
      for(int i=0; i < fields.length; ++i) {
        setColumn(rowId, internalBatch.cols[i],
            fields[i].getFieldObjectInspector(),
            soi.getStructFieldData(row, fields[i]));
      }
    } else {
      setColumn(rowId, internalBatch.cols[0], inspector, row);
    }
    if (internalBatch.size == internalBatch.getMaxSize()) {
      flushInternalBatch();
    }
  }

  @Override
  public long writeIntermediateFooter() throws IOException {
    flushInternalBatch();
    return super.writeIntermediateFooter();
  }

  @Override
  public void addRowBatch(VectorizedRowBatch batch) throws IOException {
    flushInternalBatch();
    super.addRowBatch(batch);
  }

  @Override
  public void close() throws IOException {
    flushInternalBatch();
    super.close();
  }
}
