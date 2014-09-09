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

package org.apache.hadoop.hive.ql.exec.vector;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * VectorizedColumnarSerDe is used by Vectorized query execution engine
 * for columnar based storage supported by RCFile.
 */
public class VectorizedColumnarSerDe extends ColumnarSerDe implements VectorizedSerde {

  public VectorizedColumnarSerDe() throws SerDeException {
  }

  private final BytesRefArrayWritable[] byteRefArray = new BytesRefArrayWritable[VectorizedRowBatch.DEFAULT_SIZE];
  private final ObjectWritable ow = new ObjectWritable();
  private final ByteStream.Output serializeVectorStream = new ByteStream.Output();

  /**
   * Serialize a vectorized row batch
   *
   * @param vrg
   *          Vectorized row batch to serialize
   * @param objInspector
   *          The ObjectInspector for the row object
   * @return The serialized Writable object
   * @throws SerDeException
   * @see SerDe#serialize(Object, ObjectInspector)
   */
  @Override
  public Writable serializeVector(VectorizedRowBatch vrg, ObjectInspector objInspector)
      throws SerDeException {
    try {
      // Validate that the OI is of struct type
      if (objInspector.getCategory() != Category.STRUCT) {
        throw new UnsupportedOperationException(getClass().toString()
            + " can only serialize struct types, but we got: "
            + objInspector.getTypeName());
      }

      VectorizedRowBatch batch = (VectorizedRowBatch) vrg;
      StructObjectInspector soi = (StructObjectInspector) objInspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();

      // Reset the byte buffer
      serializeVectorStream.reset();
      int count = 0;
      int rowIndex = 0;
      for (int i = 0; i < batch.size; i++) {

        // If selectedInUse is true then we need to serialize only
        // the selected indexes
        if (batch.selectedInUse) {
          rowIndex = batch.selected[i];
        } else {
          rowIndex = i;
        }

        BytesRefArrayWritable byteRow = byteRefArray[i];
        int numCols = fields.size();

        if (byteRow == null) {
          byteRow = new BytesRefArrayWritable(numCols);
          byteRefArray[i] = byteRow;
        }

        byteRow.resetValid(numCols);

        for (int p = 0; p < batch.projectionSize; p++) {
          int k = batch.projectedColumns[p];
          ObjectInspector foi = fields.get(k).getFieldObjectInspector();
          ColumnVector currentColVector = batch.cols[k];

          switch (foi.getCategory()) {
          case PRIMITIVE: {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
            if (!currentColVector.noNulls
                && (currentColVector.isRepeating || currentColVector.isNull[rowIndex])) {
              // The column is null hence write null value
              serializeVectorStream.write(new byte[0], 0, 0);
            } else {
              // If here then the vector value is not null.
              if (currentColVector.isRepeating) {
                // If the vector has repeating values then set rowindex to zero
                rowIndex = 0;
              }

              switch (poi.getPrimitiveCategory()) {
              case BOOLEAN: {
                LongColumnVector lcv = (LongColumnVector) batch.cols[k];
                // In vectorization true is stored as 1 and false as 0
                boolean b = lcv.vector[rowIndex] == 1 ? true : false;
                if (b) {
                  serializeVectorStream.write(LazyUtils.trueBytes, 0, LazyUtils.trueBytes.length);
                } else {
                  serializeVectorStream.write(LazyUtils.trueBytes, 0, LazyUtils.trueBytes.length);
                }
              }
                break;
              case BYTE:
              case SHORT:
              case INT:
              case LONG:
                LongColumnVector lcv = (LongColumnVector) batch.cols[k];
                LazyLong.writeUTF8(serializeVectorStream, lcv.vector[rowIndex]);
                break;
              case FLOAT:
              case DOUBLE:
                DoubleColumnVector dcv = (DoubleColumnVector) batch.cols[k];
                ByteBuffer b = Text.encode(String.valueOf(dcv.vector[rowIndex]));
                serializeVectorStream.write(b.array(), 0, b.limit());
                break;
              case BINARY: {
                BytesColumnVector bcv = (BytesColumnVector) batch.cols[k];
                byte[] bytes = bcv.vector[rowIndex];
                serializeVectorStream.write(bytes, 0, bytes.length);
              }
                break;
              case STRING:
              case CHAR:
              case VARCHAR: {
                // Is it correct to escape CHAR and VARCHAR?
                BytesColumnVector bcv = (BytesColumnVector) batch.cols[k];
                LazyUtils.writeEscaped(serializeVectorStream, bcv.vector[rowIndex],
                    bcv.start[rowIndex],
                    bcv.length[rowIndex],
                    serdeParams.isEscaped(), serdeParams.getEscapeChar(), serdeParams
                        .getNeedsEscape());
              }
                break;
              case TIMESTAMP:
                LongColumnVector tcv = (LongColumnVector) batch.cols[k];
                long timeInNanoSec = tcv.vector[rowIndex];
                Timestamp t = new Timestamp(0);
                TimestampUtils.assignTimeInNanoSec(timeInNanoSec, t);
                TimestampWritable tw = new TimestampWritable();
                tw.set(t);
                LazyTimestamp.writeUTF8(serializeVectorStream, tw);
                break;
              case DATE:
                LongColumnVector dacv = (LongColumnVector) batch.cols[k];
                DateWritable daw = new DateWritable((int) dacv.vector[rowIndex]);
                LazyDate.writeUTF8(serializeVectorStream, daw);
                break;
              default:
                throw new UnsupportedOperationException(
                    "Vectorizaton is not supported for datatype:"
                        + poi.getPrimitiveCategory());
              }
            }
            break;
          }
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
            throw new UnsupportedOperationException("Vectorizaton is not supported for datatype:"
                + foi.getCategory());
          default:
            throw new SerDeException("Unknown ObjectInspector category!");

          }

          byteRow.get(k).set(serializeVectorStream.getData(), count, serializeVectorStream
              .getLength() - count);
          count = serializeVectorStream.getLength();
        }

      }
      ow.set(byteRefArray);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
    return ow;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesRefArrayWritable.class;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {

    // Ideally this should throw  UnsupportedOperationException as the serde is
    // vectorized serde. But since RC file reader does not support vectorized reading this
    // is left as it is. This function will be called from VectorizedRowBatchCtx::addRowToBatch
    // to deserialize the row one by one and populate the batch. Once RC file reader supports vectorized
    // reading this serde and be standalone serde with no dependency on ColumnarSerDe.
    return super.deserialize(blob);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new UnsupportedOperationException();
  }

  /**
   * Deserializes the rowBlob into Vectorized row batch
   * @param rowBlob
   *          rowBlob row batch to deserialize
   * @param rowsInBlob
   *          Total number of rows in rowBlob to deserialize
   * @param reuseBatch
   *          VectorizedRowBatch to which the rows should be serialized   *
   * @throws SerDeException
   */
  @Override
  public void deserializeVector(Object rowBlob, int rowsInBlob,
      VectorizedRowBatch reuseBatch) throws SerDeException {

    BytesRefArrayWritable[] refArray = (BytesRefArrayWritable[]) rowBlob;
    DataOutputBuffer buffer = new DataOutputBuffer();
    for (int i = 0; i < rowsInBlob; i++) {
      Object row = deserialize(refArray[i]);
      try {
        VectorizedBatchUtil.addRowToBatch(row,
            (StructObjectInspector) cachedObjectInspector, i,
            reuseBatch, buffer);
      } catch (HiveException e) {
        throw new SerDeException(e);
      }
    }
  }
}
