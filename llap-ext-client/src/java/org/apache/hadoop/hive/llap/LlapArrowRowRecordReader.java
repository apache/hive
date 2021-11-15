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

package org.apache.hadoop.hive.llap;

import com.google.common.base.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Buffers a batch for reading one row at a time.
 */
public class LlapArrowRowRecordReader extends LlapRowRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(LlapArrowRowRecordReader.class);
  private int rowIndex = 0;
  private int batchSize = 0;

  //Buffer one batch at a time, for row retrieval
  private Object[][] currentBatch;

  public LlapArrowRowRecordReader(Configuration conf, Schema schema,
      RecordReader<NullWritable, ? extends Writable> reader) throws IOException {
    super(conf, schema, reader);
  }

  @Override
  public boolean next(NullWritable key, Row value) throws IOException {
    Preconditions.checkArgument(value != null);
    boolean hasNext = false;
    ArrowWrapperWritable batchData = (ArrowWrapperWritable) data;
    if((batchSize == 0) || (rowIndex == batchSize)) {
      //This is either the first batch or we've used up the current batch buffer
      batchSize = 0;
      rowIndex = 0;

      // since HIVE-22856, a zero length batch doesn't mean that we won't have any more batches
      // we can have more batches with data even after after a zero length batch
      // we should keep trying until we get a batch with some data or reader.next() returns false
      while (batchSize == 0 && (hasNext = reader.next(key, data))) {
        List<FieldVector> vectors = batchData.getVectorSchemaRoot().getFieldVectors();
        //hasNext implies there is some column in the batch
        Preconditions.checkState(vectors.size() > 0);
        //All the vectors have the same length,
        //we can get the number of rows from the first vector
        batchSize = vectors.get(0).getValueCount();
      }

      if (hasNext) {
        //There is another batch to buffer
        try {
          ArrowWrapperWritable wrapper = new ArrowWrapperWritable(batchData.getVectorSchemaRoot());
          currentBatch = (Object[][]) serde.deserialize(wrapper);
          StructObjectInspector rowOI = (StructObjectInspector) serde.getObjectInspector();
          setRowFromStruct(value, currentBatch[rowIndex], rowOI);
        } catch (Exception e) {
          LOG.error("Failed to fetch Arrow batch", e);
          throw new RuntimeException(e);
        }
      }
      //There were no more batches AND
      //this is either the first batch or we've used up the current batch buffer.
      //goto return false
    } else if(rowIndex < batchSize) {
      //Take a row from the current buffered batch
      hasNext = true;
      StructObjectInspector rowOI = null;
      try {
        rowOI = (StructObjectInspector) serde.getObjectInspector();
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }
      setRowFromStruct(value, currentBatch[rowIndex], rowOI);
    }
    //Always inc the batch buffer index
    //If we return false, it is just a noop
    rowIndex++;
    return hasNext;
  }

  protected AbstractSerDe createSerDe() throws SerDeException {
    return new ArrowColumnarBatchSerDe();
  }

}
