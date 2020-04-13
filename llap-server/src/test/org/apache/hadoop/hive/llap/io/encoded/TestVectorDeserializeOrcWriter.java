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
package org.apache.hadoop.hive.llap.io.encoded;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.decode.EncodedDataConsumer;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.WriterImpl;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.orc.impl.SchemaEvolution;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Unit tests for VectorDeserializeOrcWriter.
 */
public class TestVectorDeserializeOrcWriter {

  private static final int TEST_NUM_COLS = 2;

  private static Field reflectField(Class<?> classToReflect, String fieldNameValueToFetch) {
    try {
      Field reflectField = null;
      Class<?> classForReflect = classToReflect;
      do {
        try {
          reflectField = classForReflect.getDeclaredField(fieldNameValueToFetch);
        } catch (NoSuchFieldException e) {
          classForReflect = classForReflect.getSuperclass();
        }
      } while (reflectField == null || classForReflect == null);
      reflectField.setAccessible(true);
      return reflectField;
    } catch (Exception e) {
      fail("Failed to reflect " + fieldNameValueToFetch + " from " + classToReflect);
    }
    return null;
  }

  private static void reflectSetValue(Object objToReflect, String fieldNameToSet, Object valueToSet) {
    try {
      Field reflectField = reflectField(objToReflect.getClass(), fieldNameToSet);
      reflectField.set(objToReflect, valueToSet);
    } catch (Exception e) {
      fail("Failed to reflectively set " + fieldNameToSet + "=" + valueToSet);
    }
  }

  @Test
  public void testConcurrencyIssueWhileWriting() throws Exception {

    //Setup////////////////////////////////////////////////////////////////////////////////////////
    EncodedDataConsumer consumer = createBlankEncodedDataConsumer();
    Field cvbPoolField = EncodedDataConsumer.class.getDeclaredField("cvbPool");
    cvbPoolField.setAccessible(true);
    FixedSizedObjectPool<ColumnVectorBatch> cvbPool = (FixedSizedObjectPool<ColumnVectorBatch>)
        cvbPoolField.get(consumer);

    ColumnVectorBatch cvb = new ColumnVectorBatch(TEST_NUM_COLS);
    VectorizedRowBatch vrb = new VectorizedRowBatch(TEST_NUM_COLS);
    createTestVectors(cvb, vrb);

    Queue<VectorDeserializeOrcWriter.WriteOperation> writeOpQueue = new ConcurrentLinkedQueue<>();
    VectorDeserializeOrcWriter orcWriter = createOrcWriter(writeOpQueue, vrb);


    //Simulating unfortunate order of events///////////////////////////////////////////////////////
    //Add CVs to writer -> should increase their refcount
    //Happens when IO thread has generated a vector batch and hands it over to async ORC thread
    orcWriter.addBatchToWriter();

    //Return CVs to pool -> should check their refcount, and as they're 1, this should be a no-op
    //Happens when LLAPRecordReader on Tez thread received and used the batch and now wants to
    // return it for CVB recycling
    consumer.returnData(cvb);

    //Do the write -> should decrease the refcount of CVs
    //Happens when ORC thread gets to writing and hands the vectors of the batch over to ORC
    // WriterImpl for encoding and cache storage
    writeOpQueue.poll().apply(mock(WriterImpl.class), null);


    //Verifications////////////////////////////////////////////////////////////////////////////////
    //Pool should be empty as the CVB return should have been a no-op, so this call should create a
    // NEW instance of CVBs
    ColumnVectorBatch newCvb = cvbPool.take();
    assertNotEquals(newCvb, cvb);

    //Simulating a 'clean' CVB return -> the CVB now does have to make its way back to the pool
    consumer.returnData(cvb);
    newCvb = cvbPool.take();
    assertEquals(newCvb, cvb);
  }

  private static void createTestVectors(ColumnVectorBatch cvb, VectorizedRowBatch vrb) {
    for (int i = 0; i < TEST_NUM_COLS; ++i) {
      LongColumnVector cv = new LongColumnVector();
      cv.fill(i);
      cvb.cols[i] = cv;
      vrb.cols[i] = cv;
    }
  }

  private static VectorDeserializeOrcWriter createOrcWriter(
          Queue<VectorDeserializeOrcWriter.WriteOperation> writeOpQueue, VectorizedRowBatch vrb) {
    VectorDeserializeOrcWriter orcWriter = mock(VectorDeserializeOrcWriter.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS));

    reflectSetValue(orcWriter, "sourceBatch", vrb);
    reflectSetValue(orcWriter, "destinationBatch", vrb);
    reflectSetValue(orcWriter, "currentBatches", new ArrayList<VectorizedRowBatch>());
    reflectSetValue(orcWriter, "queue", writeOpQueue);
    reflectSetValue(orcWriter, "isAsync", true);
    return orcWriter;
  }

  private static EncodedDataConsumer createBlankEncodedDataConsumer() {
    return new EncodedDataConsumer(null, 1, null, null) {
      @Override
      protected void decodeBatch(EncodedColumnBatch batch, Consumer downstreamConsumer)
              throws InterruptedException {
      }

      @Override
      public SchemaEvolution getSchemaEvolution() {
        return null;
      }

      @Override
      public void consumeData(EncodedColumnBatch data) throws InterruptedException {
      }
    };
  }

}
