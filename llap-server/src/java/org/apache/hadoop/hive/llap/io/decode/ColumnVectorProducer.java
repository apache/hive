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

package org.apache.hadoop.hive.llap.io.decode;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.encoded.EncodedDataProducer;
import org.apache.hadoop.hive.llap.io.encoded.EncodedDataReader;
import org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataProducer;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.InputSplit;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/** Middle layer - gets encoded blocks, produces proto-VRBs */
public abstract class ColumnVectorProducer<BatchKey> {
  private final ListeningExecutorService executor;

  public ColumnVectorProducer(ExecutorService executor) {
    this.executor = (executor instanceof ListeningExecutorService) ?
        (ListeningExecutorService)executor : MoreExecutors.listeningDecorator(executor);
  }

  private final class UncaughtErrorHandler implements FutureCallback<Void> {
    private final EncodedDataConsumer edc;

    private UncaughtErrorHandler(EncodedDataConsumer edc) {
      this.edc = edc;
    }

    @Override
    public void onSuccess(Void result) {
      // Successful execution of reader is supposed to call setDone.
    }

    @Override
    public void onFailure(Throwable t) {
      // Reader is not supposed to throw AFTER calling setError.
      edc.setError(t);
    }
  }

  /**
   * Reads ColumnVectorBatch-es.
   * @param consumer Consumer that will receive the batches asynchronously.
   * @return Feedback that can be used to stop reading, and should be used
   *         to return consumed batches.
   * @throws IOException 
   */
  public ConsumerFeedback<ColumnVectorBatch> read(InputSplit split, List<Integer> columnIds,
      SearchArgument sarg, String[] columnNames, Consumer<ColumnVectorBatch> consumer)
          throws IOException {
    // Get the source of encoded data.
    EncodedDataProducer<BatchKey> edp = getEncodedDataProducer();
    // Create the consumer of encoded data; it will coordinate decoding to CVBs.
    final EncodedDataConsumer edc;
    if (edp instanceof OrcEncodedDataProducer) {
      edc = new OrcEncodedDataConsumer(this, consumer, columnIds.size());
    } else {
      edc = new EncodedDataConsumer(this, consumer, columnIds.size());
    }
    // Then, get the specific reader of encoded data out of the producer.
    EncodedDataReader<BatchKey> reader = edp.createReader(
        split, columnIds, sarg, columnNames, edc);
    // Set the encoded data reader as upstream feedback for encoded data consumer, and start.
    edc.init(reader);
    // This is where we send execution on separate thread; the only threading boundary for now.
    // TODO: we should NOT do this thing with handler. Reader needs to do cleanup in most cases.
    UncaughtErrorHandler errorHandler =  new UncaughtErrorHandler(edc);
    ListenableFuture<Void> future = executor.submit(reader);
    Futures.addCallback(future, errorHandler);
    return edc;
  }

  protected abstract EncodedDataProducer<BatchKey> getEncodedDataProducer();

  protected abstract void decodeBatch(EncodedDataConsumer<BatchKey> batchKeyEncodedDataConsumer,
      EncodedColumnBatch<BatchKey> batch,
      Consumer<ColumnVectorBatch> downstreamConsumer);
}
