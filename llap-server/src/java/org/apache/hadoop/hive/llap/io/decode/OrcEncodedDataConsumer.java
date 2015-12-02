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

import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonQueueMetrics;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedTreeReaderFactory;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedTreeReaderFactory.SettableTreeReader;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.TreeReaderFactory;
import org.apache.hadoop.hive.ql.io.orc.WriterImpl;
import org.apache.orc.OrcProto;

public class OrcEncodedDataConsumer
  extends EncodedDataConsumer<OrcBatchKey, OrcEncodedColumnBatch> {
  private TreeReaderFactory.TreeReader[] columnReaders;
  private int previousStripeIndex = -1;
  private OrcFileMetadata fileMetadata; // We assume one request is only for one file.
  private CompressionCodec codec;
  private OrcStripeMetadata[] stripes;
  private final boolean skipCorrupt; // TODO: get rid of this
  private final QueryFragmentCounters counters;

  public OrcEncodedDataConsumer(
      Consumer<ColumnVectorBatch> consumer, int colCount, boolean skipCorrupt,
      QueryFragmentCounters counters, LlapDaemonQueueMetrics queueMetrics) {
    super(consumer, colCount, queueMetrics);
    this.skipCorrupt = skipCorrupt;
    this.counters = counters;
  }

  public void setFileMetadata(OrcFileMetadata f) {
    assert fileMetadata == null;
    fileMetadata = f;
    stripes = new OrcStripeMetadata[f.getStripes().size()];
    // TODO: get rid of this
    codec = WriterImpl.createCodec(fileMetadata.getCompressionKind());
  }

  public void setStripeMetadata(OrcStripeMetadata m) {
    assert stripes != null;
    stripes[m.getStripeIx()] = m;
  }

  @Override
  protected void decodeBatch(OrcEncodedColumnBatch batch,
      Consumer<ColumnVectorBatch> downstreamConsumer) {
    long startTime = counters.startTimeCounter();
    int currentStripeIndex = batch.getBatchKey().stripeIx;

    boolean sameStripe = currentStripeIndex == previousStripeIndex;

    try {
      OrcStripeMetadata stripeMetadata = stripes[currentStripeIndex];
      // Get non null row count from root column, to get max vector batches
      int rgIdx = batch.getBatchKey().rgIx;
      long nonNullRowCount = -1;
      if (rgIdx == OrcEncodedColumnBatch.ALL_RGS) {
        nonNullRowCount = stripeMetadata.getRowCount();
      } else {
        OrcProto.RowIndexEntry rowIndex = stripeMetadata.getRowIndexes()[0].getEntry(rgIdx);
        nonNullRowCount = getRowCount(rowIndex);
      }
      int maxBatchesRG = (int) ((nonNullRowCount / VectorizedRowBatch.DEFAULT_SIZE) + 1);
      int batchSize = VectorizedRowBatch.DEFAULT_SIZE;
      int numCols = batch.getColumnIxs().length;
      if (columnReaders == null || !sameStripe) {
        this.columnReaders = EncodedTreeReaderFactory.createEncodedTreeReader(numCols,
            fileMetadata.getTypes(), stripeMetadata.getEncodings(), batch, codec, skipCorrupt);
        positionInStreams(columnReaders, batch, numCols, stripeMetadata);
      } else {
        repositionInStreams(this.columnReaders, batch, sameStripe, numCols, stripeMetadata);
      }
      previousStripeIndex = currentStripeIndex;

      for (int i = 0; i < maxBatchesRG; i++) {
        // for last batch in row group, adjust the batch size
        if (i == maxBatchesRG - 1) {
          batchSize = (int) (nonNullRowCount % VectorizedRowBatch.DEFAULT_SIZE);
          if (batchSize == 0) break;
        }

        ColumnVectorBatch cvb = cvbPool.take();
        assert cvb.cols.length == batch.getColumnIxs().length; // Must be constant per split.
        cvb.size = batchSize;

        for (int idx = 0; idx < batch.getColumnIxs().length; idx++) {
          cvb.cols[idx] = (ColumnVector)columnReaders[idx].nextVector(cvb.cols[idx], batchSize);
        }

        // we are done reading a batch, send it to consumer for processing
        downstreamConsumer.consumeData(cvb);
        counters.incrCounter(QueryFragmentCounters.Counter.ROWS_EMITTED, batchSize);
      }
      counters.incrTimeCounter(QueryFragmentCounters.Counter.DECODE_TIME_US, startTime);
      counters.incrCounter(QueryFragmentCounters.Counter.NUM_VECTOR_BATCHES, maxBatchesRG);
      counters.incrCounter(QueryFragmentCounters.Counter.NUM_DECODED_BATCHES);
    } catch (IOException e) {
      // Caller will return the batch.
      downstreamConsumer.setError(e);
    }
  }

  private void positionInStreams(TreeReaderFactory.TreeReader[] columnReaders,
      EncodedColumnBatch<OrcBatchKey> batch, int numCols,
      OrcStripeMetadata stripeMetadata) throws IOException {
    for (int i = 0; i < numCols; i++) {
      int columnIndex = batch.getColumnIxs()[i];
      int rowGroupIndex = batch.getBatchKey().rgIx;
      OrcProto.RowIndex rowIndex = stripeMetadata.getRowIndexes()[columnIndex];
      OrcProto.RowIndexEntry rowIndexEntry = rowIndex.getEntry(rowGroupIndex);
      columnReaders[i].seek(new RecordReaderImpl.PositionProviderImpl(rowIndexEntry));
    }
  }

  private void repositionInStreams(TreeReaderFactory.TreeReader[] columnReaders,
      EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe, int numCols,
      OrcStripeMetadata stripeMetadata) throws IOException {
    for (int i = 0; i < numCols; i++) {
      int columnIndex = batch.getColumnIxs()[i];
      int rowGroupIndex = batch.getBatchKey().rgIx;
      ColumnStreamData[] streamBuffers = batch.getColumnData()[i];
      OrcProto.RowIndex rowIndex = stripeMetadata.getRowIndexes()[columnIndex];
      OrcProto.RowIndexEntry rowIndexEntry = rowIndex.getEntry(rowGroupIndex);
      ((SettableTreeReader)columnReaders[i]).setBuffers(streamBuffers, sameStripe);
      columnReaders[i].seek(new RecordReaderImpl.PositionProviderImpl(rowIndexEntry));
    }
  }

  private long getRowCount(OrcProto.RowIndexEntry rowIndexEntry) {
    return rowIndexEntry.getStatistics().getNumberOfValues();
  }
}
