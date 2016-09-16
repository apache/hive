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

import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
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
import org.apache.hive.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedTreeReaderFactory;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedTreeReaderFactory.SettableTreeReader;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hive.orc.OrcUtils;
import org.apache.hive.orc.TypeDescription;
import org.apache.hive.orc.impl.TreeReaderFactory;
import org.apache.hadoop.hive.ql.io.orc.WriterImpl;
import org.apache.hive.orc.OrcProto;

public class OrcEncodedDataConsumer
  extends EncodedDataConsumer<OrcBatchKey, OrcEncodedColumnBatch> {
  private TreeReaderFactory.TreeReader[] columnReaders;
  private int previousStripeIndex = -1;
  private OrcFileMetadata fileMetadata; // We assume one request is only for one file.
  private CompressionCodec codec;
  private OrcStripeMetadata[] stripes;
  private final boolean skipCorrupt; // TODO: get rid of this
  private final QueryFragmentCounters counters;
  private boolean[] includedColumns;

  public OrcEncodedDataConsumer(
      Consumer<ColumnVectorBatch> consumer, int colCount, boolean skipCorrupt,
      QueryFragmentCounters counters, LlapDaemonIOMetrics ioMetrics) {
    super(consumer, colCount, ioMetrics);
    // TODO: get rid of this
    this.skipCorrupt = skipCorrupt;
    this.counters = counters;
  }

  public void setFileMetadata(OrcFileMetadata f) {
    assert fileMetadata == null;
    fileMetadata = f;
    stripes = new OrcStripeMetadata[f.getStripes().size()];
    codec = WriterImpl.createCodec(fileMetadata.getCompressionKind());
  }

  public void setStripeMetadata(OrcStripeMetadata m) {
    assert stripes != null;
    stripes[m.getStripeIx()] = m;
  }

  private static ColumnVector createColumn(List<OrcProto.Type> types,
      final int columnId, int batchSize) {
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case DATE:
        return new LongColumnVector(batchSize);
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnVector(batchSize);
      case BINARY:
      case STRING:
      case CHAR:
      case VARCHAR:
        return new BytesColumnVector(batchSize);
      case TIMESTAMP:
        return new TimestampColumnVector(batchSize);
      case DECIMAL:
        return new DecimalColumnVector(batchSize, type.getPrecision(),
            type.getScale());
      case STRUCT: {
        List<Integer> subtypeIdxs = type.getSubtypesList();
        ColumnVector[] fieldVector = new ColumnVector[subtypeIdxs.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = createColumn(types, subtypeIdxs.get(i), batchSize);
        }
        return new StructColumnVector(batchSize, fieldVector);
      }
      case UNION: {
        List<Integer> subtypeIdxs = type.getSubtypesList();
        ColumnVector[] fieldVector = new ColumnVector[subtypeIdxs.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = createColumn(types, subtypeIdxs.get(i), batchSize);
        }
        return new UnionColumnVector(batchSize, fieldVector);
      }
      case LIST:
        return new ListColumnVector(batchSize, createColumn(types, type.getSubtypes(0), batchSize));
      case MAP:
        return new MapColumnVector(batchSize,
            createColumn(types, type.getSubtypes(0), batchSize),
            createColumn(types, type.getSubtypes(1), batchSize));
      default:
        throw new IllegalArgumentException("LLAP does not support " +
            type.getKind());
    }
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
            fileMetadata.getTypes(), stripeMetadata.getEncodings(), batch, codec, skipCorrupt,
                stripeMetadata.getWriterTimezone());
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
        List<OrcProto.Type> types = fileMetadata.getTypes();
        int[] columnMapping = batch.getColumnIxs();
        for (int idx = 0; idx < batch.getColumnIxs().length; idx++) {
          if (cvb.cols[idx] == null) {
            // Orc store rows inside a root struct (hive writes it this way).
            // When we populate column vectors we skip over the root struct.
            cvb.cols[idx] = createColumn(types, columnMapping[idx], batchSize);
          }
          cvb.cols[idx].ensureSize(batchSize, false);
          columnReaders[idx].nextVector(cvb.cols[idx], null, batchSize);
        }

        // we are done reading a batch, send it to consumer for processing
        downstreamConsumer.consumeData(cvb);
        counters.incrCounter(LlapIOCounters.ROWS_EMITTED, batchSize);
      }
      counters.incrTimeCounter(LlapIOCounters.DECODE_TIME_NS, startTime);
      counters.incrCounter(LlapIOCounters.NUM_VECTOR_BATCHES, maxBatchesRG);
      counters.incrCounter(LlapIOCounters.NUM_DECODED_BATCHES);
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
      // TODO: When hive moves to java8, make updateTimezone() as default method in SettableTreeReader so that we can
      // avoid this check
      if (columnReaders[i] instanceof EncodedTreeReaderFactory.TimestampStreamReader && !sameStripe) {
        ((EncodedTreeReaderFactory.TimestampStreamReader) columnReaders[i])
                .updateTimezone(stripeMetadata.getWriterTimezone());
      }
      columnReaders[i].seek(new RecordReaderImpl.PositionProviderImpl(rowIndexEntry));
    }
  }

  private long getRowCount(OrcProto.RowIndexEntry rowIndexEntry) {
    return rowIndexEntry.getStatistics().getNumberOfValues();
  }

  @Override
  public TypeDescription getFileSchema() {
    return OrcUtils.convertTypeFromProtobuf(fileMetadata.getTypes(), 0);
  }

  @Override
  public boolean[] getIncludedColumns() {
    return includedColumns;
  }

  public void setIncludedColumns(final boolean[] includedColumns) {
    this.includedColumns = includedColumns;
  }
}
