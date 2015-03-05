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

import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.StreamUtils;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.BinaryStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.BooleanStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.ByteStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.CharacterStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.DateStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.DecimalStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.DoubleStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.FloatStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.IntStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.LongStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.ShortStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.StringStreamReader;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.readers.TimestampStreamReader;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

public class OrcEncodedDataConsumer extends EncodedDataConsumer<OrcBatchKey> {
  private RecordReaderImpl.TreeReader[] columnReaders;
  private int previousStripeIndex = -1;
  private OrcFileMetadata fileMetadata; // We assume one request is only for one file.
  private OrcStripeMetadata[] stripes;
  private final boolean skipCorrupt; // TODO: get rid of this

  public OrcEncodedDataConsumer(
      Consumer<ColumnVectorBatch> consumer, int colCount, boolean skipCorrupt) {
    super(consumer, colCount);
    this.skipCorrupt = skipCorrupt;
  }

  public void setFileMetadata(OrcFileMetadata f) {
    assert fileMetadata == null;
    fileMetadata = f;
    stripes = new OrcStripeMetadata[f.getStripes().size()];
  }

  public void setStripeMetadata(OrcStripeMetadata m) {
    assert stripes != null;
    stripes[m.getStripeIx()] = m;
  }

  @Override
  protected void decodeBatch(EncodedColumnBatch<OrcBatchKey> batch,
      Consumer<ColumnVectorBatch> downstreamConsumer) {
    int currentStripeIndex = batch.batchKey.stripeIx;

    boolean sameStripe = currentStripeIndex == previousStripeIndex;

    try {
      OrcStripeMetadata stripeMetadata = stripes[currentStripeIndex];
      // Get non null row count from root column, to get max vector batches
      int rgIdx = batch.batchKey.rgIx;
      OrcProto.RowIndexEntry rowIndex = stripeMetadata.getRowIndexes()[0].getEntry(rgIdx);
      long nonNullRowCount = getRowCount(rowIndex);
      int maxBatchesRG = (int) ((nonNullRowCount / VectorizedRowBatch.DEFAULT_SIZE) + 1);
      int batchSize = VectorizedRowBatch.DEFAULT_SIZE;
      int numCols = batch.columnIxs.length;
      if (columnReaders == null || !sameStripe) {
        this.columnReaders = createTreeReaders(numCols, batch, fileMetadata, stripeMetadata);
      } else {
        repositionInStreams(this.columnReaders, batch, sameStripe, numCols, fileMetadata,
            stripeMetadata);
      }
      previousStripeIndex = currentStripeIndex;

      for (int i = 0; i < maxBatchesRG; i++) {
        ColumnVectorBatch cvb = new ColumnVectorBatch(batch.columnIxs.length);

        // for last batch in row group, adjust the batch size
        if (i == maxBatchesRG - 1) {
          batchSize = (int) (nonNullRowCount % VectorizedRowBatch.DEFAULT_SIZE);
          cvb.size = batchSize;
        }

        for (int idx = 0; idx < batch.columnIxs.length; idx++) {
          cvb.cols[idx] = (ColumnVector)columnReaders[idx].nextVector(null, batchSize);
        }

        // we are done reading a batch, send it to consumer for processing
        downstreamConsumer.consumeData(cvb);
      }
    } catch (IOException e) {
      // Caller will return the batch.
      downstreamConsumer.setError(e);
    }
  }

  RecordReaderImpl.TreeReader[] createTreeReaders(int numCols,
      EncodedColumnBatch<OrcBatchKey> batch,
      OrcFileMetadata fileMetadata,
      OrcStripeMetadata stripeMetadata) throws IOException {
    long file = batch.batchKey.file;
    RecordReaderImpl.TreeReader[] treeReaders = new RecordReaderImpl.TreeReader[numCols];

    for (int i = 0; i < numCols; i++) {
      int columnIndex = batch.columnIxs[i];
      int rowGroupIndex = batch.batchKey.rgIx;
      EncodedColumnBatch.StreamBuffer[] streamBuffers = batch.columnData[i];
      OrcProto.Type columnType = fileMetadata.getTypes().get(columnIndex);

      // EncodedColumnBatch is already decompressed, we don't really need to pass codec.
      // But we need to know if the original data is compressed or not. This is used to skip
      // positions in row index properly. If the file is originally compressed,
      // then 1st position (compressed offset) in row index should be skipped to get
      // uncompressed offset, else 1st position should not be skipped.
      CompressionCodec codec = fileMetadata.getCompressionCodec();
      OrcProto.ColumnEncoding columnEncoding = stripeMetadata.getEncodings().get(columnIndex);
      OrcProto.RowIndex rowIndex = stripeMetadata.getRowIndexes()[columnIndex];
      OrcProto.RowIndexEntry rowIndexEntry = rowIndex.getEntry(rowGroupIndex);

      // stream buffers are arranged in enum order of stream kind
      EncodedColumnBatch.StreamBuffer present = null;
      EncodedColumnBatch.StreamBuffer data = null;
      EncodedColumnBatch.StreamBuffer dictionary = null;
      EncodedColumnBatch.StreamBuffer lengths = null;
      EncodedColumnBatch.StreamBuffer secondary = null;
      for (EncodedColumnBatch.StreamBuffer streamBuffer : streamBuffers) {
        switch(streamBuffer.streamKind) {
          case 0:
            // PRESENT stream
            present = streamBuffer;
            break;
          case 1:
            // DATA stream
            data = streamBuffer;
            break;
          case 2:
            // LENGTH stream
            lengths = streamBuffer;
            break;
          case 3:
            // DICTIONARY_DATA stream
            dictionary = streamBuffer;
            break;
          case 5:
            // SECONDARY stream
            secondary = streamBuffer;
            break;
          default:
            throw new IOException("Unexpected stream kind: " + streamBuffer.streamKind);
        }
      }

      switch (columnType.getKind()) {
        case BINARY:
          treeReaders[i] = BinaryStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case BOOLEAN:
          treeReaders[i] = BooleanStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .build();
          break;
        case BYTE:
          treeReaders[i] = ByteStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .build();
          break;
        case SHORT:
          treeReaders[i] = ShortStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case INT:
          treeReaders[i] = IntStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case LONG:
          treeReaders[i] = LongStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .skipCorrupt(skipCorrupt)
              .build();
          break;
        case FLOAT:
          treeReaders[i] = FloatStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .build();
          break;
        case DOUBLE:
          treeReaders[i] = DoubleStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .build();
          break;
        case CHAR:
        case VARCHAR:
          treeReaders[i] = CharacterStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setMaxLength(columnType.getMaximumLength())
              .setCharacterType(columnType)
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setDictionaryStream(dictionary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case STRING:
          treeReaders[i] = StringStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setDictionaryStream(dictionary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case DECIMAL:
          treeReaders[i] = DecimalStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPrecision(columnType.getPrecision())
              .setScale(columnType.getScale())
              .setPresentStream(present)
              .setValueStream(data)
              .setScaleStream(secondary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case TIMESTAMP:
          treeReaders[i] = TimestampStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setSecondsStream(data)
              .setNanosStream(secondary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .skipCorrupt(skipCorrupt)
              .build();
          break;
        case DATE:
          treeReaders[i] = DateStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        default:
          throw new UnsupportedOperationException("Data type not supported yet! " + columnType);
      }
      treeReaders[i].seek(StreamUtils.getPositionProvider(rowIndexEntry));
    }
    return treeReaders;
  }

  private void repositionInStreams(RecordReaderImpl.TreeReader[] columnReaders,
      EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe, int numCols,
      OrcFileMetadata fileMetadata, OrcStripeMetadata stripeMetadata) throws IOException {
    for (int i = 0; i < numCols; i++) {
      int columnIndex = batch.columnIxs[i];
      int rowGroupIndex = batch.batchKey.rgIx;
      EncodedColumnBatch.StreamBuffer[] streamBuffers = batch.columnData[i];
      OrcProto.Type columnType = fileMetadata.getTypes().get(columnIndex);
      OrcProto.RowIndex rowIndex = stripeMetadata.getRowIndexes()[columnIndex];
      OrcProto.RowIndexEntry rowIndexEntry = rowIndex.getEntry(rowGroupIndex);

      // stream buffers are arranged in enum order of stream kind
      EncodedColumnBatch.StreamBuffer present = null;
      EncodedColumnBatch.StreamBuffer data = null;
      EncodedColumnBatch.StreamBuffer dictionary = null;
      EncodedColumnBatch.StreamBuffer lengths = null;
      EncodedColumnBatch.StreamBuffer secondary = null;
      for (EncodedColumnBatch.StreamBuffer streamBuffer : streamBuffers) {
        switch(streamBuffer.streamKind) {
          case 0:
            // PRESENT stream
            present = streamBuffer;
            break;
          case 1:
            // DATA stream
            data = streamBuffer;
            break;
          case 2:
            // LENGTH stream
            lengths = streamBuffer;
            break;
          case 3:
            // DICTIONARY_DATA stream
            dictionary = streamBuffer;
            break;
          case 5:
            // SECONDARY stream
            secondary = streamBuffer;
            break;
          default:
            throw new IOException("Unexpected stream kind: " + streamBuffer.streamKind);
        }
      }

      switch (columnType.getKind()) {
        case BINARY:
          ((BinaryStreamReader)columnReaders[i]).setBuffers(present, data, lengths);
          break;
        case BOOLEAN:
          ((BooleanStreamReader)columnReaders[i]).setBuffers(present, data);
          break;
        case BYTE:
          ((ByteStreamReader)columnReaders[i]).setBuffers(present, data);
          break;
        case SHORT:
          ((ShortStreamReader)columnReaders[i]).setBuffers(present, data);
          break;
        case INT:
          ((IntStreamReader)columnReaders[i]).setBuffers(present, data);
          break;
        case LONG:
          ((LongStreamReader)columnReaders[i]).setBuffers(present, data);
          break;
        case FLOAT:
          ((FloatStreamReader)columnReaders[i]).setBuffers(present, data);
          break;
        case DOUBLE:
          ((DoubleStreamReader)columnReaders[i]).setBuffers(present, data);
          break;
        case CHAR:
        case VARCHAR:
          ((CharacterStreamReader)columnReaders[i]).setBuffers(present, data, lengths, dictionary,
              sameStripe);
          break;
        case STRING:
          ((StringStreamReader)columnReaders[i]).setBuffers(present, data, lengths, dictionary,
              sameStripe);
          break;
        case DECIMAL:
          ((DecimalStreamReader)columnReaders[i]).setBuffers(present, data, secondary);
          break;
        case TIMESTAMP:
          ((TimestampStreamReader)columnReaders[i]).setBuffers(present, data, secondary);
          break;
        case DATE:
          ((DateStreamReader)columnReaders[i]).setBuffers(present, data);
          break;
        default:
          throw new UnsupportedOperationException("Data type not supported yet! " + columnType);
      }

      columnReaders[i].seek(StreamUtils.getPositionProvider(rowIndexEntry));
    }
  }

  private long getRowCount(OrcProto.RowIndexEntry rowIndexEntry) {
    return rowIndexEntry.getStatistics().getNumberOfValues();
  }
}
