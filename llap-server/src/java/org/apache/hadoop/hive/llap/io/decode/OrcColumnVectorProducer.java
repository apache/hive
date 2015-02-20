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
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
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
import org.apache.hadoop.hive.llap.io.encoded.EncodedDataProducer;
import org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataProducer;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

public class OrcColumnVectorProducer extends ColumnVectorProducer<OrcBatchKey> {
  private final OrcEncodedDataProducer edp;
  private final OrcMetadataCache metadataCache;
  private boolean skipCorrupt;

  public OrcColumnVectorProducer(
      ExecutorService executor, OrcEncodedDataProducer edp, Configuration conf) {
    super(executor);
    this.edp = edp;
    this.metadataCache = OrcMetadataCache.getInstance();
    this.skipCorrupt = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ORC_SKIP_CORRUPT_DATA);
  }

  @Override
  protected EncodedDataProducer<OrcBatchKey> getEncodedDataProducer() {
    return edp;
  }

  @Override
  protected void decodeBatch(EncodedColumnBatch<OrcBatchKey> batch,
      Consumer<ColumnVectorBatch> downstreamConsumer) {
    String fileName = batch.batchKey.file;

    // OrcEncodedDataProducer should have just loaded cache entries from this file.
    // The default LRU algorithm shouldn't have dropped the entries. To make it
    // safe, untie the code from EDP into separate class and make use of loading cache. The current
    // assumption is that entries for the current file exists in metadata cache.
    try {
      OrcFileMetadata fileMetadata = metadataCache.getFileMetadata(fileName);
      OrcBatchKey stripeKey = batch.batchKey.clone();

      // To get stripe metadata we only need to know the stripe number. Oddly, stripe metadata
      // accepts BatchKey as key. We need to keep to row group index in batch key the same to
      // retrieve the stripe metadata properly. To make sure we get the correct stripe
      // metadata, set row group index to 0. That's how it is cached. See OrcEncodedDataProducer
      stripeKey.rgIx = 0;
      OrcStripeMetadata stripeMetadata = metadataCache.getStripeMetadata(stripeKey);

      // Get non null row count from root column, to get max vector batches
      int rgIdx = batch.batchKey.rgIx;
      OrcProto.RowIndexEntry rowIndex = stripeMetadata.getRowIndexes()[0].getEntry(rgIdx);
      long nonNullRowCount = getRowCount(rowIndex);
      int maxBatchesRG = (int) ((nonNullRowCount / VectorizedRowBatch.DEFAULT_SIZE) + 1);
      int batchSize = VectorizedRowBatch.DEFAULT_SIZE;
      int numCols = batch.columnIxs.length;
      RecordReaderImpl.TreeReader[] columnReaders = createTreeReaders(numCols, batch, fileMetadata,
          stripeMetadata);

      for (int i = 0; i < maxBatchesRG; i++) {
        ColumnVectorBatch cvb = new ColumnVectorBatch(batch.columnIxs.length);

        // for last batch in row group, adjust the batch size
        if (i == maxBatchesRG - 1) {
          batchSize = (int) (nonNullRowCount % VectorizedRowBatch.DEFAULT_SIZE);
          cvb.size = batchSize;
        }

        for (int idx = 0; idx < batch.columnIxs.length; idx++) {
          cvb.cols[idx] = (ColumnVector) columnReaders[idx].nextVector(null, batchSize);
        }

        // we are done reading a batch, send it to consumer for processing
        downstreamConsumer.consumeData(cvb);
      }
    } catch (IOException | CloneNotSupportedException e) {
      downstreamConsumer.setError(e);
    }
  }

  private RecordReaderImpl.TreeReader[] createTreeReaders(int numCols,
      EncodedColumnBatch<OrcBatchKey> batch,
      OrcFileMetadata fileMetadata,
      OrcStripeMetadata stripeMetadata) throws IOException {
    String file = batch.batchKey.file;
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
      int bufferSize = fileMetadata.getCompressionBufferSize();
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
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case BOOLEAN:
          treeReaders[i] = BooleanStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .build();
          break;
        case BYTE:
          treeReaders[i] = ByteStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .build();
          break;
        case SHORT:
          treeReaders[i] = ShortStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case INT:
          treeReaders[i] = IntStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case LONG:
          treeReaders[i] = LongStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .skipCorrupt(skipCorrupt)
              .build();
          break;
        case FLOAT:
          treeReaders[i] = FloatStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .build();
          break;
        case DOUBLE:
          treeReaders[i] = DoubleStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .build();
          break;
        case CHAR:
        case VARCHAR:
          treeReaders[i] = CharacterStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setMaxLength(columnType.getMaximumLength())
              .setCharacterType(columnType)
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setDictionaryStream(dictionary)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case STRING:
          treeReaders[i] = StringStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setDictionaryStream(dictionary)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case DECIMAL:
          treeReaders[i] = DecimalStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPrecision(columnType.getPrecision())
              .setScale(columnType.getScale())
              .setPresentStream(present)
              .setValueStream(data)
              .setScaleStream(secondary)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case TIMESTAMP:
          treeReaders[i] = TimestampStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setSecondsStream(data)
              .setNanosStream(secondary)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .skipCorrupt(skipCorrupt)
              .build();
          break;
        case DATE:
          treeReaders[i] = DateStreamReader.builder()
              .setFileName(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setBufferSize(bufferSize)
              .setRowIndex(rowIndexEntry)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        default:
          throw new UnsupportedOperationException("Data type not supported yet! " + columnType);
      }
    }
    return treeReaders;
  }

  private long getRowCount(OrcProto.RowIndexEntry rowIndexEntry) {
    return rowIndexEntry.getStatistics().getNumberOfValues();
  }
}
