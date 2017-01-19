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
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader;
import org.apache.hadoop.hive.llap.io.metadata.ConsumerFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.ConsumerStripeMetadata;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.OrcProto.ColumnEncoding;
import org.apache.orc.OrcProto.RowIndex;
import org.apache.orc.OrcProto.RowIndexEntry;
import org.apache.orc.OrcProto.Type;
import org.apache.orc.TypeDescription;

public class GenericColumnVectorProducer implements ColumnVectorProducer {
  private final SerDeLowLevelCacheImpl cache;
  private final BufferUsageManager bufferManager;
  private final Configuration conf;
  private final LlapDaemonCacheMetrics cacheMetrics;
  private final LlapDaemonIOMetrics ioMetrics;

  public GenericColumnVectorProducer(SerDeLowLevelCacheImpl serdeCache,
      BufferUsageManager bufferManager, Configuration conf, LlapDaemonCacheMetrics cacheMetrics,
      LlapDaemonIOMetrics ioMetrics) {
    LlapIoImpl.LOG.info("Initializing ORC column vector producer");
    this.cache = serdeCache;
    this.bufferManager = bufferManager;
    this.conf = conf;
    this.cacheMetrics = cacheMetrics;
    this.ioMetrics = ioMetrics;
  }

  @Override
  public ReadPipeline createReadPipeline(Consumer<ColumnVectorBatch> consumer, FileSplit split,
      List<Integer> columnIds, SearchArgument sarg, String[] columnNames,
      QueryFragmentCounters counters, TypeDescription schema, InputFormat<?, ?> sourceInputFormat,
      Deserializer sourceSerDe, Reporter reporter, JobConf job) throws IOException {
    cacheMetrics.incrCacheReadRequests();
    OrcEncodedDataConsumer edc = new OrcEncodedDataConsumer(
        consumer, columnIds.size(), false, counters, ioMetrics);
    TextFileMetadata fm;
    try {
      fm = new TextFileMetadata(sourceSerDe);
    } catch (SerDeException e) {
      throw new IOException(e);
    }
    edc.setFileMetadata(fm);
    // Note that we pass job config to the record reader, but use global config for LLAP IO.
    SerDeEncodedDataReader reader = new SerDeEncodedDataReader(cache,
        bufferManager, conf, split, columnIds, edc, job, reporter, sourceInputFormat,
        sourceSerDe, counters, fm.getSchema());
    edc.init(reader, reader);
    if (LlapIoImpl.LOG.isDebugEnabled()) {
      LlapIoImpl.LOG.debug("Ignoring schema: " + schema);
    }
    return edc;
  }


  public static final class TextStripeMetadata implements ConsumerStripeMetadata {
    // The writer is local to the process.
    private final String writerTimezone = TimeZone.getDefault().getID();
    private List<ColumnEncoding> encodings;
    private final int stripeIx;
    private long rowCount = -1;

    public TextStripeMetadata(int stripeIx) {
      this.stripeIx = stripeIx;
    }

    @Override
    public String getWriterTimezone() {
      return writerTimezone;
    }

    @Override
    public int getStripeIx() {
      return stripeIx;
    }

    @Override
    public long getRowCount() {
      return rowCount;
    }

    @Override
    public List<ColumnEncoding> getEncodings() {
      return encodings;
    }

    @Override
    public RowIndexEntry getRowIndexEntry(int colIx, int rgIx) {
      throw new UnsupportedOperationException();
    }

    public void setEncodings(List<ColumnEncoding> encodings) {
      this.encodings = encodings;
    }

    @Override
    public RowIndex[] getRowIndexes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsRowIndexes() {
      return false;
    }

    public void setRowCount(long value) {
      rowCount = value;
    }

    @Override
    public String toString() {
      return "[stripeIx=" + stripeIx + ", rowCount=" + rowCount + ", encodings=" + encodings + "]".replace('\n', ' ');
    }
  }


  private static final class TextFileMetadata implements ConsumerFileMetadata {
    private final List<Type> orcTypes = new ArrayList<>();
    private final TypeDescription schema;
    public TextFileMetadata(Deserializer sourceSerDe) throws SerDeException {
      TypeDescription schema = OrcInputFormat.convertTypeInfo(
          TypeInfoUtils.getTypeInfoFromObjectInspector(sourceSerDe.getObjectInspector()));
      this.schema = schema;
      addTypesFromSchema(schema);
    }

    private void addTypesFromSchema(TypeDescription schema) {
      // The same thing that WriterImpl does when writing the footer, but w/o the footer.
      OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
      List<TypeDescription> children = OrcUtils.setTypeBuilderFromSchema(type, schema);
      orcTypes.add(type.build());
      if (children == null) return;
      for(TypeDescription child : children) {
        addTypesFromSchema(child);
      }
    }

    @Override
    public List<Type> getTypes() {
      return orcTypes;
    }

    @Override
    public int getStripeCount() {
      return 1;
    }

    @Override
    public CompressionKind getCompressionKind() {
      return CompressionKind.NONE;
    }

    @Override
    public TypeDescription getSchema() {
      return schema;
    }
  }
}
