/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io.encoded;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedOrcFile;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedReader;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hadoop.hive.ql.io.orc.encoded.LlapDataReader;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.orc.OrcConf;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import static org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataReader.getFsSupplier;

/**
 * Loading data from ORC files on the disk into the cache.
 */
public class LlapOrcCacheLoader implements AutoCloseable {

  private Path path;
  private Object fileKey;
  private Configuration daemonConf;
  private DataCache cache;
  private FileMetadataCache metadataCache;
  private CacheTag cacheTag;
  private FixedSizedObjectPool<IoTrace> tracePool;

  private Supplier<FileSystem> fsSupplier;
  private Reader orcReader;
  private LlapDataReader rawDataReader;
  private EncodedReader encodedReader;


  public LlapOrcCacheLoader(Path path, Object fileKey, Configuration daemonConf, DataCache cache,
      FileMetadataCache metadataCache, CacheTag cacheTag, FixedSizedObjectPool<IoTrace> tracePool) {
    this.path = path;
    this.fileKey = fileKey;
    this.daemonConf = daemonConf;
    this.cache = cache;
    this.metadataCache = metadataCache;
    this.cacheTag = cacheTag;
    this.tracePool = tracePool;
  }

  public void init() throws IOException {
    fsSupplier = getFsSupplier(path, daemonConf);
    Object fileKey = LlapHiveUtils.createFileIdUsingFS(fsSupplier.get(), path, daemonConf);
    if(!fileKey.equals(this.fileKey)) {
      throw new IOException("File key mismatch.");
    }
    OrcFile.ReaderOptions opts = EncodedOrcFile.readerOptions(daemonConf).filesystem(fsSupplier);
    orcReader = EncodedOrcFile.createReader(path, opts);
  }

  /**
   * Pre read the provided ranges into the cache.
   */
  public void loadRanges(DiskRangeList ranges) throws IOException {
    boolean useZeroCopy = (daemonConf != null) && OrcConf.USE_ZEROCOPY.getBoolean(daemonConf);
    InStream.StreamOptions options = InStream.options()
        .withCodec(OrcCodecPool.getCodec(orcReader.getCompressionKind()))
        .withBufferSize(orcReader.getCompressionSize());

    rawDataReader = LlapRecordReaderUtils.createDefaultLlapDataReader(
        DataReaderProperties.builder()
            .withFileSystemSupplier(fsSupplier).withPath(path)
            .withCompression(options)
            .withZeroCopy(useZeroCopy)
            .build());
    rawDataReader.open();

    IoTrace ioTrace = tracePool.take();

    try {
      // Currently no thread pooling is used. Pre loading the cache doesn't have that much priority,
      // and the whole preRead will be running in a single separate thread.
      encodedReader = orcReader.encodedReader(fileKey, cache, rawDataReader, null, ioTrace, false, cacheTag, false);
      encodedReader.preReadDataRanges(ranges);
    } finally {
      tracePool.offer(ioTrace);
    }
  }

  /**
   * Pre read the file footer into the cache.
   */
  public void loadFileFooter() {
    MemoryBufferOrBuffers tailBuffers = metadataCache.getFileMetadata(fileKey);
    if (tailBuffers == null) {
      ByteBuffer tailBufferBb = orcReader.getSerializedFileFooter();
      metadataCache.putFileMetadata(fileKey, tailBufferBb, cacheTag, null);
    }
  }

  @Override
  public void close() throws IOException {
    if (orcReader != null) {
      IOUtils.closeQuietly(orcReader);
    }
    if (encodedReader != null) {
      encodedReader.close();
    }
  }
}
