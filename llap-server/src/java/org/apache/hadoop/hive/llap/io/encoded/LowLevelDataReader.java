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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
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

public class LowLevelDataReader implements AutoCloseable {

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


  public LowLevelDataReader(Path path, Object fileKey, Configuration daemonConf, DataCache cache,
      FileMetadataCache metadataCache, CacheTag cacheTag, FixedSizedObjectPool<IoTrace> tracePool) throws IOException {
    this.path = path;
    this.fileKey = fileKey;
    this.daemonConf = daemonConf;
    this.cache = cache;
    this.metadataCache = metadataCache;
    this.cacheTag = cacheTag;
    this.tracePool = tracePool;
    init();
  }

  private void init() throws IOException {
    this.fsSupplier = getFsSupplier(path, daemonConf);
    Object fileKey = HdfsUtils.getFileId(fsSupplier.get(), path,
          HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_CACHE_ALLOW_SYNTHETIC_FILEID),
          HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID),
          !HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_IO_USE_FILEID_PATH));
    if(!fileKey.equals(this.fileKey)) {
      throw new IOException("File key mismatch.");
    }
    OrcFile.ReaderOptions opts = EncodedOrcFile.readerOptions(daemonConf).filesystem(fsSupplier);
    orcReader = EncodedOrcFile.createReader(path, opts);
  }

  private static Supplier<FileSystem> getFsSupplier(final Path path, final Configuration conf) {
    return () -> {
      try {
        return path.getFileSystem(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public void read(DiskRangeList ranges) throws IOException {
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
      encodedReader = orcReader.encodedReader(fileKey, cache, rawDataReader, null, ioTrace, false, cacheTag, false);
      encodedReader.readDataRanges(ranges);
    } finally {
      tracePool.offer(ioTrace);
    }
  }

  public void readFooter() {
    MemoryBufferOrBuffers tailBuffers = metadataCache.getFileMetadata(fileKey);
    if (tailBuffers == null) {
      ByteBuffer tailBufferBb = orcReader.getSerializedFileFooter();
      metadataCache.putFileMetadata(fileKey, tailBufferBb, cacheTag, null);
    }
  }

  @Override
  public void close() throws Exception {
    if (encodedReader != null) {
      encodedReader.close();
    }
  }
}
