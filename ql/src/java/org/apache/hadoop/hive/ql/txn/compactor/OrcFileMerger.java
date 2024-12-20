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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Class to support fast merging of ORC files.
 */
public class OrcFileMerger {

  private final Configuration conf;
  private static final Logger LOG = LoggerFactory.getLogger(OrcFileMerger.class);

  public OrcFileMerger(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Merge orc files into a single file
   * @param readers list of orc file paths to be merged
   * @param outPath the path of output orc file
   * @throws IOException error happened during file operations
   */
  public void mergeFiles(List<Reader> readers, Path outPath) throws IOException {
    Writer writer = null;
    try {
      for (Reader reader : readers) {
        if (writer == null) {
          writer = setupWriter(reader, outPath);
        }
        VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
        RecordReader rows = reader.rows();
        while (rows.nextBatch(batch)) {
          if (batch != null) {
            writer.addRowBatch(batch);
          }
        }
        rows.close();
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  /**
   * Create a new instance of orc output writer. The writer parameters are collected from the orc reader.
   * @param reader orc reader of file
   * @param outPath the path of the output file
   * @return a new instance of orc writer, always non-null
   * @throws IOException error during file operation
   */
  private Writer setupWriter(Reader reader, Path outPath) throws IOException {
    OrcFile.WriterOptions options =
            OrcFile.writerOptions(conf).compress(reader.getCompression()).version(reader.getFileVersion())
                    .rowIndexStride(reader.getRowIndexStride()).inspector(reader.getObjectInspector());
    if (CompressionKind.NONE != reader.getCompression()) {
      options.bufferSize(reader.getCompressionSize()).enforceBufferSize();
    }
    Writer writer = OrcFile.createWriter(outPath, options);
    LOG.info("ORC merge file output path: {}", outPath);
    return writer;
  }

  /**
   * Check compatibility between readers.
   * @param readers list of readers
   * @return true, if the readers are compatible
   */
  public boolean checkCompatibility(final List<Reader> readers) {
    if (readers == null || readers.isEmpty()) {
      return false;
    }

    if (!readers.stream().allMatch(
      r -> readers.get(0).getSchema().equals(r.getSchema()) && readers.get(0).getCompression()
              .equals(r.getCompression()) && readers.get(0).getCompressionSize() == r.getCompressionSize() && readers
              .get(0).getFileVersion().equals(r.getFileVersion()) && readers.get(0).getWriterVersion()
              .equals(r.getWriterVersion()) && readers.get(0).getRowIndexStride() == r.getRowIndexStride())) {
      LOG.warn("Incompatible ORC file merge!");
      return false;
    }
    return true;
  }
}