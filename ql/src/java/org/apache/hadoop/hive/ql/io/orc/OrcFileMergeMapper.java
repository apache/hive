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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.merge.MergeMapper;
import org.apache.hadoop.hive.shims.CombineHiveKey;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Map task fast merging of ORC files.
 */
public class OrcFileMergeMapper extends MergeMapper implements
    Mapper<Object, OrcFileValueWrapper, Object, Object> {

  // These parameters must match for all orc files involved in merging
  CompressionKind compression = null;
  long compressBuffSize = 0;
  List<Integer> version;
  int columnCount = 0;
  int rowIndexStride = 0;

  Writer outWriter;
  private byte[] buffer;
  Path prevPath;
  private Reader reader;
  private FSDataInputStream fdis;
  public final static Log LOG = LogFactory.getLog("OrcFileMergeMapper");

  @Override
  public void configure(JobConf job) {
    super.configure(job);

    outWriter = null;
    buffer = null;
    prevPath = null;
    reader = null;
    fdis = null;
  }

  @Override
  public void map(Object key, OrcFileValueWrapper value, OutputCollector<Object, Object> output,
      Reporter reporter) throws IOException {
    try {

      OrcFileKeyWrapper k = null;
      if (key instanceof CombineHiveKey) {
        k = (OrcFileKeyWrapper) ((CombineHiveKey) key).getKey();
      } else {
        k = (OrcFileKeyWrapper) key;
      }

      fixTmpPathAlterTable(k.getInputPath().getParent());

      if (prevPath == null) {
        prevPath = k.getInputPath();
        reader = OrcFile.createReader(fs, k.inputPath);
      }

      // store the orc configuration from the first file. All other files should
      // match this configuration before merging
      if (outWriter == null) {
        compression = k.getCompression();
        compressBuffSize = k.getCompressBufferSize();
        version = k.getVersionList();
        columnCount = k.getTypes().get(0).getSubtypesCount();
        rowIndexStride = k.getRowIndexStride();

        // block size and stripe size will be from config
        outWriter = OrcFile.createWriter(outPath, OrcFile.writerOptions(jc).compress(compression)
            .inspector(reader.getObjectInspector()));
      }

      // check compatibility with subsequent files
      if ((k.getTypes().get(0).getSubtypesCount() != columnCount)) {
        throw new IOException("ORCFileMerge failed because the input files are not compatible."
            + " Column counts does not match.");
      }

      if (!k.compression.equals(compression)) {
        throw new IOException("ORCFileMerge failed because the input files are not compatible."
            + " Compression codec does not match.");
      }

      if (k.compressBufferSize != compressBuffSize) {
        throw new IOException("ORCFileMerge failed because the input files are not compatible."
            + " Compression buffer size does not match.");

      }

      if (!k.versionList.equals(version)) {
        throw new IOException("ORCFileMerge failed because the input files are not compatible."
            + " Version does not match.");
      }

      if (k.rowIndexStride != rowIndexStride) {
        throw new IOException("ORCFileMerge failed because the input files are not compatible."
            + " Row index stride does not match.");
      }

      // next file in the path
      if (!k.getInputPath().equals(prevPath)) {
        reader = OrcFile.createReader(fs, k.inputPath);
      }

      // initialize buffer to read the entire stripe
      buffer = new byte[(int) value.stripeInformation.getLength()];
      fdis = fs.open(k.inputPath);
      fdis.readFully(value.stripeInformation.getOffset(), buffer, 0,
          (int) value.stripeInformation.getLength());

      // append the stripe buffer to the new ORC file
      ((WriterImpl) outWriter).appendStripe(buffer, value.getStripeInformation(),
          value.getStripeStatistics());

      LOG.info("Merged stripe from file " + k.inputPath + " [ offset : "
          + value.getStripeInformation().getOffset() + " length: "
          + value.getStripeInformation().getLength() + " ]");

      // add user metadata to footer in case of any
      if (value.isLastStripeInFile()) {
        ((WriterImpl) outWriter).appendUserMetadata(value.getUserMetadata());
      }
    } catch (Throwable e) {
      this.exception = true;
      close();
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    // close writer
    if (outWriter == null) {
      return;
    }

    if (fdis != null) {
      fdis.close();
      fdis = null;
    }

    outWriter.close();
    outWriter = null;

    super.close();
  }
}
