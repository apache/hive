/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFileKeyWrapper;
import org.apache.hadoop.hive.ql.io.orc.OrcFileValueWrapper;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OrcFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.shims.CombineHiveKey;

/**
 * Fast file merge operator for ORC files.
 */
public class OrcFileMergeOperator extends
    AbstractFileMergeOperator<OrcFileMergeDesc> {
  public final static Logger LOG = LoggerFactory.getLogger("OrcFileMergeOperator");

  // These parameters must match for all orc files involved in merging. If it
  // does not merge, the file will be put into incompatible file set and will
  // not be merged.
  private CompressionKind compression = null;
  private int compressBuffSize = 0;
  private OrcFile.Version fileVersion;
  private OrcFile.WriterVersion writerVersion;
  private TypeDescription fileSchema;
  private int rowIndexStride = 0;

  private Map<Integer, Writer> outWriters = new HashMap<>();
  private Path prevPath;
  private FSDataInputStream fdis;
  private ObjectInspector obi;

  /** Kryo ctor. */
  protected OrcFileMergeOperator() {
    super();
  }

  public OrcFileMergeOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    Object[] keyValue = (Object[]) row;
    processKeyValuePairs(keyValue[0], keyValue[1]);
  }

  private void processKeyValuePairs(Object key, Object value)
      throws HiveException {
    String filePath = "";
    boolean exception = false;
    try {
      OrcFileValueWrapper v;
      OrcFileKeyWrapper k;
      if (key instanceof CombineHiveKey) {
        k = (OrcFileKeyWrapper) ((CombineHiveKey) key).getKey();
      } else {
        k = (OrcFileKeyWrapper) key;
      }

      // skip incompatible file, files that are missing stripe statistics are set to incompatible
      if (k.isIncompatFile()) {
        LOG.warn("Incompatible ORC file merge! Stripe statistics is missing. " + k.getInputPath());
        addIncompatibleFile(k.getInputPath());
        return;
      }

      filePath = k.getInputPath().toUri().getPath();

      Utilities.FILE_OP_LOGGER.info("OrcFileMergeOperator processing " + filePath);


      fixTmpPath(k.getInputPath().getParent());

      v = (OrcFileValueWrapper) value;

      if (prevPath == null) {
        prevPath = k.getInputPath();
      }
      if (obi == null) {
        Reader reader = OrcFile.createReader(fs, prevPath);
        obi = reader.getObjectInspector();
        try {
          reader.close();
        } catch (IOException e) {
          throw new HiveException(String.format("Unable to close reader for %s", filePath), e);
        }
      }
      LOG.info("ORC merge file input path: " + k.getInputPath());

      // store the orc configuration from the first file. All other files should
      // match this configuration before merging else will not be merged
      int bucketId = 0;
      if (conf.getIsCompactionTable()) {
        bucketId = AcidUtils.parseBucketId(new Path(filePath));
      }
      if (outWriters.get(bucketId) == null) {
        compression = k.getCompression();
        compressBuffSize = k.getCompressBufferSize();
        fileVersion = k.getFileVersion();
        writerVersion = k.getWriterVersion();
        fileSchema = k.getFileSchema();
        rowIndexStride = k.getRowIndexStride();

        OrcFile.WriterOptions options = OrcFile.writerOptions(jc)
            .compress(compression)
            .version(fileVersion)
            .rowIndexStride(rowIndexStride)
            .inspector(obi);
        // compression buffer size should only be set if compression is enabled
        if (compression != CompressionKind.NONE) {
          // enforce is required to retain the buffer sizes of old files instead of orc writer
          // inferring the optimal buffer size
          options.bufferSize(compressBuffSize).enforceBufferSize();
        }

        Path outPath = getOutPath();
        if (conf.getIsCompactionTable()) {
          outPath = getOutPath(bucketId);
        }
        outWriters.put(bucketId, OrcFile.createWriter(outPath, options));
        LOG.info("ORC merge file output path: {}", outPath);
      }

      if (!checkCompatibility(k)) {
        addIncompatibleFile(k.getInputPath());
        return;
      }

      // initialize buffer to read the entire stripe
      byte[] buffer = new byte[(int) v.getStripeInformation().getLength()];
      fdis = fs.open(k.getInputPath());
      fdis.readFully(v.getStripeInformation().getOffset(), buffer, 0,
          (int) v.getStripeInformation().getLength());

      // append the stripe buffer to the new ORC file
      outWriters.get(bucketId).appendStripe(buffer, 0, buffer.length, v.getStripeInformation(),
          v.getStripeStatistics());

      if (LOG.isInfoEnabled()) {
        LOG.info("Merged stripe from file " + k.getInputPath() + " [ offset : "
            + v.getStripeInformation().getOffset() + " length: "
            + v.getStripeInformation().getLength() + " row: "
            + v.getStripeStatistics().getColStats(0).getNumberOfValues() + " ]");
      }

      // add user metadata to footer in case of any
      if (v.isLastStripeInFile()) {
        for (Map.Entry<String, ByteBuffer> entry: v.getUserMetadata().entrySet()) {
          outWriters.get(bucketId).addUserMetadata(entry.getKey(), entry.getValue());
        }
      }
    } catch (Throwable e) {
      exception = true;
      LOG.error("Closing operator..Exception: " + ExceptionUtils.getStackTrace(e));
      throw new HiveException(e);
    } finally {
      if (exception) {
        closeOp(true);
      }
      if (fdis != null) {
        try {
          fdis.close();
        } catch (IOException e) {
          throw new HiveException(String.format("Unable to close file %s", filePath), e);
        } finally {
          fdis = null;
        }
      }
    }
  }

  private boolean checkCompatibility(OrcFileKeyWrapper k) {
    // check compatibility with subsequent files
    if (!fileSchema.equals(k.getFileSchema())) {
      LOG.warn("Incompatible ORC file merge! Schema mismatch for " + k.getInputPath());
      return false;
    }

    if (!k.getCompression().equals(compression)) {
      LOG.warn("Incompatible ORC file merge! Compression codec mismatch for " + k.getInputPath());
      return false;
    }

    if (k.getCompressBufferSize() != compressBuffSize) {
      LOG.warn("Incompatible ORC file merge! Compression buffer size mismatch for " + k.getInputPath());
      return false;

    }

    if (!k.getFileVersion().equals(fileVersion)) {
      LOG.warn("Incompatible ORC file merge! File version mismatch for " + k.getInputPath());
      return false;
    }

    if (!k.getWriterVersion().equals(writerVersion)) {
      LOG.warn("Incompatible ORC file merge! Writer version mismatch for " + k.getInputPath());
      return false;
    }

    if (k.getRowIndexStride() != rowIndexStride) {
      LOG.warn("Incompatible ORC file merge! Row index stride mismatch for " + k.getInputPath());
      return false;
    }

    return true;
  }

  @Override
  public OperatorType getType() {
    return OperatorType.ORCFILEMERGE;
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "OFM";
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    try {
      if (fdis != null) {
        fdis.close();
        fdis = null;
      }

      if (outWriters != null) {
        for (Map.Entry<Integer, Writer> outWriterEntry : outWriters.entrySet()) {
          Writer outWriter = outWriterEntry.getValue();
          outWriter.close();
        }
        outWriters.clear();
      }
    } catch (Exception e) {
      throw new HiveException("Unable to close OrcFileMergeOperator", e);
    }

    // When there are no exceptions, this has to be called always to make sure incompatible files
    // are moved properly to the destination path
    super.closeOp(abort);
  }
}
