/**
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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFileKeyWrapper;
import org.apache.hadoop.hive.ql.io.orc.OrcFileValueWrapper;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OrcFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.shims.CombineHiveKey;

/**
 * Fast file merge operator for ORC files.
 */
public class OrcFileMergeOperator extends
    AbstractFileMergeOperator<OrcFileMergeDesc> {
  public final static Log LOG = LogFactory.getLog("OrcFileMergeOperator");

  // These parameters must match for all orc files involved in merging. If it
  // does not merge, the file will be put into incompatible file set and will
  // not be merged.
  CompressionKind compression = null;
  int compressBuffSize = 0;
  OrcFile.Version version;
  int columnCount = 0;
  int rowIndexStride = 0;

  Writer outWriter;
  Path prevPath;
  private Reader reader;
  private FSDataInputStream fdis;

  @Override
  public void process(Object row, int tag) throws HiveException {
    Object[] keyValue = (Object[]) row;
    processKeyValuePairs(keyValue[0], keyValue[1]);
  }

  private void processKeyValuePairs(Object key, Object value)
      throws HiveException {
    String filePath = "";
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
        incompatFileSet.add(k.getInputPath());
        return;
      }

      filePath = k.getInputPath().toUri().getPath();

      fixTmpPath(k.getInputPath().getParent());

      v = (OrcFileValueWrapper) value;

      if (prevPath == null) {
        prevPath = k.getInputPath();
        reader = OrcFile.createReader(fs, k.getInputPath());
        if (isLogInfoEnabled) {
          LOG.info("ORC merge file input path: " + k.getInputPath());
        }
      }

      // store the orc configuration from the first file. All other files should
      // match this configuration before merging else will not be merged
      if (outWriter == null) {
        compression = k.getCompression();
        compressBuffSize = k.getCompressBufferSize();
        version = k.getVersion();
        columnCount = k.getTypes().get(0).getSubtypesCount();
        rowIndexStride = k.getRowIndexStride();

        OrcFile.WriterOptions options = OrcFile.writerOptions(jc)
            .compress(compression)
            .version(version)
            .rowIndexStride(rowIndexStride)
            .inspector(reader.getObjectInspector());
        // compression buffer size should only be set if compression is enabled
        if (compression != CompressionKind.NONE) {
          // enforce is required to retain the buffer sizes of old files instead of orc writer
          // inferring the optimal buffer size
          options.bufferSize(compressBuffSize).enforceBufferSize();
        }

        outWriter = OrcFile.createWriter(outPath, options);
        if (isLogDebugEnabled) {
          LOG.info("ORC merge file output path: " + outPath);
        }
      }

      if (!checkCompatibility(k)) {
        incompatFileSet.add(k.getInputPath());
        return;
      }

      // next file in the path
      if (!k.getInputPath().equals(prevPath)) {
        reader = OrcFile.createReader(fs, k.getInputPath());
      }

      // initialize buffer to read the entire stripe
      byte[] buffer = new byte[(int) v.getStripeInformation().getLength()];
      fdis = fs.open(k.getInputPath());
      fdis.readFully(v.getStripeInformation().getOffset(), buffer, 0,
          (int) v.getStripeInformation().getLength());

      // append the stripe buffer to the new ORC file
      outWriter.appendStripe(buffer, 0, buffer.length, v.getStripeInformation(),
          v.getStripeStatistics());

      if (isLogInfoEnabled) {
        LOG.info("Merged stripe from file " + k.getInputPath() + " [ offset : "
            + v.getStripeInformation().getOffset() + " length: "
            + v.getStripeInformation().getLength() + " row: "
            + v.getStripeStatistics().getColStats(0).getNumberOfValues() + " ]");
      }

      // add user metadata to footer in case of any
      if (v.isLastStripeInFile()) {
        outWriter.appendUserMetadata(v.getUserMetadata());
      }
    } catch (Throwable e) {
      this.exception = true;
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
    if ((k.getTypes().get(0).getSubtypesCount() != columnCount)) {
      LOG.warn("Incompatible ORC file merge! Column counts mismatch for " + k.getInputPath());
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

    if (!k.getVersion().equals(version)) {
      LOG.warn("Incompatible ORC file merge! Version mismatch for " + k.getInputPath());
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

      if (outWriter != null) {
        outWriter.close();
        outWriter = null;
      }
    } catch (Exception e) {
      throw new HiveException("Unable to close OrcFileMergeOperator", e);
    }

    // When there are no exceptions, this has to be called always to make sure incompatible files
    // are moved properly to the destination path
    super.closeOp(abort);
  }
}
