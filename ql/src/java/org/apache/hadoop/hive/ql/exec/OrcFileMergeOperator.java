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
package org.apache.hadoop.hive.ql.exec;

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

import java.io.IOException;

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
  long compressBuffSize = 0;
  OrcFile.Version version;
  int columnCount = 0;
  int rowIndexStride = 0;

  Writer outWriter;
  Path prevPath;
  private Reader reader;
  private FSDataInputStream fdis;

  @Override
  public void processOp(Object row, int tag) throws HiveException {
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
      filePath = k.getInputPath().toUri().getPath();

      fixTmpPath(k.getInputPath().getParent());

      v = (OrcFileValueWrapper) value;

      if (prevPath == null) {
        prevPath = k.getInputPath();
        reader = OrcFile.createReader(fs, k.getInputPath());
        LOG.info("ORC merge file input path: " + k.getInputPath());
      }

      // store the orc configuration from the first file. All other files should
      // match this configuration before merging else will not be merged
      if (outWriter == null) {
        compression = k.getCompression();
        compressBuffSize = k.getCompressBufferSize();
        version = k.getVersion();
        columnCount = k.getTypes().get(0).getSubtypesCount();
        rowIndexStride = k.getRowIndexStride();

        // block size and stripe size will be from config
        outWriter = OrcFile.createWriter(outPath,
            OrcFile.writerOptions(jc)
                .compress(compression)
                .version(version)
                .rowIndexStride(rowIndexStride)
                .inspector(reader.getObjectInspector()));
        LOG.info("ORC merge file output path: " + outPath);
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

      LOG.info("Merged stripe from file " + k.getInputPath() + " [ offset : "
          + v.getStripeInformation().getOffset() + " length: "
          + v.getStripeInformation().getLength() + " ]");

      // add user metadata to footer in case of any
      if (v.isLastStripeInFile()) {
        outWriter.appendUserMetadata(v.getUserMetadata());
      }
    } catch (Throwable e) {
      this.exception = true;
      closeOp(true);
      throw new HiveException(e);
    } finally {
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
      LOG.info("Incompatible ORC file merge! Column counts does not match for "
          + k.getInputPath());
      return false;
    }

    if (!k.getCompression().equals(compression)) {
      LOG.info("Incompatible ORC file merge! Compression codec does not match" +
          " for " + k.getInputPath());
      return false;
    }

    if (k.getCompressBufferSize() != compressBuffSize) {
      LOG.info("Incompatible ORC file merge! Compression buffer size does not" +
          " match for " + k.getInputPath());
      return false;

    }

    if (!k.getVersion().equals(version)) {
      LOG.info("Incompatible ORC file merge! Version does not match for "
          + k.getInputPath());
      return false;
    }

    if (k.getRowIndexStride() != rowIndexStride) {
      LOG.info("Incompatible ORC file merge! Row index stride does not match" +
          " for " + k.getInputPath());
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
    // close writer
    if (outWriter == null) {
      return;
    }

    try {
      if (fdis != null) {
        fdis.close();
        fdis = null;
      }

      outWriter.close();
      outWriter = null;
    } catch (IOException e) {
      throw new HiveException("Unable to close OrcFileMergeOperator", e);
    }
    super.closeOp(abort);
  }
}
