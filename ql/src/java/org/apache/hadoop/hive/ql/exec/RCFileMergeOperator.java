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
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileKeyBufferWrapper;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileValueBufferWrapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.RCFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.hive.shims.CombineHiveKey;

import java.io.IOException;

/**
 * Fast file merge operator for RC files.
 */
public class RCFileMergeOperator
    extends AbstractFileMergeOperator<RCFileMergeDesc> {
  public final static Log LOG = LogFactory.getLog("RCFileMergeMapper");

  RCFile.Writer outWriter;
  CompressionCodec codec = null;
  int columnNumber = 0;

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    Object[] keyValue = (Object[]) row;
    processKeyValuePairs(keyValue[0], keyValue[1]);
  }

  private void processKeyValuePairs(Object k, Object v)
      throws HiveException {
    try {

      RCFileKeyBufferWrapper key;
      if (k instanceof CombineHiveKey) {
        key = (RCFileKeyBufferWrapper) ((CombineHiveKey) k).getKey();
      } else {
        key = (RCFileKeyBufferWrapper) k;
      }
      RCFileValueBufferWrapper value = (RCFileValueBufferWrapper) v;

      fixTmpPath(key.getInputPath().getParent());

      if (outWriter == null) {
        codec = key.getCodec();
        columnNumber = key.getKeyBuffer().getColumnNumber();
        RCFileOutputFormat.setColumnNumber(jc, columnNumber);
        outWriter = new RCFile.Writer(fs, jc, outPath, null, codec);
      }

      boolean sameCodec = ((codec == key.getCodec()) || codec.getClass().equals(
          key.getCodec().getClass()));

      if ((key.getKeyBuffer().getColumnNumber() != columnNumber) ||
          (!sameCodec)) {
        throw new IOException( "RCFileMerge failed because the input files" +
            " use different CompressionCodec or have different column number" +
            " setting.");
      }

      outWriter.flushBlock(key.getKeyBuffer(), value.getValueBuffer(),
          key.getRecordLength(), key.getKeyLength(),
          key.getCompressedKeyLength());
    } catch (Throwable e) {
      this.exception = true;
      closeOp(true);
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    // close writer
    if (outWriter == null) {
      return;
    }

    try {
      outWriter.close();
    } catch (IOException e) {
      throw new HiveException("Unable to close RCFileMergeOperator", e);
    }
    outWriter = null;

    super.closeOp(abort);
  }

  @Override
  public OperatorType getType() {
    return OperatorType.RCFILEMERGE;
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "RFM";
  }
}
