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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;

public class OrcFileStripeMergeRecordReader implements
    RecordReader<OrcFileKeyWrapper, OrcFileValueWrapper> {
  private final Reader reader;
  private final Path path;
  protected Iterator<StripeInformation> iter;
  protected List<OrcProto.StripeStatistics> stripeStatistics;
  private int stripeIdx;
  private long start;
  private long end;
  private boolean skipFile;

  public OrcFileStripeMergeRecordReader(Configuration conf, FileSplit split) throws IOException {
    path = split.getPath();
    start = split.getStart();
    end = start + split.getLength();
    FileSystem fs = path.getFileSystem(conf);
    this.reader = OrcFile.createReader(path, OrcFile.readerOptions(conf).filesystem(fs));
    this.iter = reader.getStripes().iterator();
    this.stripeIdx = 0;
    this.stripeStatistics = ((ReaderImpl) reader).getOrcProtoStripeStatistics();
  }

  public Class<?> getKeyClass() {
    return OrcFileKeyWrapper.class;
  }

  public Class<?> getValueClass() {
    return OrcFileValueWrapper.class;
  }

  public OrcFileKeyWrapper createKey() {
    return new OrcFileKeyWrapper();
  }

  public OrcFileValueWrapper createValue() {
    return new OrcFileValueWrapper();
  }

  @Override
  public boolean next(OrcFileKeyWrapper key, OrcFileValueWrapper value) throws IOException {
    if (skipFile) {
      return false;
    }
    return nextStripe(key, value);
  }

  protected boolean nextStripe(OrcFileKeyWrapper keyWrapper, OrcFileValueWrapper valueWrapper)
      throws IOException {
    // missing stripe stats (old format). If numRows is 0 then its an empty file and no statistics
    // is present. We have to differentiate no stats (empty file) vs missing stats (old format).
    if ((stripeStatistics == null || stripeStatistics.isEmpty()) && reader.getNumberOfRows() > 0) {
      keyWrapper.setInputPath(path);
      keyWrapper.setIsIncompatFile(true);
      skipFile = true;
      return true;
    }

    while (iter.hasNext()) {
      StripeInformation si = iter.next();

      // if stripe offset is outside the split boundary then ignore the current
      // stripe as it will be handled by some other mapper.
      if (si.getOffset() >= start && si.getOffset() < end) {
        valueWrapper.setStripeStatistics(stripeStatistics.get(stripeIdx++));
        valueWrapper.setStripeInformation(si);
        if (!iter.hasNext()) {
          valueWrapper.setLastStripeInFile(true);
          valueWrapper.setUserMetadata(((ReaderImpl) reader).getOrcProtoUserMetadata());
        }
        keyWrapper.setInputPath(path);
        keyWrapper.setCompression(reader.getCompressionKind());
        keyWrapper.setCompressBufferSize(reader.getCompressionSize());
        keyWrapper.setFileVersion(reader.getFileVersion());
        keyWrapper.setWriterVersion(reader.getWriterVersion());
        keyWrapper.setRowIndexStride(reader.getRowIndexStride());
        keyWrapper.setFileSchema(reader.getSchema());
      } else {
        stripeIdx++;
        continue;
      }
      return true;
    }

    return false;
  }

  /**
   * Default progress will be based on number of files processed.
   * @return 0.0 to 1.0 of the input byte range
   */
  public float getProgress() throws IOException {
    return 0.0f;
  }

  public long getPos() throws IOException {
    return 0;
  }

  protected void seek(long pos) throws IOException {
  }

  public long getStart() {
    return 0;
  }

  public void close() throws IOException {
  }

}
