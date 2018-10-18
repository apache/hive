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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.FileFormatProxy;
import org.apache.hadoop.hive.metastore.Metastore.SplitInfo;
import org.apache.hadoop.hive.metastore.Metastore.SplitInfos;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.impl.OrcTail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** File format proxy for ORC. */
public class OrcFileFormatProxy implements FileFormatProxy {
  private static final Logger LOG = LoggerFactory.getLogger(OrcFileFormatProxy.class);

  @Override
  public SplitInfos applySargToMetadata(
      SearchArgument sarg, ByteBuffer fileMetadata) throws IOException {
    // TODO: ideally we should store shortened representation of only the necessary fields
    //       in HBase; it will probably require custom SARG application code.
    OrcTail orcTail = ReaderImpl.extractFileTail(fileMetadata);
    OrcProto.Footer footer = orcTail.getFooter();
    int stripeCount = footer.getStripesCount();
    boolean[] result = OrcInputFormat.pickStripesViaTranslatedSarg(
        sarg, orcTail.getWriterVersion(),
        footer.getTypesList(), orcTail.getStripeStatistics(), stripeCount);
    // For ORC case, send the boundaries of the stripes so we don't have to send the footer.
    SplitInfos.Builder sb = SplitInfos.newBuilder();
    List<StripeInformation> stripes = orcTail.getStripes();
    boolean isEliminated = true;
    for (int i = 0; i < result.length; ++i) {
      if (result != null && !result[i]) continue;
      isEliminated = false;
      StripeInformation si = stripes.get(i);
      if (LOG.isDebugEnabled()) {
        LOG.debug("PPD is adding a split " + i + ": " + si.getOffset() + ", " + si.getLength());
      }
      sb.addInfos(SplitInfo.newBuilder().setIndex(i)
          .setOffset(si.getOffset()).setLength(si.getLength()));
    }
    return isEliminated ? null : sb.build();
  }

  public ByteBuffer[] getAddedColumnsToCache() {
    return null; // Nothing so far.
  }

  public ByteBuffer[][] getAddedValuesToCache(List<ByteBuffer> metadata) {
    throw new UnsupportedOperationException(); // Nothing so far (and shouldn't be called).
  }

  public ByteBuffer getMetadataToCache(
      FileSystem fs, Path path, ByteBuffer[] addedVals) throws IOException {
    // For now, there's nothing special to return in addedVals. Just return the footer.
    return OrcFile.createReader(fs, path).getSerializedFileFooter();
  }
}
