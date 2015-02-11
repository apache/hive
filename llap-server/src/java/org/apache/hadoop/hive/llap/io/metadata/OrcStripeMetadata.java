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
package org.apache.hadoop.hive.llap.io.metadata;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.MetadataReader;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.StripeFooter;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;

public class OrcStripeMetadata {
  List<ColumnEncoding> encodings;
  List<Stream> streams;
  RowIndex[] rowIndexes;

  public OrcStripeMetadata(
      MetadataReader mr, StripeInformation stripe, boolean[] includes) throws IOException {
    StripeFooter footer = mr.readStripeFooter(stripe);
    streams = footer.getStreamsList();
    encodings = footer.getColumnsList();
    rowIndexes = mr.readRowIndex(stripe, footer, includes, null);
  }

  public boolean hasAllIndexes(boolean[] includes) {
    for (int i = 0; i < includes.length; ++i) {
      if (includes[i] && rowIndexes[i] == null) return false;
    }
    return true;
  }

  public void loadMissingIndexes(
      MetadataReader mr, StripeInformation stripe, boolean[] includes) throws IOException {
    // TODO: should we save footer to avoid a read here?
    rowIndexes = mr.readRowIndex(stripe, null, includes, rowIndexes);
  }

  public RowIndex[] getRowIndexes() {
    return rowIndexes;
  }

  public void setRowIndexes(RowIndex[] rowIndexes) {
    this.rowIndexes = rowIndexes;
  }

  public List<ColumnEncoding> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<ColumnEncoding> encodings) {
    this.encodings = encodings;
  }

  public List<Stream> getStreams() {
    return streams;
  }

  public void setStreams(List<Stream> streams) {
    this.streams = streams;
  }

}
