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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;

public class OrcMetadata {
  private CompressionKind compressionKind;
  private int compressionBufferSize;
  private List<OrcProto.Type> types;
  private List<StripeInformation> stripes;
  private Map<Integer, List<OrcProto.ColumnEncoding>> stripeToColEncodings;
  private Map<Integer, OrcProto.RowIndex[]> stripeToRowIndexEntries;

  public Map<Integer, List<OrcProto.ColumnEncoding>> getStripeToColEncodings() {
    return stripeToColEncodings;
  }

  public void setStripeToColEncodings(
      Map<Integer, List<OrcProto.ColumnEncoding>> stripeToColEncodings) {
    this.stripeToColEncodings = stripeToColEncodings;
  }

  public Map<Integer, OrcProto.RowIndex[]> getStripeToRowIndexEntries() {
    return stripeToRowIndexEntries;
  }

  public void setStripeToRowIndexEntries(
      Map<Integer, OrcProto.RowIndex[]> stripeToRowIndexEntries) {
    this.stripeToRowIndexEntries = stripeToRowIndexEntries;
  }

  public List<StripeInformation> getStripes() {
    return stripes;
  }

  public void setStripes(List<StripeInformation> stripes) {
    this.stripes = stripes;
  }

  public CompressionKind getCompressionKind() {
    return compressionKind;
  }

  public void setCompressionKind(CompressionKind compressionKind) {
    this.compressionKind = compressionKind;
  }

  public int getCompressionBufferSize() {
    return compressionBufferSize;
  }

  public void setCompressionBufferSize(int compressionBufferSize) {
    this.compressionBufferSize = compressionBufferSize;
  }

  public List<OrcProto.Type> getTypes() {
    return types;
  }

  public void setTypes(List<OrcProto.Type> types) {
    this.types = types;
  }
}
