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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;

/**
 * Value for OrcFileMergeMapper. Contains stripe related information for the
 * current orc file that is being merged.
 */
public class OrcFileValueWrapper implements WritableComparable<OrcFileValueWrapper> {

  protected StripeInformation stripeInformation;
  protected OrcProto.StripeStatistics stripeStatistics;
  protected List<OrcProto.UserMetadataItem> userMetadata;
  protected boolean lastStripeInFile;

  public List<OrcProto.UserMetadataItem> getUserMetadata() {
    return userMetadata;
  }

  public void setUserMetadata(List<OrcProto.UserMetadataItem> userMetadata) {
    this.userMetadata = userMetadata;
  }

  public boolean isLastStripeInFile() {
    return lastStripeInFile;
  }

  public void setLastStripeInFile(boolean lastStripeInFile) {
    this.lastStripeInFile = lastStripeInFile;
  }

  public OrcProto.StripeStatistics getStripeStatistics() {
    return stripeStatistics;
  }

  public void setStripeStatistics(OrcProto.StripeStatistics stripeStatistics) {
    this.stripeStatistics = stripeStatistics;
  }

  public StripeInformation getStripeInformation() {
    return stripeInformation;
  }

  public void setStripeInformation(StripeInformation stripeInformation) {
    this.stripeInformation = stripeInformation;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public int compareTo(OrcFileValueWrapper o) {
    if (stripeInformation.getOffset() < o.getStripeInformation().getOffset()) {
      return -1;
    } else if (stripeInformation.getOffset() > o.getStripeInformation().getOffset()) {
      return 1;
    } else {
      return 0;
    }
  }

}
