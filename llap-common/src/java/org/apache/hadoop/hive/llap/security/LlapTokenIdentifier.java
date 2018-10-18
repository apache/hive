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

package org.apache.hadoop.hive.llap.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import com.google.common.base.Preconditions;

public class LlapTokenIdentifier extends AbstractDelegationTokenIdentifier {
  private static final String KIND = "LLAP_TOKEN";
  public static final Text KIND_NAME = new Text(KIND);
  private String clusterId;
  private String appId;
  private boolean isSigningRequired;

  public LlapTokenIdentifier() {
    super();
  }

  public LlapTokenIdentifier(Text owner, Text renewer, Text realUser,
      String clusterId, String appId, boolean isSigningRequired) {
    super(owner, renewer, realUser);
    Preconditions.checkNotNull(clusterId);
    this.clusterId = clusterId;
    this.appId = appId == null ? "" : appId;
    this.isSigningRequired = isSigningRequired;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(clusterId);
    out.writeUTF(appId);
    out.writeBoolean(isSigningRequired);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    clusterId = in.readUTF();
    Preconditions.checkNotNull(clusterId);
    appId = in.readUTF();
    isSigningRequired = in.readBoolean();
    appId = appId == null ? "" : appId;
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  public String getAppId() {
    return appId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public boolean isSigningRequired() {
    return isSigningRequired;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = prime * super.hashCode() + (StringUtils.isBlank(appId) ? 0 : appId.hashCode());
    result = prime * result + (isSigningRequired ? 1231 : 1237);
    return prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof LlapTokenIdentifier) || !super.equals(obj)) return false;
    LlapTokenIdentifier other = (LlapTokenIdentifier) obj;
    return isSigningRequired == other.isSigningRequired && (StringUtils.isBlank(appId)
        ? StringUtils.isBlank(other.appId) : appId.equals(other.appId))
        && (clusterId == null ? other.clusterId == null : clusterId.equals(other.clusterId));
  }

  @Override
  public String toString() {
    return KIND + "; " + super.toString() + ", cluster " + clusterId
        + ", app ID " + appId + ", signing " + isSigningRequired;
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}
