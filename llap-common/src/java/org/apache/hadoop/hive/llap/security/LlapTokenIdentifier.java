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

package org.apache.hadoop.hive.llap.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

/** For now, a LLAP token gives access to any LLAP server. */
public class LlapTokenIdentifier extends AbstractDelegationTokenIdentifier {
  private static final String KIND = "LLAP_TOKEN";
  public static final Text KIND_NAME = new Text(KIND);

  public LlapTokenIdentifier() {
    super();
  }

  public LlapTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    // Nothing right now.
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    // Nothing right now.
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public int hashCode() {
    // Nothing else right now.
    return super.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    // Nothing else right now.
    return super.equals(other);
  }

  @Override
  public String toString() {
    return KIND + "; " + super.toString();
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}
