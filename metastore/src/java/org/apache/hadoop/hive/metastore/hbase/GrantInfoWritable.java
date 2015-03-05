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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class to serialize grant information.  There is not a corresponding thrift object.
 */
class GrantInfoWritable implements Writable {
  String principalName;
  PrincipalType principalType;
  int addTime;
  String grantor;
  PrincipalType grantorType;
  boolean grantOption;

  GrantInfoWritable() {
  }

  /**
   *
   * @param name name of the user or role
   * @param type whether this is a user or a role
   * @param addTime time user was added to role
   * @param grantor user or role who granted this principal into the role
   * @param grantorType whether the grantor was a user or a role
   * @param withGrantOption whether this principal has the grant option
   */
  GrantInfoWritable(String name, PrincipalType type, int addTime, String grantor,
                    PrincipalType grantorType, boolean withGrantOption) {
    principalName = name;
    principalType = type;
    this.addTime = addTime;
    this.grantor = grantor;
    this.grantorType = grantorType;
    grantOption = withGrantOption;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HBaseUtils.writeStr(out, principalName);
    out.writeInt(principalType.getValue());
    out.writeInt(addTime);
    HBaseUtils.writeStr(out, grantor);
    out.writeInt(grantorType.getValue());
    out.writeBoolean(grantOption);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    principalName = HBaseUtils.readStr(in);
    principalType = PrincipalType.findByValue(in.readInt());
    addTime = in.readInt();
    grantor = HBaseUtils.readStr(in);
    grantorType = PrincipalType.findByValue(in.readInt());
    grantOption = in.readBoolean();
  }
}
