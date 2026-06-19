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

package org.apache.hadoop.hive.llap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

public class FieldDesc implements Writable {
  private String name;
  private TypeInfo typeInfo;

  public FieldDesc() {
  }

  public FieldDesc(String name, TypeInfo typeInfo) {
    this.name = name;
    this.typeInfo = typeInfo;
  }

  public String getName() {
    return name;
  }

  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  @Override
  public String toString() {
    return getName() + ":" + getTypeInfo().toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(name);
    out.writeUTF(typeInfo.toString());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    name = in.readUTF();
    typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(in.readUTF());
  }
}