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

package org.apache.hadoop.hive.serde2.dynamic_type;

import org.apache.thrift.protocol.TType;

/**
 * DynamicSerDeStruct.
 *
 */
public class DynamicSerDeStruct extends DynamicSerDeStructBase {

  // production is: struct this.name { FieldList() }

  private static final int FD_FIELD_LIST = 0;

  public DynamicSerDeStruct(int i) {
    super(i);
  }

  public DynamicSerDeStruct(thrift_grammar p, int i) {
    super(p, i);
  }

  @Override
  public String toString() {
    String result = "struct " + name + "(";
    result += getFieldList().toString();
    result += ")";
    return result;
  }

  @Override
  public DynamicSerDeFieldList getFieldList() {
    return (DynamicSerDeFieldList) jjtGetChild(FD_FIELD_LIST);
  }

  @Override
  public byte getType() {
    return TType.STRUCT;
  }

}
