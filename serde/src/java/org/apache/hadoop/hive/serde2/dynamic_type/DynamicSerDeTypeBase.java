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

import java.io.Serializable;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

/**
 * DynamicSerDeTypeBase.
 *
 */
public abstract class DynamicSerDeTypeBase extends DynamicSerDeSimpleNode
    implements Serializable {
  private static final long serialVersionUID = 1L;

  public DynamicSerDeTypeBase(int i) {
    super(i);
  }

  public DynamicSerDeTypeBase(thrift_grammar p, int i) {
    super(p, i);
  }

  public void initialize() {
    // for base type, do nothing. Other types, like structs may initialize
    // internal data
    // structures.
  }

  public Class getRealType() throws SerDeException {
    throw new SerDeException("Not implemented in base");
  }

  public Object get(Object obj) {
    throw new RuntimeException("Not implemented in base");
  }

  public abstract Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException;

  public abstract void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException, IllegalAccessException;

  @Override
  public String toString() {
    return "BAD";
  }

  public byte getType() {
    return -1;
  }

  public boolean isPrimitive() {
    return true;
  }

  public boolean isList() {
    return false;
  }

  public boolean isMap() {
    return false;
  }

}
