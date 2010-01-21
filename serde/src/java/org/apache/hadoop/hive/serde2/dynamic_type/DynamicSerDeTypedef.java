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

package org.apache.hadoop.hive.serde2.dynamic_type;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

public class DynamicSerDeTypedef extends DynamicSerDeTypeBase {

  // production is: typedef DefinitionType() this.name

  private final static int FD_DEFINITION_TYPE = 0;

  public DynamicSerDeTypedef(int i) {
    super(i);
  }

  public DynamicSerDeTypedef(thrift_grammar p, int i) {
    super(p, i);
  }

  private DynamicSerDeSimpleNode getDefinitionType() {
    return (DynamicSerDeSimpleNode) jjtGetChild(FD_DEFINITION_TYPE);
  }

  public DynamicSerDeTypeBase getMyType() {
    DynamicSerDeSimpleNode child = getDefinitionType();
    DynamicSerDeTypeBase ret = (DynamicSerDeTypeBase) child.jjtGetChild(0);
    return ret;
  }

  @Override
  public String toString() {
    String result = "typedef " + name + "(";
    result += getDefinitionType().toString();
    result += ")";
    return result;
  }

  @Override
  public byte getType() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException {
    throw new RuntimeException("not implemented");
  }

  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException,
      IllegalAccessException {
    throw new RuntimeException("not implemented");
  }

}
