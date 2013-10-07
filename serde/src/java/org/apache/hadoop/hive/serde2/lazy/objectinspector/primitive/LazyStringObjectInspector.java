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
package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

/**
 * A WritableStringObjectInspector inspects a Text Object.
 */
public class LazyStringObjectInspector extends
    AbstractPrimitiveLazyObjectInspector<Text> implements StringObjectInspector {

  private boolean escaped;
  private byte escapeChar;

  protected LazyStringObjectInspector() {
    super();
  }

  LazyStringObjectInspector(boolean escaped, byte escapeChar) {
    super(TypeInfoFactory.stringTypeInfo);
    this.escaped = escaped;
    this.escapeChar = escapeChar;
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new LazyString((LazyString) o);
  }

  @Override
  public Text getPrimitiveWritableObject(Object o) {
    return o == null ? null : ((LazyString) o).getWritableObject();
  }

  @Override
  public String getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((LazyString) o).getWritableObject().toString();
  }

  public boolean isEscaped() {
    return escaped;
  }

  public byte getEscapeChar() {
    return escapeChar;
  }

}
