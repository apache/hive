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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.io.Text;

/**
 * A WritableStringObjectInspector inspects a Text Object.
 */
public class WritableStringObjectInspector extends
    AbstractPrimitiveWritableObjectInspector implements
    SettableStringObjectInspector {

  WritableStringObjectInspector() {
    super(PrimitiveObjectInspectorUtils.stringTypeEntry);
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new Text((Text) o);
  }

  @Override
  public Text getPrimitiveWritableObject(Object o) {
    return o == null ? null : (Text) o;
  }

  @Override
  public String getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((Text) o).toString();
  }

  @Override
  public Object create(Text value) {
    Text r = new Text();
    if (value != null) {
      r.set(value);
    }
    return r;
  }

  @Override
  public Object create(String value) {
    Text r = new Text();
    if (value != null) {
      r.set(value);
    }
    return r;
  }

  @Override
  public Object set(Object o, Text value) {
    Text r = (Text) o;
    if (value != null) {
      r.set(value);
    }
    return o;
  }

  @Override
  public Object set(Object o, String value) {
    Text r = (Text) o;
    if (value != null) {
      r.set(value);
    }
    return o;
  }

}
