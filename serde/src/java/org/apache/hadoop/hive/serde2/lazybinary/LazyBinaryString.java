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
package org.apache.hadoop.hive.serde2.lazybinary;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * The serialization of LazyBinaryString is very simple: start A end bytes[] ->
 * |---------------------------------|
 * 
 * Section A is just an array of bytes which are exactly the Text contained in
 * this object.
 * 
 */
public class LazyBinaryString extends
    LazyBinaryPrimitive<WritableStringObjectInspector, Text> {

  LazyBinaryString(WritableStringObjectInspector OI) {
    super(OI);
    data = new Text();
  }

  public LazyBinaryString(LazyBinaryString copy) {
    super(copy);
    data = new Text(copy.data);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    assert (length > -1);
    data.set(bytes.getData(), start, length);
  }
}
