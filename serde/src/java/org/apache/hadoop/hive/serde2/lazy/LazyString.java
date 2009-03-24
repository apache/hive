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
package org.apache.hadoop.hive.serde2.lazy;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;

/**
 * LazyObject for storing a value of String.
 */
public class LazyString extends LazyPrimitive<String> {

  public LazyString() {
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    // In the future, we should allow returning a Text Object to save the UTF-8
    // decoding/encoding, and the creation of new String object.
    try {
      data = Text.decode(bytes.getData(), start, length);
    } catch (CharacterCodingException e) {
      data = null;
    }
  }

}
