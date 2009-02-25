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

import org.apache.hadoop.io.Text;

/**
 * LazyObject stores an object in a range of bytes in a byte[].
 * 
 * A LazyObject can represent anything.
 *
 */
public class LazyObject {

  protected byte[] bytes;
  protected int start;
  protected int length;
  
  protected LazyObject() {
    bytes = null;
    start = 0;
    length = 0;
  }
  
  protected LazyObject(byte[] bytes, int start, int length) {
    setAll(bytes, start, length);
  }

  protected void setAll(byte[] bytes, int start, int length) {
    this.bytes = bytes;
    this.start = start;
    this.length = length;
  }
  
}
