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

public interface LazyObjectBase {

  /**
   * Set the data for this LazyObjectBase. We take ByteArrayRef instead of byte[] so
   * that we will be able to drop the reference to byte[] by a single
   * assignment. The ByteArrayRef object can be reused across multiple rows.
   * 
   * @param bytes
   *          The wrapper of the byte[].
   * @param start
   *          The start position inside the bytes.
   * @param length
   *          The length of the data, starting from "start"
   * @see ByteArrayRef
   */
  void init(ByteArrayRef bytes, int start, int length);

  /**
   * called for null binary, hbase columns, for example
   */
  void setNull();

  /**
   * If the LazyObjectBase is a primitive Object, then deserialize it and return the
   * actual primitive Object. Otherwise (array, map, struct), return this.
   */
  Object getObject();

}
