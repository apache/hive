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

package org.apache.hadoop.hive.llap.io.api;

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;

import javax.annotation.Nullable;

public interface LlapIo<T> {
  /**
   * @param sourceInputFormat source input format to be wrapped if possible
   * @param serde source serializer and deserializer
   * @return InputFormat wrapped by LLAP-IO caching and elevator primitive if possible, {@code null} otherwise.
   */
  @Nullable
  InputFormat<NullWritable, T> getInputFormat(InputFormat<?, ?> sourceInputFormat, Deserializer serde);

  /**
   * closing llap io cache at JVM shutdown.
   */
  void close();

  /**
   * @return String with cache status.
   */
  String getMemoryInfo();
  /**
   * purge is best effort and will just release the buffers that are unlocked (refCount == 0). This is typically
   * called when the system is idle.
   *
   * @return Total amount of purged bytes.
   */
  long purge();

  /**
   * Used in cases where IO-Elevator mechanics is not usable.
   * Used with InputFormat of type {@link org.apache.hadoop.hive.ql.io.LlapCacheOnlyInputFormatInterface}.
   *
   * @param inputFormat source inputFormat.
   */
  void initCacheOnlyInputFormat(InputFormat<?, ?> inputFormat);
}
