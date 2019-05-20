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

package org.apache.hadoop.hive.ql.io;

import java.util.Set;

import javax.annotation.Nullable;

/**
 * Subclasses represent a storage format for the
 * CREATE TABLE ... STORED AS ... command. Subclasses are
 * found via the ServiceLoader facility.
 */
public interface StorageFormatDescriptor {
  /**
   * Return the set of names this storage format is known as.
   */
  Set<String> getNames();
  /**
   * Return the name of the input format as a string
   */
  String getInputFormat();
  /**
   * Return the name of the output format as a string
   */
  String getOutputFormat();
  /**
   * Return the name of the serde as a string or null
   */
  @Nullable String getSerde();

}
