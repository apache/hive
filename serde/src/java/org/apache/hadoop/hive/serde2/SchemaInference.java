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
package org.apache.hadoop.hive.serde2;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public interface SchemaInference {
  /**
   * Infer Hive compatible schema from provided file. The purpose of this method is to optionally
   * allow SerDes to implement schema inference for CREATE TABLE LIKE FILE support.
   *
   * @param file Fully qualified path to file to infer schema from (hadoop compatible URI + filename)
   * @return List of FieldSchema that was derived from the provided file
   * @throws SerDeException
   */
   List<FieldSchema> readSchema(Configuration conf, String file) throws SerDeException;
}
