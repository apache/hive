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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;

import java.util.List;

public interface ColumnDescriptorSupplier {
  /**
   * Gets a column descriptor for the provided table and list of columns. If no matching column descriptor exists,
   * returns null. The column descriptor is considered a match if it has the same number of columns, and each column has
   * the same name, type, and comment in the same order.
   *
   * @param cols the columns to match
   * @param mt the table to search
   * @return an existing MColumnDescriptor if found, null otherwise
   */
  MColumnDescriptor getColumnDescriptor(List<FieldSchema> cols, MTable mt) throws MetaException;
}
