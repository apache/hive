/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * This interface defines an API which can be used to process incompatible changes to
 * tables in the {@link HiveAlterHandler}
 */
public interface IMetaStoreIncompatibleChangeHandler  {

  /**
   * Checks if the old table can be altered to the new table. If not, throws
   * {@link InvalidOperationException}
   * @param oldTable
   * @param newTable
   * @throws InvalidOperationException
   */
  void allowChange(Configuration conf, Table oldTable, Table newTable)
      throws InvalidOperationException;
}