/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

public abstract class DefaultHiveMetaHook implements HiveMetaHook {
  /**
   * Called after successfully INSERT [OVERWRITE] statement is executed.
   * @param table table definition
   * @param overwrite true if it is INSERT OVERWRITE
   *
   * @throws MetaException
   */
  public abstract void commitInsertTable(Table table, boolean overwrite) throws MetaException;

  /**
   * called before commit insert method is called
   * @param table table definition
   * @param overwrite true if it is INSERT OVERWRITE
   *
   * @throws MetaException
   */
  public abstract void preInsertTable(Table table, boolean overwrite) throws MetaException;

  /**
   * called in case pre commit or commit insert fail.
   * @param table table definition
   * @param overwrite true if it is INSERT OVERWRITE
   *
   * @throws MetaException
   */
  public abstract void rollbackInsertTable(Table table, boolean overwrite) throws MetaException;
}
