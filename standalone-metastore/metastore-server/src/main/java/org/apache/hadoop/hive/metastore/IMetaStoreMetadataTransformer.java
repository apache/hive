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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * This interface provides a way for users to implement a custom metadata transformer for tables/partitions.
 * This transformer can match and manipulate the data to be returned to the data processor
 * The classes implementing this interface should be in the HMS classpath, if this configuration is turned on.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IMetaStoreMetadataTransformer {

 /**
  * @param tables A list of tables to be transformed.
  * @param processorCapabilities A array of String capabilities received from the data processor
  * @param processorId String ID used for logging purpose.
  * @return Map A Map of transformed objects keyed by Table and value is list of required capabilities
  * @throws MetaException
  */
  // TODO HiveMetaException or MetaException
  public Map<Table, List<String>> transform(List<Table> tables, List<String> processorCapabilities,
      String processorId) throws MetaException;


 /**
  * @param parts A list of Partition objects to be transformed
  * @param processorCapabilities A array of String capabilities received from the data processor
  * @param processorId String ID used for logging purpose.
  * @return Map A Map of transformed objects keyed by Partition and value is list of required capabilities
  * @throws MetaException
  */
  // TODO HiveMetaException or MetaException
  public List<Partition> transformPartitions(List<Partition> parts, Table table, List<String> processorCapabilities,
      String processorId) throws MetaException;

 /**
  * @param table A table object to be transformed prior to the creation of the table
  * @param processorCapabilities A array of String capabilities received from the data processor
  * @param processorId String ID used for logging purpose.
  * @return Table An altered Table based on the processor capabilities
  * @throws MetaException
  */
 public Table transformCreateTable(Table table, List<String> processorCapabilities,
     String processorId) throws MetaException;

 /**
  * @param db A database object to be transformed, mainly db location
  * @param processorCapabilities A array of String capabilities received from the data processor
  * @param processorId String ID used for logging purpose.
  * @return Database An altered Database based on the processor capabilities
  * @throws MetaException
  */
 public Database transformDatabase(Database db, List<String> processorCapabilities,
     String processorId) throws MetaException;

  /**
  * @param oldTable A table object to be transformed prior to the alteration of a table
  * @param processorCapabilities A array of String capabilities received from the data processor
  * @param processorId String ID used for logging purpose.
  * @return Table An altered Table based on the processor capabilities
  * @throws MetaException
  */
  public Table transformAlterTable(Table oldTable, Table newTable, List<String> processorCapabilities,
     String processorId) throws MetaException;
}
