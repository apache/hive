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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.api;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hive.hcatalog.common.HCatException;

import java.util.List;

/**
 * Interface to serialize HCat API elements.
 */
abstract class MetadataSerializer {

  // Prevent construction outside the get() method.
  protected MetadataSerializer() {}

  /**
   * Static getter method for the appropriate MetadataSerializer implementation.
   * @return MetadataSerializer sub-class.
   * @throws HCatException On failure to construct a concrete MetadataSerializer.
   */
  public static MetadataSerializer get() throws HCatException {
    return new MetadataJSONSerializer();
  }

  /**
   * Serializer for HCatTable instances.
   * @param hcatTable The HCatTable operand, to be serialized.
   * @return Serialized (i.e. String-ified) HCatTable.
   * @throws HCatException On failure to serialize.
   */
  public abstract String serializeTable(HCatTable hcatTable) throws HCatException ;

  /**
   * Deserializer for HCatTable string-representations.
   * @param hcatTableStringRep Serialized HCatTable String (gotten from serializeTable()).
   * @return Deserialized HCatTable instance.
   * @throws HCatException On failure to deserialize (e.g. incompatible serialization format, etc.)
   */
  public abstract HCatTable deserializeTable(String hcatTableStringRep) throws HCatException;

  /**
   * Serializer for HCatPartition instances.
   * @param hcatPartition The HCatPartition operand, to be serialized.
   * @return Serialized (i.e. String-ified) HCatPartition.
   * @throws HCatException On failure to serialize.
   */
  public abstract String serializePartition(HCatPartition hcatPartition) throws HCatException;

  /**
   * Deserializer for HCatPartition string-representations.
   * @param hcatPartitionStringRep Serialized HCatPartition String (gotten from serializePartition()).
   * @return Deserialized HCatPartition instance.
   * @throws HCatException On failure to deserialize (e.g. incompatible serialization format, etc.)
   */
  public abstract HCatPartition deserializePartition(String hcatPartitionStringRep) throws HCatException;

  /**
   * Serializer for HCatPartitionSpec.
   * @param hcatPartitionSpec HCatPartitionSpec instance to be serialized.
   * @return Serialized string-representations.
   * @throws HCatException On failure to serialize.
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract List<String> serializePartitionSpec(HCatPartitionSpec hcatPartitionSpec) throws HCatException;

  /**
   * Deserializer for HCatPartitionSpec string-representations.
   * @param hcatPartitionSpecStrings List of strings to be converted into an HCatPartitionSpec.
   * @return Deserialized HCatPartitionSpec instance.
   * @throws HCatException On failure to deserialize. (e.g. incompatible serialization format, etc.)
   */
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public abstract HCatPartitionSpec deserializePartitionSpec(List<String> hcatPartitionSpecStrings) throws HCatException;

}
