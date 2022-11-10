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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * MetadataSerializer implementation, that serializes HCat API elements into JSON.
 */
class MetadataJSONSerializer extends MetadataSerializer {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataJSONSerializer.class);

  MetadataJSONSerializer() throws HCatException {}

  @Override
  public String serializeTable(HCatTable hcatTable) throws HCatException {
    try {
      return new TSerializer(new TJSONProtocol.Factory())
          .toString(hcatTable.toHiveTable());
    }
    catch (TException exception) {
      throw new HCatException("Could not serialize HCatTable: " + hcatTable, exception);
    }
  }

  @Override
  public HCatTable deserializeTable(String hcatTableStringRep) throws HCatException {
    try {
      Table table = new Table();
      new TDeserializer(new TJSONProtocol.Factory()).deserialize(table, hcatTableStringRep, "UTF-8");
      return new HCatTable(table);
    }
    catch(TException exception) {
      if (LOG.isDebugEnabled())
        LOG.debug("Could not de-serialize from: " + hcatTableStringRep);
      throw new HCatException("Could not de-serialize HCatTable.", exception);
    }
  }

  @Override
  public String serializePartition(HCatPartition hcatPartition) throws HCatException {
    try {
      return new TSerializer(new TJSONProtocol.Factory())
          .toString(hcatPartition.toHivePartition());
    }
    catch (TException exception) {
      throw new HCatException("Could not serialize HCatPartition: " + hcatPartition, exception);
    }
  }

  @Override
  public HCatPartition deserializePartition(String hcatPartitionStringRep) throws HCatException {
    try {
      Partition partition = new Partition();
      new TDeserializer(new TJSONProtocol.Factory()).deserialize(partition, hcatPartitionStringRep, "UTF-8");
      return new HCatPartition(null, partition);
    }
    catch(TException exception) {
      if (LOG.isDebugEnabled())
        LOG.debug("Could not de-serialize partition from: " + hcatPartitionStringRep);
      throw new HCatException("Could not de-serialize HCatPartition.", exception);
    }
  }

  @Override
  @InterfaceAudience.LimitedPrivate({"Hive"})
  @InterfaceStability.Evolving
  public List<String> serializePartitionSpec(HCatPartitionSpec hcatPartitionSpec) throws HCatException {
    try {
      List<String> stringReps = new ArrayList<String>();
      TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
      for (PartitionSpec partitionSpec : hcatPartitionSpec.partitionSpecProxy.toPartitionSpec()) {
        stringReps.add(serializer.toString(partitionSpec));
      }
      return stringReps;
    }
    catch (TException serializationException) {
      throw new HCatException("Failed to serialize!", serializationException);
    }
  }

  @Override
  public HCatPartitionSpec deserializePartitionSpec(List<String> hcatPartitionSpecStrings) throws HCatException {
    try {
      List<PartitionSpec> partitionSpecList = new ArrayList<PartitionSpec>();
      TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
      for (String stringRep : hcatPartitionSpecStrings) {
        PartitionSpec partSpec = new PartitionSpec();
        deserializer.deserialize(partSpec, stringRep, "UTF-8");
        partitionSpecList.add(partSpec);
      }
      return new HCatPartitionSpec(null, PartitionSpecProxy.Factory.get(partitionSpecList));
    }
    catch (TException deserializationException) {
      throw new HCatException("Failed to deserialize!", deserializationException);
    }
  }
}
