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
package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Class used to serialize the partition information read from the metadata server that maps to a partition. */
public class PartInfo implements Serializable {

  private static Logger LOG = LoggerFactory.getLogger(PartInfo.class);
  /** The serialization version */
  private static final long serialVersionUID = 1L;

  /** The partition data-schema. */
  private HCatSchema partitionSchema;

  /** The information about which input storage handler to use */
  private String storageHandlerClassName;
  private String inputFormatClassName;
  private String outputFormatClassName;
  private String serdeClassName;

  /** HCat-specific properties set at the partition */
  private final Properties hcatProperties;

  /** The data location. */
  private final String location;

  /** The map of partition key names and their values. */
  private Map<String, String> partitionValues;

  /** Job properties associated with this parition */
  Map<String, String> jobProperties;

  /**
   * The table info associated with this partition.
   * Not serialized per PartInfo instance. Constant, per table.
   */
  transient HCatTableInfo tableInfo;

  /**
   * Instantiates a new hcat partition info.
   * @param partitionSchema the partition schema
   * @param storageHandler the storage handler
   * @param location the location
   * @param hcatProperties hcat-specific properties at the partition
   * @param jobProperties the job properties
   * @param tableInfo the table information
   */
  public PartInfo(HCatSchema partitionSchema, HiveStorageHandler storageHandler,
          String location, Properties hcatProperties,
          Map<String, String> jobProperties, HCatTableInfo tableInfo) {
    this.partitionSchema = partitionSchema;
    this.location = location;
    this.hcatProperties = hcatProperties;
    this.jobProperties = jobProperties;
    this.tableInfo = tableInfo;

    this.storageHandlerClassName = storageHandler.getClass().getName();
    this.inputFormatClassName = storageHandler.getInputFormatClass().getName();
    this.serdeClassName = storageHandler.getSerDeClass().getName();
    this.outputFormatClassName = storageHandler.getOutputFormatClass().getName();
  }

  /**
   * Gets the value of partitionSchema.
   * @return the partitionSchema
   */
  public HCatSchema getPartitionSchema() {
    return partitionSchema;
  }

  /**
   * @return the storage handler class name
   */
  public String getStorageHandlerClassName() {
    return storageHandlerClassName;
  }

  /**
   * @return the inputFormatClassName
   */
  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  /**
   * @return the outputFormatClassName
   */
  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }

  /**
   * @return the serdeClassName
   */
  public String getSerdeClassName() {
    return serdeClassName;
  }

  /**
   * Gets the input storage handler properties.
   * @return HCat-specific properties set at the partition
   */
  public Properties getInputStorageHandlerProperties() {
    return hcatProperties;
  }

  /**
   * Gets the value of location.
   * @return the location
   */
  public String getLocation() {
    return location;
  }

  /**
   * Sets the partition values.
   * @param partitionValues the new partition values
   */
  public void setPartitionValues(Map<String, String> partitionValues) {
    this.partitionValues = partitionValues;
  }

  /**
   * Gets the partition values.
   * @return the partition values
   */
  public Map<String, String> getPartitionValues() {
    return partitionValues;
  }

  /**
   * Gets the job properties.
   * @return a map of the job properties
   */
  public Map<String, String> getJobProperties() {
    return jobProperties;
  }

  /**
   * Gets the HCatalog table information.
   * @return the table information
   */
  public HCatTableInfo getTableInfo() {
    return tableInfo;
  }

  void setTableInfo(HCatTableInfo thatTableInfo) {
    this.tableInfo = thatTableInfo;
    restoreLocalInfoFromTableInfo();
  }

  /**
   * Undoes the effects of compression( dedupWithTableInfo() ) during serialization,
   * and restores PartInfo fields to return original data.
   * Can be called idempotently, repeatably.
   */
  private void restoreLocalInfoFromTableInfo() {
    assert tableInfo != null : "TableInfo can't be null at this point.";
    if (partitionSchema == null) {
      partitionSchema = tableInfo.getDataColumns();
    }

    if (storageHandlerClassName == null) {
      storageHandlerClassName = tableInfo.getStorerInfo().getStorageHandlerClass();
    }

    if (inputFormatClassName == null) {
      inputFormatClassName = tableInfo.getStorerInfo().getIfClass();
    }

    if (outputFormatClassName == null) {
      outputFormatClassName = tableInfo.getStorerInfo().getOfClass();
    }

    if (serdeClassName == null) {
      serdeClassName = tableInfo.getStorerInfo().getSerdeClass();
    }
  }

  /**
   * Finds commonalities with TableInfo, and suppresses (nulls) fields if they are identical
   */
  private void dedupWithTableInfo() {
    assert tableInfo != null : "TableInfo can't be null at this point.";
    if (partitionSchema != null) {
      if (partitionSchema.equals(tableInfo.getDataColumns())) {
        partitionSchema = null;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Can't suppress data-schema. Partition-schema and table-schema seem to differ! "
              + " partitionSchema: " + partitionSchema.getFields()
              + " tableSchema: " + tableInfo.getDataColumns());
        }
      }
    }

    if (storageHandlerClassName != null) {
      if (storageHandlerClassName.equals(tableInfo.getStorerInfo().getStorageHandlerClass())) {
        storageHandlerClassName = null;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition's storageHandler (" + storageHandlerClassName + ") " +
              "differs from table's storageHandler (" + tableInfo.getStorerInfo().getStorageHandlerClass() + ").");
        }
      }
    }

    if (inputFormatClassName != null) {
      if (inputFormatClassName.equals(tableInfo.getStorerInfo().getIfClass())) {
        inputFormatClassName = null;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition's InputFormat (" + inputFormatClassName + ") " +
              "differs from table's InputFormat (" + tableInfo.getStorerInfo().getIfClass() + ").");
        }
      }
    }

    if (outputFormatClassName != null) {
      if (outputFormatClassName.equals(tableInfo.getStorerInfo().getOfClass())) {
        outputFormatClassName = null;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition's OutputFormat (" + outputFormatClassName + ") " +
              "differs from table's OutputFormat (" + tableInfo.getStorerInfo().getOfClass() + ").");
        }
      }
    }

    if (serdeClassName != null) {
      if (serdeClassName.equals(tableInfo.getStorerInfo().getSerdeClass())) {
        serdeClassName = null;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Partition's SerDe (" + serdeClassName + ") " +
              "differs from table's SerDe (" + tableInfo.getStorerInfo().getSerdeClass() + ").");
        }
      }
    }
  }

  /**
   * Serialization method used by java serialization.
   * Suppresses serialization of redundant information that's already available from
   * TableInfo before writing out, so as to minimize amount of serialized space but
   * restore it back before returning, so that PartInfo object is still usable afterwards
   * (See HIVE-8485 and HIVE-11344 for details.)
   */
  private void writeObject(ObjectOutputStream oos)
      throws IOException {
    dedupWithTableInfo();
    oos.defaultWriteObject();
    restoreLocalInfoFromTableInfo();
  }


}
