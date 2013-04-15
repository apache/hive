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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.InputFormat;

/**
 * PartitionDesc.
 *
 */
@Explain(displayName = "Partition")
public class PartitionDesc implements Serializable, Cloneable {
  private static final long serialVersionUID = 2L;
  private TableDesc tableDesc;
  private java.util.LinkedHashMap<String, String> partSpec;
  private java.lang.Class<? extends org.apache.hadoop.hive.serde2.Deserializer> deserializerClass;
  private Class<? extends InputFormat> inputFileFormatClass;
  private Class<? extends HiveOutputFormat> outputFileFormatClass;
  private java.util.Properties properties;
  private String serdeClassName;

  private transient String baseFileName;

  public void setBaseFileName(String baseFileName) {
    this.baseFileName = baseFileName;
  }

  public PartitionDesc() {
  }

  public PartitionDesc(final TableDesc table,
      final java.util.LinkedHashMap<String, String> partSpec) {
    this(table, partSpec, null, null, null, null, null);
  }

  public PartitionDesc(final TableDesc table,
      final java.util.LinkedHashMap<String, String> partSpec,
      final Class<? extends Deserializer> serdeClass,
      final Class<? extends InputFormat> inputFileFormatClass,
      final Class<?> outputFormat, final java.util.Properties properties,
      final String serdeClassName) {
    this.tableDesc = table;
    this.properties = properties;
    this.partSpec = partSpec;
    deserializerClass = serdeClass;
    this.inputFileFormatClass = inputFileFormatClass;
    if (outputFormat != null) {
      outputFileFormatClass = HiveFileFormatUtils
          .getOutputFormatSubstitute(outputFormat);
    }
    if (serdeClassName != null) {
      this.serdeClassName = serdeClassName;
    } else if (properties != null) {
      this.serdeClassName = properties
          .getProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB);
    }
  }

  public PartitionDesc(final org.apache.hadoop.hive.ql.metadata.Partition part)
      throws HiveException {
    tableDesc = Utilities.getTableDesc(part.getTable());
    properties = part.getMetadataFromPartitionSchema();
    partSpec = part.getSpec();
    deserializerClass = part.getDeserializer(properties).getClass();
    inputFileFormatClass = part.getInputFormatClass();
    outputFileFormatClass = part.getOutputFormatClass();
    serdeClassName = properties
        .getProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB);
    ;
  }

  public PartitionDesc(final org.apache.hadoop.hive.ql.metadata.Partition part,
      final TableDesc tblDesc) throws HiveException {
    tableDesc = tblDesc;
    properties = part.getSchemaFromTableSchema(tblDesc.getProperties()); // each partition maintains a large properties
    partSpec = part.getSpec();
    // deserializerClass = part.getDeserializer(properties).getClass();
    Deserializer deserializer;
    try {
      deserializer = SerDeUtils.lookupDeserializer(
          properties.getProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB));
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
    deserializerClass = deserializer.getClass();
    inputFileFormatClass = part.getInputFormatClass();
    outputFileFormatClass = part.getOutputFormatClass();
    serdeClassName = properties.getProperty(
        org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB);
  }

  @Explain(displayName = "")
  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public void setTableDesc(TableDesc tableDesc) {
    this.tableDesc = tableDesc;
  }

  @Explain(displayName = "partition values")
  public java.util.LinkedHashMap<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(final java.util.LinkedHashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  public java.lang.Class<? extends org.apache.hadoop.hive.serde2.Deserializer> getDeserializerClass() {
    if (deserializerClass == null && tableDesc != null) {
      setDeserializerClass(tableDesc.getDeserializerClass());
    }
    return deserializerClass;
  }

  public void setDeserializerClass(
      final java.lang.Class<? extends org.apache.hadoop.hive.serde2.Deserializer> serdeClass) {
    deserializerClass = serdeClass;
  }

  public Class<? extends InputFormat> getInputFileFormatClass() {
    if (inputFileFormatClass == null && tableDesc != null) {
      setInputFileFormatClass(tableDesc.getInputFileFormatClass());
    }
    return inputFileFormatClass;
  }

  /**
   * Return a deserializer object corresponding to the tableDesc.
   */
  public Deserializer getDeserializer() throws Exception {
    Deserializer de = deserializerClass.newInstance();
    de.initialize(null, properties);
    return de;
  }

  public void setInputFileFormatClass(
      final Class<? extends InputFormat> inputFileFormatClass) {
    this.inputFileFormatClass = inputFileFormatClass;
  }

  public Class<? extends HiveOutputFormat> getOutputFileFormatClass() {
    if (outputFileFormatClass == null && tableDesc != null) {
      setOutputFileFormatClass(tableDesc.getOutputFileFormatClass());
    }
    return outputFileFormatClass;
  }

  public void setOutputFileFormatClass(final Class<?> outputFileFormatClass) {
    this.outputFileFormatClass = HiveFileFormatUtils
        .getOutputFormatSubstitute(outputFileFormatClass);
  }

  @Explain(displayName = "properties", normalExplain = false)
  public java.util.Properties getProperties() {
    if (properties == null && tableDesc != null) {
      return tableDesc.getProperties();
    }
    return properties;
  }

  public void setProperties(final java.util.Properties properties) {
    this.properties = properties;
  }

  /**
   * @return the serdeClassName
   */
  @Explain(displayName = "serde")
  public String getSerdeClassName() {
    if (serdeClassName == null && tableDesc != null) {
      setSerdeClassName(tableDesc.getSerdeClassName());
    }
    return serdeClassName;
  }

  /**
   * @param serdeClassName
   *          the serde Class Name to set
   */
  public void setSerdeClassName(String serdeClassName) {
    this.serdeClassName = serdeClassName;
  }

  @Explain(displayName = "name")
  public String getTableName() {
    return getProperties().getProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME);
  }

  @Explain(displayName = "input format")
  public String getInputFileFormatClassName() {
    return getInputFileFormatClass().getName();
  }

  @Explain(displayName = "output format")
  public String getOutputFileFormatClassName() {
    return getOutputFileFormatClass().getName();
  }

  @Explain(displayName = "base file name", normalExplain = false)
  public String getBaseFileName() {
    return baseFileName;
  }

  @Override
  public PartitionDesc clone() {
    PartitionDesc ret = new PartitionDesc();

    ret.setSerdeClassName(serdeClassName);
    ret.setDeserializerClass(deserializerClass);
    ret.inputFileFormatClass = inputFileFormatClass;
    ret.outputFileFormatClass = outputFileFormatClass;
    if (properties != null) {
      Properties newProp = new Properties();
      Enumeration<Object> keysProp = properties.keys();
      while (keysProp.hasMoreElements()) {
        Object key = keysProp.nextElement();
        newProp.put(key, properties.get(key));
      }
      ret.setProperties(newProp);
    }
    ret.tableDesc = (TableDesc) tableDesc.clone();
    // The partition spec is not present
    if (partSpec != null) {
      ret.partSpec = new java.util.LinkedHashMap<String, String>();
      ret.partSpec.putAll(partSpec);
    }
    return ret;
  }

  /**
   * Attempt to derive a virtual <code>base file name</code> property from the
   * path. If path format is unrecognized, just use the full path.
   *
   * @param path
   *          URI to the partition file
   */
  void deriveBaseFileName(String path) {
    PlanUtils.configureInputJobPropertiesForStorageHandler(tableDesc);

    if (path == null) {
      return;
    }
    try {
      Path p = new Path(path);
      baseFileName = p.getName();
    } catch (Exception ex) {
      // don't really care about the exception. the goal is to capture the
      // the last component at the minimum - so set to the complete path
      baseFileName = path;
    }
  }
}
