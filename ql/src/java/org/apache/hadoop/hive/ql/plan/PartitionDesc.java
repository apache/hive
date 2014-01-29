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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * PartitionDesc.
 *
 */
@Explain(displayName = "Partition")
public class PartitionDesc implements Serializable, Cloneable {

  static {
    TABLE_INTERNER = Interners.newWeakInterner();
    STRING_INTERNER = Interners.newWeakInterner();
    CLASS_INTERNER = Interners.newWeakInterner();
  }

  private static final Interner<TableDesc> TABLE_INTERNER;
  private static final Interner<String> STRING_INTERNER;
  private static final Interner<Class<?>> CLASS_INTERNER;

  private TableDesc tableDesc;
  private LinkedHashMap<String, String> partSpec;
  private Class<? extends InputFormat> inputFileFormatClass;
  private Class<? extends HiveOutputFormat> outputFileFormatClass;
  private Properties properties;

  private String baseFileName;

  public void setBaseFileName(String baseFileName) {
    this.baseFileName = baseFileName;
  }

  public PartitionDesc() {    
  }

  public PartitionDesc(final TableDesc table, final LinkedHashMap<String, String> partSpec) {
    setTableDesc(table);
    this.partSpec = partSpec;
  }

  public PartitionDesc(final Partition part) throws HiveException {
    setTableDesc(Utilities.getTableDesc(part.getTable()));
    setProperties(part.getMetadataFromPartitionSchema());
    partSpec = part.getSpec();
    setInputFileFormatClass(part.getInputFormatClass());
    setOutputFileFormatClass(part.getOutputFormatClass());
  }

  public PartitionDesc(final Partition part,final TableDesc tblDesc) throws HiveException {
    setTableDesc(tblDesc);
    setProperties(part.getSchemaFromTableSchema(tblDesc.getProperties())); // each partition maintains a large properties
    partSpec = part.getSpec();
    setOutputFileFormatClass(part.getInputFormatClass());
    setOutputFileFormatClass(part.getOutputFormatClass());
  }

  @Explain(displayName = "")
  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public void setTableDesc(TableDesc tableDesc) {
    this.tableDesc = TABLE_INTERNER.intern(tableDesc);
  }

  @Explain(displayName = "partition values")
  public LinkedHashMap<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(final LinkedHashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

    public Class<? extends InputFormat> getInputFileFormatClass() {
    if (inputFileFormatClass == null && tableDesc != null) {
      setInputFileFormatClass(tableDesc.getInputFileFormatClass());
    }
    return inputFileFormatClass;
  }

  /**
   * Return a deserializer object corresponding to the partitionDesc.
   */
  public Deserializer getDeserializer(Configuration conf) throws Exception {
    Properties schema = getProperties();
    String clazzName = schema.getProperty(serdeConstants.SERIALIZATION_LIB);
    if (clazzName == null) {
      throw new IllegalStateException("Property " + serdeConstants.SERIALIZATION_LIB +
          " cannot be null");
    }
    Deserializer deserializer = ReflectionUtils.newInstance(conf.getClassByName(clazzName)
        .asSubclass(Deserializer.class), conf);
    deserializer.initialize(conf, schema);
    return deserializer;
  }

  public void setInputFileFormatClass(
      final Class<? extends InputFormat> inputFileFormatClass) {
    if (inputFileFormatClass == null) {
      this.inputFileFormatClass = null;
    } else {
      this.inputFileFormatClass = (Class<? extends InputFormat>) CLASS_INTERNER.intern(inputFileFormatClass);
    }
  }

  public Class<? extends HiveOutputFormat> getOutputFileFormatClass() {
    if (outputFileFormatClass == null && tableDesc != null) {
      setOutputFileFormatClass(tableDesc.getOutputFileFormatClass());
    }
    return outputFileFormatClass;
  }

  public void setOutputFileFormatClass(final Class<?> outputFileFormatClass) {
    Class<? extends HiveOutputFormat> outputClass = outputFileFormatClass == null ? null :
      HiveFileFormatUtils.getOutputFormatSubstitute(outputFileFormatClass,false);
    if (outputClass != null) {
      this.outputFileFormatClass = (Class<? extends HiveOutputFormat>) 
        CLASS_INTERNER.intern(outputClass);
    } else {
      this.outputFileFormatClass = outputClass;
    }
  }

  @Explain(displayName = "properties", normalExplain = false)
  public Properties getProperties() {
    if (properties == null && tableDesc != null) {
      return tableDesc.getProperties();
    }
    return properties;
  }

  public Properties getOverlayedProperties(){
    if (tableDesc != null) {
      Properties overlayedProps = new Properties(tableDesc.getProperties());
      overlayedProps.putAll(getProperties());
      return overlayedProps;
    } else {
      return getProperties();
    }
  }

  public void setProperties(final Properties properties) {
    this.properties = properties;
    for (Enumeration<?> keys =  properties.propertyNames(); keys.hasMoreElements();) {
      String key = (String) keys.nextElement();
      String oldValue = properties.getProperty(key);
      if (oldValue != null) {
        String value = STRING_INTERNER.intern(oldValue);
        properties.setProperty(key, value);
      }
    }
  }

  /**
   * @return the serdeClassName
   */
  @Explain(displayName = "serde")
  public String getSerdeClassName() {
    return getProperties().getProperty(serdeConstants.SERIALIZATION_LIB);
  }

  @Explain(displayName = "name")
  public String getTableName() {
    return getProperties().getProperty(hive_metastoreConstants.META_TABLE_NAME);
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

  public boolean isPartitioned() {
    return partSpec != null && !partSpec.isEmpty();
  }

  @Override
  public PartitionDesc clone() {
    PartitionDesc ret = new PartitionDesc();

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
  public void deriveBaseFileName(String path) {
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
