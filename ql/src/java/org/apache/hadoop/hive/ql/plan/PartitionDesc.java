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

package org.apache.hadoop.hive.ql.plan;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.CopyOnFirstWriteProperties;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * PartitionDesc.
 *
 */
@Explain(displayName = "Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class PartitionDesc implements Serializable, Cloneable {

  private static final Interner<Class<?>> CLASS_INTERNER = Interners.newWeakInterner();

  private TableDesc tableDesc;
  private LinkedHashMap<String, String> partSpec;
  private Class<? extends InputFormat> inputFileFormatClass;
  private Class<? extends OutputFormat> outputFileFormatClass;
  private Properties properties;

  private String baseFileName;

  private VectorPartitionDesc vectorPartitionDesc;

  public void setBaseFileName(String baseFileName) {
    this.baseFileName = baseFileName.intern();
  }

  public PartitionDesc() {
  }

  private final static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PartitionDesc.class);

  public PartitionDesc(final TableDesc table, final LinkedHashMap<String, String> partSpec) {
    this.tableDesc = table;
    setPartSpec(partSpec);
  }

  public PartitionDesc(final Partition part, final TableDesc tableDesc) throws HiveException {
    PartitionDescConstructorHelper(part, tableDesc, true);
    if (Utilities.isInputFileFormatSelfDescribing(this)) {
      // if IF is self describing no need to send column info per partition, since its not used anyway.
      Table tbl = part.getTable();
      setProperties(MetaStoreUtils.getSchemaWithoutCols(part.getTPartition().getSd(),
          part.getParameters(), tbl.getDbName(), tbl.getTableName(), tbl.getPartitionKeys()));
    } else {
      setProperties(part.getMetadataFromPartitionSchema());
    }
  }

  public PartitionDesc(final Partition part) throws HiveException {
    this(part, getTableDesc(part.getTable()));
  }

  /**
   * @param part Partition
   * @param tblDesc Table Descriptor
   * @param usePartSchemaProperties Use Partition Schema Properties to set the
   * partition descriptor properties. This is usually set to true by the caller
   * if the table is partitioned, i.e. if the table has partition columns.
   * @throws HiveException
   */
  public PartitionDesc(final Partition part,final TableDesc tblDesc,
    boolean usePartSchemaProperties)
    throws HiveException {
    PartitionDescConstructorHelper(part, tblDesc, usePartSchemaProperties);
    //We use partition schema properties to set the partition descriptor properties
    // if usePartSchemaProperties is set to true.
    if (usePartSchemaProperties) {
      setProperties(part.getMetadataFromPartitionSchema());
    } else {
      // each partition maintains a large properties
      setProperties(part.getSchemaFromTableSchema(tblDesc.getProperties()));
    }
  }

  private void PartitionDescConstructorHelper(final Partition part,final TableDesc tblDesc, boolean setInputFileFormat)
    throws HiveException {

    PlanUtils.configureInputJobPropertiesForStorageHandler(tblDesc);

    this.tableDesc = tblDesc;

    setPartSpec(part.getSpec());
    if (setInputFileFormat) {
      setInputFileFormatClass(part.getInputFormatClass());
    } else {
      setOutputFileFormatClass(part.getInputFormatClass());
    }
    setOutputFileFormatClass(part.getOutputFormatClass());
  }

  @Explain(displayName = "", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public void setTableDesc(TableDesc tableDesc) {
    this.tableDesc = tableDesc;
  }

  @Explain(displayName = "partition values", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public LinkedHashMap<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(final LinkedHashMap<String, String> partSpec) {
    StringInternUtils.internValuesInMap(partSpec);
    this.partSpec = partSpec;
  }

  public Class<? extends InputFormat> getInputFileFormatClass() {
    if (inputFileFormatClass == null && tableDesc != null) {
      setInputFileFormatClass(tableDesc.getInputFileFormatClass());
    }
    return inputFileFormatClass;
  }

  public String getDeserializerClassName() {
    Properties schema = getProperties();
    String clazzName = schema.getProperty(serdeConstants.SERIALIZATION_LIB);
    if (clazzName == null) {
      throw new IllegalStateException("Property " + serdeConstants.SERIALIZATION_LIB +
          " cannot be null");
    }

    return clazzName;
  }

  /**
   * Return a deserializer object corresponding to the partitionDesc.
   */
  public Deserializer getDeserializer(Configuration conf) throws Exception {
    Properties schema = getProperties();
    String clazzName = getDeserializerClassName();
    Deserializer deserializer = ReflectionUtil.newInstance(conf.getClassByName(clazzName)
        .asSubclass(Deserializer.class), conf);
    SerDeUtils.initializeSerDe(deserializer, conf, getTableDesc().getProperties(), schema);
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

  public Class<? extends OutputFormat> getOutputFileFormatClass() {
    if (outputFileFormatClass == null && tableDesc != null) {
      setOutputFileFormatClass(tableDesc.getOutputFileFormatClass());
    }
    return outputFileFormatClass;
  }

  public void setOutputFileFormatClass(final Class<?> outputFileFormatClass) {
    Class<? extends OutputFormat> outputClass = outputFileFormatClass == null ? null :
      HiveFileFormatUtils.getOutputFormatSubstitute(outputFileFormatClass);
    if (outputClass != null) {
      this.outputFileFormatClass = (Class<? extends HiveOutputFormat>)
        CLASS_INTERNER.intern(outputClass);
    } else {
      this.outputFileFormatClass = outputClass;
    }
  }

  public Properties getProperties() {
    if (properties == null && tableDesc != null) {
      return tableDesc.getProperties();
    }
    return properties;
  }

  @Explain(displayName = "properties", explainLevels = { Level.EXTENDED })
  public Map getPropertiesExplain() {
    return HiveStringUtils.getPropertiesExplain(getProperties());
  }

  public void setProperties(final Properties properties) {
    if (properties instanceof CopyOnFirstWriteProperties) {
      this.properties = properties;
    } else {
      internProperties(properties);
      this.properties = new CopyOnFirstWriteProperties(properties);
    }
  }

  public static TableDesc getTableDesc(Table table) {
    return Utilities.getTableDesc(table);
  }

  private static void internProperties(Properties properties) {
    for (Enumeration<?> keys =  properties.propertyNames(); keys.hasMoreElements();) {
      String key = (String) keys.nextElement();
      String oldValue = properties.getProperty(key);
      if (oldValue != null) {
        properties.setProperty(key, oldValue.intern());
      }
    }
  }

  /**
   * @return the serdeClassName
   */
  @Explain(displayName = "serde", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getSerdeClassName() {
    return getProperties().getProperty(serdeConstants.SERIALIZATION_LIB);
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return getProperties().getProperty(hive_metastoreConstants.META_TABLE_NAME);
  }

  @Explain(displayName = "input format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getInputFileFormatClassName() {
    return getInputFileFormatClass().getName();
  }

  @Explain(displayName = "output format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOutputFileFormatClassName() {
    return getOutputFileFormatClass().getName();
  }

  @Explain(displayName = "base file name", explainLevels = { Level.EXTENDED })
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
      ret.setProperties((Properties) properties.clone());
    }
    ret.tableDesc = (TableDesc) tableDesc.clone();
    // The partition spec is not present
    if (partSpec != null) {
      ret.partSpec = new LinkedHashMap<>(partSpec);
    }
    if (vectorPartitionDesc != null) {
      ret.vectorPartitionDesc = vectorPartitionDesc.clone();
    }
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    boolean cond = o instanceof PartitionDesc;
    if (!cond) {
      return false;
    }

    PartitionDesc other = (PartitionDesc) o;
    Class<? extends InputFormat> input1 = getInputFileFormatClass();
    Class<? extends InputFormat> input2 = other.getInputFileFormatClass();
    cond = (input1 == null && input2 == null) || (input1 != null && input1.equals(input2));
    if (!cond) {
      return false;
    }

    Class<? extends OutputFormat> output1 = getOutputFileFormatClass();
    Class<? extends OutputFormat> output2 = other.getOutputFileFormatClass();
    cond = (output1 == null && output2 == null) || (output1 != null && output1.equals(output2));
    if (!cond) {
      return false;
    }

    Properties properties1 = getProperties();
    Properties properties2 = other.getProperties();
    cond = (properties1 == null && properties2 == null) ||
        (properties1 != null && properties1.equals(properties2));
    if (!cond) {
      return false;
    }

    TableDesc tableDesc1 = getTableDesc();
    TableDesc tableDesc2 = other.getTableDesc();
    cond = (tableDesc1 == null && tableDesc2 == null) ||
        (tableDesc1 != null && tableDesc1.equals(tableDesc2));
    if (!cond) {
      return false;
    }

    Map<String, String> partSpec1 = getPartSpec();
    Map<String, String> partSpec2 = other.getPartSpec();
    cond = (partSpec1 == null && partSpec2 == null) ||
        (partSpec1 != null && partSpec1.equals(partSpec2));
    if (!cond) {
      return false;
    }

    VectorPartitionDesc vecPartDesc1 = getVectorPartitionDesc();
    VectorPartitionDesc vecPartDesc2 = other.getVectorPartitionDesc();
    return (vecPartDesc1 == null && vecPartDesc2 == null) ||
        (vecPartDesc1 != null && vecPartDesc1.equals(vecPartDesc2));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = result * prime +
        (getInputFileFormatClass() == null ? 0 : getInputFileFormatClass().hashCode());
    result = result * prime +
        (getOutputFileFormatClass() == null ? 0 : getOutputFileFormatClass().hashCode());
    result = result * prime + (getProperties() == null ? 0 : getProperties().hashCode());
    result = result * prime + (getTableDesc() == null ? 0 : getTableDesc().hashCode());
    result = result * prime + (getPartSpec() == null ? 0 : getPartSpec().hashCode());
    result = result * prime +
        (getVectorPartitionDesc() == null ? 0 : getVectorPartitionDesc().hashCode());
    return result;
  }

  /**
   * Attempt to derive a virtual <code>base file name</code> property from the
   * path. If path format is unrecognized, just use the full path.
   *
   * @param path
   *          URI to the partition file
   */
  public void deriveBaseFileName(Path path) {

    if (path == null) {
      return;
    }
    baseFileName = path.getName().intern();
  }

  public void intern(Interner<TableDesc> interner) {
    this.tableDesc = interner.intern(tableDesc);
  }

  public void setVectorPartitionDesc(VectorPartitionDesc vectorPartitionDesc) {
    this.vectorPartitionDesc = vectorPartitionDesc;
  }

  public VectorPartitionDesc getVectorPartitionDesc() {
    return vectorPartitionDesc;
  }

  @Override
  public String toString() {
    return "PartitionDesc [tableDesc=" + tableDesc + ", partSpec=" + partSpec
        + ", inputFileFormatClass=" + inputFileFormatClass
        + ", outputFileFormatClass=" + outputFileFormatClass + ", properties="
        + properties + ", baseFileName=" + baseFileName
        + ", vectorPartitionDesc=" + vectorPartitionDesc + "]";
  }
}
