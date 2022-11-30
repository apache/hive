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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hive.common.util.ReflectionUtil;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * TableDesc.
 *
 */
public class TableDesc implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;
  private Class<? extends InputFormat> inputFileFormatClass;
  private Class<? extends OutputFormat> outputFileFormatClass;
  private java.util.Properties properties;
  private Map<String, String> jobProperties;
  private Map<String, String> jobSecrets;
  public static final String SECRET_PREFIX = "TABLE_SECRET";
  public static final String SECRET_DELIMIT = "#";

  public TableDesc() {
  }

  /**
   * @param inputFormatClass
   * @param outputFormatClass
   * @param properties must contain serde class name associate with this table.
   */
  public TableDesc(
      final Class<? extends InputFormat> inputFormatClass,
      final Class<?> outputFormatClass, final Properties properties) {
    this.inputFileFormatClass = inputFormatClass;
    outputFileFormatClass = HiveFileFormatUtils
        .getOutputFormatSubstitute(outputFormatClass);
    setProperties(properties);
  }

  public Class<? extends AbstractSerDe> getSerDeClass() {
    try {
      return (Class<? extends AbstractSerDe>) Class.forName(
          getSerdeClassName(), true, Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Class<? extends InputFormat> getInputFileFormatClass() {
    return inputFileFormatClass;
  }

  public AbstractSerDe getSerDe() throws Exception {
    return getDeserializer(null);
  }

  /**
   * Return a deserializer object corresponding to the tableDesc.
   */
  public AbstractSerDe getDeserializer(Configuration conf) throws Exception {
    return getSerDe(conf, false);
  }

  public AbstractSerDe getSerDe(Configuration conf, boolean ignoreError) throws Exception {
    AbstractSerDe de = ReflectionUtil.newInstance(getSerDeClass().asSubclass(AbstractSerDe.class), conf);
    try {
      de.initialize(conf, properties, null);
    } catch (SerDeException sde) {
      if (!ignoreError) {
        throw sde;
      }
    }
    return de;
  }

  public void setInputFileFormatClass(
      final Class<? extends InputFormat> inputFileFormatClass) {
    this.inputFileFormatClass = inputFileFormatClass;
  }

  public Class<? extends OutputFormat> getOutputFileFormatClass() {
    return outputFileFormatClass;
  }

  public void setOutputFileFormatClass(Class<?> outputFileFormatClass) {
    this.outputFileFormatClass = HiveFileFormatUtils
        .getOutputFormatSubstitute(outputFileFormatClass);
  }

  public Properties getProperties() {
    return properties;
  }

  @Explain(displayName = "properties", explainLevels = { Level.EXTENDED })
  public Map getPropertiesExplain() {
    return PlanUtils.getPropertiesForExplain(getProperties());
  }

  public void setProperties(final Properties properties) {
    StringInternUtils.internValuesInMap((Map) properties);
    this.properties = properties;
  }

  public void setJobProperties(Map<String, String> jobProperties) {
    this.jobProperties = jobProperties;
  }

  @Explain(displayName = "jobProperties", explainLevels = { Level.EXTENDED })
  public Map<String, String> getJobProperties() {
    return jobProperties;
  }

  public void setJobSecrets(Map<String, String> jobSecrets) {
    this.jobSecrets = jobSecrets;
  }

  public Map<String, String> getJobSecrets() {
    return jobSecrets;
  }

  /**
   * @return the serdeClassName
   */
  @Explain(displayName = "serde")
  public String getSerdeClassName() {
    return properties.getProperty(serdeConstants.SERIALIZATION_LIB);
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return properties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
  }

  public String getFullTableName() {
    String tableName = getTableName();
    String metaTable = properties.getProperty("metaTable");
    if (metaTable != null && tableName != null) {
      return tableName + "." + metaTable;
    }
    return tableName;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbName() {
    return properties.getProperty(hive_metastoreConstants.META_TABLE_DB);
  }

  @Explain(displayName = "input format")
  public String getInputFileFormatClassName() {
    return getInputFileFormatClass().getName();
  }

  @Explain(displayName = "output format")
  public String getOutputFileFormatClassName() {
    return getOutputFileFormatClass().getName();
  }

  public boolean isNonNative() {
    return (properties.getProperty(hive_metastoreConstants.META_TABLE_STORAGE) != null);
  }

  public boolean isSetBucketingVersion() {
    return properties.getProperty(hive_metastoreConstants.TABLE_BUCKETING_VERSION) != null;
  }
  public int getBucketingVersion() {
    return Utilities.getBucketingVersion(
        properties.getProperty(hive_metastoreConstants.TABLE_BUCKETING_VERSION));
  }

  @Override
  public Object clone() {
    TableDesc ret = new TableDesc();
    ret.setInputFileFormatClass(inputFileFormatClass);
    ret.setOutputFileFormatClass(outputFileFormatClass);
    Properties newProp = new Properties();
    Enumeration<Object> keysProp = properties.keys();
    while (keysProp.hasMoreElements()) {
      Object key = keysProp.nextElement();
      newProp.put(key, properties.get(key));
    }

    ret.setProperties(newProp);
    if (jobProperties != null) {
      ret.jobProperties = new LinkedHashMap<String, String>(jobProperties);
    }
    return ret;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result +
        ((inputFileFormatClass == null) ? 0 : inputFileFormatClass.hashCode());
    result = prime * result +
        ((outputFileFormatClass == null) ? 0 : outputFileFormatClass.hashCode());
    result = prime * result + ((properties == null) ? 0 : properties.hashCode());
    result = prime * result + ((jobProperties == null) ? 0 : jobProperties.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof TableDesc)) {
      return false;
    }

    TableDesc target = (TableDesc) o;
    boolean ret = true;
    ret = ret && (inputFileFormatClass == null ? target.inputFileFormatClass == null :
      inputFileFormatClass.equals(target.inputFileFormatClass));
    ret = ret && (outputFileFormatClass == null ? target.outputFileFormatClass == null :
      outputFileFormatClass.equals(target.outputFileFormatClass));
    ret = ret && (properties == null ? target.properties == null :
      properties.equals(target.properties));
    ret = ret && (jobProperties == null ? target.jobProperties == null :
      jobProperties.equals(target.jobProperties));
    return ret;
  }

  @Override
  public String toString() {
    return "TableDesc [inputFileFormatClass=" + inputFileFormatClass
        + ", outputFileFormatClass=" + outputFileFormatClass + ", properties="
        + properties + ", jobProperties=" + jobProperties + "]";
  }
}
