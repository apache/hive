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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.ReflectionUtil;

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
    this.properties = properties;
  }

  public Class<? extends Deserializer> getDeserializerClass() {
    try {
      return (Class<? extends Deserializer>) Class.forName(
          getSerdeClassName(), true, Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Class<? extends InputFormat> getInputFileFormatClass() {
    return inputFileFormatClass;
  }

  public Deserializer getDeserializer() throws Exception {
    return getDeserializer(null);
  }

  /**
   * Return a deserializer object corresponding to the tableDesc.
   */
  public Deserializer getDeserializer(Configuration conf) throws Exception {
    return getDeserializer(conf, false);
  }

  public Deserializer getDeserializer(Configuration conf, boolean ignoreError) throws Exception {
    Deserializer de = ReflectionUtil.newInstance(
        getDeserializerClass().asSubclass(Deserializer.class), conf);
    if (ignoreError) {
      SerDeUtils.initializeSerDeWithoutErrorCheck(de, conf, properties, null);
    } else {
      SerDeUtils.initializeSerDe(de, conf, properties, null);
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
    return HiveStringUtils.getPropertiesExplain(getProperties());
  }

  public void setProperties(final Properties properties) {
    this.properties = properties;
  }

  public void setJobProperties(Map<String, String> jobProperties) {
    this.jobProperties = jobProperties;
  }

  @Explain(displayName = "jobProperties", explainLevels = { Level.EXTENDED })
  public Map<String, String> getJobProperties() {
    return jobProperties;
  }

  /**
   * @return the serdeClassName
   */
  @Explain(displayName = "serde", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getSerdeClassName() {
    return properties.getProperty(serdeConstants.SERIALIZATION_LIB);
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return properties
        .getProperty(hive_metastoreConstants.META_TABLE_NAME);
  }

  @Explain(displayName = "input format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getInputFileFormatClassName() {
    return getInputFileFormatClass().getName();
  }

  @Explain(displayName = "output format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOutputFileFormatClassName() {
    return getOutputFileFormatClass().getName();
  }

  public boolean isNonNative() {
    return (properties.getProperty(hive_metastoreConstants.META_TABLE_STORAGE) != null);
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
}
