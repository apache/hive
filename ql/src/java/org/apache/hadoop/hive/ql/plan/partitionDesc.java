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

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;

@explain(displayName="Partition")
public class partitionDesc implements Serializable, Cloneable {
  private static final long serialVersionUID = 2L;
	private tableDesc table;
  private java.util.LinkedHashMap<String, String> partSpec;  
  private java.lang.Class<? extends  org.apache.hadoop.hive.serde2.Deserializer> deserializerClass;
  private Class<? extends InputFormat> inputFileFormatClass;
  private Class<? extends HiveOutputFormat> outputFileFormatClass;
  private java.util.Properties properties;
  private String serdeClassName;
  
  public partitionDesc() { }
  
  public partitionDesc(
    final tableDesc table,
    final java.util.LinkedHashMap<String, String> partSpec) {
    this(table, partSpec, null, null, null, null, null);
  }
  
  public partitionDesc(
  		final tableDesc table,
  		final java.util.LinkedHashMap<String, String> partSpec,
      final Class<? extends Deserializer> serdeClass,
      final Class<? extends InputFormat> inputFileFormatClass,
      final Class<?> outputFormat,
      final java.util.Properties properties, final String serdeClassName) {
  	this.table = table;
    this.partSpec = partSpec;
    this.deserializerClass = serdeClass;
    this.inputFileFormatClass = inputFileFormatClass;
		if (outputFormat != null)
			this.outputFileFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(outputFormat);
    this.properties = properties;
		if (properties != null)
			this.serdeClassName = properties.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB);
  }
  
  public partitionDesc(final org.apache.hadoop.hive.ql.metadata.Partition part)  throws HiveException{
  	this.table = Utilities.getTableDesc(part.getTable());
  	this.partSpec = part.getSpec();
  	this.deserializerClass = part.getDeserializer().getClass();
  	this.inputFileFormatClass = part.getInputFormatClass();
  	this.outputFileFormatClass = part.getOutputFormatClass();
  	this.properties = part.getSchema();
  	this.serdeClassName = properties.getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB);;
  }

  @explain(displayName="")
  public tableDesc getTableDesc() {
    return this.table;
  }
  public void setTableDesc(final tableDesc table) {
    this.table = table;
  }
  
  @explain(displayName="partition values")
  public java.util.LinkedHashMap<String, String> getPartSpec() {
    return this.partSpec;
  }
  public void setPartSpec(final java.util.LinkedHashMap<String, String> partSpec) {
    this.partSpec=partSpec;
  }
  
  public java.lang.Class<? extends  org.apache.hadoop.hive.serde2.Deserializer> getDeserializerClass() {
		if (this.deserializerClass == null && this.table !=null)
			setDeserializerClass(this.table.getDeserializerClass());
    return this.deserializerClass;
  }
  
  public void setDeserializerClass(final java.lang.Class<? extends  org.apache.hadoop.hive.serde2.Deserializer> serdeClass) {
    this.deserializerClass = serdeClass;
  }
  
  public Class<? extends InputFormat> getInputFileFormatClass() {
  	if (this.inputFileFormatClass == null && this.table !=null)
  		setInputFileFormatClass (this.table.getInputFileFormatClass());
    return this.inputFileFormatClass;
  }
  
  /**
   * Return a deserializer object corresponding to the tableDesc
   */
  public Deserializer getDeserializer() throws Exception {
    Deserializer de = this.deserializerClass.newInstance();
    de.initialize(null, properties);
    return de;
  }
  
  public void setInputFileFormatClass(final Class<? extends InputFormat> inputFileFormatClass) {
    this.inputFileFormatClass=inputFileFormatClass;
  }
  
  public Class<? extends HiveOutputFormat> getOutputFileFormatClass() {
  	if (this.outputFileFormatClass == null && this.table !=null)
  		setOutputFileFormatClass( this.table.getOutputFileFormatClass());
    return this.outputFileFormatClass;
  }
  
  public void setOutputFileFormatClass(final Class<?> outputFileFormatClass) {
    this.outputFileFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(outputFileFormatClass);
  }
  
  @explain(displayName="properties", normalExplain=false)
  public java.util.Properties getProperties() {
    if(this.table !=null)
      return this.table.getProperties();
    return this.properties;
  }
  
  public void setProperties(final java.util.Properties properties) {
    this.properties = properties;
  }
  /**
   * @return the serdeClassName
   */
  @explain(displayName="serde")
  public String getSerdeClassName() {
  	if(this.serdeClassName == null && this.table !=null)
  		setSerdeClassName(this.table.getSerdeClassName());
    return this.serdeClassName;
  }
  /**
   * @param serdeClassName the serde Class Name to set
   */
  public void setSerdeClassName(String serdeClassName) {
    this.serdeClassName = serdeClassName;
  }
  
  @explain(displayName="name")
  public String getTableName() {
    return getProperties().getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME);
  }
  
  @explain(displayName="input format")
  public String getInputFileFormatClassName() {
    return getInputFileFormatClass().getName();
  }
  
  @explain(displayName="output format")
  public String getOutputFileFormatClassName() {
    return getOutputFileFormatClass().getName();
  }
  
  public partitionDesc clone() {
  	partitionDesc ret = new partitionDesc();

    ret.setSerdeClassName(serdeClassName);
    ret.setDeserializerClass(deserializerClass);
    ret.inputFileFormatClass = this.inputFileFormatClass;
    ret.outputFileFormatClass = this.outputFileFormatClass;
    if(this.properties != null) {
      Properties newProp = new Properties();
      Enumeration<Object> keysProp = properties.keys(); 
      while (keysProp.hasMoreElements()) {
        Object key = keysProp.nextElement();
        newProp.put(key, properties.get(key));
      }
      ret.setProperties(newProp);
    }
  	ret.table = (tableDesc)this.table.clone();
  	ret.partSpec = new java.util.LinkedHashMap<String, String>();
  	ret.partSpec.putAll(this.partSpec);
  	return ret;
  }
}
