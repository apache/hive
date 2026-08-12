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

import java.io.Serializable;
import java.util.Properties;

/** Information about the storer to use for writing the data. */
public class StorerInfo implements Serializable {

  /** The serialization version */
  private static final long serialVersionUID = 1L;

  /** The properties for the storage handler */
  private Properties properties;

  private String ofClass;

  private String ifClass;

  private String serdeClass;

  private String storageHandlerClass;

  /**
   * Initialize the storer information.
   * @param ifClass the input format class
   * @param ofClass the output format class
   * @param serdeClass the SerDe class
   * @param storageHandlerClass the storage handler class
   * @param properties the properties for the storage handler
   */
  public StorerInfo(String ifClass, String ofClass, String serdeClass, String storageHandlerClass, Properties properties) {
    super();
    this.ifClass = ifClass;
    this.ofClass = ofClass;
    this.serdeClass = serdeClass;
    this.storageHandlerClass = storageHandlerClass;
    this.properties = properties;
  }

  /**
   * @return the input format class
   */
  public String getIfClass() {
    return ifClass;
  }

  /**
   * @param ifClass the input format class
   */
  public void setIfClass(String ifClass) {
    this.ifClass = ifClass;
  }

  /**
   * @return the output format class
   */
  public String getOfClass() {
    return ofClass;
  }

  /**
   * @return the serdeClass
   */
  public String getSerdeClass() {
    return serdeClass;
  }

  /**
   * @return the storageHandlerClass
   */
  public String getStorageHandlerClass() {
    return storageHandlerClass;
  }

  /**
   * @return the storer properties
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * @param properties the storer properties to set 
   */
  public void setProperties(Properties properties) {
    this.properties = properties;
  }


}
