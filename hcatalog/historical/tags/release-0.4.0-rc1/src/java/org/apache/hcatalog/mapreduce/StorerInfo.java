/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hcatalog.mapreduce;

import java.io.Serializable;
import java.util.Properties;

/** Info about the storer to use for writing the data */
public class StorerInfo implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;

    //TODO remove this
    /** The name of the input storage driver class */
    private String inputSDClass;

    //TODO remove this
    /** The name of the output storage driver class */
    private String outputSDClass;

    /** The properties for the storage driver */
    private Properties properties;

    private String ofClass;

    private String ifClass;

    private String serdeClass;

    private String storageHandlerClass;


    //TODO remove this
    /**
     * Initialize the storage driver
     * @param inputSDClass
     * @param outputSDClass
     * @param properties
     */
    public StorerInfo(String inputSDClass, String outputSDClass, Properties properties) {
      super();
      this.inputSDClass = inputSDClass;
      this.outputSDClass = outputSDClass;
      this.properties = properties;
    }

    /**
     * Initialize the storage driver
     * @param inputSDClass
     * @param outputSDClass
     * @param properties
     */
    public StorerInfo(String inputSDClass, String outputSDClass, String ifClass, String ofClass, String serdeClass, String storageHandlerClass, Properties properties) {
      super();
      this.inputSDClass = inputSDClass;
      this.outputSDClass = outputSDClass;
      this.ifClass =ifClass;
      this.ofClass = ofClass;
      this.serdeClass = serdeClass;
      this.storageHandlerClass = storageHandlerClass;
      this.properties = properties;
    }

    /**
     * @return the inputSDClass
     */
    public String getInputSDClass() {
      return inputSDClass;
    }

    /**
     * @return the outputSDClass
     */
    public String getOutputSDClass() {
      return outputSDClass;
    }

public String getIfClass() {
        return ifClass;
    }

    public void setIfClass(String ifClass) {
        this.ifClass = ifClass;
    }

    public String getOfClass() {
        return ofClass;
    }

    public String getSerdeClass() {
        return serdeClass;
    }

    public String getStorageHandlerClass() {
        return storageHandlerClass;
    }

    /**
     * @return the properties
     */
    public Properties getProperties() {
      return properties;
    }

    /**
     * @param properties the properties to set
     */
    public void setProperties(Properties properties) {
      this.properties = properties;
    }


}
