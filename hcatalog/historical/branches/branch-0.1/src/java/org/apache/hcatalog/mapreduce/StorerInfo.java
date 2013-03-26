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
class StorerInfo implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;

    /** The name of the input storage driver class */
    private String inputSDClass;

    /** The name of the output storage driver class */
    private String outputSDClass;

    /** The properties for the storage driver */
    private Properties properties;


    /**
     * Initialize the storage driver
     * @param inputSDClass
     * @param outputSDClass
     * @param properties
     */
    StorerInfo(String inputSDClass, String outputSDClass, Properties properties) {
      super();
      this.inputSDClass = inputSDClass;
      this.outputSDClass = outputSDClass;
      this.properties = properties;
    }

    /**
     * @return the inputSDClass
     */
    public String getInputSDClass() {
      return inputSDClass;
    }

    /**
     * @param inputSDClass the inputSDClass to set
     */
    public void setInputSDClass(String inputSDClass) {
      this.inputSDClass = inputSDClass;
    }

    /**
     * @return the outputSDClass
     */
    public String getOutputSDClass() {
      return outputSDClass;
    }

    /**
     * @param outputSDClass the outputSDClass to set
     */
    public void setOutputSDClass(String outputSDClass) {
      this.outputSDClass = outputSDClass;
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
