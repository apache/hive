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
package org.apache.hadoop.hive.common.metrics;

import java.io.IOException;

import javax.management.DynamicMBean;

/**
 * MBean definition for metrics tracking from jmx
 */

public interface MetricsMBean extends DynamicMBean {

    /**
     * Check if we're tracking a certain named key/metric
     */
    public abstract boolean hasKey(String name);

    /**
     * Add a key/metric and its value to track
     * @param name Name of the key/metric
     * @param value value associated with the key
     * @throws Exception
     */
    public abstract void put(String name, Object value) throws IOException;

    /**
     *
     * @param name
     * @return value associated with the key
     * @throws Exception
     */
    public abstract Object get(String name) throws IOException;
    

    /**
     * Removes all the keys and values from this MetricsMBean. 
     */
    void clear();
}
