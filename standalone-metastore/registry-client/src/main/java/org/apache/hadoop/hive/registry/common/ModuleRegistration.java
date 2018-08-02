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

package org.apache.hadoop.hive.registry.common;

import org.apache.hadoop.hive.registry.common.util.FileStorage;

import java.util.List;
import java.util.Map;

/**
 * An interface expected to be implemented by independent modules so that they can be registered with web service module on startup
 */
public interface ModuleRegistration {

    /**
     *
     * @param config module specific config from the yaml file
     */
    void init (Map<String, Object> config);

    /**
     *
     * @return list of resources to register with the web service module to handle end points for this module
     */
    List<Object> getResources ();
}
