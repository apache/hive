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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * HiveStorageAuthorizationHandler defines a pluggable interface for
 * authorization of storage based tables in Hive. A Storage authorization
 * handler consists of a bundle of the following:
 *
 *<ul>
 *<li>getURI
 *</ul>
 *
 * Storage authorization handler classes are plugged in using the STORED BY 'classname'
 * clause in CREATE TABLE.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface HiveStorageAuthorizationHandler{

    /**
     * @return get URI for authentication implementation,
     * should return uri with table properties.
     */
    public URI getURIForAuth(Map<String, String> tableProperties) throws URISyntaxException;
}