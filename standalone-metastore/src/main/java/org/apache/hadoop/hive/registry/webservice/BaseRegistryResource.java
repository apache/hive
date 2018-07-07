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
package org.apache.hadoop.hive.registry.webservice;

import org.apache.hadoop.hive.registry.ISchemaRegistry;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseRegistryResource {

    private static final Logger LOG = LoggerFactory.getLogger(BaseRegistryResource.class);

    final ISchemaRegistry schemaRegistry;

    // Hack: Adding number in front of sections to get the ordering in generated swagger documentation correct
    static final String OPERATION_GROUP_SCHEMA = "1. Schema";
    static final String OPERATION_GROUP_SERDE = "2. Serializer/Deserializer";
    static final String OPERATION_GROUP_OTHER = "3. Other";



    BaseRegistryResource(ISchemaRegistry schemaRegistry) {
        Preconditions.checkNotNull(schemaRegistry, "SchemaRegistry can not be null");
        this.schemaRegistry = schemaRegistry;
    }


    static void checkValueAsNullOrEmpty(String name, String value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Parameter " + name + " is null");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Parameter " + name + " is empty");
        }
    }
    
}
