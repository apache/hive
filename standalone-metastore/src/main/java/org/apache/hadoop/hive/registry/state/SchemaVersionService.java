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
package org.apache.hadoop.hive.registry.state;

import org.apache.hadoop.hive.registry.CompatibilityResult;
import org.apache.hadoop.hive.registry.SchemaMetadata;
import org.apache.hadoop.hive.registry.SchemaMetadataInfo;
import org.apache.hadoop.hive.registry.SchemaVersionInfo;
import org.apache.hadoop.hive.registry.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.errors.SchemaNotFoundException;

import java.util.Collection;

/**
 *
 */
public interface SchemaVersionService {

    public void updateSchemaVersionState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaNotFoundException;

    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException;

    SchemaMetadataInfo getSchemaMetadata(long schemaVersionId) throws SchemaNotFoundException;

    SchemaVersionInfo getSchemaVersionInfo(long schemaVersionId) throws SchemaNotFoundException;

    CompatibilityResult checkForCompatibility(SchemaMetadata schemaMetadata,
                                              String toSchemaText,
                                              String existingSchemaText);

    Collection<SchemaVersionInfo> getAllSchemaVersions(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException;
}
