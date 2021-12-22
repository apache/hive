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

package org.apache.hadoop.hive.ql.exec.repl.atlas;

import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.io.InputStream;

/**
 * Atlas RESTClient interface for Atlas' REST APIs.
 */
public interface AtlasRestClient {

  InputStream exportData(AtlasExportRequest request) throws SemanticException;

  AtlasImportResult importData(AtlasImportRequest request, AtlasReplInfo atlasReplInfo) throws Exception;

  AtlasServer getServer(String endpoint, HiveConf conf) throws SemanticException;

  String getEntityGuid(final String entityType, final String attributeName, final String qualifiedName)
          throws SemanticException;

  boolean getStatus(HiveConf conf) throws SemanticException;
}
