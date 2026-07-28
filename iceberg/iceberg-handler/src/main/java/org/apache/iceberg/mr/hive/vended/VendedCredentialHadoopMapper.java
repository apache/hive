/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.vended;

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.io.StorageCredential;

/**
 * Maps Iceberg {@link StorageCredential} config entries to Hadoop
 * FileSystem properties for Tez/LLAP paths that use {@code FileSystem.get()} instead of Iceberg
 * FileIO alone.
 */
public interface VendedCredentialHadoopMapper {

  /** Returns true when {@code prefix} uses this provider's URI scheme (for example {@code s3://}). */
  boolean supportsPrefix(String prefix);

  /** Returns true when {@code icebergKey} belongs to this provider's Iceberg property namespace. */
  boolean supportsConfigKey(String icebergKey);

  /**
   * Storage scope extracted from {@code prefix}: bucket name for object stores, or ADLS
   * {@code container@account.dfs.core.windows.net} authority.
   */
  String scopeFromPrefix(String prefix);

  /**
   * Hadoop configuration key for {@code icebergKey}, or {@code null} when there is no Hadoop
   * equivalent (Iceberg FileIO still receives the value via the serialized credentials blob).
   */
  String toHadoopProperty(String scope, String icebergKey);

  /**
   * Fixed, non-secret Hadoop properties (not tied to a single vended value) required to activate
   * the mapped credentials — e.g. ABFS {@code fs.azure.account.auth.type.<account>...=SAS} so a
   * fixed SAS token is actually used. Defaults to none.
   */
  default Map<String, String> additionalNonSecretHadoopProperties(String scope, Map<String, String> config) {
    return Collections.emptyMap();
  }
}
