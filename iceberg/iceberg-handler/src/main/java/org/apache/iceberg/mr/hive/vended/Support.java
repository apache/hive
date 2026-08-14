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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;

/** Public helpers for {@link org.apache.iceberg.mr.hive.IcebergVendedCredentialUtil}. */
public final class Support {

  private Support() {
  }

  public static String storagePrefixFromLocation(String location) {
    return PrefixUtil.storagePrefixFromLocation(location);
  }

  public static String scopeFromPrefix(String prefix) {
    return PrefixUtil.scopeFromPrefix(prefix);
  }

  public static HadoopMapper mapperFor(StorageCredential credential) {
    return HadoopMappers.forCredential(credential);
  }

  public static String toHadoopProperty(HadoopMapper mapper, String scope, String icebergKey) {
    if (mapper == null || scope == null) {
      return null;
    }
    return mapper.toHadoopProperty(scope, icebergKey);
  }

  public static Map<String, String> additionalNonSecretHadoopProperties(
      HadoopMapper mapper, String scope, Map<String, String> config) {
    if (mapper == null) {
      return Map.of();
    }
    return mapper.additionalNonSecretHadoopProperties(scope, config);
  }

  public static List<StorageCredential> credentialsFromFileIoProperties(Table table, FileIO io) {
    Map<String, String> props = io.properties();
    if (props == null || props.isEmpty()) {
      return List.of();
    }
    return HadoopMappers.credentialsFromProperties(storagePrefixFromLocation(table.location()), props);
  }
}
