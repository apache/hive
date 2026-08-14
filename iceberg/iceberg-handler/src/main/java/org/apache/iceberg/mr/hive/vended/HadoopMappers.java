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
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.mr.hive.vended.hadoop.AdlsMapper;
import org.apache.iceberg.mr.hive.vended.hadoop.GcsMapper;
import org.apache.iceberg.mr.hive.vended.hadoop.OssMapper;
import org.apache.iceberg.mr.hive.vended.hadoop.S3Mapper;

/** Selects the Hadoop mapper for a vended {@link StorageCredential}. */
final class HadoopMappers {

  private static final List<HadoopMapper> MAPPERS =
      List.of(
          S3Mapper.INSTANCE,
          GcsMapper.INSTANCE,
          AdlsMapper.INSTANCE,
          OssMapper.INSTANCE);

  private HadoopMappers() {
  }

  static HadoopMapper forCredential(StorageCredential credential) {
    if (credential == null) {
      return null;
    }
    String prefix = credential.prefix();
    HadoopMapper configMapper = null;
    for (HadoopMapper mapper : MAPPERS) {
      if (mapper.supportsPrefix(prefix)) {
        return mapper;
      }
      if (configMapper == null && credential.config().keySet().stream()
          .anyMatch(mapper::supportsConfigKey)) {
        configMapper = mapper;
      }
    }
    return configMapper;
  }

  static List<StorageCredential> credentialsFromProperties(
      String prefix, Map<String, String> properties) {
    for (HadoopMapper mapper : MAPPERS) {
      List<StorageCredential> credentials = mapper.credentialsFromProperties(prefix, properties);
      if (!credentials.isEmpty()) {
        return credentials;
      }
    }
    return List.of();
  }
}
