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

package org.apache.iceberg.mr.hive.vended.hadoop;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.io.StorageCredential;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOssMapper {

  @Test
  public void mapsAliasKeysToHadoopProperties() {
    assertThat(OssMapper.INSTANCE.toHadoopProperty("my-bucket", "oss-access-key-id"))
        .isEqualTo("fs.oss.accessKeyId");
    assertThat(OssMapper.INSTANCE.toHadoopProperty("my-bucket", "oss-secret-access-key"))
        .isEqualTo("fs.oss.accessKeySecret");
    assertThat(OssMapper.INSTANCE.toHadoopProperty("my-bucket", "oss.security-token"))
        .isEqualTo("fs.oss.securityToken");
    assertThat(OssMapper.INSTANCE.toHadoopProperty("my-bucket", "oss-endpoint"))
        .isEqualTo("fs.oss.endpoint");
  }

  @Test
  public void credentialsFromAliasPropertiesUsesCanonicalKeys() {
    List<StorageCredential> credentials =
        OssMapper.INSTANCE.credentialsFromProperties(
            "oss://my-bucket/",
            Map.of(
                "oss-access-key-id", "oss-access",
                "oss-secret-access-key", "oss-secret",
                "oss.security-token", "oss-token",
                "oss-endpoint", "oss-cn-hangzhou.aliyuncs.com"));

    assertThat(credentials).hasSize(1);
    assertThat(credentials.getFirst().config())
        .containsEntry(AliyunProperties.CLIENT_ACCESS_KEY_ID, "oss-access")
        .containsEntry(AliyunProperties.CLIENT_ACCESS_KEY_SECRET, "oss-secret")
        .containsEntry(AliyunProperties.CLIENT_SECURITY_TOKEN, "oss-token")
        .containsEntry(AliyunProperties.OSS_ENDPOINT, "oss-cn-hangzhou.aliyuncs.com");
  }
}
