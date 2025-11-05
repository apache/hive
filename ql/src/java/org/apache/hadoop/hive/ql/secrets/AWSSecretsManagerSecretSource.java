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
package org.apache.hadoop.hive.ql.secrets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of SecretSource which loads secrets from AWS Secrets Manager.
 * The format of the uri is "aws-sm:///{key-name-or-arn}"
 * It uses AWS SDK v2 with Guava cache to fetch and refresh the secret, the environment must be setup so that the default
 * client can load the secret else it will fail.
 * It expects the secret fetched to be a json string with "password" as the key for password, this is default for
 * redshift, rds or external database configs. It does not make use of any other fields.
 */
public class AWSSecretsManagerSecretSource implements SecretSource {
  // Do not create cache here, it fails to initialize in non-aws aware environments.
  private volatile LoadingCache<String, String> cache = null;
  private volatile SecretsManagerClient client = null;
  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * @return Fixed string aws-sm.
   */
  @Override
  public String getURIScheme() {
    return "aws-sm";
  }

  /**
   * This load the secret from aws-secrets manager.
   * @param uri The uri should be of format: aws-sm:///{key-arn-or-name}
   * @return The secret fetched from AWS.
   * @throws IOException
   */
  @Override
  public String getSecret(URI uri) throws IOException {
    Preconditions.checkArgument(getURIScheme().equals(uri.getScheme()));
    initCache();
    String key = uri.getPath();
    key = key.substring(1); // remove the leading slash.

    String secretsString;
    try {
      secretsString = cache.get(key);
    } catch (ExecutionException e) {
      // Wrap any exception from the above service call to IOException.
      throw new IOException("Error trying to get secret", e.getCause());
    }
    if (secretsString == null) {
      throw new IOException("secret was not found");
    }
    try {
      JsonNode passwd = mapper.readTree(secretsString).get("password");
      if (passwd == null) {
        throw new IOException("Expected \"password\" field in secrets json.");
      }
      return passwd.asText();
    } catch (JsonProcessingException e) {
      // Suppress the nested exception, since it may contain info from the secretsString, which should not be leaked.
      throw new IOException("Exception while parsing secretstring as json. Check secret string stored.");
    }
  }

  private void initCache() {
    // DCL based cache setup, to ensure thread safety.
    if (cache == null) {
      synchronized (this) {
        if (cache == null) {
          client = SecretsManagerClient.builder().build();
          cache = CacheBuilder.newBuilder()
              .expireAfterWrite(1, TimeUnit.HOURS)
              .maximumSize(100)
              .build(new CacheLoader<String, String>() {
                @Override
                public String load(String secretId) throws Exception {
                  GetSecretValueRequest request = GetSecretValueRequest.builder()
                      .secretId(secretId)
                      .build();
                  GetSecretValueResponse response = client.getSecretValue(request);
                  return response.secretString();
                }
              });
        }
      }
    }
  }

  @VisibleForTesting
  void setCache(LoadingCache<String, String> cache) {
    this.cache = cache;
  }

  @VisibleForTesting
  void setClient(SecretsManagerClient client) {
    this.client = client;
  }

  /**
   * Clean the resources in the class.
   */
  @Override
  public void close() {
    if (cache != null) {
      cache.invalidateAll();
      cache.cleanUp();
    }
    if (client != null) {
      client.close();
    }
  }
}
