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

package org.apache.iceberg.mr.hive;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SerializationUtil;
import org.springframework.util.CollectionUtils;

/**
 * Propagates vended storage credentials from an Iceberg {@link Table}'s {@link FileIO} to Hive job
 * configuration so Tez/LLAP executors can access object storage without static catalog keys.
 */
public final class IcebergVendedCredentialUtil {

  public static final String ACCESS_KEY_ID = "s3.access-key-id";
  public static final String SECRET_ACCESS_KEY = "s3.secret-access-key";
  public static final String SESSION_TOKEN = "s3.session-token";
  public static final String ENDPOINT = "s3.endpoint";
  public static final String PATH_STYLE_ACCESS = "s3.path-style-access";

  private static final ImmutableSet<String> SECRET_ICEBERG_KEYS = ImmutableSet.of(
      ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN);

  private IcebergVendedCredentialUtil() {
  }

  /**
   * Copies vended credentials from the table FileIO into Hive job configuration.
   *
   * <p>Follows the HIVE-20651 split used by {@code JdbcStorageHandler}: sensitive values (including
   * the serialized {@link StorageCredential} list) go to {@code jobSecrets}; non-secret config such
   * as endpoint and path-style access go to {@code jobProperties}.
   *
   * @param table loaded Iceberg table
   * @param catalogName Hive catalog name ({@link InputFormatConfig#CATALOG_NAME})
   * @param jobProperties Tez/MR non-secret job properties; may be {@code null}
   * @param jobSecrets sensitive keys and serialized credentials; may be {@code null}
   * @param conf session conf used to preserve host-side endpoint overrides
   */
  public static void propagateToJob(Table table, String catalogName, Map<String, String> jobProperties,
      Map<String, String> jobSecrets, Configuration conf) {

    List<StorageCredential> credentials =
        withConfigurationOverrides(catalogName, extractCredentials(table), conf);

    if (credentials.isEmpty()) {
      return;
    }

    if (jobSecrets != null) {
      jobSecrets.put(
          InputFormatConfig.VENDED_STORAGE_CREDENTIALS,
          SerializationUtil.serializeToBase64(Lists.newArrayList(credentials)));
    }

    for (StorageCredential credential : credentials) {
      addCredentialEntries(catalogName, credential, jobProperties, jobSecrets, conf);
    }
  }

  /**
   * Writes each key in one vended {@link StorageCredential} into job configuration.
   *
   * <p>Derives the S3 bucket from {@link StorageCredential#prefix()} and delegates to
   * {@link #addCredentialEntry} for every entry in {@link StorageCredential#config()}, which maps
   * Iceberg keys to catalog-level and per-bucket S3A job properties or secrets.
   */
  private static void addCredentialEntries(String catalogName, StorageCredential credential,
      Map<String, String> jobProperties, Map<String, String> jobSecrets, Configuration conf) {

    String bucket = bucketFromPrefix(credential.prefix());
    for (Map.Entry<String, String> entry : credential.config().entrySet()) {
      addCredentialEntry(
          catalogName, bucket, entry.getKey(), entry.getValue(), jobProperties, jobSecrets, conf);
    }
  }

  /**
   * Routes one Iceberg credential config entry into job properties or secrets.
   *
   * <p>Skips blank values, applies session catalog overrides via {@link #resolveCredentialValue},
   * then sends non-secret keys (endpoint, path-style access, etc.) to {@code jobProperties} and
   * secret keys (access key, secret key, session token) to {@code jobSecrets}. Either map may be
   * {@code null} when {@link #propagateToJob} is called for only properties or only secrets.
   */
  private static void addCredentialEntry(String catalogName, String bucket, String icebergKey, String value,
      Map<String, String> jobProperties, Map<String, String> jobSecrets, Configuration conf) {

    if (StringUtils.isBlank(value)) {
      return;
    }
    String resolvedValue = resolveCredentialValue(catalogName, icebergKey, value, conf);

    if (jobProperties != null && !isSecretKey(icebergKey)) {
      addNonSecretCredentialEntry(catalogName, bucket, icebergKey, resolvedValue, jobProperties);
    }

    if (jobSecrets != null && isSecretKey(icebergKey)) {
      addSecretCredentialEntry(bucket, icebergKey, resolvedValue, jobSecrets);
    }
  }

  /**
   * Adds one non-secret vended value to {@code jobProperties} for Iceberg and Hadoop S3A.
   *
   * <p>When {@code catalogName} is set, writes {@code iceberg.catalog.&lt;catalog&gt;.&lt;key&gt;}.
   * When {@code bucket} is set, also writes the matching {@code fs.s3a.bucket.&lt;bucket&gt;.*}
   * key if {@link #toS3aBucketProperty} maps the Iceberg key.
   */
  private static void addNonSecretCredentialEntry(String catalogName, String bucket, String icebergKey, String value,
      Map<String, String> jobProperties) {

    if (catalogName != null) {
      String catalogConfigKey =
          IcebergCatalogProperties.catalogPropertyConfigKey(catalogName, icebergKey);
      jobProperties.putIfAbsent(catalogConfigKey, value);
    }

    if (bucket != null) {
      String s3aKey = toS3aBucketProperty(bucket, icebergKey);
      if (s3aKey != null) {
        jobProperties.putIfAbsent(s3aKey, value);
      }
    }
  }

  /** Writes Hadoop S3A per-bucket keys only; Iceberg secrets are carried in the serialized blob. */
  private static void addSecretCredentialEntry(String bucket, String icebergKey, String value,
      Map<String, String> jobSecrets) {
    if (bucket != null) {
      String s3aSecretKey = toS3aBucketProperty(bucket, icebergKey);
      if (s3aSecretKey != null) {
        jobSecrets.put(s3aSecretKey, value);
      }
    }
  }

  /**
   * Applies vended credentials to the table FileIO, merging session/catalog conf overrides (e.g. S3 endpoint).
   * Used on executors after deserialization and on HS2 commit when the table is taken from query state.
   */
  public static void applyFromJobConf(Table table, Configuration conf) {
    if (table == null || conf == null) {
      return;
    }

    String catalogName = conf.get(InputFormatConfig.CATALOG_NAME);
    if (shouldSkipApplyFromJobConf(catalogName, conf)) {
      return;
    }

    FileIO io = table.io();
    if (!(io instanceof SupportsStorageCredentials credentialIo)) {
      return;
    }

    List<StorageCredential> credentials = resolveCredentialsForApply(table, credentialIo, conf);
    if (credentials != null && !credentials.isEmpty()) {
      credentialIo.setCredentials(withConfigurationOverrides(catalogName, credentials, conf));
    }
  }

  /**
   * Returns true when the job carries no serialized vended credentials and the catalog is not
   * configured for credential vending (no {@code vended-credentials} REST delegation header).
   * Otherwise apply may restore credentials from the job conf or from the table FileIO.
   */
  private static boolean shouldSkipApplyFromJobConf(String catalogName, Configuration conf) {
    return StringUtils.isBlank(conf.get(InputFormatConfig.VENDED_STORAGE_CREDENTIALS)) &&
        !IcebergCatalogProperties.requestsVendedCredentials(catalogName, conf);
  }

  /**
   * Chooses which vended credentials {@link #applyFromJobConf} should install on the FileIO.
   *
   * <p>Uses the first non-empty source: credentials already on the FileIO (typical on HS2 after
   * table load), else the base64 list from {@link InputFormatConfig#VENDED_STORAGE_CREDENTIALS} on
   * the task {@code conf} (propagated at plan time), else {@link #extractCredentials(Table)} from
   * the table (including FileIO property fallbacks).
   */
  private static List<StorageCredential> resolveCredentialsForApply(
      Table table, SupportsStorageCredentials credentialIo, Configuration conf) {

    List<StorageCredential> credentials = credentialIo.credentials();
    if (CollectionUtils.isEmpty(credentials)) {
      String serialized = conf.get(InputFormatConfig.VENDED_STORAGE_CREDENTIALS);
      if (StringUtils.isNotBlank(serialized)) {
        @SuppressWarnings("unchecked")
        List<StorageCredential> deserialized =
            SerializationUtil.deserializeFromBase64(serialized);
        credentials = deserialized;
      }
    }

    if (credentials == null || credentials.isEmpty()) {
      credentials = extractCredentials(table);
    }
    return credentials;
  }

  /**
   * Returns true when the table catalog is configured for REST vended storage credentials.
   */
  static boolean requestsVendedCredentials(Properties properties, Configuration configuration) {
    if (properties == null) {
      return false;
    }
    return IcebergCatalogProperties.requestsVendedCredentials(
        properties.getProperty(InputFormatConfig.CATALOG_NAME), configuration);
  }

  /**
   * Loads a table and, if needed, bypasses the query-level cache so REST vended credentials are present on the FileIO.
   * When vended credentials are not requested for the catalog, returns the cached table without an extra load.
   */
  static Table getTableWithVendedCredentials(Properties properties, Configuration configuration) {
    Table table = IcebergTableUtil.getTable(configuration, properties);
    if (requestsVendedCredentials(properties, configuration) && extractCredentials(table).isEmpty()) {
      table = IcebergTableUtil.getTable(configuration, properties, true);
    }
    return table;
  }

  /**
   * Reloads vended credentials at job launch when compile-time propagation missed them.
   * Non-secret config is written to {@code jobConf}; secrets are merged into {@code TableDesc#getJobSecrets()}
   * for {@link org.apache.hadoop.hive.ql.plan.PlanUtils#configureJobConf} (HIVE-20651).
   */
  static void refreshVendedCredentialsIfMissing(TableDesc tableDesc, JobConf jobConf, Configuration configuration) {
    if (tableDesc == null || tableDesc.getProperties() == null ||
        hasSerializedCredentials(tableDesc.getJobSecrets()) ||
        StringUtils.isNotBlank(jobConf.get(InputFormatConfig.VENDED_STORAGE_CREDENTIALS))) {
      return;
    }

    Properties props = tableDesc.getProperties();
    if (!requestsVendedCredentials(props, configuration)) {
      return;
    }

    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);
    if (catalogName == null) {
      return;
    }

    try {
      Table table = getTableWithVendedCredentials(props, configuration);
      Map<String, String> jobProps = new LinkedHashMap<>();
      Map<String, String> secrets = new LinkedHashMap<>();
      propagateToJob(table, catalogName, jobProps, secrets, configuration);
      jobProps.forEach(jobConf::set);
      mergeJobSecrets(tableDesc, secrets);
    } catch (NoSuchTableException ex) {
      // Table may not exist yet for CTAS; credentials will not be available.
    }
  }

  /** Copies {@code jobSecrets} onto {@code jobConf} so the Tez driver can use S3A before tasks start. */
  static void applyJobSecretsToJobConf(TableDesc tableDesc, JobConf jobConf) {
    Map<String, String> secrets = tableDesc.getJobSecrets();
    if (secrets != null) {
      secrets.forEach(jobConf::set);
    }
  }

  private static boolean hasSerializedCredentials(Map<String, String> jobSecrets) {
    return jobSecrets != null &&
        StringUtils.isNotBlank(jobSecrets.get(InputFormatConfig.VENDED_STORAGE_CREDENTIALS));
  }

  private static void mergeJobSecrets(TableDesc tableDesc, Map<String, String> secrets) {
    if (secrets.isEmpty()) {
      return;
    }
    Map<String, String> existing = tableDesc.getJobSecrets();
    if (existing == null) {
      tableDesc.setJobSecrets(new LinkedHashMap<>(secrets));
    } else {
      secrets.forEach(existing::putIfAbsent);
    }
  }

  private static boolean isSecretKey(String icebergKey) {
    return SECRET_ICEBERG_KEYS.contains(icebergKey);
  }

  static List<StorageCredential> extractCredentials(Table table) {
    if (table == null) {
      return List.of();
    }
    FileIO io = table.io();
    if (io instanceof SupportsStorageCredentials credentialIo) {
      List<StorageCredential> credentials = credentialIo.credentials();
      if (credentials != null && !credentials.isEmpty()) {
        return credentials;
      }
    }
    return credentialsFromFileIoProperties(table, io);
  }

  private static List<StorageCredential> credentialsFromFileIoProperties(Table table, FileIO io) {
    Map<String, String> props = io.properties();
    if (props == null || StringUtils.isBlank(props.get(ACCESS_KEY_ID)) ||
        StringUtils.isBlank(props.get(SECRET_ACCESS_KEY))) {
      return List.of();
    }
    Map<String, String> config = new LinkedHashMap<>();
    putIfPresent(config, props, ACCESS_KEY_ID);
    putIfPresent(config, props, SECRET_ACCESS_KEY);
    putIfPresent(config, props, SESSION_TOKEN);
    putIfPresent(config, props, ENDPOINT);
    putIfPresent(config, props, PATH_STYLE_ACCESS);
    putIfPresent(config, props, "client.region");
    return List.of(StorageCredential.create(credentialPrefix(table), config));
  }

  private static void putIfPresent(Map<String, String> target, Map<String, String> source, String key) {
    if (source.containsKey(key) && StringUtils.isNotBlank(source.get(key))) {
      target.put(key, source.get(key));
    }
  }

  private static String credentialPrefix(Table table) {
    String location = table.location();
    if (StringUtils.isBlank(location)) {
      return "";
    }
    String bucket = bucketFromPrefix(location);
    if (bucket != null) {
      return "s3://" + bucket + "/";
    }
    return location.endsWith("/") ? location : location + "/";
  }

  /**
   * REST catalogs vend credentials together with S3 connectivity settings such as endpoint
   * and path-style access. These settings reflect the catalog's network view and may reference
   * hosts that are not reachable from Hive (for example, an internal {@code minio:9000}
   * hostname).
   *
   * Catalog S3 properties configured in the Hive session (for example,
   * {@code iceberg.catalog.ice01.s3.endpoint}) override the corresponding vended connectivity
   * settings so the driver and executors use reachable endpoints. Vended credentials are
   * preserved; only non-secret connectivity properties are overridden.
   */
  private static List<StorageCredential> withConfigurationOverrides(
      String catalogName, List<StorageCredential> credentials, Configuration conf) {

    if (credentials.isEmpty() || conf == null || catalogName == null) {
      return credentials;
    }

    List<StorageCredential> updated = Lists.newArrayListWithCapacity(credentials.size());
    for (StorageCredential credential : credentials) {
      Map<String, String> credsConfig = new LinkedHashMap<>(credential.config());
      applyCatalogConfigOverrides(catalogName, credsConfig, conf);
      updated.add(StorageCredential.create(credential.prefix(), credsConfig));
    }

    return updated;
  }

  /**
   * Applies session-level catalog overrides for {@link #ENDPOINT} and
   * {@link #PATH_STYLE_ACCESS} to the given credential configuration.
   *
   * <p>Override values are read from {@code conf} using
   * {@link IcebergCatalogProperties#catalogPropertyConfigKey(String, String)}.
   * If a non-blank override is configured, it replaces the corresponding value
   * in {@code config}; otherwise the existing (vended) value is retained.
   */
  private static void applyCatalogConfigOverrides(
      String catalogName, Map<String, String> config, Configuration conf) {
    for (String icebergKey : List.of(ENDPOINT, PATH_STYLE_ACCESS)) {
      String override =
          conf.get(IcebergCatalogProperties.catalogPropertyConfigKey(catalogName, icebergKey));
      if (StringUtils.isNotBlank(override)) {
        config.put(icebergKey, override);
      }
    }
  }

  private static String resolveCredentialValue(
      String catalogName, String icebergKey, String vendedValue, Configuration conf) {
    if (conf == null || catalogName == null) {
      return vendedValue;
    }
    String override =
        conf.get(IcebergCatalogProperties.catalogPropertyConfigKey(catalogName, icebergKey));
    return StringUtils.isNotBlank(override) ? override : vendedValue;
  }

  private static String bucketFromPrefix(String prefix) {
    if (StringUtils.isBlank(prefix)) {
      return null;
    }
    try {
      URI uri = new URI(prefix.endsWith("/") ? prefix : prefix + "/");
      if ("s3".equalsIgnoreCase(uri.getScheme()) || "s3a".equalsIgnoreCase(uri.getScheme())) {
        return uri.getHost();
      }
    } catch (Exception ignored) {
      // fall through
    }
    if (prefix.startsWith("s3://") || prefix.startsWith("s3a://")) {
      String withoutScheme = prefix.substring(prefix.indexOf("://") + 3);
      int slash = withoutScheme.indexOf('/');
      return slash >= 0 ? withoutScheme.substring(0, slash) : withoutScheme;
    }
    return null;
  }

  private static String toS3aBucketProperty(String bucket, String icebergKey) {
    String bucketPrefix = "fs.s3a.bucket." + bucket + ".";
    return switch (icebergKey) {
      case ACCESS_KEY_ID -> bucketPrefix + "access.key";
      case SECRET_ACCESS_KEY -> bucketPrefix + "secret.key";
      case SESSION_TOKEN -> bucketPrefix + "session.token";
      case ENDPOINT -> bucketPrefix + "endpoint";
      case PATH_STYLE_ACCESS -> bucketPrefix + "path.style.access";
      case "client.region" -> bucketPrefix + "endpoint.region";
      default -> null;
    };
  }
}
