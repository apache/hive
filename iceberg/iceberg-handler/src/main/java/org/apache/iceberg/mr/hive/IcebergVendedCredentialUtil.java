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

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SerializationUtil;

/**
 * Propagates vended storage credentials from an Iceberg {@link Table}'s {@link FileIO} to Hive job
 * configuration so Tez/LLAP executors can access object storage without static catalog keys.
 *
 * <p>Limitation: credentials are minted at compile/job launch and are not refreshed — work that
 * outlives the vended token lifetime fails authentication.
 */
public final class IcebergVendedCredentialUtil {

  /** Vended config safe to display; any key not listed here routes to the secrets channel. */
  private static final ImmutableSet<String> NON_SECRET_ICEBERG_KEYS = ImmutableSet.of(
      S3FileIOProperties.ENDPOINT, S3FileIOProperties.PATH_STYLE_ACCESS, AwsClientProperties.CLIENT_REGION);

  private IcebergVendedCredentialUtil() {
  }

  /**
   * Copies vended credentials from the table FileIO into Hive job configuration.
   *
   * <p>Follows the HIVE-20651 split used by {@code JdbcStorageHandler}: sensitive values (including
   * the serialized {@link StorageCredential} list) go to {@code jobSecrets}; non-secret config such
   * as endpoint and path-style access go to {@code jobProperties}.
   *
   * @param table loaded Iceberg table; its catalog-qualified name keys the credentials blob,
   *        since REST catalogs mint credentials per table (the table pointer carries the same
   *        name, so executors resolve the matching key)
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
          InputFormatConfig.vendedCredentialsKey(table.name()),
          serializeToSingleLineBase64(Lists.newArrayList(credentials)));
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

  /** Writes Hadoop S3A per-bucket keys only; Iceberg secrets are carried in the serialized blob.
   * First table wins on a shared bucket, matching the non-secret entries, so an endpoint and its
   * key always come from the same credential. */
  private static void addSecretCredentialEntry(String bucket, String icebergKey, String value,
      Map<String, String> jobSecrets) {
    if (bucket != null) {
      String s3aSecretKey = toS3aBucketProperty(bucket, icebergKey);
      if (s3aSecretKey != null) {
        jobSecrets.putIfAbsent(s3aSecretKey, value);
      }
    }
  }

  /**
   * Applies vended credentials to the table FileIO, merging session/catalog conf overrides (e.g. S3 endpoint).
   * Used on executors after deserialization and on HS2 commit when the table is taken from query state.
   */
  public static void applyFromJobConf(Table table, Configuration conf) {
    applyFromJobConf(table, conf != null ? conf.get(InputFormatConfig.CATALOG_NAME) : null, conf);
  }

  /** Variant for callers that resolve the catalog name per table (HS2 commit paths, where the
   * job-level {@link InputFormatConfig#CATALOG_NAME} is not set). */
  public static void applyFromJobConf(Table table, String catalogName, Configuration conf) {
    if (table == null || conf == null) {
      return;
    }

    if (shouldSkipApplyFromJobConf(table.name(), catalogName, conf)) {
      return;
    }

    FileIO io = table.io();
    if (!(io instanceof SupportsStorageCredentials credentialIo)) {
      return;
    }

    List<StorageCredential> credentials = resolveCredentialsForApply(table, credentialIo, conf);
    if (!credentials.isEmpty()) {
      credentialIo.setCredentials(withConfigurationOverrides(catalogName, credentials, conf));
    }
  }

  /**
   * Returns true when the job carries no serialized vended credentials and the catalog is not
   * configured for credential vending (no {@code vended-credentials} REST delegation header).
   * Otherwise apply may restore credentials from the job conf or from the table FileIO.
   */
  private static boolean shouldSkipApplyFromJobConf(String tableName, String catalogName, Configuration conf) {
    return StringUtils.isBlank(conf.get(InputFormatConfig.vendedCredentialsKey(tableName))) &&
        !IcebergCatalogProperties.requestsVendedCredentials(catalogName, conf);
  }

  /**
   * Chooses which vended credentials {@link #applyFromJobConf} should install on the FileIO.
   *
   * <p>Uses the first non-empty source: credentials already on the FileIO (typical on HS2 after
   * table load), else the base64 list from {@link InputFormatConfig#VENDED_STORAGE_CREDENTIALS} on
   * the task {@code conf} (restored from the HIVE-20651 Credentials channel into table properties
   * by {@code Utilities#copyJobSecretToTableProperties} and copied to the task-local conf), else
   * {@link #extractCredentials(Table)} from the table (including FileIO property fallbacks).
   */
  private static List<StorageCredential> resolveCredentialsForApply(
      Table table, SupportsStorageCredentials credentialIo, Configuration conf) {

    List<StorageCredential> credentials = credentialIo.credentials();
    if (credentials != null && !credentials.isEmpty()) {
      return credentials;
    }
    String serialized = conf.get(InputFormatConfig.vendedCredentialsKey(table.name()));
    if (StringUtils.isNotBlank(serialized)) {
      return SerializationUtil.deserializeFromBase64(serialized);
    }
    return extractCredentials(table);
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
        hasSerializedCredentials(tableDesc.getJobSecrets())) {
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

  /**
   * Returns a copy of the table over a fresh secret-free {@link FileIO}, safe to serialize into
   * job properties. {@code SerializableTable.copyOf} embeds {@code table.io()} — and with it the
   * vended credentials — into the serialized bytes, so the copy is rebuilt from the same table
   * metadata over a FileIO that keeps only the allowlisted non-secret properties: unknown keys
   * never ship, whatever the storage provider. Executors restore the credentials from the
   * HIVE-20651 Credentials channel via {@link #applyFromJobConf}
   * ({@code SupportsStorageCredentials#setCredentials}).
   */
  static Table secretFreeCopy(Table table, Configuration conf) {
    if (table instanceof BaseMetadataTable metadataTable) {
      Table base = secretFreeCopy(metadataTable.table(), conf);
      return MetadataTableUtils.createMetadataTableInstance(base, metadataTableType(metadataTable));
    }
    Preconditions.checkState(table instanceof HasTableOperations,
        "Cannot build a secret-free copy of %s (%s)", table.name(), table.getClass().getName());
    TableMetadata metadata = ((HasTableOperations) table).operations().current();

    FileIO io = table.io();
    Map<String, String> ioProps = new LinkedHashMap<>();
    if (io.properties() != null) {
      io.properties().forEach((key, value) -> {
        if (NON_SECRET_ICEBERG_KEYS.contains(key)) {
          ioProps.put(key, value);
        }
      });
    }
    FileIO cleanIo = CatalogUtil.loadFileIO(io.getClass().getName(), ioProps, conf);

    return new BaseTable(
        new StaticTableOperations(metadata, cleanIo, table.locationProvider()), table.name());
  }

  /** {@code metadataTableType()} is package-private; the name suffix is the type by construction. */
  private static MetadataTableType metadataTableType(BaseMetadataTable metadataTable) {
    String name = metadataTable.name();
    MetadataTableType type = MetadataTableType.from(name.substring(name.lastIndexOf('.') + 1));
    Preconditions.checkState(type != null, "Cannot resolve metadata table type from %s", name);
    return type;
  }

  /**
   * Single-line base64, unlike {@link SerializationUtil#serializeToBase64} which MIME-wraps with
   * CR/LF. The blob is restored into table properties and copied to the task conf through
   * {@code Utilities#copyTablePropertiesToConf}, whose {@code escapeJava} turns CR/LF into literal
   * {@code \r\n} — and {@code r}/{@code n} are valid base64 alphabet, corrupting the decoded
   * stream. {@link SerializationUtil#deserializeFromBase64} uses the MIME decoder, which accepts
   * unwrapped input.
   */
  private static String serializeToSingleLineBase64(Object obj) {
    return Base64.getEncoder().encodeToString(SerializationUtil.serializeToBytes(obj));
  }

  private static boolean hasSerializedCredentials(Map<String, String> jobSecrets) {
    return jobSecrets != null && jobSecrets.keySet().stream()
        .anyMatch(key -> key.startsWith(InputFormatConfig.VENDED_STORAGE_CREDENTIALS));
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
    return !NON_SECRET_ICEBERG_KEYS.contains(icebergKey);
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
    if (props == null || StringUtils.isBlank(props.get(S3FileIOProperties.ACCESS_KEY_ID)) ||
        StringUtils.isBlank(props.get(S3FileIOProperties.SECRET_ACCESS_KEY))) {
      return List.of();
    }
    Map<String, String> config = new LinkedHashMap<>();
    putIfPresent(config, props, S3FileIOProperties.ACCESS_KEY_ID);
    putIfPresent(config, props, S3FileIOProperties.SECRET_ACCESS_KEY);
    putIfPresent(config, props, S3FileIOProperties.SESSION_TOKEN);
    putIfPresent(config, props, S3FileIOProperties.ENDPOINT);
    putIfPresent(config, props, S3FileIOProperties.PATH_STYLE_ACCESS);
    putIfPresent(config, props, AwsClientProperties.CLIENT_REGION);
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
   * Applies session-level catalog overrides to every entry of the given credential configuration,
   * through the same {@link #resolveCredentialValue} used for the job-property entries — one
   * resolver, one scope, so both channels always carry the same values.
   */
  private static void applyCatalogConfigOverrides(
      String catalogName, Map<String, String> config, Configuration conf) {
    config.replaceAll((icebergKey, value) -> resolveCredentialValue(catalogName, icebergKey, value, conf));
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

  /** Authority (bucket) of a storage prefix such as {@code s3://bucket/path}, any scheme.
   * Plain string parse: {@code URI.getHost()} rejects legal bucket names with underscores. */
  @SuppressWarnings("java:S1075") // storage URI path separator, not a filesystem path
  private static String bucketFromPrefix(String prefix) {
    int schemeEnd = prefix == null ? -1 : prefix.indexOf("://");
    if (schemeEnd < 0) {
      return null;
    }
    String withoutScheme = prefix.substring(schemeEnd + 3);
    int slash = withoutScheme.indexOf('/');
    String bucket = slash >= 0 ? withoutScheme.substring(0, slash) : withoutScheme;
    return StringUtils.defaultIfBlank(bucket, null);
  }

  private static String toS3aBucketProperty(String bucket, String icebergKey) {
    String bucketPrefix = "fs.s3a.bucket." + bucket + ".";
    return switch (icebergKey) {
      case S3FileIOProperties.ACCESS_KEY_ID -> bucketPrefix + "access.key";
      case S3FileIOProperties.SECRET_ACCESS_KEY -> bucketPrefix + "secret.key";
      case S3FileIOProperties.SESSION_TOKEN -> bucketPrefix + "session.token";
      case S3FileIOProperties.ENDPOINT -> bucketPrefix + "endpoint";
      case S3FileIOProperties.PATH_STYLE_ACCESS -> bucketPrefix + "path.style.access";
      case AwsClientProperties.CLIENT_REGION -> bucketPrefix + "endpoint.region";
      default -> null;
    };
  }
}
