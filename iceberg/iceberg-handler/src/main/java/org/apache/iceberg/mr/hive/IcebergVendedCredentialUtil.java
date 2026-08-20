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
import org.apache.hadoop.hive.conf.HiveConfUtil;
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
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.rest.catalog.RestCatalogAccessDelegation;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.vended.HadoopMapper;
import org.apache.iceberg.mr.hive.vended.Support;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
   * <p>Derives storage scope from {@link StorageCredential#prefix()} and delegates to
   * {@link #addCredentialEntry} for every entry in {@link StorageCredential#config()}, which maps
   * Iceberg keys to catalog-level and provider-specific Hadoop job properties or secrets.
   */
  private static void addCredentialEntries(String catalogName, StorageCredential credential,
      Map<String, String> jobProperties, Map<String, String> jobSecrets, Configuration conf) {

    HadoopMapper mapper = Support.mapperFor(credential);
    String scope = mapper != null ? mapper.scopeFromPrefix(credential.prefix()) :
        Support.scopeFromPrefix(credential.prefix());
    for (Map.Entry<String, String> entry : credential.config().entrySet()) {
      addCredentialEntry(
          catalogName, scope, mapper, entry.getKey(), entry.getValue(), jobProperties, jobSecrets, conf);
    }
    if (jobProperties != null) {
      Support.additionalNonSecretHadoopProperties(mapper, scope, credential.config())
          .forEach(jobProperties::putIfAbsent);
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
  private static void addCredentialEntry(String catalogName, String scope,
      HadoopMapper mapper, String icebergKey, String value,
      Map<String, String> jobProperties, Map<String, String> jobSecrets, Configuration conf) {

    if (StringUtils.isBlank(value)) {
      return;
    }
    String resolvedValue = resolveCredentialValue(catalogName, icebergKey, value, conf);

    if (jobProperties != null && !isSecretKey(icebergKey, conf)) {
      addNonSecretCredentialEntry(catalogName, scope, mapper, icebergKey, resolvedValue, jobProperties);
    }

    if (jobSecrets != null && isSecretKey(icebergKey, conf)) {
      addSecretCredentialEntry(scope, mapper, icebergKey, resolvedValue, jobSecrets);
    }
  }

  /**
   * Adds one non-secret vended value to {@code jobProperties} for Iceberg and Hadoop.
   *
   * <p>When {@code catalogName} is set, writes {@code iceberg.catalog.&lt;catalog&gt;.&lt;key&gt;}.
   * When a Hadoop mapper is present, also writes the matching provider-specific Hadoop key.
   */
  private static void addNonSecretCredentialEntry(String catalogName, String scope,
      HadoopMapper mapper, String icebergKey, String value,
      Map<String, String> jobProperties) {

    if (catalogName != null) {
      String catalogConfigKey =
          IcebergCatalogProperties.catalogPropertyConfigKey(catalogName, icebergKey);
      jobProperties.putIfAbsent(catalogConfigKey, value);
    }

    if (mapper != null && scope != null) {
      String hadoopKey = Support.toHadoopProperty(mapper, scope, icebergKey);
      if (hadoopKey != null) {
        jobProperties.putIfAbsent(hadoopKey, value);
      }
    }
  }

  /** Writes Hadoop keys only; Iceberg secrets are carried in the serialized blob. */
  private static void addSecretCredentialEntry(String scope, HadoopMapper mapper,
      String icebergKey, String value, Map<String, String> jobSecrets) {
    if (mapper != null && scope != null) {
      String hadoopSecretKey = Support.toHadoopProperty(mapper, scope, icebergKey);
      if (hadoopSecretKey != null) {
        jobSecrets.putIfAbsent(hadoopSecretKey, value);
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
        !RestCatalogAccessDelegation.requestsVendedCredentials(catalogName, conf);
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
    return RestCatalogAccessDelegation.requestsVendedCredentials(
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
    Map<String, String> ioProps = nonSecretFileIoProperties(io, conf);
    FileIO cleanIo = CatalogUtil.loadFileIO(io.getClass().getName(), ioProps, conf);

    return new BaseTable(
        new StaticTableOperations(metadata, cleanIo, table.locationProvider()), table.name());
  }

  private static Map<String, String> nonSecretFileIoProperties(FileIO io, Configuration conf) {
    Map<String, String> ioProps = new LinkedHashMap<>();
    try {
      io.properties().forEach((k, v) -> {
        if (!isSecretKey(k, conf)) {
          ioProps.put(k, v);
        }
      });
    } catch (UnsupportedOperationException ex) {
      // OSSFileIO does not expose catalog properties; nothing to strip before serialization.
    }
    return ioProps;
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

  /**
   * A vended config key is secret — routed to the Credentials channel, never job properties, and
   * stripped from the serialized table's FileIO — exactly when {@code hive.conf.hidden.list} covers
   * it. That is Hive's own registry of sensitive configuration (the same one behind
   * {@code HiveConf#isHiddenConfig}) that masks values in EXPLAIN, the Tez UI, and ATS.
   */
  private static boolean isSecretKey(String key, Configuration conf) {
    return HiveConfUtil.getHiddenSet(conf).stream().anyMatch(key::startsWith);
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
    return Support.credentialsFromFileIoProperties(table, io);
  }

  /**
   * REST catalogs vend credentials together with storage connectivity settings such as endpoint
   * and path-style access. These settings reflect the catalog's network view and may reference
   * hosts that are not reachable from Hive (for example, an internal {@code s3.ozone:9878}
   * hostname).
   *
   * Catalog properties configured in the Hive session (for example,
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
}
