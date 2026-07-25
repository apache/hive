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

import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IcebergVendedCredentialUtil}: copying REST-vended storage credentials into Hive
 * job configuration at plan time and re-applying them on executors.
 */
public class TestIcebergVendedCredentialUtil {

  /**
   * Tests {@link IcebergVendedCredentialUtil#requestsVendedCredentials(Properties, Configuration)}.
   *
   * <p><b>Setup:</b> table properties name catalog {@code ice01}; session {@code conf} has no
   * delegation header, then gains {@code vended-credentials}.
   *
   * <p><b>Expects:</b> {@code false} without the header, {@code true} with it — propagation and
   * apply logic must not run for ordinary REST catalogs.
   *
   * <p><b>Why:</b> Iceberg REST only vends storage credentials when the client sends
   * {@code X-Iceberg-Access-Delegation: vended-credentials}; Hive mirrors that opt-in via catalog
   * config so static catalog keys are not replaced unnecessarily.
   */
  @Test
  public void requestsVendedCredentialsRequiresDelegationHeader() {
    Configuration conf = new Configuration();
    Properties props = new java.util.Properties();
    props.setProperty(InputFormatConfig.CATALOG_NAME, "ice01");

    assertThat(IcebergVendedCredentialUtil.requestsVendedCredentials(props, conf)).isFalse();

    conf.set(
        "iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation",
        "vended-credentials");
    assertThat(IcebergVendedCredentialUtil.requestsVendedCredentials(props, conf)).isTrue();
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#propagateToJob} when both {@code jobProperties} and
   * {@code jobSecrets} are non-null (full compile-time path after both storage-handler hooks).
   *
   * <p><b>Setup:</b> vended credential with internal endpoint {@code http://minio:9000}; session
   * {@code conf} sets {@code iceberg.catalog.ice01.s3.endpoint} to {@code http://host:9000}.
   *
   * <p><b>Expects:</b> endpoint and path-style in {@code jobProps} (Iceberg catalog keys and S3A
   * per-bucket keys) with host endpoint; access/secret and serialized blob in {@code jobSecrets}
   * only; blob endpoint also host; no secret material in {@code jobProps}.
   *
   * <p><b>Why:</b> HIVE-20651 keeps secrets in {@code TableDesc#jobSecrets} (later Hadoop
   * {@code Credentials}) and non-secrets in job conf. Catalogs often vend an internal S3 endpoint;
   * HS2 overrides with a reachable host endpoint at propagation time so Tez tasks and Iceberg FileIO
   * agree on connectivity while still using vended keys.
   */
  @Test
  public void propagateToJobMapsIcebergAndS3aProperties() {
    Configuration conf = new Configuration();
    conf.set("iceberg.catalog.ice01." + S3FileIOProperties.ENDPOINT, "http://host:9000");

    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(
                    S3FileIOProperties.ACCESS_KEY_ID, "access",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret",
                    S3FileIOProperties.ENDPOINT, "http://minio:9000",
                    S3FileIOProperties.PATH_STYLE_ACCESS, "true"))));

    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");
    Map<String, String> jobProps = Maps.newHashMap();
    Map<String, String> jobSecrets = Maps.newHashMap();

    IcebergVendedCredentialUtil.propagateToJob(table, "ice01", jobProps, jobSecrets, conf);

    assertThat(jobProps)
        .containsEntry(
            "iceberg.catalog.ice01." + S3FileIOProperties.ENDPOINT,
            "http://host:9000")
        .containsEntry(
            "fs.s3a.bucket.my-bucket.endpoint",
            "http://host:9000")
        .containsEntry(
            "fs.s3a.bucket.my-bucket.path.style.access",
            "true")
        .doesNotContainKey(InputFormatConfig.vendedCredentialsKey("db.t"))
        .doesNotContainKey("fs.s3a.bucket.my-bucket.access.key")
        .doesNotContainKey(
            "iceberg.catalog.ice01." + S3FileIOProperties.ACCESS_KEY_ID);

    assertThat(jobSecrets)
        .containsEntry("fs.s3a.bucket.my-bucket.access.key", "access")
        .containsEntry("fs.s3a.bucket.my-bucket.secret.key", "secret")
        .doesNotContainKey(
            "iceberg.catalog.ice01." + S3FileIOProperties.ACCESS_KEY_ID)
        .doesNotContainKey(
            "iceberg.catalog.ice01." + S3FileIOProperties.SECRET_ACCESS_KEY)
        .satisfies(map ->
            assertThat(map.get(InputFormatConfig.vendedCredentialsKey("db.t")))
                .isNotBlank());

    List<StorageCredential> serialized =
        SerializationUtil.deserializeFromBase64(
            jobSecrets.get(InputFormatConfig.vendedCredentialsKey("db.t")));

    assertThat(serialized.getFirst().prefix()).isEqualTo("s3://my-bucket/");
    assertThat(serialized.getFirst().config())
        .containsEntry(S3FileIOProperties.ENDPOINT, "http://host:9000")
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "secret")
        .containsEntry(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#propagateToJob} with {@code jobSecrets == null}.
   *
   * <p><b>Setup:</b> same mixed secret/non-secret credential as the full test; only
   * {@code jobProperties} map is passed (models {@code configureInputJobProperties} /
   * {@code overlayTableProperties}).
   *
   * <p><b>Expects:</b> overridden endpoint in {@code jobProps}; no serialized blob and no Iceberg
   * access-key property in {@code jobProps}.
   *
   * <p><b>Why:</b> Hive calls the job-properties hook without a secrets map. This hook must add
   * connectivity settings for S3A and Iceberg but must not place keys or the credential blob in
   * plain job properties — those are filled later by {@code configureInputJobCredentials}.
   */
  @Test
  public void propagateNonSecretsOnlyWhenJobSecretsNull() {
    Configuration conf = new Configuration();
    conf.set("iceberg.catalog.ice01." + S3FileIOProperties.ENDPOINT, "http://host:9000");

    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(
                    S3FileIOProperties.ACCESS_KEY_ID, "access",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret",
                    S3FileIOProperties.ENDPOINT, "http://minio:9000",
                    S3FileIOProperties.PATH_STYLE_ACCESS, "true"))));

    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");
    Map<String, String> jobProps = Maps.newHashMap();

    IcebergVendedCredentialUtil.propagateToJob(table, "ice01", jobProps, null, conf);

    assertThat(jobProps)
        .containsEntry(
            "iceberg.catalog.ice01." + S3FileIOProperties.ENDPOINT,
            "http://host:9000")
        .containsEntry("fs.s3a.bucket.my-bucket.endpoint", "http://host:9000")
        .containsEntry("fs.s3a.bucket.my-bucket.path.style.access", "true")
        .doesNotContainKey(InputFormatConfig.vendedCredentialsKey("db.t"))
        .doesNotContainKey(
            "iceberg.catalog.ice01." + S3FileIOProperties.ACCESS_KEY_ID);
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#extractCredentials(Table)} when
   * {@link SupportsStorageCredentials#credentials()} is empty.
   *
   * <p><b>Setup:</b> FileIO initialized with {@code s3.access-key-id} / {@code s3.secret-access-key}
   * in {@link FileIO#properties()} but an empty credential list.
   *
   * <p><b>Expects:</b> one synthetic {@link StorageCredential} with the access key from properties.
   *
   * <p><b>Why:</b> after {@code loadTable}, some REST clients populate {@code FileIO} properties
   * before the credential list; propagation must still find vended keys or plan/launch would skip
   * credential vending even though the table load succeeded.
   */
  @Test
  public void extractCredentialsFromFileIoPropertiesWhenCredentialListEmpty() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.initialize(
        Map.of(
            S3FileIOProperties.ACCESS_KEY_ID, "access",
            S3FileIOProperties.SECRET_ACCESS_KEY, "secret",
            S3FileIOProperties.ENDPOINT, "http://minio:9000"));
    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), "s3://my-bucket/warehouse/t", Map.of());
    Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "db.t");

    StorageCredential extracted = IcebergVendedCredentialUtil.extractCredentials(table).getFirst();
    assertThat(extracted.prefix()).isEqualTo("s3://my-bucket/");
    assertThat(extracted.config())
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "secret")
        .containsEntry(S3FileIOProperties.ENDPOINT, "http://minio:9000");
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#propagateToJob} with {@code jobProperties == null}.
   *
   * <p><b>Setup:</b> credential with access key and secret only; only {@code jobSecrets} map is
   * passed (models {@code configureInputJobCredentials} for input and output tables).
   *
   * <p><b>Expects:</b> serialized blob and {@code fs.s3a.bucket.*.access.key/secret.key} in
   * {@code jobSecrets}; no catalog-prefixed secret keys, no endpoint in {@code jobSecrets}.
   *
   * <p><b>Why:</b> the credentials hook receives only the secrets map. Propagation must still run
   * without NPE, write material for {@code PlanUtils#configureJobConf} to move into
   * {@code Credentials}, and avoid duplicating non-secrets or catalog-level secret keys that belong
   * in job properties or only in the serialized blob.
   */
  @Test
  public void propagateSecretsOnlyWhenJobPropertiesNull() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(S3FileIOProperties.ACCESS_KEY_ID, "access",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret"))));
    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");
    Map<String, String> jobSecrets = Maps.newHashMap();

    IcebergVendedCredentialUtil.propagateToJob(table, "ice01", null, jobSecrets, new Configuration());

    assertThat(jobSecrets)
        .containsEntry("fs.s3a.bucket.my-bucket.access.key", "access")
        .containsEntry("fs.s3a.bucket.my-bucket.secret.key", "secret")
        .doesNotContainKey(
            "iceberg.catalog.ice01." + S3FileIOProperties.ACCESS_KEY_ID)
        .doesNotContainKey(
            "iceberg.catalog.ice01." + S3FileIOProperties.SECRET_ACCESS_KEY)
        .doesNotContainKey("fs.s3a.bucket.my-bucket.endpoint")
        .satisfies(map ->
            assertThat(map.get(InputFormatConfig.vendedCredentialsKey("db.t")))
                .isNotBlank());

    List<StorageCredential> serialized =
        SerializationUtil.deserializeFromBase64(
            jobSecrets.get(InputFormatConfig.vendedCredentialsKey("db.t")));

    assertThat(serialized.getFirst().prefix()).isEqualTo("s3://my-bucket/");
    assertThat(serialized.getFirst().config())
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "secret");
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#applyFromJobConf} for a non-vending catalog.
   *
   * <p><b>Setup:</b> task {@code conf} has catalog name only — no serialized credentials blob and
   * no {@code vended-credentials} delegation header.
   *
   * <p><b>Expects:</b> FileIO credential list stays empty after apply.
   *
   * <p><b>Why:</b> {@link IcebergVendedCredentialUtil#shouldSkipApplyFromJobConf} must return early
   * so tables that use static catalog S3 configuration are not cleared or rewritten on executors.
   */
  @Test
  public void applyFromJobConfSkipsWhenVendedCredentialsNotRequested() {
    CredentialFileIO fileIO = new CredentialFileIO();
    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");

    Configuration conf = new Configuration();
    conf.set("iceberg.catalog", "ice01");

    IcebergVendedCredentialUtil.applyFromJobConf(table, conf);

    assertThat(fileIO.credentials()).isEmpty();
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#applyFromJobConf} endpoint override on an existing FileIO.
   *
   * <p><b>Setup:</b> FileIO already has vended credentials with {@code http://minio:9000}; task
   * {@code conf} requests vending and sets host endpoint {@code http://host:9000}.
   *
   * <p><b>Expects:</b> after apply, FileIO credential endpoint is {@code http://host:9000}.
   *
   * <p><b>Why:</b> HS2 may retain credentials from {@code loadTable} with the catalog-internal
   * endpoint; apply must re-merge session catalog S3 settings so commit paths and tasks use the
   * same reachable endpoint as plan-time propagation.
   */
  @Test
  public void applyFromJobConfOverridesEndpointOnExistingCredentials() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(
                    S3FileIOProperties.ACCESS_KEY_ID, "access",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret",
                    S3FileIOProperties.ENDPOINT, "http://minio:9000"))));
    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");

    Configuration conf = new Configuration();
    conf.set("iceberg.catalog", "ice01");
    conf.set(
        "iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation",
        "vended-credentials");
    conf.set("iceberg.catalog.ice01." + S3FileIOProperties.ENDPOINT, "http://host:9000");

    IcebergVendedCredentialUtil.applyFromJobConf(table, conf);

    StorageCredential applied =
        ((SupportsStorageCredentials) table.io()).credentials().getFirst();
    assertThat(applied.prefix()).isEqualTo("s3://my-bucket/");
    assertThat(applied.config())
        .containsEntry(S3FileIOProperties.ENDPOINT, "http://host:9000")
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "secret");
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#applyFromJobConf} on a task with an empty FileIO.
   *
   * <p><b>Setup:</b> {@code propagateToJob} fills job maps on HS2; keys are copied into a task
   * {@code Configuration} as on Tez/LLAP; executor table is deserialized with a fresh FileIO (no
   * credentials).
   *
   * <p><b>Expects:</b> after apply, executor FileIO has one credential with the vended access key.
   *
   * <p><b>Why:</b> executors do not re-run REST {@code loadTable}; they rebuild the table from job
   * conf. {@link IcebergVendedCredentialUtil#resolveCredentialsForApply} must read the serialized
   * blob (or S3A secret keys) from the task conf and call {@code setCredentials} so Iceberg I/O
   * can access S3 during the task.
   */
  @Test
  public void applyFromJobConfRestoresCredentialsOnExecutor() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(S3FileIOProperties.ACCESS_KEY_ID, "access",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret"))));
    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");

    Map<String, String> jobProps = Maps.newHashMap();
    Map<String, String> jobSecrets = Maps.newHashMap();
    IcebergVendedCredentialUtil.propagateToJob(
        table, "ice01", jobProps, jobSecrets, new Configuration());

    Configuration taskConf = new Configuration();
    jobProps.forEach(taskConf::set);
    jobSecrets.forEach(taskConf::set);

    CredentialFileIO executorIo = new CredentialFileIO();
    Table executorTable =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", executorIo), "db.t");
    IcebergVendedCredentialUtil.applyFromJobConf(executorTable, taskConf);

    assertThat(((SupportsStorageCredentials) executorTable.io()).credentials()).hasSize(1);
    StorageCredential applied =
        ((SupportsStorageCredentials) executorTable.io()).credentials().getFirst();
    assertThat(applied.prefix()).isEqualTo("s3://my-bucket/");
    assertThat(applied.config())
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "secret");
  }

  /**
   * Tests the full HIVE-20651 secret round trip: {@code propagateToJob} →
   * {@link org.apache.hadoop.hive.ql.plan.PlanUtils#configureJobConf} (secrets → Hadoop
   * {@code Credentials}, cleared from the TableDesc) → task UGI →
   * {@code Utilities#copyJobSecretToTableProperties} → {@code Utilities#copyTablePropertiesToConf}
   * → {@link IcebergVendedCredentialUtil#applyFromJobConf}.
   *
   * <p><b>Setup:</b> the exact chain Hive runs — HS2 fills {@code TableDesc#jobSecrets}, PlanUtils
   * moves them to the job {@code Credentials} (asserted: nothing lands in the plain JobConf), the
   * task restores them into table properties and copies them into the task-local conf the same way
   * {@code MapRecordProcessor}/{@code FileSinkOperator} do.
   *
   * <p><b>Expects:</b> the executor FileIO receives the vended credential.
   *
   * <p><b>Why:</b> this is the only sanctioned transport for secrets — they must survive the real
   * restore path, including {@code copyTablePropertiesToConf}'s {@code escapeJava}, which corrupts
   * MIME-wrapped base64 (CR/LF become literal {@code \r\n} whose {@code r}/{@code n} bytes are
   * valid base64 alphabet). The blob must therefore be single-line base64.
   */
  @Test
  public void applyFromJobConfRestoresCredentialsViaCredentialsChannel() throws Exception {
    CredentialFileIO hs2Io = new CredentialFileIO();
    hs2Io.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(S3FileIOProperties.ACCESS_KEY_ID, "access",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret"))));
    Table hs2Table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", hs2Io), "db.t");

    TableDesc tableDesc = new TableDesc();
    Properties tableProps = new Properties();
    tableProps.setProperty(hive_metastoreConstants.META_TABLE_NAME, "db.t");
    tableDesc.setProperties(tableProps);

    Map<String, String> jobSecrets = Maps.newHashMap();
    IcebergVendedCredentialUtil.propagateToJob(hs2Table, "ice01", null, jobSecrets, new Configuration());
    tableDesc.setJobSecrets(jobSecrets);

    JobConf jobConf = new JobConf();
    PlanUtils.configureJobConf(tableDesc, jobConf);
    assertThat(jobConf.get(InputFormatConfig.vendedCredentialsKey("db.t"))).isNull();
    assertThat(tableDesc.getJobSecrets()).isEmpty();

    UserGroupInformation taskUgi = UserGroupInformation.createRemoteUser("task-user");
    taskUgi.addCredentials(jobConf.getCredentials());

    JobConf taskConf = new JobConf();
    taskConf.set(InputFormatConfig.CATALOG_NAME, "ice01");
    taskConf.set(
        "iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation",
        "vended-credentials");

    CredentialFileIO executorIo = new CredentialFileIO();
    Table executorTable =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", executorIo), "db.t");

    taskUgi.doAs((PrivilegedExceptionAction<Void>) () -> {
      Utilities.copyJobSecretToTableProperties(tableDesc);
      Utilities.copyTablePropertiesToConf(tableDesc, taskConf);
      IcebergVendedCredentialUtil.applyFromJobConf(executorTable, taskConf);
      return null;
    });

    assertThat(executorIo.credentials()).hasSize(1);
    StorageCredential applied = executorIo.credentials().getFirst();
    assertThat(applied.prefix()).isEqualTo("s3://my-bucket/");
    assertThat(applied.config())
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "secret");
  }

  /**
   * Tests {@link HiveIcebergStorageHandler#configureJobConf} secret hygiene.
   *
   * <p><b>Setup:</b> vending-enabled catalog; {@code TableDesc#jobSecrets} already holds the
   * serialized blob and a per-bucket S3A secret key (filled at compile time by
   * {@code configureInputJobCredentials}).
   *
   * <p><b>Expects:</b> after {@code configureJobConf}, neither the blob nor the S3A secret key
   * appears in the {@code JobConf}; the jobSecrets map is left for
   * {@code PlanUtils#configureJobConf} to move into Hadoop {@code Credentials}.
   *
   * <p><b>Why:</b> {@code TezTask} strips {@code hive.conf.hidden.list} entries <em>before</em>
   * storage handlers run ({@code createConfiguration} precedes
   * {@code configureJobConfAndExtractJars}), so any secret written to the JobConf here ships in
   * plaintext with the DAG plan. Secrets may only travel via the Credentials channel.
   */
  @Test
  public void configureJobConfKeepsSecretsOutOfJobConf() {
    Configuration conf = new Configuration();
    conf.set(
        "iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation",
        "vended-credentials");
    HiveIcebergStorageHandler handler = new HiveIcebergStorageHandler();
    handler.setConf(conf);

    Properties props = new Properties();
    props.setProperty(InputFormatConfig.CATALOG_NAME, "ice01");
    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(props);

    String secretS3aKey = "fs.s3a.bucket.my-bucket.secret.key";
    Map<String, String> jobSecrets = Maps.newHashMap();
    jobSecrets.put(InputFormatConfig.vendedCredentialsKey("db.t"), "blob");
    jobSecrets.put(secretS3aKey, "secret");
    tableDesc.setJobSecrets(jobSecrets);

    JobConf jobConf = new JobConf();
    jobConf.setBoolean(HiveConf.ConfVars.HIVE_IN_TEST_IDE.varname, true);

    handler.configureJobConf(tableDesc, jobConf);

    assertThat(jobConf.get(InputFormatConfig.vendedCredentialsKey("db.t"))).isNull();
    assertThat(jobConf.get(secretS3aKey)).isNull();
    assertThat(tableDesc.getJobSecrets())
        .containsEntry(InputFormatConfig.vendedCredentialsKey("db.t"), "blob")
        .containsEntry(secretS3aKey, "secret");
  }

  /**
   * Tests per-table credential resolution when two vended tables publish into one configuration.
   *
   * <p><b>Setup:</b> REST catalogs mint credentials per table; two tables on different buckets go
   * through the full chain — {@code propagateToJob} → {@code PlanUtils#configureJobConf} → one
   * task UGI → per-table restore — and both publish into a <em>shared</em> conf the way merged
   * map-works do. {@code Utilities#copyTableJobPropertiesToConf} is skip-if-present for table
   * properties, so with a single global blob key the second table would read the first table's
   * credentials.
   *
   * <p><b>Expects:</b> each executor table's FileIO receives the credential vended for it —
   * table 2 gets the bucket-b credential, not bucket-a's.
   *
   * <p><b>Why:</b> the blob key must be table-scoped (like {@code iceberg.mr.serialized.table.*})
   * for multi-table queries; key collisions would silently apply another table's credentials.
   */
  @Test
  public void applyFromJobConfResolvesPerTableCredentialsInSharedConf() throws Exception {
    TableDesc tableDesc1 = tableDescNamed("db.t1");
    Map<String, String> secrets1 = Maps.newHashMap();
    IcebergVendedCredentialUtil.propagateToJob(
        tableWithCredential("s3://bucket-a/", "access-a", "db.t1"), "ice01", null, secrets1, new Configuration());
    tableDesc1.setJobSecrets(secrets1);

    TableDesc tableDesc2 = tableDescNamed("db.t2");
    Map<String, String> secrets2 = Maps.newHashMap();
    IcebergVendedCredentialUtil.propagateToJob(
        tableWithCredential("s3://bucket-b/", "access-b", "db.t2"), "ice01", null, secrets2, new Configuration());
    tableDesc2.setJobSecrets(secrets2);

    JobConf jobConf = new JobConf();
    PlanUtils.configureJobConf(tableDesc1, jobConf);
    PlanUtils.configureJobConf(tableDesc2, jobConf);

    UserGroupInformation taskUgi = UserGroupInformation.createRemoteUser("task-user");
    taskUgi.addCredentials(jobConf.getCredentials());

    JobConf sharedConf = new JobConf();
    sharedConf.set(InputFormatConfig.CATALOG_NAME, "ice01");
    sharedConf.set(
        "iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation",
        "vended-credentials");

    CredentialFileIO executorIo1 = new CredentialFileIO();
    Table executorTable1 =
        new BaseTable(new StaticTableOperations("s3://bucket-a/t1", executorIo1), "db.t1");
    CredentialFileIO executorIo2 = new CredentialFileIO();
    Table executorTable2 =
        new BaseTable(new StaticTableOperations("s3://bucket-b/t2", executorIo2), "db.t2");

    taskUgi.doAs((PrivilegedExceptionAction<Void>) () -> {
      Utilities.copyJobSecretToTableProperties(tableDesc1);
      Utilities.copyJobSecretToTableProperties(tableDesc2);
      Utilities.copyTableJobPropertiesToConf(tableDesc1, sharedConf);
      Utilities.copyTableJobPropertiesToConf(tableDesc2, sharedConf);
      IcebergVendedCredentialUtil.applyFromJobConf(executorTable1, sharedConf);
      IcebergVendedCredentialUtil.applyFromJobConf(executorTable2, sharedConf);
      return null;
    });

    assertThat(executorIo1.credentials()).hasSize(1);
    assertThat(executorIo1.credentials().getFirst().prefix()).isEqualTo("s3://bucket-a/");
    assertThat(executorIo1.credentials().getFirst().config())
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access-a");

    assertThat(executorIo2.credentials()).hasSize(1);
    assertThat(executorIo2.credentials().getFirst().prefix()).isEqualTo("s3://bucket-b/");
    assertThat(executorIo2.credentials().getFirst().config())
        .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "access-b");
  }

  private static TableDesc tableDescNamed(String tableName) {
    Properties tableProps = new Properties();
    tableProps.setProperty(hive_metastoreConstants.META_TABLE_NAME, tableName);
    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(tableProps);
    return tableDesc;
  }

  private static Table tableWithCredential(String prefix, String accessKey, String tableName) {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                prefix,
                Map.of(S3FileIOProperties.ACCESS_KEY_ID, accessKey,
                    S3FileIOProperties.SECRET_ACCESS_KEY, accessKey + "-secret"))));
    return new BaseTable(new StaticTableOperations(prefix + "t", fileIO), tableName);
  }

  /**
   * Tests the strip → ship → setCredentials flow for vended catalogs.
   *
   * <p><b>Setup:</b> HS2-side table with a credentialed FileIO (secret in the credential list and
   * in the FileIO properties) over a real metadata file on local disk. {@code secretFreeCopy}
   * rebuilds the table over a fresh FileIO keeping only allowlisted non-secret properties before
   * {@code serializeTable}; credentials travel separately via {@code propagateToJob} (modelling
   * the HIVE-20651 restore); {@code HiveTableUtil#deserializeTable} restores them on the
   * deserialized FileIO.
   *
   * <p><b>Expects:</b> the serialized table carries no secret bytes; the deserialized table has
   * the original qualified name, schema, FileIO class with non-secret properties only, and the
   * vended credentials restored from the Credentials-channel key.
   *
   * <p><b>Why:</b> a serialized table embeds its FileIO, so the copy shipped through job
   * properties must carry a FileIO with no secret state; executors re-attach the credentials
   * through {@code SupportsStorageCredentials#setCredentials}.
   */
  @Test
  public void vendedSerializedTableShipsSecretFreeAndRestoresCredentials() throws Exception {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.initialize(
        Map.of(
            S3FileIOProperties.SECRET_ACCESS_KEY, "prop-secret-11bb",
            S3FileIOProperties.ENDPOINT, "http://minio:9000"));
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(S3FileIOProperties.ACCESS_KEY_ID, "access",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "cred-secret-11bb"))));

    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), "s3://my-bucket/warehouse/t", Map.of());
    String metadataLocation = "file://" +
        java.nio.file.Files.createTempDirectory("iceberg-strip-test").resolve("v1.metadata.json");
    TableMetadataParser.write(metadata, fileIO.newOutputFile(metadataLocation));
    Table hs2Table = new BaseTable(new StaticTableOperations(metadataLocation, fileIO), "ice01.db.t");

    Configuration conf = new Configuration();
    String serialized = HiveTableUtil.serializeTable(
        IcebergVendedCredentialUtil.secretFreeCopy(hs2Table, conf), conf, new Properties(), null);

    String rawBytes = new String(
        java.util.Base64.getMimeDecoder().decode(serialized), StandardCharsets.ISO_8859_1);
    assertThat(rawBytes)
        .doesNotContain("prop-secret-11bb")
        .doesNotContain("cred-secret-11bb")
        .contains("http://minio:9000");

    Map<String, String> jobSecrets = Maps.newHashMap();
    IcebergVendedCredentialUtil.propagateToJob(hs2Table, "ice01", null, jobSecrets, new Configuration());

    JobConf taskConf = new JobConf();
    taskConf.set(InputFormatConfig.SERIALIZED_TABLE_PREFIX + "db.t", serialized);
    jobSecrets.forEach(taskConf::set);
    taskConf.set(InputFormatConfig.CATALOG_NAME, "ice01");

    Table executorTable = HiveTableUtil.deserializeTable(taskConf, "db.t");

    assertThat(executorTable).isNotNull();
    assertThat(executorTable.name()).isEqualTo("ice01.db.t");
    assertThat(executorTable.schema().asStruct()).isEqualTo(schema.asStruct());
    assertThat(executorTable.io()).isInstanceOf(CredentialFileIO.class);
    assertThat(executorTable.io().properties())
        .containsEntry(S3FileIOProperties.ENDPOINT, "http://minio:9000")
        .doesNotContainKey(S3FileIOProperties.SECRET_ACCESS_KEY);
    assertThat(((SupportsStorageCredentials) executorTable.io()).credentials()).hasSize(1);
  }

  /**
   * Tests the strip → ship → setCredentials flow for metadata tables ({@code db.t.files}-style
   * queries).
   *
   * <p><b>Setup:</b> a {@code FILES} metadata table over a credentialed base; the secret-free
   * copy is serialized and the table restored through {@code HiveTableUtil#deserializeTable}.
   *
   * <p><b>Expects:</b> the serialized bytes carry no secrets; the deserialized table keeps the
   * metadata-table name and schema over a secret-free FileIO with the credentials restored.
   *
   * <p><b>Why:</b> metadata tables share the base table FileIO and would leak the same way; the
   * secret-free copy must recreate the metadata-table view over the clean FileIO.
   */
  @Test
  public void vendedMetadataTableSerializesSecretFreeAndRestoresCredentials() throws Exception {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.initialize(Map.of(S3FileIOProperties.SECRET_ACCESS_KEY, "prop-secret-3fd2"));
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(S3FileIOProperties.SECRET_ACCESS_KEY, "cred-secret-3fd2"))));

    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), "s3://my-bucket/warehouse/t", Map.of());
    String metadataLocation = "file://" +
        java.nio.file.Files.createTempDirectory("iceberg-strip-meta-test").resolve("v1.metadata.json");
    TableMetadataParser.write(metadata, fileIO.newOutputFile(metadataLocation));
    Table baseTable = new BaseTable(new StaticTableOperations(metadataLocation, fileIO), "ice01.db.t");
    Table filesTable = MetadataTableUtils.createMetadataTableInstance(baseTable, MetadataTableType.FILES);

    Configuration conf = new Configuration();
    String serialized = HiveTableUtil.serializeTable(
        IcebergVendedCredentialUtil.secretFreeCopy(filesTable, conf), conf, new Properties(), null);

    String rawBytes = new String(
        java.util.Base64.getMimeDecoder().decode(serialized), StandardCharsets.ISO_8859_1);
    assertThat(rawBytes).doesNotContain("cred-secret-3fd2").doesNotContain("prop-secret-3fd2");

    Map<String, String> jobSecrets = Maps.newHashMap();
    IcebergVendedCredentialUtil.propagateToJob(filesTable, "ice01", null, jobSecrets, new Configuration());

    JobConf taskConf = new JobConf();
    taskConf.set(InputFormatConfig.SERIALIZED_TABLE_PREFIX + "db.t.files", serialized);
    jobSecrets.forEach(taskConf::set);
    taskConf.set(InputFormatConfig.CATALOG_NAME, "ice01");

    Table executorTable = HiveTableUtil.deserializeTable(taskConf, "db.t.files");

    assertThat(executorTable).isNotNull();
    assertThat(executorTable.name()).isEqualTo(filesTable.name());
    assertThat(executorTable.schema().asStruct()).isEqualTo(filesTable.schema().asStruct());
    assertThat(executorTable.io().properties())
        .doesNotContainKey(S3FileIOProperties.SECRET_ACCESS_KEY);
    assertThat(((SupportsStorageCredentials) executorTable.io()).credentials()).hasSize(1);
  }

  /**
   * Tests the HS2 commit-path variant: {@code collectOutputs} resolves the catalog name per table
   * (the job-level {@code CATALOG_NAME} is not set in the DAG conf) and session endpoint overrides
   * must still reach the live table's FileIO through {@code setCredentials}.
   */
  @Test
  public void applyFromJobConfWithExplicitCatalogAppliesEndpointOverride() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(S3FileIOProperties.ACCESS_KEY_ID, "commit-access",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "commit-secret-4fa1",
                    S3FileIOProperties.ENDPOINT, "http://minio:9000"))));
    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), "s3://my-bucket/warehouse/t", Map.of());
    Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "ice01.db.t");

    JobConf jobConf = new JobConf();
    jobConf.set("iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation", "vended-credentials");
    jobConf.set("iceberg.catalog.ice01." + S3FileIOProperties.ENDPOINT, "http://localhost:9100");

    IcebergVendedCredentialUtil.applyFromJobConf(table, "ice01", jobConf);

    assertThat(fileIO.credentials()).hasSize(1);
    assertThat(fileIO.credentials().get(0).config())
        .containsEntry(S3FileIOProperties.ENDPOINT, "http://localhost:9100")
        .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "commit-secret-4fa1");
  }

  /**
   * Pins the leak that makes the secret-free copy before serialization necessary.
   *
   * <p><b>Setup:</b> {@code SerializableTable.copyOf} stores a <em>reference</em> to the live
   * table's FileIO in a non-transient field, and Java serialization walks it — so serializing an
   * unsanitized copy embeds the FileIO's credential list and secret properties in the blob. The
   * stub mirrors {@code S3FileIO}'s serialized shape: {@code properties} and
   * {@code storageCredentials} are both non-transient there too (the real implementation is
   * covered by the Gravitino qtest).
   *
   * <p><b>Expects:</b> secrets ARE present in the raw serialized bytes when no sanitization runs.
   *
   * <p><b>Why:</b> guards the premise. If Iceberg ever stops serializing FileIO secret state,
   * this test fails and serializing tables as-is becomes safe again.
   */
  @Test
  public void serializableTableCopyLeaksIoSecretsWithoutSanitization() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.initialize(Map.of(S3FileIOProperties.SECRET_ACCESS_KEY, "prop-secret-77aa"));
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(S3FileIOProperties.ACCESS_KEY_ID, "cred-access-77aa",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "cred-secret-77aa"))));
    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), "s3://my-bucket/warehouse/t", Map.of());
    Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "db.t");

    String unsanitized = SerializationUtil.serializeToBase64(SerializableTable.copyOf(table));

    String rawBytes = new String(
        java.util.Base64.getMimeDecoder().decode(unsanitized), StandardCharsets.ISO_8859_1);
    assertThat(rawBytes)
        .contains("cred-access-77aa")
        .contains("cred-secret-77aa")
        .contains("prop-secret-77aa");
  }

  /** Public with a no-arg constructor: the secret-free copy instantiates it reflectively via
   * {@code CatalogUtil.loadFileIO}. File operations delegate to local Hadoop IO so metadata
   * files can be written and read in tests. */
  public static class CredentialFileIO implements FileIO, SupportsStorageCredentials {
    private List<StorageCredential> credentials = List.of();
    private Map<String, String> properties = Map.of();
    private transient HadoopFileIO localFiles;

    @Override
    public void initialize(Map<String, String> props) {
      this.properties = props;
    }

    private HadoopFileIO localFiles() {
      if (localFiles == null) {
        localFiles = new HadoopFileIO(new Configuration());
      }
      return localFiles;
    }

    @Override
    public org.apache.iceberg.io.InputFile newInputFile(String path) {
      return localFiles().newInputFile(path);
    }

    @Override
    public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
      return localFiles().newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      localFiles().deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public void close() {
      // No-op: test stub does not hold resources.
    }

    @Override
    public List<StorageCredential> credentials() {
      return credentials;
    }

    @Override
    public void setCredentials(List<StorageCredential> creds) {
      this.credentials = creds;
    }
  }
}
