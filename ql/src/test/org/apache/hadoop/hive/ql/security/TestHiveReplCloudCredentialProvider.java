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
package org.apache.hadoop.hive.ql.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TestHiveCredentialProviders;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.KeyStoreSpi;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHiveReplCloudCredentialProvider {

  private static final String HS2_CREDSTORE_PATH_STR =
    Constants.HIVE_REPL_CLOUD_SCHEME_NAME + "://file/path/to/hiveserver2/creds.localjceks";
  private static final String HDFS_CREDSTORE_PATH_STR =
    Constants.HIVE_REPL_CLOUD_KEYSTORE_TYPE + "://hdfs@source.com:8020/hive-replication/creds.jceks";
  private static final String HS2_CREDSTORE_PASSWORD = "hs2CredstorePassword";
  private static final String HDFS_CREDSTORE_PASSWORD = "hdfsCredstorePassword";
  private static final FsPermission FS_PERMISSION = FsPermission.createImmutable((short) 420);
  private static final String ACCESS_ALIAS = "fs.s3a.access.key";
  private static final String SECRET_ALIAS = "fs.s3a.secret.key";
  private static final String SOME_S3A_ACCESS_KEY = "some-s3a-access-key";
  private static final String SOME_S3A_SECRET_KEY = "some-s3a-secret-key";
  private static final String NON_EXISTING_ALIAS = "non.existing.alias";
  private static final String NON_EXISTING_KEY = "non-existing-key";

  @Rule
  public ExpectedException exception = ExpectedException.none();
  private Map<String, String> env;
  private FileSystem fs;
  private File hs2KeystoreFile;
  private KeyStore hs2Keystore;
  private KeyStoreSpi hs2KeystoreSpi;
  private List<String> hs2KeystoreAliases;
  private Path hdfsCredstorePath;
  private KeyStore hdfsKeystore;
  private KeyStoreSpi hdfsKeystoreSpi;
  private List<String> hdfsKeystoreAliases;
  private HiveReplCloudCredentialProvider credentialProvider;

  @Before
  public void setup() throws Exception {
    setEnvironmentVariables();
    mockFileSystem();
    mockHS2KeystoreFile();
    mockHS2Keystore();
    mockHdfsKeystore();
    createCredentialProvider();
  }

  private void setEnvironmentVariables() throws Exception {
    env = new HashMap<>();
    env.put(Constants.HADOOP_CREDENTIAL_PASSWORD_ENVVAR, HS2_CREDSTORE_PASSWORD);
    TestHiveCredentialProviders.setEnv(env);
  }

  private void mockFileSystem() throws IOException {
    fs = mock(FileSystem.class);
    hdfsCredstorePath = ProviderUtils.unnestUri(new Path(HDFS_CREDSTORE_PATH_STR).toUri());
    when(fs.exists(hdfsCredstorePath)).thenReturn(true);
    FileStatus hdfsCredstoreFileStatus = mock(FileStatus.class);
    when(hdfsCredstoreFileStatus.getPermission()).thenReturn(FS_PERMISSION);
    when(fs.getFileStatus(hdfsCredstorePath)).thenReturn(hdfsCredstoreFileStatus);
    when(fs.open(hdfsCredstorePath)).thenReturn(mock(FSDataInputStream.class));
  }

  private void mockHS2KeystoreFile() {
    hs2KeystoreFile = mock(File.class);
    when(hs2KeystoreFile.exists()).thenReturn(true);
    when(hs2KeystoreFile.length()).thenReturn(1024L);
  }

  private void mockHS2Keystore() throws Exception {
    hs2KeystoreAliases = new ArrayList<>();
    hs2KeystoreSpi = mock(KeyStoreSpi.class);
    hs2Keystore = createMockedKeystore(hs2KeystoreSpi);
    setCredstoreEntry(hs2KeystoreSpi,
                      Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PASSWORD, HDFS_CREDSTORE_PASSWORD);
    when(hs2KeystoreSpi.engineAliases()).thenReturn(Collections.enumeration(hs2KeystoreAliases));
  }

  private void mockHdfsKeystore() throws Exception {
    hdfsKeystoreAliases = new ArrayList<>();
    hdfsKeystoreSpi = mock(KeyStoreSpi.class);
    hdfsKeystore = createMockedKeystore(hdfsKeystoreSpi);
    setCredstoreEntry(hdfsKeystoreSpi, ACCESS_ALIAS, SOME_S3A_ACCESS_KEY);
    setCredstoreEntry(hdfsKeystoreSpi, SECRET_ALIAS, SOME_S3A_SECRET_KEY);
    when(hdfsKeystoreSpi.engineAliases()).thenReturn(Collections.enumeration(hdfsKeystoreAliases));
  }

  private KeyStore createMockedKeystore(KeyStoreSpi keyStoreSpi) throws Exception {
    KeyStore keyStoreMock = new KeyStore(keyStoreSpi, null, "test"){ };
    keyStoreMock.load(null);
    return keyStoreMock;
  }

  private void setCredstoreEntry(KeyStoreSpi keystoreSpi, String alias, String key)
      throws Exception {
    String password;
    if (keystoreSpi == hs2KeystoreSpi) {
      if (env.containsKey(Constants.HADOOP_CREDENTIAL_PASSWORD_ENVVAR)) {
        password = HS2_CREDSTORE_PASSWORD;
      } else {
        password = Constants.HADOOP_CREDENTIAL_PASSWORD_DEFAULT;
      }
      hs2KeystoreAliases.add(alias);
    } else if (keystoreSpi == hdfsKeystoreSpi) {
      password = HDFS_CREDSTORE_PASSWORD;
      hdfsKeystoreAliases.add(alias);
    } else {
      throw new IllegalArgumentException("Unknown keystoreSpi: " + keystoreSpi);
    }
    when(keystoreSpi.engineContainsAlias(alias)).thenReturn(true);
    SecretKeySpec keySpec = null;
    if (key != null) {
      keySpec = createKeySpec(key);
    }
    when(keystoreSpi.engineGetKey(alias, password.toCharArray())).thenReturn(keySpec);
  }

  private SecretKeySpec createKeySpec(String key) {
    return new SecretKeySpec(key.getBytes(), Constants.HIVE_REPL_CLOUD_ALGORITHM);
  }

  private void createCredentialProvider() throws IOException {
    URI hs2KeystoreURI = new Path(HS2_CREDSTORE_PATH_STR).toUri();
    HiveConf conf = new HiveConf();
    conf.set(Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PATH, HDFS_CREDSTORE_PATH_STR);
    conf.set(Constants.HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG, HS2_CREDSTORE_PATH_STR);
    Factory factory = new Factory();
    credentialProvider = spy(
        (HiveReplCloudCredentialProvider) factory.createProvider(hs2KeystoreURI, conf)
    );
    assertNotNull(credentialProvider);
  }

  @Test
  public void testFactoryForNonMatchingScheme() throws Exception {
    URI hdfsCredstoreUri = new URI(HDFS_CREDSTORE_PATH_STR);
    Factory factory = new Factory();
    CredentialProvider provider = factory.createProvider(hdfsCredstoreUri, new HiveConf());
    assertNull(provider);
  }

  @Test
  public void testCreationWithDefaultPassword() throws Exception {
    unsetEnvironmentVariables();
    mockHS2Keystore();
    createCredentialProvider();
  }

  private void unsetEnvironmentVariables() throws Exception {
    env = Collections.emptyMap();
    TestHiveCredentialProviders.setEnv(env);
  }

  @Test
  public void testHs2KeystoreNotExist() throws Exception {
    expectIOException();
    when(hs2KeystoreFile.exists()).thenReturn(false);
    createCredentialProvider();
  }

  @Test
  public void testHs2KeystoreEmpty() throws Exception {
    expectIOException();
    when(hs2KeystoreFile.length()).thenReturn(0L);
    createCredentialProvider();
  }

  @Test
  public void testHdfsPasswordNotFoundInHS2Keystore() throws Exception {
    expectIOException();
    setCredstoreEntry(hs2KeystoreSpi, Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PASSWORD, null);
    createCredentialProvider();
  }

  @Test
  public void testHdfsKeystoreNotExist() throws Exception {
    expectIOException();
    when(fs.exists(hdfsCredstorePath)).thenReturn(false);
    createCredentialProvider();
  }

  @Test
  public void testGetCredentialEntry() throws Exception {
    CredentialEntry accessKeyEntry = credentialProvider.getCredentialEntry(ACCESS_ALIAS);
    assertArrayEquals(SOME_S3A_ACCESS_KEY.toCharArray(), accessKeyEntry.getCredential());
    CredentialEntry secretKeyEntry = credentialProvider.getCredentialEntry(SECRET_ALIAS);
    assertArrayEquals(SOME_S3A_SECRET_KEY.toCharArray(), secretKeyEntry.getCredential());
    CredentialEntry unknownEntry = credentialProvider.getCredentialEntry(NON_EXISTING_ALIAS);
    assertNull(unknownEntry);
  }

  @Test
  public void testGetAliases() throws Exception {
    List<String> aliases = credentialProvider.getAliases();
    assertNotNull(aliases);
    assertEquals(2, aliases.size());
    assertTrue(aliases.contains(ACCESS_ALIAS));
    assertTrue(aliases.contains(SECRET_ALIAS));
  }

  @Test
  public void testCreateCredentialEntry() throws Exception {
    assertFalse(credentialProvider.isChanged());
    CredentialEntry credentialEntry = credentialProvider.createCredentialEntry(
        NON_EXISTING_ALIAS, NON_EXISTING_KEY.toCharArray()
    );
    assertTrue(credentialProvider.isChanged());
    assertEquals(NON_EXISTING_ALIAS, credentialEntry.getAlias());
    assertArrayEquals(NON_EXISTING_KEY.toCharArray(), credentialEntry.getCredential());
    verify(credentialProvider).setKeyEntry(NON_EXISTING_ALIAS, createKeySpec(NON_EXISTING_KEY),
                                           HDFS_CREDSTORE_PASSWORD.toCharArray(), null);
  }

  @Test
  public void testCreateAlreadyExistingCredentialEntry() throws IOException {
    expectIOException();
    credentialProvider.createCredentialEntry(ACCESS_ALIAS, SOME_S3A_ACCESS_KEY.toCharArray());
  }

  @Test
  public void testDeleteCredentialEntry() throws Exception {
    assertFalse(credentialProvider.isChanged());
    credentialProvider.deleteCredentialEntry(SECRET_ALIAS);
    assertTrue(credentialProvider.isChanged());
    verify(credentialProvider).deleteEntry(SECRET_ALIAS);
  }

  @Test
  public void testDeleteNonExistingCredentialEntry() throws IOException {
    expectIOException();
    credentialProvider.deleteCredentialEntry(NON_EXISTING_ALIAS);
  }

  @Test
  public void testFlush() throws Exception {
    testDeleteCredentialEntry();
    assertTrue(credentialProvider.isChanged());
    credentialProvider.flush();
    assertFalse(credentialProvider.isChanged());
    verify(credentialProvider).store(any(), eq(HDFS_CREDSTORE_PASSWORD.toCharArray()));
  }

  @Test
  public void testFlushWithoutChanged() throws Exception {
    assertFalse(credentialProvider.isChanged());
    credentialProvider.flush();
    assertFalse(credentialProvider.isChanged());
    verify(credentialProvider, never()).store(any(), eq(HDFS_CREDSTORE_PASSWORD.toCharArray()));
  }

  @Test
  public void testGetters() {
    assertEquals(Constants.HIVE_REPL_CLOUD_KEYSTORE_TYPE, credentialProvider.getKeyStoreType());
    assertEquals(Constants.HIVE_REPL_CLOUD_ALGORITHM, credentialProvider.getAlgorithm());
  }

  private void expectIOException() {
    exception.expect(IOException.class);
  }

  private class Factory extends HiveReplCloudCredentialProvider.Factory {
    @Override
    HiveReplCloudCredentialProvider createCredentialProviderInstance(URI providerName,
                                                                     Configuration conf)
        throws IOException {
      return new HiveReplCloudCredentialProviderMocked(providerName, conf);
    }
  }

  private class HiveReplCloudCredentialProviderMocked extends HiveReplCloudCredentialProvider {

    public HiveReplCloudCredentialProviderMocked(URI hs2KeystoreURI, Configuration conf)
        throws IOException {
      super(hs2KeystoreURI, conf);
    }

    @Override
    FileSystem createFileSystem(Configuration conf) throws IOException {
      return fs;
    }

    @Override
    File createHS2KeystoreFileInstance(Path hs2KeystorePath) throws URISyntaxException {
      return hs2KeystoreFile;
    }

    @Override
    KeyStore loadHS2Keystore(File hs2KeystoreFile, char[] hs2KeystorePassword)
        throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
      return hs2Keystore;
    }

    @Override
    KeyStore loadHdfsKeystore()
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
      return hdfsKeystore;
    }
  }

}