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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Credential provider for replicating to the cloud.
 * It works the following way:
 * <ol>
 *   <li>
 *     Load the HS2 keystore file
 *   </li>
 *   <li>
 *     Gets a password from the HS2 keystore file
 *   </li>
 *   <li>
 *     This password will be used to load another keystore file, located on HDFS,
 *     that contains the cloud credentials for the Hive cloud replication
 *   </li>
 * </ol>
 */
@InterfaceAudience.Private
public class HiveReplCloudCredentialProvider extends CredentialProvider {

  private static final Logger LOG = LoggerFactory.getLogger(HiveReplCloudCredentialProvider.class);

  private final Path path;
  private final FileSystem fs;
  private final char[] keystorePassword;
  private final KeyStore keystore;
  private FsPermission permissions;
  private final Lock readLock;
  private final Lock writeLock;
  private boolean changed = false;

  public HiveReplCloudCredentialProvider(URI hs2KeystoreURI, Configuration conf) throws IOException {
    super();
    this.path = getKeystorePath(conf);
    this.fs = createFileSystem(conf);
    this.keystorePassword = tryLoadKeystorePasswordFromHS2Keystore(hs2KeystoreURI, conf);
    this.keystore = tryLoadHdfsKeystore();
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
  }

  private Path getKeystorePath(Configuration conf) {
    String keystorePathStr = conf.get(Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PATH);
    URI keystoreUri = new Path(keystorePathStr).toUri();
    LOG.debug("Hive cloud replication keystore URI is {}", keystoreUri);
    return ProviderUtils.unnestUri(keystoreUri);
  }

  @VisibleForTesting
  FileSystem createFileSystem(Configuration conf) throws IOException {
    return path.getFileSystem(conf);
  }

  private char[] tryLoadKeystorePasswordFromHS2Keystore(URI hs2KeystoreURI, Configuration conf)
      throws IOException {
    try {
      char[] hs2KeystorePassword = getHS2KeystorePassword(conf);
      KeyStore hs2Keystore = tryLoadHS2Keystore(hs2KeystoreURI, hs2KeystorePassword);
      return getKeystorePassword(hs2Keystore, hs2KeystorePassword);
    } catch (KeyStoreException e) {
      throw new IOException("Can't create keystore", e);
    } catch (GeneralSecurityException e) {
      throw new IOException("Can't load keystore " + hs2KeystoreURI, e);
    } catch (URISyntaxException e) {
      throw new IOException("URI Syntax error: " + hs2KeystoreURI, e);
    }
  }

  private char[] getHS2KeystorePassword(Configuration conf) throws IOException {
    LOG.debug("Getting HS2 keystore password");
    char[] hs2KeystorePassword = ProviderUtils.locatePassword(
        Constants.HADOOP_CREDENTIAL_PASSWORD_ENVVAR,
        conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_CREDENTIAL_PASSWORD_FILE_KEY)
    );
    if (hs2KeystorePassword == null) {
      hs2KeystorePassword = Constants.HADOOP_CREDENTIAL_PASSWORD_DEFAULT.toCharArray();
      LOG.debug("Using default HS2 keystore password");
    }
    return hs2KeystorePassword;
  }

  private KeyStore tryLoadHS2Keystore(URI hs2KeystoreURI, char[] hs2KeystorePassword)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
      URISyntaxException {
    Path hs2KeystorePath = ProviderUtils.unnestUri(hs2KeystoreURI);
    File hs2KeystoreFile = createHS2KeystoreFileInstance(hs2KeystorePath);
    if (hs2KeystoreFile.exists() && hs2KeystoreFile.length() > 0) {
      return loadHS2Keystore(hs2KeystoreFile, hs2KeystorePassword);
    } else {
      throw new IOException("HS2 keystore does not exist under: " + hs2KeystoreURI);
    }
  }

  @VisibleForTesting
  KeyStore loadHS2Keystore(File hs2KeystoreFile, char[] hs2KeystorePassword)
      throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
    LOG.debug("Start loading HS2 keystore: {}", hs2KeystoreFile.getPath());
    KeyStore hs2Keystore = KeyStore.getInstance(getKeyStoreType());
    try (InputStream in = Files.newInputStream(hs2KeystoreFile.toPath())) {
      hs2Keystore.load(in, hs2KeystorePassword);
    }
    LOG.debug("Finished loading HS2 keystore: {}", hs2KeystoreFile.getPath());
    return hs2Keystore;
  }

  @VisibleForTesting
  File createHS2KeystoreFileInstance(Path hs2KeystorePath) throws URISyntaxException {
    return new File(new URI(hs2KeystorePath.toString()));
  }

  private char[] getKeystorePassword(KeyStore hs2Keystore, char[] hs2KeystorePassword)
      throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException, IOException {
    LOG.debug("Getting Hive cloud replication Keystore password " +
              Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PASSWORD + " from HS2 keystore");
    Key keystorePasswordKey = hs2Keystore.getKey(
        Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PASSWORD, hs2KeystorePassword
    );
    if (keystorePasswordKey == null) {
      throw new IOException("Hive cloud replication keystore password " +
                            Constants.HIVE_REPL_CLOUD_CREDENTIAL_PROVIDER_PASSWORD +
                            " cannot be found in HS2 keystore!");
    }
    return bytesToChars(keystorePasswordKey);
  }

  private KeyStore tryLoadHdfsKeystore() throws IOException {
    try {
      if (fs.exists(getPath())) {
        stashOriginalFilePermissions();
        return loadHdfsKeystore();
      } else {
        throw new IOException("Hive cloud replication keystore does not exist: " + getPathStr());
      }
    } catch (KeyStoreException e) {
      throw new IOException("Can't create keystore", e);
    } catch (GeneralSecurityException e) {
      throw new IOException("Can't load keystore " + getPathStr(), e);
    }
  }

  /**
   * Save off permissions in case we need to rewrite the keystore in {@link #flush()}
   * @throws IOException propagated from {@link FileSystem#getFileStatus(Path)}.
   */
  private void stashOriginalFilePermissions() throws IOException {
    LOG.debug("Start stashing HDFS file permissions: {}", getPath());
    FileStatus fileStatus = fs.getFileStatus(getPath());
    permissions = fileStatus.getPermission();
    LOG.debug("Finished stashing HDFS file permissions: {}", getPath());
  }

  @VisibleForTesting
  KeyStore loadHdfsKeystore()
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    LOG.debug("Start loading HDFS keystore: {}", getPath());
    KeyStore hdfsKeystore = KeyStore.getInstance(getKeyStoreType());
    try (InputStream in = fs.open(getPath())) {
      hdfsKeystore.load(in, keystorePassword);
    }
    LOG.debug("Finished loading HDFS keystore: {}", getPath());
    return hdfsKeystore;
  }

  protected String getKeyStoreType() {
    return Constants.HIVE_REPL_CLOUD_KEYSTORE_TYPE;
  }

  protected String getAlgorithm() {
    return Constants.HIVE_REPL_CLOUD_ALGORITHM;
  }

  @Override
  public CredentialEntry getCredentialEntry(String alias) throws IOException {
    readLock.lock();
    try {
      if (!keystore.containsAlias(alias)) {
        return null;
      }
      SecretKeySpec key = (SecretKeySpec) keystore.getKey(alias, keystorePassword);
      return new HiveCloudReplCredentialEntry(alias, bytesToChars(key));
    } catch (KeyStoreException e) {
      throw new IOException("Can't get credential " + alias + " from " + getPathStr(), e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can't get algorithm for credential " + alias + " from " + getPathStr(), e);
    } catch (UnrecoverableKeyException e) {
      throw new IOException("Can't recover credential " + alias + " from " + getPathStr(), e);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getAliases() throws IOException {
    readLock.lock();
    try {
      return Collections.list(keystore.aliases());
    } catch (KeyStoreException e) {
      throw new IOException("Can't get aliases from " + getPathStr(), e);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public CredentialEntry createCredentialEntry(String alias, char[] credential) throws IOException {
    writeLock.lock();
    try {
      if (keystore.containsAlias(alias)) {
        throw new IOException("Credential " + alias + " already exists in " + getPathStr());
      }
      SecretKeySpec key = new SecretKeySpec(charsToBytes(credential), getAlgorithm());
      setKeyEntry(alias, key, keystorePassword, null);
      changed = true;
      return new HiveCloudReplCredentialEntry(alias, credential);
    } catch (KeyStoreException e) {
      throw new IOException("Problem looking up credential " + alias + " in " + getPathStr(), e);
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  void setKeyEntry(String alias, SecretKeySpec key, char[] password, Certificate[] cert)
      throws KeyStoreException {
    keystore.setKeyEntry(alias, key, password, cert);
  }

  @Override
  public void deleteCredentialEntry(String alias) throws IOException {
    writeLock.lock();
    try {
      if (keystore.containsAlias(alias)) {
        deleteEntry(alias);
        changed = true;
      } else {
        throw new IOException("Credential " + alias + " does not exist in " + getPathStr());
      }
    } catch (KeyStoreException e) {
      throw new IOException("Problem removing " + alias + " from " + getPathStr(), e);
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  void deleteEntry(String alias) throws KeyStoreException {
    keystore.deleteEntry(alias);
  }

  @Override
  public void flush() throws IOException {
    writeLock.lock();
    try {
      if (!changed) {
        LOG.debug("Keystore hasn't changed, returning.");
        return;
      }
      LOG.debug("Start flushing keystore: {}", getPathStr());
      try (OutputStream out = FileSystem.create(fs, getPath(), permissions)) {
        store(out, keystorePassword);
      } catch (KeyStoreException e) {
        throw new IOException("Can't store keystore " + getPathStr(), e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("No such algorithm storing keystore " + getPathStr(), e);
      } catch (CertificateException e) {
        throw new IOException("Certificate exception storing keystore " + getPathStr(), e);
      }
      LOG.debug("Finished flushing keystore: {}", getPathStr());
      changed = false;
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  void store(OutputStream out, char[] keystorePassword)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    keystore.store(out, keystorePassword);
  }

  public static byte[] charsToBytes(char[] credential) {
    return new String(credential).getBytes(StandardCharsets.UTF_8);
  }

  public static char[] bytesToChars(Key key) {
    return bytesToChars(key.getEncoded());
  }

  public static char[] bytesToChars(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8).toCharArray();
  }

  public Path getPath() {
    return path;
  }

  public String getPathStr() {
    return getPath().toString();
  }

  public boolean isChanged() {
    return changed;
  }

  public static class HiveCloudReplCredentialEntry extends CredentialProvider.CredentialEntry {
    protected HiveCloudReplCredentialEntry(String alias, char[] credential) {
      super(alias, credential);
    }
  }

  public static class Factory extends CredentialProviderFactory {
    @Override
    public CredentialProvider createProvider(URI providerName, Configuration conf)
        throws IOException {
      if (Constants.HIVE_REPL_CLOUD_SCHEME_NAME.equals(providerName.getScheme())) {
        return createCredentialProviderInstance(providerName, conf);
      }
      return null;
    }

    @VisibleForTesting
    HiveReplCloudCredentialProvider createCredentialProviderInstance(
        URI providerName, Configuration conf) throws IOException {
      return new HiveReplCloudCredentialProvider(providerName, conf);
    }
  }

}
