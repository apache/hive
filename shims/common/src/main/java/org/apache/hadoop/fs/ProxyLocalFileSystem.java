/**
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

package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.MD5Hash;

/****************************************************************
 * A Proxy for LocalFileSystem
 *
 * As an example, it serves uri's corresponding to: 'pfile:///' namespace with using a
 * LocalFileSystem
 *****************************************************************/

public class ProxyLocalFileSystem extends FilterFileSystem {

  protected LocalFileSystem localFs;

  /**
   * URI scheme
   */
  private String scheme;

  public ProxyLocalFileSystem() {
    localFs = new LocalFileSystem();
  }

  public ProxyLocalFileSystem(FileSystem fs) {
    throw new RuntimeException("Unsupported Constructor");
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    // create a proxy for the local filesystem
    // the scheme/authority serving as the proxy is derived
    // from the supplied URI
    this.scheme = name.getScheme();
    String nameUriString = name.toString();

    String authority = name.getAuthority() != null ? name.getAuthority() : "";
    String proxyUriString = scheme + "://" + authority + "/";

    fs = ShimLoader.getHadoopShims().createProxyFileSystem(localFs, URI.create(proxyUriString));

    fs.initialize(name, conf);
  }

  @Override
  public String getScheme() {
    return scheme;
  }

  @Override
  // For pfile, calculate the checksum for use in testing
  public FileChecksum getFileChecksum(Path f) throws IOException {
    if (scheme.equalsIgnoreCase("pfile") && fs.isFile(f)) {
      return getPFileChecksum(f);
    }
    return fs.getFileChecksum(f);
  }

  private FileChecksum getPFileChecksum(Path f) throws IOException {
    MessageDigest md5Digest;
    try {
      md5Digest = MessageDigest.getInstance("MD5");
      MD5Hash md5Hash = new MD5Hash(getMD5Checksum(fs.open(f)));
      return new PFileChecksum(md5Hash, md5Digest.getAlgorithm());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Calculate MD5 checksum from data in FSDataInputStream
   * @param fsInputStream
   * @return byte array with md5 checksum bytes
   * @throws Exception
   */
  static byte[] getMD5Checksum(FSDataInputStream fsInputStream) throws Exception {
    byte[] buffer = new byte[1024];
    MessageDigest md5Digest = MessageDigest.getInstance("MD5");
    int numRead = 0;
    while (numRead != -1) {
      numRead = fsInputStream.read(buffer);
      if (numRead > 0) {
        md5Digest.update(buffer, 0, numRead);
      }
    }
    fsInputStream.close();
    return md5Digest.digest();
  }

  /**
   * Checksum implementation for PFile uses in testing
   */
  public static class PFileChecksum extends FileChecksum {
    private MD5Hash md5;
    private String algorithmName;

    public PFileChecksum(MD5Hash md5, String algorithmName) {
      this.md5 = md5;
      this.algorithmName = algorithmName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      md5.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      md5.readFields(in);
    }

    @Override
    public String getAlgorithmName() {
      return algorithmName;
    }

    @Override
    public int getLength() {
      if (md5 != null) {
        return md5.getDigest().length;
      }
      return 0;
    }

    @Override
    public byte[] getBytes() {
      if (md5 != null) {
        return md5.getDigest();
      }
      return new byte[0];
    }
  }
}
