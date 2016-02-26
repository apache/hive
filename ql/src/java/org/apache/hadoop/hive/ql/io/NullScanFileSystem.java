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

package org.apache.hadoop.hive.ql.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * The bogus filesystem that makes Hive not read files for nullscans via lies and deceit.
 * Relies on the fact that the input format doesn't actually have to touch the file.
 * Add method implementations if really needed.
 */
public class NullScanFileSystem extends FileSystem {
  public static String getBase() {
    return getBaseScheme() + "://null/";
  }

  public static String getBaseScheme() {
    return "nullscan";
  }

  private final Token<?>[] DEFAULT_EMPTY_TOKEN_ARRAY = new Token<?>[0];

  public NullScanFileSystem() {
  }

  @Override
  public String getScheme() {
    return getBaseScheme();
  }

  @Override
  public URI getUri() {
    try {
      return new URI(getBase());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return true;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return new FileStatus[] { new FileStatus(0, false, 0, 0, 0, new Path(f, "null")) };
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path(getBase());
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return new FileStatus(0, false, 0, 0, 0, f);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws
      IOException {
    return DEFAULT_EMPTY_TOKEN_ARRAY;
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    return null;
  }
}