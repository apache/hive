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
package org.apache.hadoop.hive.ql.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Test-only {@link RawLocalFileSystem} wrapper that advertises itself under the
 * synthetic {@value #SCHEME} scheme. Files still live on the local disk (backed
 * by {@link RawLocalFileSystem}), so tests can inspect them with plain
 * {@link java.io.File} calls, but every {@link Path} that Hive resolves via
 * {@link FileSystem#getFileSystem(URI, Configuration)} sees {@code fakes3://…}.
 *
 * <p>Tests using this class typically add {@value #SCHEME} to
 * {@code org.apache.hadoop.hive.common.FileUtils.NON_ATOMIC_RENAME_SCHEMES} so
 * {@code FileUtils.isNonAtomicRenameFs} treats it like S3A and drives the
 * non-atomic-rename branch of the move logic. Remove it again in an
 * {@code @AfterClass}/{@code @AfterAll} hook so no other test sees the mutation.
 *
 * <p>Register with:
 * <pre>
 *   conf.setClass("fs." + FakeS3FileSystem.SCHEME + ".impl",
 *                 FakeS3FileSystem.class, FileSystem.class);
 *   conf.setBoolean("fs." + FakeS3FileSystem.SCHEME + ".impl.disable.cache", true);
 * </pre>
 * Disabling the FS cache is important — otherwise a per-test {@code TemporaryFolder}
 * root will leak between tests through the cached FS instance.
 */
public final class FakeS3FileSystem extends RawLocalFileSystem {

  /** URI scheme this FS advertises. */
  public static final String SCHEME = "fakes3";

  private URI uri;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    String authority = name.getAuthority() == null ? "" : name.getAuthority();
    this.uri = URI.create(SCHEME + "://" + authority + "/");
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public URI getUri() {
    return uri != null ? uri : URI.create(SCHEME + ":///");
  }

  // RawLocalFileSystem's DeprecatedRawLocalFileStatus lazy-loads permissions via
  //   new File(getPath().toUri())
  // and File(URI) requires scheme=="file", so it throws on every getPermission()
  // call for our fakes3:// URIs. Replace the returned statuses with plain
  // FileStatus objects whose permission field is populated at construction time,
  // so getPermission() is a simple field read that never hits the broken loader.
  private static FileStatus withPermission(FileStatus s) throws IOException {
    return new FileStatus(s.getLen(), s.isDirectory(), s.getReplication(), s.getBlockSize(),
        s.getModificationTime(), s.getAccessTime(), new FsPermission((short) 0644),
        "hive", "hive", s.isSymlink() ? s.getSymlink() : null, s.getPath());
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return withPermission(super.getFileStatus(f));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    FileStatus[] arr = super.listStatus(f);
    for (int i = 0; i < arr.length; i++) {
      arr[i] = withPermission(arr[i]);
    }
    return arr;
  }
}
