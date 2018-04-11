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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;

/****************************************************************
 * A FileSystem that can serve a given scheme/authority using some
 * other file system. In that sense, it serves as a proxy for the
 * real/underlying file system
 *****************************************************************/

public class ProxyFileSystem extends FilterFileSystem {

  protected String myScheme;
  protected String myAuthority;
  protected URI myUri;

  protected String realScheme;
  protected String realAuthority;
  protected URI realUri;



  protected Path swizzleParamPath(Path p) {
    String pathUriString = p.toUri().toString();
    URI newPathUri = URI.create(pathUriString);
    return new Path (realScheme, realAuthority, newPathUri.getPath());
  }

  private Path swizzleReturnPath(Path p) {
    String pathUriString = p.toUri().toString();
    URI newPathUri = URI.create(pathUriString);
    return new Path (myScheme, myAuthority, newPathUri.getPath());
  }

  protected FileStatus swizzleFileStatus(FileStatus orig, boolean isParam) {
    FileStatus ret =
      new FileStatus(orig.getLen(), orig.isDir(), orig.getReplication(),
                     orig.getBlockSize(), orig.getModificationTime(),
                     orig.getAccessTime(), orig.getPermission(),
                     orig.getOwner(), orig.getGroup(),
                     isParam ? swizzleParamPath(orig.getPath()) :
                     swizzleReturnPath(orig.getPath()));
    return ret;
  }

  public ProxyFileSystem() {
    throw new RuntimeException ("Unsupported constructor");
  }

  public ProxyFileSystem(FileSystem fs) {
    throw new RuntimeException ("Unsupported constructor");
  }

  /**
   *
   * @param p
   * @return
   * @throws IOException
   */
  @Override
  public Path resolvePath(final Path p) throws IOException {
    // Return the fully-qualified path of path f resolving the path
    // through any symlinks or mount point
    checkPath(p);
    return getFileStatus(p).getPath();
  }

  /**
   * Create a proxy file system for fs.
   *
   * @param fs FileSystem to create proxy for
   * @param myUri URI to use as proxy. Only the scheme and authority from
   *              this are used right now
   */
  public ProxyFileSystem(FileSystem fs, URI myUri) {
    super(fs);

    URI realUri = fs.getUri();
    this.realScheme = realUri.getScheme();
    this.realAuthority=realUri.getAuthority();
    this.realUri = realUri;

    this.myScheme = myUri.getScheme();
    this.myAuthority=myUri.getAuthority();
    this.myUri = myUri;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    try {
      URI realUri = new URI (realScheme, realAuthority,
                            name.getPath(), name.getQuery(), name.getFragment());
      super.initialize(realUri, conf);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public URI getUri() {
    return myUri;
  }

  @Override
  public String getName() {
    return getUri().toString();
  }

  @Override
  public Path makeQualified(Path path) {
    return swizzleReturnPath(super.makeQualified(swizzleParamPath(path)));
  }


  @Override
  protected void checkPath(final Path path) {
    super.checkPath(swizzleParamPath(path));
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
    long len) throws IOException {
    return super.getFileBlockLocations(swizzleFileStatus(file, true),
                                       start, len);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return super.open(swizzleParamPath(f), bufferSize);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return super.append(swizzleParamPath(f), bufferSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return super.create(swizzleParamPath(f), permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    return super.setReplication(swizzleParamPath(src), replication);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path dest = swizzleParamPath(dst);
    // Make sure for existing destination we return false as per FileSystem api contract
    return super.isFile(dest) ? false : super.rename(swizzleParamPath(src), dest);
  }

  @Override
  protected void rename(Path src, Path dst, Rename... options)
      throws IOException {
    super.rename(swizzleParamPath(src), swizzleParamPath(dst), options);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return super.delete(swizzleParamPath(f), recursive);
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    return super.deleteOnExit(swizzleParamPath(f));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    FileStatus[] orig = super.listStatus(swizzleParamPath(f));
    FileStatus[] ret = new FileStatus [orig.length];
    for (int i=0; i<orig.length; i++) {
      ret[i] = swizzleFileStatus(orig[i], false);
    }
    return ret;
  }

  @Override
  public Path getHomeDirectory() {
    return swizzleReturnPath(super.getHomeDirectory());
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    super.setWorkingDirectory(swizzleParamPath(newDir));
  }

  @Override
  public Path getWorkingDirectory() {
    return swizzleReturnPath(super.getWorkingDirectory());
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return super.mkdirs(swizzleParamPath(f), permission);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    super.copyFromLocalFile(delSrc, swizzleParamPath(src), swizzleParamPath(dst));
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path[] srcs, Path dst)
    throws IOException {
    super.copyFromLocalFile(delSrc, overwrite, srcs, swizzleParamPath(dst));
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
    throws IOException {
    super.copyFromLocalFile(delSrc, overwrite, src, swizzleParamPath(dst));
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    super.copyToLocalFile(delSrc, swizzleParamPath(src), dst);
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return super.startLocalOutput(swizzleParamPath(fsOutputFile), tmpLocalFile);
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    super.completeLocalOutput(swizzleParamPath(fsOutputFile), tmpLocalFile);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return super.getContentSummary(swizzleParamPath(f));
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws IOException {
    return swizzleFileStatus(super.getFileLinkStatus(swizzleParamPath(f)), false);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return swizzleFileStatus(super.getFileStatus(swizzleParamPath(f)), false);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return super.getFileChecksum(swizzleParamPath(f));
  }

  @Override
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    super.setOwner(swizzleParamPath(p), username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
    super.setTimes(swizzleParamPath(p), mtime, atime);
  }

  @Override
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
    super.setPermission(swizzleParamPath(p), permission);
  }
}

