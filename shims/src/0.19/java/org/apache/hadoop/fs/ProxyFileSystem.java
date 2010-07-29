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

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

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

  

  private Path swizzleParamPath(Path p) {
    return new Path (realScheme, realAuthority, p.toUri().getPath());
  }

  private Path swizzleReturnPath(Path p) {
    return new Path (myScheme, myAuthority, p.toUri().getPath());
  }

  private FileStatus swizzleFileStatus(FileStatus orig, boolean isParam) {
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

  public void initialize(URI name, Configuration conf) throws IOException {
    try {
      URI realUri = new URI (realScheme, realAuthority,
                            name.getPath(), name.getQuery(), name.getFragment());
      super.initialize(realUri, conf);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public URI getUri() {
    return myUri;
  }

  public String getName() {
    return getUri().toString();
  }

  public Path makeQualified(Path path) {
    return swizzleReturnPath(super.makeQualified(swizzleParamPath(path)));
  }


  protected void checkPath(Path path) {
    super.checkPath(swizzleParamPath(path));
  }

  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
    long len) throws IOException {
    return super.getFileBlockLocations(swizzleFileStatus(file, true),
                                       start, len);
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return super.open(swizzleParamPath(f), bufferSize);
  }

  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return super.append(swizzleParamPath(f), bufferSize, progress);
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return super.create(swizzleParamPath(f), permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }

  public boolean setReplication(Path src, short replication) throws IOException {
    return super.setReplication(swizzleParamPath(src), replication);
  }

  public boolean rename(Path src, Path dst) throws IOException {
    return super.rename(swizzleParamPath(src), swizzleParamPath(dst));
  }
  
  public boolean delete(Path f, boolean recursive) throws IOException {
    return super.delete(swizzleParamPath(f), recursive);
  }

  public boolean deleteOnExit(Path f) throws IOException {
    return super.deleteOnExit(swizzleParamPath(f));
  }    
    
  public FileStatus[] listStatus(Path f) throws IOException {
    FileStatus[] orig = super.listStatus(swizzleParamPath(f));
    FileStatus[] ret = new FileStatus [orig.length];
    for (int i=0; i<orig.length; i++) {
      ret[i] = swizzleFileStatus(orig[i], false);
    }
    return ret;
  }
  
  public Path getHomeDirectory() {
    return swizzleReturnPath(super.getHomeDirectory());
  }

  public void setWorkingDirectory(Path newDir) {
    super.setWorkingDirectory(swizzleParamPath(newDir));
  }
  
  public Path getWorkingDirectory() {
    return swizzleReturnPath(super.getWorkingDirectory());
  }
  
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return super.mkdirs(swizzleParamPath(f), permission);
  }

  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    super.copyFromLocalFile(delSrc, swizzleParamPath(src), swizzleParamPath(dst));
  }

  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
                                Path[] srcs, Path dst)
    throws IOException {
    super.copyFromLocalFile(delSrc, overwrite, srcs, swizzleParamPath(dst));
  }
  
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
                                Path src, Path dst)
    throws IOException {
    super.copyFromLocalFile(delSrc, overwrite, src, swizzleParamPath(dst));
  }

  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    super.copyToLocalFile(delSrc, swizzleParamPath(src), dst);
  }

  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return super.startLocalOutput(swizzleParamPath(fsOutputFile), tmpLocalFile);
  }

  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    super.completeLocalOutput(swizzleParamPath(fsOutputFile), tmpLocalFile);
  }

  public long getUsed() throws IOException{
    return super.getUsed();
  }
  
  public long getDefaultBlockSize() {
    return super.getDefaultBlockSize();
  }
  
  public short getDefaultReplication() {
    return super.getDefaultReplication();
  }

  public ContentSummary getContentSummary(Path f) throws IOException {
    return super.getContentSummary(swizzleParamPath(f));
  }

  public FileStatus getFileStatus(Path f) throws IOException {
    return swizzleFileStatus(super.getFileStatus(swizzleParamPath(f)), false);
  }

  public FileChecksum getFileChecksum(Path f) throws IOException {
    return super.getFileChecksum(swizzleParamPath(f));
  }

  public Configuration getConf() {
    return super.getConf();
  }
  
  public void close() throws IOException {
    super.close();
    super.close();
  }

  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    super.setOwner(swizzleParamPath(p), username, groupname);
  }

  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
    super.setTimes(swizzleParamPath(p), mtime, atime);
  }

  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
    super.setPermission(swizzleParamPath(p), permission);
  }
}
  
