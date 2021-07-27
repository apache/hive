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

package org.apache.hadoop.hive.ql.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 *
 */
public class SingleFileSystem extends FileSystem {

  @Override
  public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3, short arg4, long arg5,
      Progressable arg6) throws IOException {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public boolean delete(Path arg0, boolean arg1) throws IOException {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public FileStatus getFileStatus(Path arg0) throws IOException {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public URI getUri() {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public Path getWorkingDirectory() {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public FileStatus[] listStatus(Path arg0) throws FileNotFoundException, IOException {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public FSDataInputStream open(Path arg0, int arg1) throws IOException {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public boolean rename(Path arg0, Path arg1) throws IOException {
    throw new RuntimeException("Unimplemented!");

  }

  @Override
  public void setWorkingDirectory(Path arg0) {
    throw new RuntimeException("Unimplemented!");

  }

}
