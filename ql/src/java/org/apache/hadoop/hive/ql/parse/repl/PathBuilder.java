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
package org.apache.hadoop.hive.ql.parse.repl;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

/**
 * Path builder to stitch together paths with different components might be useful as utils across
 * replication semantic analyzer atleast.
 */
public class PathBuilder {
  private ArrayList<String> descendants = new ArrayList<>();
  private String basePath;

  public PathBuilder(String basePath) {
    this.basePath = basePath;
  }

  public PathBuilder addDescendant(String path) {
    descendants.add(path);
    return this;
  }

  public Path build() {
    Path result = new Path(this.basePath);
    for (String descendant : descendants) {
      result = new Path(result, descendant);
    }
    return result;
  }

  public static Path fullyQualifiedHDFSUri(Path input, FileSystem hdfsFileSystem)
      throws SemanticException {
    URI uri = input.toUri();
    String scheme = hdfsFileSystem.getScheme();
    String authority = hdfsFileSystem.getUri().getAuthority();
    String path = uri.getPath();
    try {
      return new Path(new URI(scheme, authority, path, null, null));
    } catch (URISyntaxException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(), e);
    }
  }
}
