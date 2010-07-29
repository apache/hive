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
 * A Proxy for LocalFileSystem
 *
 * Serves uri's corresponding to 'pfile:///' namespace with using
 * a LocalFileSystem 
 *****************************************************************/

public class ProxyLocalFileSystem extends FilterFileSystem {

  protected LocalFileSystem localFs;

  public ProxyLocalFileSystem() {
    // this is a dummy call to get around the fact that FFS has 
    // no zero-arg constructor
    super(null);
    localFs = new LocalFileSystem();
  }

  public ProxyLocalFileSystem(FileSystem fs) {
    super(fs);
    throw new RuntimeException ("Unsupported Constructor");
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    // create a proxy for the local filesystem
    // the scheme/authority serving as the proxy is derived
    // from the supplied URI

    String scheme = name.getScheme();
    String authority = name.getAuthority() != null ? name.getAuthority() : "";
    String proxyUriString = name + "://" + authority + "/";
    fs = new ProxyFileSystem(localFs, URI.create(proxyUriString));

    fs.initialize(name, conf);
  }
}
