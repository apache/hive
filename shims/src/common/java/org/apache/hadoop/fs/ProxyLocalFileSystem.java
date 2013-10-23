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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.HadoopShims;

/****************************************************************
 * A Proxy for LocalFileSystem
 *
 * Serves uri's corresponding to 'pfile:///' namespace with using
 * a LocalFileSystem
 *****************************************************************/

public class ProxyLocalFileSystem extends FilterFileSystem {

  protected LocalFileSystem localFs;

  public ProxyLocalFileSystem() {
    localFs = new LocalFileSystem();
  }

  public ProxyLocalFileSystem(FileSystem fs) {
    throw new RuntimeException ("Unsupported Constructor");
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    // create a proxy for the local filesystem
    // the scheme/authority serving as the proxy is derived
    // from the supplied URI
    String scheme = name.getScheme();
    String nameUriString = name.toString();
    if (Shell.WINDOWS) {
      // Replace the encoded backward slash with forward slash
      // Remove the windows drive letter
      nameUriString = nameUriString.replaceAll("%5C", "/")
          .replaceFirst("/[c-zC-Z]:", "/")
          .replaceFirst("^[c-zC-Z]:", "");
      name = URI.create(nameUriString);
    }

    String authority = name.getAuthority() != null ? name.getAuthority() : "";
    String proxyUriString = nameUriString + "://" + authority + "/";

    fs = ShimLoader.getHadoopShims().createProxyFileSystem(
        localFs, URI.create(proxyUriString));

    fs.initialize(name, conf);
  }
}
