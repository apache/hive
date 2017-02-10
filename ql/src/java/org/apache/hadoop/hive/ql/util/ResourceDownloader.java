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

package org.apache.hadoop.hive.ql.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ResourceDownloader {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceDownloader.class);
  private final DependencyResolver dependencyResolver;
  private final Configuration conf;
  private final File resourceDir;

  public ResourceDownloader(Configuration conf, String resourceDirPath) {
    this.dependencyResolver = new DependencyResolver();
    this.conf = conf;
    this.resourceDir = new File(resourceDirPath);
    ensureDirectory(resourceDir);
  }

  /**
   * @param path
   * @return URI corresponding to the path.
   */
  public static URI createURI(String path) throws URISyntaxException {
    return new URI(path);
  }

  public static boolean isIvyUri(String value) throws URISyntaxException {
    return "ivy".equalsIgnoreCase(createURI(value).getScheme());
  }

  public static boolean isFileUri(String value) {
    String scheme = null;
    try {
      scheme = createURI(value).getScheme();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
    return (scheme == null) || scheme.equalsIgnoreCase("file");
  }

  public List<URI> resolveAndDownload(String source, boolean convertToUnix)
      throws URISyntaxException, IOException {
    return resolveAndDownloadInternal(createURI(source), null, convertToUnix, true);
  }

  public List<URI> downloadExternal(URI source, String subDir, boolean convertToUnix)
      throws URISyntaxException, IOException {
    return resolveAndDownloadInternal(source, subDir, convertToUnix, false);
  }

  private List<URI> resolveAndDownloadInternal(URI source, String subDir,
      boolean convertToUnix, boolean isLocalAllowed) throws URISyntaxException, IOException {
    switch (getURLType(source)) {
    case FILE: return isLocalAllowed ? Lists.newArrayList(source) : null;
    case IVY: return dependencyResolver.downloadDependencies(source);
    case OTHER: return Lists.newArrayList(
        createURI(downloadResource(source, subDir, convertToUnix)));
    default: throw new AssertionError(getURLType(source));
    }
  }

  private String downloadResource(URI srcUri, String subDir, boolean convertToUnix)
      throws IOException, URISyntaxException {
    LOG.info("converting to local " + srcUri);
    File destinationDir = (subDir == null) ? resourceDir : new File(resourceDir, subDir);
    ensureDirectory(destinationDir);
    File destinationFile = new File(destinationDir, new Path(srcUri.toString()).getName());
    String dest = destinationFile.getCanonicalPath();
    if (destinationFile.exists()) {
      return dest;
    }
    FileSystem fs = FileSystem.get(srcUri, conf);
    fs.copyToLocalFile(new Path(srcUri.toString()), new Path(dest));
    // add "execute" permission to downloaded resource file (needed when loading dll file)
    FileUtil.chmod(dest, "ugo+rx", true);
    return dest;
  }

  private static void ensureDirectory(File resourceDir) {
    boolean doesExist = resourceDir.exists();
    if (doesExist && !resourceDir.isDirectory()) {
      throw new RuntimeException(resourceDir + " is not a directory");
    }
    if (!doesExist && !resourceDir.mkdirs()) {
      throw new RuntimeException("Couldn't create directory " + resourceDir);
    }
  }

  private enum UriType { IVY, FILE, OTHER };
  private static ResourceDownloader.UriType getURLType(URI value) throws URISyntaxException {
    String scheme = value.getScheme();
    if (scheme == null) return UriType.FILE;
    scheme = scheme.toLowerCase();
    if ("ivy".equals(scheme)) return UriType.IVY;
    if ("file".equals(scheme)) return UriType.FILE;
    return UriType.OTHER;
  }
}
