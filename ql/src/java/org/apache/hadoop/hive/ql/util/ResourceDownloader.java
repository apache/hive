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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    return UriType.IVY == getURLType(createURI(value));
  }

  public static boolean isHdfsUri(String value) throws URISyntaxException {
    return UriType.HDFS == getURLType(createURI(value));
  }

  public static boolean isFileUri(String value) {
    try {
      return UriType.FILE == getURLType(createURI(value));
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
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
    case FILE: return isLocalAllowed ? Collections.singletonList(source) : null;
    case IVY: return dependencyResolver.downloadDependencies(source);
    case HDFS:
    case OTHER:
      return Collections.singletonList(createURI(downloadResource(source, subDir, convertToUnix)));
    default: throw new AssertionError(getURLType(source));
    }
  }

  private String downloadResource(URI srcUri, String subDir, boolean convertToUnix)
      throws IOException, URISyntaxException {
    LOG.info("converting to local {}", srcUri);
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
    try {
      FileUtils.forceMkdir(resourceDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private enum UriType { IVY, FILE, HDFS, OTHER };

  /**
   * If the URI has no scheme defined, the default is {@link UriType#FILE}
   */
  private static ResourceDownloader.UriType getURLType(URI value) {
    String scheme = StringUtils.lowerCase(value.getScheme());
    if (scheme == null) return UriType.FILE;
    if ("file".equals(scheme)) return UriType.FILE;
    if ("hdfs".equals(scheme)) return UriType.HDFS;
    if ("ivy".equals(scheme))  return UriType.IVY;
    return UriType.OTHER;
  }
}
