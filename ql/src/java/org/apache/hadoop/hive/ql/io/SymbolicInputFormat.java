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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SymbolicInputFormat implements ReworkMapredInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(SymbolicInputFormat.class);

  private static final Pattern GLOB_METACHARS = Pattern.compile("[\\\\{}\\[\\]*?]");

  public void rework(HiveConf job, MapredWork work) throws IOException {
    Map<Path, PartitionDesc> pathToParts = work.getMapWork().getPathToPartitionInfo();
    List<Path> toRemovePaths = new ArrayList<>();
    Map<Path, PartitionDesc> toAddPathToPart = new HashMap<>();
    Map<Path, List<String>> pathToAliases = work.getMapWork().getPathToAliases();
    List<Path> allowedRoots = getAllowedRoots(job);
    FileSystem defaultFs = FileSystem.get(job);

    for (Map.Entry<Path, PartitionDesc> pathPartEntry : pathToParts.entrySet()) {
      Path path = pathPartEntry.getKey();
      PartitionDesc partDesc = pathPartEntry.getValue();
      // this path points to a symlink path
      if (partDesc.getInputFileFormatClass().equals(
          SymlinkTextInputFormat.class)) {
        // change to TextInputFormat
        partDesc.setInputFileFormatClass(TextInputFormat.class);
        FileSystem fileSystem = path.getFileSystem(job);
        FileStatus fStatus = fileSystem.getFileStatus(path);
        FileStatus[] symlinks = null;
        if (!fStatus.isDir()) {
          symlinks = new FileStatus[] { fStatus };
        } else {
          symlinks = fileSystem.listStatus(path, FileUtils.HIDDEN_FILES_PATH_FILTER);
        }
        toRemovePaths.add(path);
        List<String> aliases = pathToAliases.remove(path);
        for (FileStatus symlink : symlinks) {
          BufferedReader reader = null;
          try {
            reader = new BufferedReader(new InputStreamReader(
                fileSystem.open(symlink.getPath())));

            partDesc.setInputFileFormatClass(TextInputFormat.class);

            String line;
            while ((line = reader.readLine()) != null) {
              for (Path match : resolveTargets(job, defaultFs, allowedRoots, line)) {
                Path schemaLessPath = Path.getPathWithoutSchemeAndAuthority(match);
                StringInternUtils.internUriStringsInPath(schemaLessPath);
                toAddPathToPart.put(schemaLessPath, partDesc);
                pathToAliases.put(schemaLessPath, aliases);
              }
            }
          } finally {
            org.apache.hadoop.io.IOUtils.closeStream(reader);
          }
        }
      }
    }

    for (Entry<Path, PartitionDesc> toAdd : toAddPathToPart.entrySet()) {
      work.getMapWork().addPathToPartitionInfo(toAdd.getKey(), toAdd.getValue());
    }
    for (Path toRemove : toRemovePaths) {
      work.getMapWork().removePathToPartitionInfo(toRemove);
    }
  }

  /**
   * Expands a symlink file line to the matching paths, which must all be under an allowed root.
   *
   * @param defaultFs used to qualify lines that have no scheme
   */
  static List<Path> resolveTargets(Configuration conf, FileSystem defaultFs, List<Path> allowedRoots, String line)
      throws IOException {
    Path pattern = new Path(line);
    if (pattern.toUri().getScheme() == null) {
      pattern = pattern.makeQualified(defaultFs.getUri(), defaultFs.getWorkingDirectory());
    }
    // Check before listing, so a line outside the allowed roots is not listed.
    checkAllowed(pattern, allowedRoots, line);
    FileStatus[] statuses = pattern.getFileSystem(conf).globStatus(pattern);
    if (statuses == null) {
      return Collections.emptyList();
    }
    List<Path> matches = new ArrayList<>();
    for (FileStatus status : statuses) {
      Path match = status.getPath();
      // A glob like {../x} can match outside the allowed roots, so check each match too.
      checkAllowed(match, allowedRoots, line);
      // The match is globbed again when read, so it must not contain glob characters.
      if (GLOB_METACHARS.matcher(match.toUri().getPath()).find()) {
        throw new IOException("Symlink target " + line + " matches a path with glob metacharacters");
      }
      matches.add(match);
    }
    return matches;
  }

  private static void checkAllowed(Path target, List<Path> roots, String line) throws IOException {
    for (Path root : roots) {
      if (FileUtils.isPathWithinSubtree(target, root)) {
        return;
      }
    }
    throw new IOException("Symlink target " + line + " is outside of the allowed locations. Additional locations"
        + " can be configured with " + ConfVars.HIVE_SYMLINK_ALLOWED_TARGET_PATHS.varname);
  }

  /**
   * Returns the allowed roots. Relative or unparsable entries are skipped with a warning.
   */
  static List<Path> getAllowedRoots(Configuration conf) {
    URI defaultUri = FileSystem.getDefaultUri(conf);
    List<Path> allowedRoots = new ArrayList<>();
    for (String root : HiveConf.getTrimmedStringsVar(conf, ConfVars.HIVE_SYMLINK_ALLOWED_TARGET_PATHS)) {
      if (root.isEmpty()) {
        continue;
      }
      try {
        Path rootPath = new Path(root);
        if (!rootPath.isAbsolute()) {
          LOG.warn("Ignoring relative path {} in {}", root, ConfVars.HIVE_SYMLINK_ALLOWED_TARGET_PATHS.varname);
          continue;
        }
        allowedRoots.add(rootPath.toUri().getScheme() == null ? rootPath.makeQualified(defaultUri, null) : rootPath);
      } catch (IllegalArgumentException e) {
        LOG.warn("Ignoring invalid path {} in {}", root, ConfVars.HIVE_SYMLINK_ALLOWED_TARGET_PATHS.varname, e);
      }
    }
    return Collections.unmodifiableList(allowedRoots);
  }
}
