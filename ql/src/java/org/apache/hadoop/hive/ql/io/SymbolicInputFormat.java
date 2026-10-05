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

public class SymbolicInputFormat implements ReworkMapredInputFormat {

  private static final Pattern GLOB_METACHARS = Pattern.compile("[\\\\{}\\[\\]*?]");

  public void rework(HiveConf job, MapredWork work) throws IOException {
    Map<Path, PartitionDesc> pathToParts = work.getMapWork().getPathToPartitionInfo();
    List<Path> toRemovePaths = new ArrayList<>();
    Map<Path, PartitionDesc> toAddPathToPart = new HashMap<>();
    Map<Path, List<String>> pathToAliases = work.getMapWork().getPathToAliases();
    List<Path> allowedRoots = getAllowedRoots(job);

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
              for (Path match : resolveTargets(job, symlink.getPath(), allowedRoots, line)) {
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
   * Expands a symlink file line, accepting only matches under the symlink file's directory or an allowed root.
   */
  static List<Path> resolveTargets(Configuration conf, Path symlinkFile, List<Path> allowedRoots, String line)
      throws IOException {
    // Qualified against the default filesystem, so no filesystem is created for an arbitrary scheme.
    FileSystem defaultFs = FileSystem.get(conf);
    List<Path> roots = new ArrayList<>(allowedRoots);
    roots.add(qualify(defaultFs, symlinkFile.getParent()));
    Path pattern = qualify(defaultFs, new Path(line));
    // Matches are checked too, as a glob can climb out of the root.
    checkAllowed(pattern, roots);
    FileStatus[] statuses = pattern.getFileSystem(conf).globStatus(pattern);
    if (statuses == null) {
      return Collections.emptyList();
    }
    List<Path> matches = new ArrayList<>();
    for (FileStatus status : statuses) {
      Path match = qualify(defaultFs, status.getPath());
      checkAllowed(match, roots);
      // FileInputFormat globs the match again, so it must not contain glob metacharacters.
      if (GLOB_METACHARS.matcher(match.toUri().getPath()).find()) {
        throw new IOException("Symlink target " + match + " contains glob metacharacters");
      }
      matches.add(match);
    }
    return matches;
  }

  private static void checkAllowed(Path target, List<Path> roots) throws IOException {
    for (Path root : roots) {
      if (FileUtils.isPathWithinSubtree(target, root)) {
        return;
      }
    }
    throw new IOException("Symlink target " + target + " is outside of the allowed locations. Additional locations"
        + " can be configured with " + ConfVars.HIVE_SYMLINK_ALLOWED_TARGET_PATHS.varname);
  }

  static List<Path> getAllowedRoots(Configuration conf) throws IOException {
    List<Path> allowedRoots = new ArrayList<>();
    for (String root : HiveConf.getTrimmedStringsVar(conf, ConfVars.HIVE_SYMLINK_ALLOWED_TARGET_PATHS)) {
      if (!root.isEmpty()) {
        Path rootPath = new Path(root);
        allowedRoots.add(qualify(rootPath.getFileSystem(conf), rootPath));
      }
    }
    return allowedRoots;
  }

  private static Path qualify(FileSystem fs, Path path) {
    return new Path(path.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toUri().normalize());
  }
}
