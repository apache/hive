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

package org.apache.hadoop.hive.common;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Collection of file manipulation utilities common across Hive.
 */
public final class FileUtils {

  /**
   * Variant of Path.makeQualified that qualifies the input path against the
   * default file system indicated by the configuration
   *
   * This does not require a FileSystem handle in most cases - only requires the
   * Filesystem URI. This saves the cost of opening the Filesystem - which can
   * involve RPCs - as well as cause errors
   *
   * @param path
   *          path to be fully qualified
   * @param conf
   *          Configuration file
   * @return path qualified relative to default file system
   */
  public static Path makeQualified(Path path, Configuration conf) throws IOException {

    if (!path.isAbsolute()) {
      // in this case we need to get the working directory
      // and this requires a FileSystem handle. So revert to
      // original method.
      return path.makeQualified(FileSystem.get(conf));
    }

    URI fsUri = FileSystem.getDefaultUri(conf);
    URI pathUri = path.toUri();

    String scheme = pathUri.getScheme();
    String authority = pathUri.getAuthority();

    if (scheme != null && (authority != null || fsUri.getAuthority() == null)) {
      return path;
    }

    if (scheme == null) {
      scheme = fsUri.getScheme();
    }

    if (authority == null) {
      authority = fsUri.getAuthority();
      if (authority == null) {
        authority = "";
      }
    }

    return new Path(scheme + ":" + "//" + authority + pathUri.getPath());
  }

  private FileUtils() {
    // prevent instantiation
  }


  public static String makePartName(List<String> partCols,
      List<String> vals) {

    StringBuilder name = new StringBuilder();
    for (int i = 0; i < partCols.size(); i++) {
      if (i > 0) {
        name.append(Path.SEPARATOR);
      }
      name.append(escapePathName((partCols.get(i)).toLowerCase()));
      name.append('=');
      name.append(escapePathName(vals.get(i)));
    }
    return name.toString();
  }

  // NOTE: This is for generating the internal path name for partitions. Users
  // should always use the MetaStore API to get the path name for a partition.
  // Users should not directly take partition values and turn it into a path
  // name by themselves, because the logic below may change in the future.
  //
  // In the future, it's OK to add new chars to the escape list, and old data
  // won't be corrupt, because the full path name in metastore is stored.
  // In that case, Hive will continue to read the old data, but when it creates
  // new partitions, it will use new names.
  static BitSet charToEscape = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
    }
    char[] clist = new char[] { '"', '#', '%', '\'', '*', '/', ':', '=', '?',
        '\\', '\u007F' };
    for (char c : clist) {
      charToEscape.set(c);
    }
  }

  static boolean needsEscaping(char c) {
    return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
  }

  public static String escapePathName(String path) {

    // __HIVE_DEFAULT_NULL__ is the system default value for null and empty string. We should
    // TODO: we should allow user to specify default partition or HDFS file location.
    if (path == null || path.length() == 0) {
      return "__HIVE_DEFAULT_PARTITION__";
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%');
        sb.append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  public static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.valueOf(path.substring(i + 1, i + 3), 16);
        } catch (Exception e) {
          code = -1;
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Recursively lists status for all files starting from a particular
   * directory (or individual file as base case).
   *
   * @param fs file system
   *
   * @param fileStatus starting point in file system
   *
   * @param results receives enumeration of all files found
   */
  public static void listStatusRecursively(FileSystem fs, FileStatus fileStatus,
    List<FileStatus> results)
    throws IOException {

    if (fileStatus.isDir()) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
        listStatusRecursively(fs, stat, results);
      }
    } else {
      results.add(fileStatus);
    }
  }
}
