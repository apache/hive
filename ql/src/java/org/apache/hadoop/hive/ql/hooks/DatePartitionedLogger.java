/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.hooks;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.util.Clock;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

/**
 * Class to create proto reader and writer for a date partitioned directory structure.
 *
 * @param <T> The proto message type.
 */
public class DatePartitionedLogger<T extends MessageLite> {
  // Everyone has permission to write, but with sticky set so that delete is restricted.
  // This is required, since the path is same for all users and everyone writes into it.
  private static final FsPermission DIR_PERMISSION = FsPermission.createImmutable((short)01777);

  private final Parser<T> parser;
  private final Path basePath;
  private final Configuration conf;
  private final Clock clock;
  private final FileSystem fileSystem;

  public DatePartitionedLogger(Parser<T> parser, Path baseDir, Configuration conf, Clock clock)
      throws IOException {
    this.conf = conf;
    this.clock = clock;
    this.parser = parser;
    this.fileSystem = baseDir.getFileSystem(conf);
    if (!fileSystem.exists(baseDir)) {
      fileSystem.mkdirs(baseDir);
      fileSystem.setPermission(baseDir, DIR_PERMISSION);
    }
    this.basePath = fileSystem.resolvePath(baseDir);
  }

  /**
   * Creates a writer for the given fileName, with date as today.
   */
  public ProtoMessageWriter<T> getWriter(String fileName) throws IOException {
    Path filePath = getPathForDate(getNow().toLocalDate(), fileName);
    return new ProtoMessageWriter<>(conf, filePath, parser);
  }

  /**
   * Creates a reader for the given filePath, no validation is done.
   */
  public ProtoMessageReader<T> getReader(Path filePath) throws IOException {
    return new ProtoMessageReader<>(conf, filePath, parser);
  }

  /**
   * Create a path for the given date and fileName. This can be used to create a reader.
   */
  public Path getPathForDate(LocalDate date, String fileName) throws IOException {
    Path path = new Path(basePath, getDirForDate(date));
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path);
      fileSystem.setPermission(path, DIR_PERMISSION);
    }
    return new Path(path, fileName);
  }

  /**
   * Extract the date from the directory name, this should be a directory created by this class.
   */
  public LocalDate getDateFromDir(String dirName) {
    if (!dirName.startsWith("date=")) {
      throw new IllegalArgumentException("Invalid directory: "+ dirName);
    }
    return LocalDate.parse(dirName.substring(5), DateTimeFormatter.ISO_LOCAL_DATE);
  }

  /**
   * Returns the directory name for a given date.
   */
  public String getDirForDate(LocalDate date) {
    return "date=" + DateTimeFormatter.ISO_LOCAL_DATE.format(date);
  }

  /**
   * Find next available directory, after the given directory.
   */
  public String getNextDirectory(String currentDir) throws IOException {
    // Fast check, if the next day directory exists return it.
    String nextDate = getDirForDate(getDateFromDir(currentDir).plusDays(1));
    if (fileSystem.exists(new Path(basePath, nextDate))) {
      return nextDate;
    }
    // Have to scan the directory to find min date greater than currentDir.
    String dirName = null;
    for (FileStatus status : fileSystem.listStatus(basePath)) {
      String name = status.getPath().getName();
      // String comparison is good enough, since its of form date=yyyy-MM-dd
      if (name.compareTo(currentDir) > 0 && (dirName == null || name.compareTo(dirName) < 0)) {
        dirName = name;
      }
    }
    return dirName;
  }

  /**
   * Returns new or changed files in the given directory. The offsets are used to find
   * changed files.
   */
  public List<Path> scanForChangedFiles(String subDir, Map<String, Long> currentOffsets)
      throws IOException {
    Path dirPath = new Path(basePath, subDir);
    List<Path> newFiles = new ArrayList<>();
    if (!fileSystem.exists(dirPath)) {
      return newFiles;
    }
    for (FileStatus status : fileSystem.listStatus(dirPath)) {
      String fileName = status.getPath().getName();
      Long offset = currentOffsets.get(fileName);
      // If the offset was never added or offset < fileSize.
      if (offset == null || offset < status.getLen()) {
        newFiles.add(new Path(dirPath, fileName));
      }
    }
    return newFiles;
  }

  /**
   * Returns the current time, using the underlying clock in UTC time.
   */
  public LocalDateTime getNow() {
    // Use UTC date to ensure reader date is same on all timezones.
    return LocalDateTime.ofEpochSecond(clock.getTime() / 1000, 0, ZoneOffset.UTC);
  }

  public Configuration getConfig() {
    return conf;
  }
}
