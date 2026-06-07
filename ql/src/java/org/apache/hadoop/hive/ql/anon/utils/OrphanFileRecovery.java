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

package org.apache.hadoop.hive.ql.anon.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OrphanFileRecovery {

  private static final Logger LOG = LoggerFactory.getLogger(OrphanFileRecovery.class);

  public static final String TMP_SUFFIX = ".anon.tmp";

  public static final long DEFAULT_AGE_THRESHOLD_MS = 24L * 60L * 60L * 1000L;

  public static final class Report {
    public int tmpsDeleted;
    public int skipped;
    @Override public String toString() {
      return "OrphanFileRecovery{tmpsDeleted=" + tmpsDeleted
          + ", skipped=" + skipped + '}';
    }
  }

  private OrphanFileRecovery() {
  }

  public static Report scan(final Configuration conf, final Path root,
                            final long ageThresholdMs) throws IOException {
    final FileSystem fs = root.getFileSystem(conf);
    final long cutoff = System.currentTimeMillis() - ageThresholdMs;
    final Report r = new Report();
    if (!fs.exists(root)) {
      LOG.warn("OrphanFileRecovery: root path {} does not exist", root);
      return r;
    }

    final List<FileStatus> tmps = new ArrayList<>();
    final RemoteIterator<LocatedFileStatus> it = fs.listFiles(root, true);
    while (it.hasNext()) {
      final LocatedFileStatus s = it.next();
      if (s.isFile() && s.getPath().getName().endsWith(TMP_SUFFIX)) {
        tmps.add(s);
      }
    }

    for (final FileStatus s : tmps) {
      if (s.getModificationTime() > cutoff) {
        r.skipped++;
        continue;
      }
      if (fs.delete(s.getPath(), false)) {
        r.tmpsDeleted++;
        LOG.info("OrphanFileRecovery: deleted orphan tmp {}", s.getPath());
      } else {
        r.skipped++;
        LOG.warn("OrphanFileRecovery: failed to delete orphan tmp {}", s.getPath());
      }
    }

    LOG.info("OrphanFileRecovery: scan of {} -> {}", root, r);
    return r;
  }

  public static Report scan(final Configuration conf, final Path root) throws IOException {
    return scan(conf, root, DEFAULT_AGE_THRESHOLD_MS);
  }

  public static void main(final String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("usage: OrphanFileRecovery <hdfs-path> [age-hours]");
      System.exit(2);
    }
    final long ageMs = (args.length >= 2)
        ? Long.parseLong(args[1]) * 3600_000L
        : DEFAULT_AGE_THRESHOLD_MS;
    final Configuration conf = new Configuration();
    final Path root = new Path(args[0]);
    final Report r = scan(conf, root, ageMs);
    System.out.println(r);
  }
}
