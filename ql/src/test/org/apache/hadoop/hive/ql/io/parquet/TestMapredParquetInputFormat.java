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
package org.apache.hadoop.hive.ql.io.parquet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import org.apache.parquet.hadoop.ParquetInputFormat;

public class TestMapredParquetInputFormat {
  @Test
  public void testDefaultConstructor() {
    new MapredParquetInputFormat();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConstructorWithParquetInputFormat() {
    new MapredParquetInputFormat(
        (ParquetInputFormat<ArrayWritable>) mock(ParquetInputFormat.class)
        );
  }

  /**
   * The input paths are listed as FileInputFormat lists them: hidden files and directories left out, subdirectories
   * walked when recursive and kept as entries when not, an input file listed as itself.
   */
  @Test
  public void testListsTheFilesFileInputFormatLists() throws Exception {
    java.nio.file.Path root = Files.createTempDirectory("parquet-list");
    try {
      Path[] inputs = inputs(root);
      for (boolean recursive : new boolean[] {true, false}) {
        JobConf job = new JobConf();
        FileInputFormat.setInputPaths(job, inputs);
        job.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, recursive);
        List<String> expected = listed(new PlainFileInputFormat().list(job));

        assertEquals(4, expected.size());
        assertEquals(expected, listed(new MapredParquetInputFormat().listStatus(job)));
      }
    } finally {
      FileSystem.getLocal(new JobConf()).delete(new Path(root.toUri()), true);
    }
  }

  /** Each listed path costs one listing call and no status call: a glob of the path costs an object store two. */
  @Test
  public void testListsEachPathWithOneCall() throws Exception {
    java.nio.file.Path root = Files.createTempDirectory("parquet-list-calls");
    try {
      JobConf job = new JobConf();
      FileInputFormat.setInputPaths(job, inputs(root));
      job.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true);
      job.setClass("fs.file.impl", CountingLocalFileSystem.class, FileSystem.class);
      job.setBoolean("fs.file.impl.disable.cache", true);
      CountingLocalFileSystem.LISTINGS.set(0);
      CountingLocalFileSystem.STATUSES.set(0);

      new MapredParquetInputFormat().listStatus(job);
      assertEquals(0, CountingLocalFileSystem.STATUSES.get());
      // p=1, its union subdirectory, p=2 and the input file
      assertEquals(4, CountingLocalFileSystem.LISTINGS.get());
    } finally {
      FileSystem.getLocal(new JobConf()).delete(new Path(root.toUri()), true);
    }
  }

  /** Two partition directories, with hidden files, a hidden and a union subdirectory, and a file. */
  private static Path[] inputs(java.nio.file.Path root) throws IOException {
    java.nio.file.Path p1 = root.resolve("p=1");
    java.nio.file.Path union = p1.resolve("HIVE_UNION_SUBDIR_1");
    java.nio.file.Path p2 = root.resolve("p=2");
    for (java.nio.file.Path dir : Arrays.asList(p1.resolve(".hive-staging"), union.resolve("_tmp"), p2)) {
      Files.createDirectories(dir);
    }
    Files.write(p1.resolve("a.parquet"), new byte[10]);
    Files.write(p1.resolve("_SUCCESS"), new byte[0]);
    Files.write(p1.resolve(".a.parquet.swp"), new byte[1]);
    Files.write(p1.resolve(".hive-staging").resolve("b.parquet"), new byte[2]);
    Files.write(union.resolve("c.parquet"), new byte[3]);
    Files.write(union.resolve("_tmp").resolve("d.parquet"), new byte[4]);
    Files.write(p2.resolve("e.parquet"), new byte[5]);
    Files.write(root.resolve("f.parquet"), new byte[6]);
    return new Path[] {new Path(p1.toUri()), new Path(p2.toUri()), new Path(root.resolve("f.parquet").toUri())};
  }

  private static List<String> listed(FileStatus[] statuses) {
    return Arrays.stream(statuses)
        .map(status -> status.getPath().toUri().getPath() + (status.isDirectory() ? "/" : " " + status.getLen()))
        .sorted()
        .toList();
  }

  private static final class PlainFileInputFormat extends FileInputFormat<NullWritable, ArrayWritable> {
    FileStatus[] list(JobConf job) throws IOException {
      return listStatus(job);
    }

    @Override
    public RecordReader<NullWritable, ArrayWritable> getRecordReader(InputSplit split, JobConf conf,
        Reporter reporter) {
      throw new UnsupportedOperationException();
    }
  }

  /** Counts the listing and status calls made on it. */
  public static final class CountingLocalFileSystem extends LocalFileSystem {
    static final AtomicInteger LISTINGS = new AtomicInteger();
    static final AtomicInteger STATUSES = new AtomicInteger();

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
      LISTINGS.incrementAndGet();
      return super.listLocatedStatus(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      LISTINGS.incrementAndGet();
      return super.listStatus(f);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      STATUSES.incrementAndGet();
      return super.getFileStatus(f);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
      STATUSES.incrementAndGet();
      return super.globStatus(pathPattern, filter);
    }
  }
}
