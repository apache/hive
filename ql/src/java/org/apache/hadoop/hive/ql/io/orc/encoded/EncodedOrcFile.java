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
package org.apache.hadoop.hive.ql.io.orc.encoded;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;

/**
 * Factory for encoded ORC readers and options.
 */
public class EncodedOrcFile {

  /**
   * Extends ReaderOptions to accept a file system supplier
   * instead of a fully initialized fs object.
   */
  public static class EncodedReaderOptions extends ReaderOptions {

    private Supplier<FileSystem> fileSystemSupplier;

    public EncodedReaderOptions(Configuration configuration) {
      super(configuration);
    }

    public EncodedReaderOptions filesystem(Supplier<FileSystem> fsSupplier) {
      this.fileSystemSupplier = fsSupplier;
      return this;
    }

    @Override
    public EncodedReaderOptions filesystem(FileSystem fs) {
      this.fileSystemSupplier = () -> fs;
      return this;
    }

    @Override
    public FileSystem getFilesystem() {
      return fileSystemSupplier != null ? fileSystemSupplier.get() : null;
    }
  }

  public static Reader createReader(
      Path path, ReaderOptions options) throws IOException {
    return new ReaderImpl(path, options);
  }

  public static EncodedReaderOptions readerOptions(Configuration conf) {
    return new EncodedReaderOptions(conf);
  }
}
