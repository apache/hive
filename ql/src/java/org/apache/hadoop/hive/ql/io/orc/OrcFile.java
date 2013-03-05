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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.IOException;

/**
 * Contains factory methods to read or write ORC files.
 */
public final class OrcFile {

  public static final String MAGIC = "ORC";
  public static final String COMPRESSION = "orc.compress";
  static final String DEFAULT_COMPRESSION = "ZLIB";
  public static final String COMPRESSION_BLOCK_SIZE = "orc.compress.size";
  static final String DEFAULT_COMPRESSION_BLOCK_SIZE = "262144";
  public static final String STRIPE_SIZE = "orc.stripe.size";
  static final String DEFAULT_STRIPE_SIZE = "268435456";
  public static final String ROW_INDEX_STRIDE = "orc.row.index.stride";
  static final String DEFAULT_ROW_INDEX_STRIDE = "10000";
  public static final String ENABLE_INDEXES = "orc.create.index";

  // unused
  private OrcFile() {}

  /**
   * Create an ORC file reader.
   * @param fs file system
   * @param path file name to read from
   * @return a new ORC file reader.
   * @throws IOException
   */
  public static Reader createReader(FileSystem fs, Path path
                                    ) throws IOException {
    return new ReaderImpl(fs, path);
  }

  /**
   * Create an ORC file streamFactory.
   * @param fs file system
   * @param path filename to write to
   * @param inspector the ObjectInspector that inspects the rows
   * @param stripeSize the number of bytes in a stripe
   * @param compress how to compress the file
   * @param bufferSize the number of bytes to compress at once
   * @param rowIndexStride the number of rows between row index entries or
   *                       0 to suppress all indexes
   * @return a new ORC file streamFactory
   * @throws IOException
   */
  public static Writer createWriter(FileSystem fs,
                                    Path path,
                                    ObjectInspector inspector,
                                    long stripeSize,
                                    CompressionKind compress,
                                    int bufferSize,
                                    int rowIndexStride) throws IOException {
    return new WriterImpl(fs, path, inspector, stripeSize, compress,
      bufferSize, rowIndexStride);
  }

}
