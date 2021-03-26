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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderImpl extends org.apache.orc.impl.ReaderImpl
                        implements Reader {

  private static final Logger LOG = LoggerFactory.getLogger(ReaderImpl.class);

  private final ObjectInspector inspector;

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  @Override
  public org.apache.hadoop.hive.ql.io.orc.CompressionKind getCompression() {
    for (CompressionKind value: org.apache.hadoop.hive.ql.io.orc.CompressionKind.values()) {
      if (value.getUnderlying() == this.getCompressionKind()) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unknown compression kind " +
        this.getCompressionKind());
  }

  /**
  * Constructor that let's the user specify additional options.
   * @param path pathname for file
   * @param options options for reading
   * @throws IOException
   */
  public ReaderImpl(Path path, OrcFile.ReaderOptions options) throws IOException {
    super(path, options);
    this.inspector = OrcStruct.createObjectInspector(0, getTypes());
  }

  @Override
  public ByteBuffer getSerializedFileFooter() {
    return tail.getSerializedTail();
  }

  @Override
  public RecordReader rows() throws IOException {
    return rowsOptions(new Options());
  }

  @Override
  public RecordReader rowsOptions(Options options) throws IOException {
    return rowsOptions(options, null);
  }

  @Override
  public RecordReader rowsOptions(Options options, Configuration conf) throws IOException {
    LOG.info("Reading ORC rows from " + path + " with " + options);
    return new RecordReaderImpl(this, options, conf);
  }



  @Override
  public RecordReader rows(boolean[] include) throws IOException {
    return rowsOptions(new Options().include(include));
  }

  @Override
  public RecordReader rows(long offset, long length, boolean[] include
                           ) throws IOException {
    return rowsOptions(new Options().include(include).range(offset, length));
  }

  @Override
  public RecordReader rows(long offset, long length, boolean[] include,
                           SearchArgument sarg, String[] columnNames
                           ) throws IOException {
    return rowsOptions(new Options().include(include).range(offset, length)
        .searchArgument(sarg, columnNames));
  }

  @Override
  public String toString() {
    return "Hive " + super.toString();
  }
}
