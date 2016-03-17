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

package org.apache.hadoop.hive.ql.io.orc.encoded;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.orc.DataReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;


class ReaderImpl extends org.apache.hadoop.hive.ql.io.orc.ReaderImpl implements Reader {

  public ReaderImpl(Path path, ReaderOptions options) throws IOException {
    super(path, options);
  }

  @Override
  public EncodedReader encodedReader(
      Object fileKey, DataCache dataCache, DataReader dataReader, PoolFactory pf) throws IOException {
    return new EncodedReaderImpl(fileKey, types,
        codec, bufferSize, rowIndexStride, dataCache, dataReader, pf);
  }
}
