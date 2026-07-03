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

package org.apache.hadoop.hive.ql.anon.perf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public final class RowWriterFactory {


  public static RowWriter create(final TestContext ctx, final Path path, final Configuration conf) throws IOException {
    switch (ctx.fileType) {
      case ORC:
        return new OrcRowWriter(ctx, path, conf);
      case PARQUET:
        return new ParquetRowWriter(ctx, path, conf);
      default:
        throw new RuntimeException("unsupported type: " + ctx.fileType.name());
    }
  }
}
