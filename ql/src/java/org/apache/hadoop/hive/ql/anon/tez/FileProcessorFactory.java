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

package org.apache.hadoop.hive.ql.anon.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.anonymize.RowAnonymizer;
import org.apache.hadoop.hive.ql.anon.extract.Extractor;
import org.apache.hadoop.io.WritableComparable;

import java.util.List;

public final class FileProcessorFactory {

  public static FileProcessor create(final FileType fileType, final Configuration conf, final Extractor extractor, final RowAnonymizer rowAnonymizer,
                                     final RowContext rowContext, final List<WritableComparable> keys) {
    switch (fileType) {
      case ORC: {
        return new OrcFileProcessor(conf, extractor, rowAnonymizer, rowContext, keys, new Stats());
      }
      case PARQUET: {
        return new ParquetFileProcessor(conf, extractor, rowAnonymizer, rowContext, keys);
      }
      default: {
        throw new RuntimeException("bad type: " + fileType);
      }
    }
  }

}
