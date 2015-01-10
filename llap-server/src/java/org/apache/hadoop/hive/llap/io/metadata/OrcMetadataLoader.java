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

package org.apache.hadoop.hive.llap.io.metadata;

import static org.apache.hadoop.hive.ql.io.orc.OrcFile.readerOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.io.orc.OrcFile;
import org.apache.hadoop.hive.llap.io.orc.Reader;
import org.apache.hadoop.hive.llap.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;

public class OrcMetadataLoader implements Callable<OrcMetadata> {
  private FileSystem fs;
  private Path path;
  private Configuration conf;

  public OrcMetadataLoader(FileSystem fs, Path path, Configuration conf) {
    this.fs = fs;
    this.path = path;
    this.conf = conf;
  }

  @Override
  public OrcMetadata call() throws Exception {
    Reader reader = OrcFile.createLLAPReader(path, readerOptions(conf).filesystem(fs));
    OrcMetadata orcMetadata = new OrcMetadata();
    orcMetadata.setCompressionKind(reader.getCompression());
    orcMetadata.setCompressionBufferSize(reader.getCompressionSize());
    List<StripeInformation> stripes = reader.getStripes();
    orcMetadata.setStripes(stripes);
    Map<Integer, List<OrcProto.ColumnEncoding>> stripeColEnc = new HashMap<Integer, List<OrcProto.ColumnEncoding>>();
    Map<Integer, OrcProto.RowIndex[]> stripeRowIndices = new HashMap<Integer, OrcProto.RowIndex[]>();
    RecordReader rows = reader.rows();
    for (int i = 0; i < stripes.size(); i++) {
      stripeColEnc.put(i, rows.getColumnEncodings(i));
      stripeRowIndices.put(i, rows.getRowIndexEntries(i));
    }
    orcMetadata.setStripeToColEncodings(stripeColEnc);
    orcMetadata.setStripeToRowIndexEntries(stripeRowIndices);
    return orcMetadata;
  }
}
