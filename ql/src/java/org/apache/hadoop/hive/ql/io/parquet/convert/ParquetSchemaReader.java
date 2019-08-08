/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.convert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;

import java.io.IOException;

public class ParquetSchemaReader {
  public static GroupType read(Path parquetFile) throws IOException {

    Configuration conf = new Configuration();
    ParquetMetadata metaData;
    try {
      metaData = ParquetFileReader.readFooter(conf, parquetFile);
    } catch (IOException e) {
      throw new IOException("Error reading footer from: " + parquetFile, e);
    }
    return metaData.getFileMetaData().getSchema();
  }
}
