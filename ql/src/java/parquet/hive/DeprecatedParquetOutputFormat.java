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
package parquet.hive;

import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * Deprecated name of the parquet-hive output format. This class exists
 * simply to provide backwards compatibility with users who specified
 * this name in the Hive metastore. All users should now use
 * STORED AS PARQUET
 */
@Deprecated
public class DeprecatedParquetOutputFormat extends MapredParquetOutputFormat {

  public DeprecatedParquetOutputFormat() {
    super();
  }

  public DeprecatedParquetOutputFormat(final OutputFormat<Void, ArrayWritable> mapreduceOutputFormat) {
    super(mapreduceOutputFormat);
  }
}
