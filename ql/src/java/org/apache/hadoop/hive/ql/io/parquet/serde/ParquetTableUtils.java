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
package org.apache.hadoop.hive.ql.io.parquet.serde;

import org.apache.hadoop.mapred.JobConf;

import java.util.Map;
import java.util.TimeZone;

public class ParquetTableUtils {
  // Parquet table properties
  public static final String PARQUET_INT96_WRITE_ZONE_PROPERTY = "parquet.mr.int96.write.zone";

  // This is not a TimeZone we convert into and print out, rather a delta, an adjustment we use.
  // More precisely the lack of an adjustment in case of UTC
  public static final String PARQUET_INT96_NO_ADJUSTMENT_ZONE = "UTC";

  /**
   * Propagates the parquet timezone property to the job configuration from the table property
   * or sets the default
   * @param jc the job conf to set the parquet timezone property on
   * @param tableProps the table properties which may contain the parquet timezone
   */
  public static void setParquetTimeZoneIfAbsent(JobConf jc, Map<?, ?> tableProps) {
    if (tableProps != null && jc != null) {
      if (tableProps.containsKey(PARQUET_INT96_WRITE_ZONE_PROPERTY)) {
        jc.set(PARQUET_INT96_WRITE_ZONE_PROPERTY,
            (String)tableProps.get(PARQUET_INT96_WRITE_ZONE_PROPERTY));
      } else {
        jc.set(PARQUET_INT96_WRITE_ZONE_PROPERTY, TimeZone.getDefault().getID());
      }
    }
  }
}
