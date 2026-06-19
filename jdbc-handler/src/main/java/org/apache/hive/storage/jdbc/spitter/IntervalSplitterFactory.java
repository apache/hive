/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.spitter;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

public class IntervalSplitterFactory {
  public static IntervalSplitter newIntervalSpitter(TypeInfo typeInfo) throws IOException {
    PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new LongIntervalSpitter();
      case FLOAT:
      case DOUBLE:
        return new DoubleIntervalSplitter();
      case DECIMAL:
        return new DecimalIntervalSplitter();
      case TIMESTAMP:
        return new TimestampIntervalSplitter();
      case DATE:
        return new DateIntervalSplitter();
      default:
        throw new IOException("partitionColumn is " + primitiveTypeInfo.getPrimitiveCategory() +
                ", only numeric/date/timestamp type can be a partition column");
    }
  }
}
