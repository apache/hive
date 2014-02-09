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

import org.apache.hadoop.io.ArrayWritable;

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;

/**
 *
 * A MapWritableReadSupport, encapsulates the tuples
 *
 */
public class DataWritableRecordConverter extends RecordMaterializer<ArrayWritable> {

  private final DataWritableGroupConverter root;

  public DataWritableRecordConverter(final GroupType requestedSchema, final GroupType tableSchema) {
    this.root = new DataWritableGroupConverter(requestedSchema, tableSchema);
  }

  @Override
  public ArrayWritable getCurrentRecord() {
    return root.getCurrentArray();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
