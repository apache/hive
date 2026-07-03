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

import org.apache.hadoop.hive.ql.anon.btree.LocatorSchemaItem;
import org.apache.hadoop.hive.ql.anon.btree.StructValueList;
import org.apache.hadoop.hive.ql.anon.btree.ValueItem;
import org.apache.hadoop.hive.ql.anon.index.IndexReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hive.ql.anon.utils.Utils.writableToBytes;

public final class IndexResultConverter {

  public static BytesWritable convert(final IndexReader indexReader, final List<WritableComparable> keys) throws IOException {
    final MapWritable mwIndexResult = new MapWritable();
    for (final WritableComparable key : keys) {
      final Writable result = indexReader.seek(key);
      if (result == null) {
        continue;
      }
      final StructValueList valueList = (StructValueList) result;
      for (final ValueItem valueItem : valueList.getItems()) {
        final Writable file = valueItem.filePath;
        if (!mwIndexResult.containsKey(file)) {
          mwIndexResult.put(file, new MapWritable());
        }

        final MapWritable locatorToSchema = (MapWritable) mwIndexResult.get(file);
        for (final LocatorSchemaItem lsi : valueItem.getItemList()) {
          locatorToSchema.put(lsi.rowLocator, lsi.schemaId);
        }
      }
    }
    return new BytesWritable(writableToBytes(mwIndexResult));
  }

}
