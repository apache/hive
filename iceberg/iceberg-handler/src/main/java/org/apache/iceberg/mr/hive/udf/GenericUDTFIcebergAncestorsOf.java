/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.udf;

import java.util.List;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.SnapshotUtil;

@Description(
    name = "iceberg_ancestors_of",
    value =
        """
            _FUNC_(tableName, [snapshotId]) - Returns a table of all ancestors of the \
            specified snapshot (or the current snapshot if omitted) for the given Iceberg table.""",
    extended =
        """
            Example:
              > SELECT * FROM _FUNC_('default.my_iceberg_table');
              > SELECT * FROM _FUNC_('default.my_iceberg_table', 123456789012345);

            Note: This function should generally be used in a standalone SELECT query.
            If you include a FROM clause without restricting the input rows (e.g. LIMIT 1),
            the UDTF will be evaluated for every row in the source table, causing multiplied output.
            """)
@UDFType(deterministic = false)
public class GenericUDTFIcebergAncestorsOf extends GenericUDTF {

  private transient LongObjectInspector snapshotIdOI;
  private final transient Object[] forwardObj = new Object[2];

  private String serializedTable;
  private transient Table icebergTable;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length < 1 || args.length > 2) {
      throw new UDFArgumentException(
          "iceberg_ancestors_of takes 1 or 2 arguments: tableName (string) and optional snapshotId (long).");
    }

    if (!(args[0] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("The first argument (tableName) must be a string.");
    }

    if (args.length == 2) {
      if (!(args[1] instanceof LongObjectInspector)) {
        throw new UDFArgumentException("The second argument (snapshotId) must be a long.");
      }
      snapshotIdOI = (LongObjectInspector) args[1];
    }

    loadIcebergTable(args);

    List<String> fieldNames = Lists.newArrayList();
    List<ObjectInspector> fieldOIs = Lists.newArrayList();

    fieldNames.add("snapshot_id");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);

    fieldNames.add("timestamp");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  private void loadIcebergTable(ObjectInspector[] args) throws UDFArgumentException {
    if (SessionState.get() != null && ObjectInspectorUtils.isConstantObjectInspector(args[0])) {
      String tableNameStr =
          ((ConstantObjectInspector) args[0]).getWritableConstantValue().toString();
      try {
        HiveConf hiveConf = SessionState.getSessionConf();
        org.apache.hadoop.hive.ql.metadata.Table hiveTable =
            Hive.get(hiveConf).getTable(tableNameStr);
        Table table = IcebergTableUtil.getTable(hiveConf, hiveTable.getTTable());

        this.serializedTable = SerializationUtil.serializeToBase64(SerializableTable.copyOf(table));
      } catch (Exception e) {
        throw new UDFArgumentException("Cannot load Iceberg table: " + tableNameStr);
      }
    }
  }

  @Override
  public void process(Object[] args) throws HiveException {
    if (icebergTable == null && serializedTable != null) {
      icebergTable = SerializationUtil.deserializeFromBase64(serializedTable);
    }

    if (icebergTable == null) {
      return; // Could not deserialize table
    }

    Long snapshotId = null;

    if (args.length == 2 && args[1] != null) {
      snapshotId = snapshotIdOI.get(args[1]);
    }

    Long currentSnapshotId = snapshotId;
    if (currentSnapshotId == null) {
      if (icebergTable.currentSnapshot() == null) {
        return;
      }
      currentSnapshotId = icebergTable.currentSnapshot().snapshotId();
    }

    Iterable<Snapshot> ancestors =
        SnapshotUtil.ancestorsOf(currentSnapshotId, icebergTable::snapshot);

    for (Snapshot snapshot : ancestors) {
      forwardObj[0] = snapshot.snapshotId();
      forwardObj[1] = Timestamp.ofEpochMilli(snapshot.timestampMillis());
      forward(forwardObj);
    }
  }

  @Override
  public void close() throws HiveException {
  }
}
