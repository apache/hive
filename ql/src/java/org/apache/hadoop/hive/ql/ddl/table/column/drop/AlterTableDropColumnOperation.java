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

package org.apache.hadoop.hive.ql.ddl.table.column.drop;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Operation process of dropping column.
 */
public class AlterTableDropColumnOperation extends AbstractAlterTableOperation<AlterTableDropColumnDesc> {

    public AlterTableDropColumnOperation(DDLOperationContext context, AlterTableDropColumnDesc desc) {
        super(context, desc);
    }

    private static final Set<String> VALID_SERIALIZATION_LIBS = ImmutableSet.of(
            MetadataTypedColumnsetSerDe.class.getName(), LazySimpleSerDe.class.getName(), ColumnarSerDe.class.getName(),
            ParquetHiveSerDe.class.getName(), OrcSerde.class.getName(), "org.apache.iceberg.mr.hive.HiveIcebergSerDe");

    @Override
    protected void doAlteration(Table table, Partition partition) throws HiveException {
        StorageDescriptor sd = getStorageDescriptor(table, partition);
        String serializationLib = sd.getSerdeInfo().getSerializationLib();
        AvroSerdeUtils.handleAlterTableForAvro(context.getConf(), serializationLib, table.getTTable().getParameters());

        if ("org.apache.hadoop.hive.serde.thrift.columnsetSerDe".equals(serializationLib)) {
            context.getConsole().printInfo("Dropping column for columnsetSerDe and changing to LazySimpleSerDe");
            sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
        }

        boolean isOrcSchemaEvolution = serializationLib.equals(OrcSerde.class.getName()) &&
                AlterTableUtils.isSchemaEvolutionEnabled(table, context.getConf());
        if (isOrcSchemaEvolution) {
            throw new HiveException(ErrorMsg.CANNOT_DROP_COLUMN, desc.getDbTableName());
        }

        if (ParquetHiveSerDe.isParquetTable(table) && AlterTableUtils.isSchemaEvolutionEnabled(table, context.getConf()) &&
                !desc.isCascade() && table.isPartitioned()) {
            LOG.warn("Cannot drop column from a partitioned parquet table without the CASCADE option");
            throw new HiveException(ErrorMsg.CANNOT_DROP_COLUMN, desc.getDbTableName());
        }

        List<FieldSchema> cols = new ArrayList<>(sd.getCols());
        int droppingIndex = -1;
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).getName().equalsIgnoreCase(desc.getColName())) {
                droppingIndex = i;
                break;
            }
        }
        if (droppingIndex == -1) {
            if (desc.isIfExists()) {
                return;
            }
            throw new HiveException(ErrorMsg.COLUMN_NOT_FOUND, desc.getColName());
        }
        cols.remove(droppingIndex);
        sd.setCols(cols);
    }
}
