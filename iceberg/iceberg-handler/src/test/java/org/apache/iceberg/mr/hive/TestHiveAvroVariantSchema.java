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

package org.apache.iceberg.mr.hive;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hive.iceberg.org.apache.avro.LogicalTypes;
import org.apache.hive.iceberg.org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.VariantLogicalType;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

/**
 * The Avro schema Hive's AvroSerDe stores for a VARIANT column, parsed by Iceberg's Avro with Iceberg's variant
 * logical type registered. Hive relocates Iceberg's Avro, so the schema goes over as its JSON text.
 */
public class TestHiveAvroVariantSchema {

  @Test
  public void testHiveVariantRecordIsAValidIcebergVariant() throws Exception {
    // Iceberg registers its logical types globally when its Avro class loads
    Class.forName("org.apache.iceberg.avro.Avro");
    Assert.assertTrue(LogicalTypes.getCustomRegisteredTypes().containsKey("variant"));

    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "v");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "variant");
    new AvroSerDe().initialize(new Configuration(), props, null);
    String literal = props.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());

    // parsing applies the registered type, which keeps the record only if its validation passes
    Schema variant = new Schema.Parser().parse(literal).getField("v").schema().getTypes().get(1);
    Assert.assertTrue(variant.getLogicalType() instanceof VariantLogicalType);
    Assert.assertTrue(LogicalTypes.fromSchema(variant) instanceof VariantLogicalType);
    Assert.assertEquals(AvroSchemaUtil.convert(Types.VariantType.get()).getFields(), variant.getFields());
  }
}
