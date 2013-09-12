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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.pig;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.UDFContext;
import org.junit.Test;

public class TestPigHCatUtil {

  @Test
  public void testGetBagSubSchema() throws Exception {

    // Define the expected schema.
    ResourceFieldSchema[] bagSubFieldSchemas = new ResourceFieldSchema[1];
    bagSubFieldSchemas[0] = new ResourceFieldSchema().setName("innertuple")
      .setDescription("The tuple in the bag").setType(DataType.TUPLE);

    ResourceFieldSchema[] innerTupleFieldSchemas = new ResourceFieldSchema[1];
    innerTupleFieldSchemas[0] =
      new ResourceFieldSchema().setName("innerfield").setType(DataType.CHARARRAY);

    bagSubFieldSchemas[0].setSchema(new ResourceSchema().setFields(innerTupleFieldSchemas));
    ResourceSchema expected = new ResourceSchema().setFields(bagSubFieldSchemas);

    // Get the actual converted schema.
    HCatSchema hCatSchema = new HCatSchema(Lists.newArrayList(
      new HCatFieldSchema("innerLlama", HCatFieldSchema.Type.STRING, null)));
    HCatFieldSchema hCatFieldSchema =
      new HCatFieldSchema("llama", HCatFieldSchema.Type.ARRAY, hCatSchema, null);
    ResourceSchema actual = PigHCatUtil.getBagSubSchema(hCatFieldSchema);

    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testGetBagSubSchemaConfigured() throws Exception {

    // NOTE: pig-0.8 sets client system properties by actually getting the client
    // system properties. Starting in pig-0.9 you must pass the properties in.
    // When updating our pig dependency this will need updated.
    System.setProperty(HCatConstants.HCAT_PIG_INNER_TUPLE_NAME, "t");
    System.setProperty(HCatConstants.HCAT_PIG_INNER_FIELD_NAME, "FIELDNAME_tuple");
    UDFContext.getUDFContext().setClientSystemProps(System.getProperties());

    // Define the expected schema.
    ResourceFieldSchema[] bagSubFieldSchemas = new ResourceFieldSchema[1];
    bagSubFieldSchemas[0] = new ResourceFieldSchema().setName("t")
      .setDescription("The tuple in the bag").setType(DataType.TUPLE);

    ResourceFieldSchema[] innerTupleFieldSchemas = new ResourceFieldSchema[1];
    innerTupleFieldSchemas[0] =
      new ResourceFieldSchema().setName("llama_tuple").setType(DataType.CHARARRAY);

    bagSubFieldSchemas[0].setSchema(new ResourceSchema().setFields(innerTupleFieldSchemas));
    ResourceSchema expected = new ResourceSchema().setFields(bagSubFieldSchemas);

    // Get the actual converted schema.
    HCatSchema actualHCatSchema = new HCatSchema(Lists.newArrayList(
      new HCatFieldSchema("innerLlama", HCatFieldSchema.Type.STRING, null)));
    HCatFieldSchema actualHCatFieldSchema =
      new HCatFieldSchema("llama", HCatFieldSchema.Type.ARRAY, actualHCatSchema, null);
    ResourceSchema actual = PigHCatUtil.getBagSubSchema(actualHCatFieldSchema);

    Assert.assertEquals(expected.toString(), actual.toString());

    // Clean up System properties that were set by this test
    System.clearProperty(HCatConstants.HCAT_PIG_INNER_TUPLE_NAME);
    System.clearProperty(HCatConstants.HCAT_PIG_INNER_FIELD_NAME);
  }
}
