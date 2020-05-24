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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data.schema;

import java.io.PrintStream;



import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestHCatSchemaUtils.
 */
public class TestHCatSchemaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestHCatSchemaUtils.class);

  @Test
  public void testSimpleOperation() throws Exception {
    String typeString = "struct<name:string,studentid:int,"
        + "contact:struct<phNo:string,email:string>,"
        + "currently_registered_courses:array<string>,"
        + "current_grades:map<string,string>,"
        + "phNos:array<struct<phNo:string,type:string>>,blah:array<int>>";

    TypeInfo ti = TypeInfoUtils.getTypeInfoFromTypeString(typeString);

    HCatSchema hsch = HCatSchemaUtils.getHCatSchemaFromTypeString(typeString);
    LOG.info("Type name : {}", ti.getTypeName());
    LOG.info("HCatSchema : {}", hsch);
    assertEquals(hsch.size(), 1);
    // Looks like HCatFieldSchema.getTypeString() lower-cases its results
    assertEquals(ti.getTypeName().toLowerCase(), hsch.get(0).getTypeString());
    assertEquals(hsch.get(0).getTypeString(), typeString.toLowerCase());
  }
  
  @Test
  public void testHCatFieldSchemaConversion() throws Exception {
	  FieldSchema stringFieldSchema = new FieldSchema("name1", serdeConstants.STRING_TYPE_NAME, "comment1");
	  HCatFieldSchema stringHCatFieldSchema = HCatSchemaUtils.getHCatFieldSchema(stringFieldSchema);
	  assertEquals(stringHCatFieldSchema.getName(), "name1");
	  assertEquals(stringHCatFieldSchema.getCategory(), Category.PRIMITIVE);
	  assertEquals(stringHCatFieldSchema.getComment(), "comment1");

	  FieldSchema listFieldSchema = new FieldSchema("name1", "array<tinyint>", "comment1");
	  HCatFieldSchema listHCatFieldSchema = HCatSchemaUtils.getHCatFieldSchema(listFieldSchema);
	  assertEquals(listHCatFieldSchema.getName(), "name1");
	  assertEquals(listHCatFieldSchema.getCategory(), Category.ARRAY);
	  assertEquals(listHCatFieldSchema.getComment(), "comment1");

	  FieldSchema mapFieldSchema = new FieldSchema("name1", "map<string,int>", "comment1");
	  HCatFieldSchema mapHCatFieldSchema = HCatSchemaUtils.getHCatFieldSchema(mapFieldSchema);
	  assertEquals(mapHCatFieldSchema.getName(), "name1");
	  assertEquals(mapHCatFieldSchema.getCategory(), Category.MAP);
	  assertEquals(mapHCatFieldSchema.getComment(), "comment1");

	  FieldSchema structFieldSchema = new FieldSchema("name1", "struct<s:string,i:tinyint>", "comment1");
	  HCatFieldSchema structHCatFieldSchema = HCatSchemaUtils.getHCatFieldSchema(structFieldSchema);
	  assertEquals(structHCatFieldSchema.getName(), "name1");
	  assertEquals(structHCatFieldSchema.getCategory(), Category.STRUCT);
	  assertEquals(structHCatFieldSchema.getComment(), "comment1");
  }

  @SuppressWarnings("unused")
  private void pretty_print(PrintStream pout, HCatSchema hsch) throws HCatException {
    pretty_print(pout, hsch, "");
  }


  private void pretty_print(PrintStream pout, HCatSchema hsch, String prefix) throws HCatException {
    int i = 0;
    for (HCatFieldSchema field : hsch.getFields()) {
      pretty_print(pout, field, prefix + "." + (field.getName() == null ? i : field.getName()));
      i++;
    }
  }

  private void pretty_print(PrintStream pout, HCatFieldSchema hfsch, String prefix) throws HCatException {

    Category tcat = hfsch.getCategory();
    if (Category.STRUCT == tcat) {
      pretty_print(pout, hfsch.getStructSubSchema(), prefix);
    } else if (Category.ARRAY == tcat) {
      pretty_print(pout, hfsch.getArrayElementSchema(), prefix);
    } else if (Category.MAP == tcat) {
      pout.println(prefix + ".mapkey:\t" + hfsch.getMapKeyType().toString());
      pretty_print(pout, hfsch.getMapValueSchema(), prefix + ".mapvalue:");
    } else {
      pout.println(prefix + "\t" + hfsch.getType().toString());
    }
  }

}
