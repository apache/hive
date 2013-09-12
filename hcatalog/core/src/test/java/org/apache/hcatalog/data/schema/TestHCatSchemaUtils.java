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
package org.apache.hcatalog.data.schema;

import java.io.PrintStream;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.schema.TestHCatSchemaUtils} instead
 */
public class TestHCatSchemaUtils extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHCatSchemaUtils.class);

  public void testSimpleOperation() throws Exception {
    String typeString = "struct<name:string,studentid:int,"
        + "contact:struct<phno:string,email:string>,"
        + "currently_registered_courses:array<string>,"
        + "current_grades:map<string,string>,"
        + "phnos:array<struct<phno:string,type:string>>,blah:array<int>>";

    TypeInfo ti = TypeInfoUtils.getTypeInfoFromTypeString(typeString);

    HCatSchema hsch = HCatSchemaUtils.getHCatSchemaFromTypeString(typeString);
    LOG.info("Type name : {}", ti.getTypeName());
    LOG.info("HCatSchema : {}", hsch);
    assertEquals(hsch.size(), 1);
    assertEquals(ti.getTypeName(), hsch.get(0).getTypeString());
    assertEquals(hsch.get(0).getTypeString(), typeString);
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
