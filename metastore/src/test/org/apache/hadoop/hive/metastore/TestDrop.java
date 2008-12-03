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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties;

import org.apache.hadoop.hive.metastore.api.MetaException;

public class TestDrop extends MetaStoreTestBase {

  public TestDrop() throws Exception {
  }


  public void testDrop() throws Exception {
    try {
      DB db = DB.createDB("foo1", conf_);
      assertEquals(0, db.getTables(".+").size());
      //drop and delete data
      Table bar1 = Table.create(db, "bar1", createSchema("foo1","bar1"), conf_);
      assertEquals(1, db.getTables(".+").size());
      bar1.drop();
      assertFalse(fileSys_.exists(bar1.getPath()));
      assertEquals(0, db.getTables(".+").size());
      
      //drop and don't delete the data, external table
      Properties schema2 = createSchema("foo2","bar2");
      schema2.setProperty("EXTERNAL", "TRUE");
      Table bar2 = Table.create(db, "bar2", schema2, conf_);
      assertEquals(1, db.getTables(".+").size());
      bar2.drop();
      assertTrue(fileSys_.exists(bar2.getPath()));
      assertEquals(0, db.getTables(".+").size());
      
      cleanup();
    } catch(MetaException e) {
      e.printStackTrace();
      throw e;
    }
  }

}
