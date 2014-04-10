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

package org.apache.hive.hcatalog.streaming;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class TestDelimitedInputWriter {
  @Test
  public void testFieldReordering() throws Exception {

    ArrayList<String> colNames = Lists.newArrayList(new String[]{"col1", "col2", "col3", "col4", "col5"});
    {//1)  test dropping fields - first middle  & last
      String[] fieldNames = {null, "col2", null, "col4", null};
      int[] mapping = DelimitedInputWriter.getFieldReordering(fieldNames, colNames);
      Assert.assertTrue(Arrays.equals(mapping, new int[]{-1, 1, -1, 3, -1}));
    }

    {//2)  test reordering
      String[] fieldNames = {"col5", "col4", "col3", "col2", "col1"};
      int[] mapping = DelimitedInputWriter.getFieldReordering(fieldNames, colNames);
      Assert.assertTrue( Arrays.equals(mapping, new int[]{4,3,2,1,0}) );
    }

    {//3)  test bad field names
      String[] fieldNames = {"xyz", "abc", "col3", "col4", "as"};
      try {
        DelimitedInputWriter.getFieldReordering(fieldNames, colNames);
        Assert.fail();
      } catch (InvalidColumn e) {
        // should throw
      }
    }

    {//4)  test few field names
      String[] fieldNames = {"col3", "col4"};
      int[] mapping = DelimitedInputWriter.getFieldReordering(fieldNames, colNames);
      Assert.assertTrue( Arrays.equals(mapping, new int[]{2,3}) );
    }

    {//5)  test extra field names
      String[] fieldNames = {"col5", "col4", "col3", "col2", "col1", "col1"};
      try {
      DelimitedInputWriter.getFieldReordering(fieldNames, colNames);
      Assert.fail();
      } catch (InvalidColumn e) {
        //show throw
      }
    }
  }
}
