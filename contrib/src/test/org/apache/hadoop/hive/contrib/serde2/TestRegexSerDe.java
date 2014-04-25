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
package org.apache.hadoop.hive.contrib.serde2;

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Text;

/**
 * TestRegexSerDe.
 *
 */
public class TestRegexSerDe extends TestCase {

  private SerDe createSerDe(String fieldNames, String fieldTypes,
      String inputRegex, String outputFormatString) throws Throwable {
    Properties schema = new Properties();
    schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
    schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);
    schema.setProperty("input.regex", inputRegex);
    schema.setProperty("output.format.string", outputFormatString);

    RegexSerDe serde = new RegexSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), schema, null);
    return serde;
  }

  /**
   * Test the LazySimpleSerDe class.
   */
  public void testRegexSerDe() throws Throwable {
    try {
      // Create the SerDe
      SerDe serDe = createSerDe(
          "host,identity,user,time,request,status,size,referer,agent",
          "string,string,string,string,string,string,string,string,string",
          "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") " 
          + "([0-9]*) ([0-9]*) ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\")",
          "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s");

      // Data
      Text t = new Text(
          "127.0.0.1 - - [26/May/2009:00:00:00 +0000] "
              + "\"GET /someurl/?track=Blabla(Main) HTTP/1.1\" 200 5864 - "
              + "\"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) " 
              + "AppleWebKit/525.19 (KHTML, like Gecko) Chrome/1.0.154.65 Safari/525.19\"");

      // Deserialize
      Object row = serDe.deserialize(t);
      ObjectInspector rowOI = serDe.getObjectInspector();

      System.out.println("Deserialized row: " + row);

      // Serialize
      Text serialized = (Text) serDe.serialize(row, rowOI);
      assertEquals(t, serialized);

      // Do some changes (optional)
      ObjectInspector standardWritableRowOI = ObjectInspectorUtils
          .getStandardObjectInspector(rowOI, ObjectInspectorCopyOption.WRITABLE);
      Object standardWritableRow = ObjectInspectorUtils.copyToStandardObject(
          row, rowOI, ObjectInspectorCopyOption.WRITABLE);

      // Serialize
      serialized = (Text) serDe.serialize(standardWritableRow,
          standardWritableRowOI);
      assertEquals(t, serialized);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

}
