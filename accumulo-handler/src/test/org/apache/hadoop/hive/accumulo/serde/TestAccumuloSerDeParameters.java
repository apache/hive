/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.serde;

import java.util.Properties;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestAccumuloSerDeParameters {

  @Test
  public void testParseColumnVisibility() throws SerDeException {
    Properties properties = new Properties();
    Configuration conf = new Configuration();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowid,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2,field3");
    properties.setProperty(serdeConstants.LIST_TYPE_NAME, "string,string,string");
    properties.setProperty(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY, "foo&bar");

    AccumuloSerDeParameters params = new AccumuloSerDeParameters(conf, properties,
        AccumuloSerDe.class.getName());

    ColumnVisibility cv = params.getTableVisibilityLabel();

    Assert.assertEquals(new ColumnVisibility("foo&bar"), cv);
  }

  @Test
  public void testParseAuthorizationsFromConf() throws SerDeException {
    Configuration conf = new Configuration(false);
    conf.set(AccumuloSerDeParameters.AUTHORIZATIONS_KEY, "foo,bar");

    Authorizations auths = AccumuloSerDeParameters.getAuthorizationsFromConf(conf);
    Assert.assertEquals(new Authorizations("foo,bar"), auths);
  }

  @Test
  public void testParseAuthorizationsFromnProperties() throws SerDeException {
    Configuration conf = new Configuration();
    Properties properties = new Properties();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowid,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2,field3");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");
    properties.setProperty(AccumuloSerDeParameters.AUTHORIZATIONS_KEY, "foo,bar");

    AccumuloSerDeParameters params = new AccumuloSerDeParameters(conf, properties,
        AccumuloSerDe.class.getName());

    Authorizations auths = params.getAuthorizations();
    Assert.assertEquals(new Authorizations("foo,bar"), auths);
  }

  @Test
  public void testNullAuthsFromProperties() throws SerDeException {
    Configuration conf = new Configuration();
    Properties properties = new Properties();

    properties.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, ":rowid,cf:f2,cf:f3");
    properties.setProperty(serdeConstants.LIST_COLUMNS, "field1,field2,field3");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");

    AccumuloSerDeParameters params = new AccumuloSerDeParameters(conf, properties,
        AccumuloSerDe.class.getName());

    Authorizations auths = params.getAuthorizations();
    Assert.assertNull(auths);
  }

  @Test
  public void testNullAuthsFromConf() throws SerDeException {
    Configuration conf = new Configuration(false);

    Authorizations auths = AccumuloSerDeParameters.getAuthorizationsFromConf(conf);
    Assert.assertNull(auths);
  }
}
