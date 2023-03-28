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
package org.apache.hadoop.hive.metastore.properties;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.hive.metastore.properties.PropertyType.DATETIME;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DOUBLE;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.JSON;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.LONG;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

/**
 * Checking the property types.
 */
public class PropertyTypeTest {
  private static final double EPSILON = 1e-9;

  @Test public void testRegistered() {
    for(String str : Arrays.asList("boolean", "integer", "long", "double", "date", "string")) {
      PropertyType type =  PropertyType.get(str);
      Assert.assertNotNull(str, type);
      Assert.assertEquals(str, type.getName());
    }
    PropertyType type =  PropertyType.get("boolean");
    Assert.assertFalse(PropertyType.register(type));

    PropertyType varchar = new PropertyType<String>("varchar") {
      public String cast(Object value) {
        return value == null? null : value.toString();
      }

      @Override
      public String parse(String str) {
        return str;
      }

    };
    Assert.assertTrue(PropertyType.register(varchar));
    type = PropertyType.get("varchar");
    Assert.assertTrue(type == varchar);

    type = new PropertyType<Boolean>("boolean") {
      public Boolean cast(Object value) {
        return value instanceof Boolean? ((Boolean) value) : value != null ? parse(value.toString()) : null;
      }

      @Override
      public Boolean parse(String str) {
        return Boolean.valueOf(str);
      }
    };
    try {
      PropertyType.register(type);
      Assert.fail("type is already registered");
    } catch(IllegalArgumentException xill) {
      Assert.assertTrue(xill.getMessage().contains("boolean"));
    }
  }

  @Test public void testString() {
    Assert.assertNull(STRING.cast(null));
    Assert.assertNull(STRING.format(null));
    Assert.assertNull(STRING.parse(null));
    Assert.assertEquals("42", STRING.cast("42"));
    Assert.assertEquals("42", STRING.cast(42));
    Assert.assertEquals("42", STRING.format("42"));
    Assert.assertEquals("42", STRING.parse("42"));
  }

  @Test public void testInteger() {
    Assert.assertNull(INTEGER.cast(null));
    Assert.assertNull(INTEGER.format(null));
    Assert.assertNull(INTEGER.parse(null));
    Assert.assertEquals(42, (int) INTEGER.cast("42"));
    Assert.assertEquals(42, (int) INTEGER.cast(42));
    Assert.assertNull(INTEGER.cast("foobar"));
    Assert.assertEquals("42", INTEGER.format(42));
    Assert.assertEquals(42, (int) INTEGER.parse("42"));
    Assert.assertNull(INTEGER.parse("foobar"));
  }

  @Test public void testLong() {
    Assert.assertNull(LONG.cast(null));
    Assert.assertNull(LONG.format(null));
    Assert.assertNull(LONG.parse(null));
    Assert.assertEquals(42L, (long) LONG.cast("42"));
    Assert.assertEquals(42L, (long) LONG.cast(42));
    Assert.assertNull(LONG.cast("foobar"));
    Assert.assertEquals("42", LONG.format(42L));
    Assert.assertEquals(42L, (long) LONG.parse("42"));
    Assert.assertNull(LONG.parse("foobar"));
  }

  @Test public void testDouble() {
    Assert.assertNull(DOUBLE.cast(null));
    Assert.assertNull(DOUBLE.format(null));
    Assert.assertNull(DOUBLE.parse(null));
    Assert.assertEquals(42.0d, (double) DOUBLE.cast("42"), EPSILON);
    Assert.assertEquals(42.0d, (double) DOUBLE.cast(42), EPSILON);
    Assert.assertNull(DOUBLE.cast("foobar"));
    Assert.assertEquals("42.0", DOUBLE.format(42.0d));
    Assert.assertEquals(42.0d, (double) DOUBLE.parse("42"), EPSILON);
    Assert.assertNull(DOUBLE.parse("foobar"));
  }

  @Test public void testDatetime() {
    String dateStr = "2007-12-03T10:15:30.00Z";
    Date date = Date.from(Instant.parse(dateStr));
    Assert.assertNull(DATETIME.cast(null));
    Assert.assertNull(DATETIME.format(null));
    Assert.assertNull(DATETIME.parse(null));
    Assert.assertEquals(date, DATETIME.cast(date));
    Assert.assertEquals(date, DATETIME.cast(dateStr));
    Assert.assertEquals(date, DATETIME.cast(date.getTime()));
    Assert.assertEquals(dateStr, DATETIME.format(date));
    Assert.assertEquals(date, DATETIME.parse(dateStr));
    Assert.assertNull(DATETIME.parse("foobar"));
  }
  @Test public void testJson() {
    Assert.assertNull(JSON.cast(null));
    Assert.assertNull(JSON.format(null));
    Assert.assertNull(JSON.parse(null));
    Map<String, Object> map = new TreeMap<>();
    map.put("i", 12d);
    map.put("s", "foobar");
    Assert.assertEquals(42d, JSON.cast("42"));
    Assert.assertEquals(42d, JSON.parse("42"));
    String msrc = "{\"i\":12.0,\"s\":\"foobar\"}";
    Assert.assertEquals(map, JSON.parse(msrc));
    Assert.assertEquals(msrc, JSON.format(map));
  }
}
