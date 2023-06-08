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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
public class DigesterTest {

  private final String UUID_OID = "6ba7b812-9dad-11d1-80b4-00c04fd430c8";
  private final UUID UUID_IN = UUID.fromString(UUID_OID);

  @Test
  public void testStability() {
    boolean b = true;
    char c = 'a';
    int i4 = 10;
    double f8 = 10.0d;
    String str = "some string";

    Digester d = new Digester(UUID_IN);
    // go thru types and create one version tag
    d.digest(b).digest(c).digest(i4).digest(f8).digest(str);

    UUID uuid0 = d.getUUID();
    String stru0 = uuid0.toString();
    Assert.assertNotNull(stru0);

    String stru1 = "295613e2-4b93-5c6f-94df-18ebb2634c08";
    Assert.assertEquals(stru1, stru0);
    Assert.assertEquals(UUID.fromString(stru1), uuid0);
  }

  @Test
  public void testSpread() {
    Digester d = new Digester();
    boolean b = true;
    char c = 'a';
    int i4 = 10;
    double f8 = 10.0d;
    Date date = new Date();
    String str = "some string";

    // go thru types and create one version tag
    d.digest(b).digest(c).digest(i4);
    d.digest(f8).digest(date).digest(str);

    UUID uuid0 = d.getUUID();
    String stru0 = uuid0.toString();
    Assert.assertNotNull(stru0);

    // change order should change version tag
    d.digest(b).digest(i4).digest(c);
    d.digest(f8).digest(date).digest(str);

    UUID uuid1 = d.getUUID();
    String stru1 = uuid1.toString();
    Assert.assertNotNull(stru1);

    Assert.assertNotEquals(stru0, stru1);
  }

  @Test
  public void testDigestList() {
    List<Integer> li0 = new ArrayList<>();
    li0.add(42);

    Digester d = new Digester();
    d.digest(li0);
    Digester d2 = d.copy();

    UUID uuid0 = d.getUUID();
    String stru0 = uuid0.toString();
    Assert.assertNotNull(stru0);
    Assert.assertEquals(uuid0, d2.getUUID());

    // list(42) != list(42, 24)
    li0.add(24);
    d.digest(li0);
    UUID uuid1 = d.getUUID();
    String stru1 = uuid1.toString();
    Assert.assertNotNull(stru1);
    Assert.assertNotEquals(stru0, stru1);

    // create the equivalent list(42, 24)
    List<Integer> li1 = new ArrayList<>();
    li1.add(42);
    li1.add(24);
    d.digest(li1);
    uuid0 = d.getUUID();
    stru0 = uuid0.toString();
    // list(42, 24) == list(42, 24)
    Assert.assertEquals(stru0, stru1);
  }

  @Test
  public void testObjects() {
    UUID base = null;
    Digester dd = new Digester();
    Set<Integer> s42 = new HashSet<>();
    s42.add(42);
    Object[] args = new Object[]{true, '1', (short) 2, 3, 4.0f, 5.0d, 6L, "seven", new Date(), s42};
    for (int i = 0; i < 5; ++i) {
      args[3] = 3 + i;
      UUID u0 = dd.digest(args).getUUID();
//            String s1 = u0.toString();
//            Assert.assertNotNull(s1);
      if (base != null) {
        Assert.assertNotEquals(base, u0);
      }
      base = u0;
    }
  }

  @Test
  public void testStream() {
    UUID nsTest = UUID.nameUUIDFromBytes("testStream".getBytes());
    String str0 = "Life, the Universe and Everything";
    InputStream in = new ByteArrayInputStream(str0.getBytes());
    Digester dd = new Digester(nsTest);
    UUID u42 = dd.digest(in).getUUID();
    String str1 = "May the Force be with you";
    in = new ByteArrayInputStream(str1.getBytes());
    UUID u0405 =  dd.digest(in).getUUID();
    Assert.assertNotEquals(u42, u0405);
    // digesting by input stream or byte array leads to same result
    Assert.assertEquals(u0405, dd.digest(str1.getBytes()).getUUID());
    Assert.assertEquals(u42, dd.digest(str0.getBytes()).getUUID());
    // digesting with different digester from same ns lead to same result
    Digester dd1 = new Digester(nsTest);
    Assert.assertEquals(u0405, dd1.digest(str1.getBytes()).getUUID());
    Assert.assertEquals(u42, dd1.digest(str0.getBytes()).getUUID());
    // digesting with different digester from different ns lead to different result
    dd1 = new Digester();
    Assert.assertNotEquals(u0405, dd1.digest(str1.getBytes()).getUUID());
    Assert.assertNotEquals(u42, dd1.digest(str0.getBytes()).getUUID());
  }

  @Test
  public void testObjects2() {
    UUID base = null;
    Set<Integer> s42 = new HashSet<>();
    s42.add(42);
    Digester dd = new Digester();
    UUID u0 = null;
    for (int i = 0; i < 5; ++i) {
      u0 = dd.digest(true)
          .digest('1')
          .digest((short) 2)
          .digest(3 + i)
          .digest(4.0f)
          .digest(5.0d)
          .digest(6L)
          .digest("seven")
          .digest(new Date())
          .digest(s42)
          .digest(u0)
          .getUUID();
//            String s1 = u0.toString();
//            Assert.assertNotNull(s1);
      if (base != null) {
        Assert.assertNotEquals(base, u0);
      }
      base = u0;
    }
  }

}
