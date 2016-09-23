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
package org.apache.hive.service.auth.ldap;

import java.io.IOException;
import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import org.junit.Before;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestChainFilter {

  private FilterFactory factory;
  private HiveConf conf;

  @Mock
  public Filter filter1;

  @Mock
  public Filter filter2;

  @Mock
  public Filter filter3;

  @Mock
  public FilterFactory factory1;

  @Mock
  public FilterFactory factory2;

  @Mock
  public FilterFactory factory3;

  @Mock
  private DirSearch search;

  @Before
  public void setup() {
    conf = new HiveConf();
    factory = new ChainFilterFactory(factory1, factory2, factory3);
  }

  @Test
  public void testFactoryAllNull() {
    assertNull(factory.getInstance(conf));
  }

  @Test
  public void testFactoryAllEmpty() {
    FilterFactory emptyFactory = new ChainFilterFactory();
    assertNull(emptyFactory.getInstance(conf));
  }

  @Test
  public void testFactory() throws AuthenticationException {
    when(factory1.getInstance(any(HiveConf.class))).thenReturn(filter1);
    when(factory2.getInstance(any(HiveConf.class))).thenReturn(filter2);
    when(factory3.getInstance(any(HiveConf.class))).thenReturn(filter3);

    Filter filter = factory.getInstance(conf);

    filter.apply(search, "User");
    verify(filter1, times(1)).apply(search, "User");
    verify(filter2, times(1)).apply(search, "User");
    verify(filter3, times(1)).apply(search, "User");
  }

  @Test(expected = AuthenticationException.class)
  public void testApplyNegative() throws AuthenticationException, NamingException, IOException {
    doThrow(AuthenticationException.class).when(filter3).apply((DirSearch) anyObject(), anyString());

    when(factory1.getInstance(any(HiveConf.class))).thenReturn(filter1);
    when(factory3.getInstance(any(HiveConf.class))).thenReturn(filter3);

    Filter filter = factory.getInstance(conf);

    filter.apply(search, "User");
  }
}
