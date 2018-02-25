/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class TestBeeLineOpts {
  @Test
  public void testPropertyNamesSet() throws Exception {
    BeeLine mockBeeLine = mock(BeeLine.class);
    when(mockBeeLine.isBeeLine()).thenReturn(true);
    when(mockBeeLine.getReflector()).thenReturn(new Reflector(mockBeeLine));
    BeeLineOpts beeLineOpts = new BeeLineOpts(mockBeeLine, System.getProperties());
    Assert.assertFalse(beeLineOpts.propertyNamesSet().contains("conf"));
  }
}
