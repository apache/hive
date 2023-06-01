/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.tez.dag.api.DAG;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link DagUtils} where {@link UserGroupInformation#isSecurityEnabled()} is true.
 */
public class TestDagUtilsSecurityEnabled {

  @BeforeClass
  public static void setup() {
    System.setProperty("java.security.krb5.kdc", "");
    System.setProperty("java.security.krb5.realm", "FAKE.REALM");
    Configuration conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
  }

  @AfterClass
  public static void clear() {
    System.clearProperty("java.security.krb5.kdc");
    System.clearProperty("java.security.krb5.realm");
    Configuration conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.SIMPLE, conf);
    UserGroupInformation.setConfiguration(conf);
  }

  @Test
  public void testAddCredentialsWithCredentialSupplierNewTokenAdded() {
    IncrementalIntDagCredentialSupplier supplier = new IncrementalIntDagCredentialSupplier();
    DagUtils dagUtils = new DagUtils(Collections.singletonList(supplier));
    DAG dag = DAG.create("test_credentials_dag");

    dagUtils.addCredentials(mock(MapWork.class), dag, null);
    Token<?> newToken = dag.getCredentials().getToken(supplier.getTokenAlias());
    assertEquals(String.valueOf(1), new String(newToken.getIdentifier()));
  }

  @Test
  public void testAddCredentialsWithCredentialSupplierTokenExistsNothingAdded() {
    IncrementalIntDagCredentialSupplier supplier = new IncrementalIntDagCredentialSupplier();
    DagUtils dagUtils = new DagUtils(Collections.singletonList(supplier));
    DAG dag = DAG.create("test_credentials_dag");
    Token<TokenIdentifier> oldToken = new Token<>();
    // Add explicitly the token in the DAG before calling addCredentials simulating the use-case where the DAG has already the token
    dag.getCredentials().addToken(supplier.getTokenAlias(), oldToken);
    
    dagUtils.addCredentials(mock(MapWork.class), dag, null);
    Token<?> newToken = dag.getCredentials().getToken(supplier.getTokenAlias());
    assertEquals(oldToken, newToken);
    assertEquals(0, supplier.tokenCalls);
  }
}
