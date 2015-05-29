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

package org.apache.hive.service.auth;

import javax.security.sasl.AuthenticationException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import junit.framework.TestCase;
import org.apache.hadoop.hive.conf.HiveConf;

public class TestLdapAuthenticationProviderImpl extends TestCase {

  private static HiveConf hiveConf;
  private static byte[] hiveConfBackup;

  @Override
  public void setUp() throws Exception {
      hiveConf = new HiveConf();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      hiveConf.writeXml(baos);
      baos.close();
      hiveConfBackup = baos.toByteArray();
      hiveConf.set("hive.server2.authentication.ldap.url", "localhost");
      FileOutputStream fos = new FileOutputStream(new File(hiveConf.getHiveSiteLocation().toURI()));
      hiveConf.writeXml(fos);
      fos.close();
  }

  public void testLdapEmptyPassword() {
    LdapAuthenticationProviderImpl ldapImpl = new LdapAuthenticationProviderImpl();
    try {
      ldapImpl.Authenticate("user", "");
      assertFalse(true);
    } catch (AuthenticationException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("a null or blank password has been provided"));
    }
  }

  @Override
  public void tearDown() throws Exception {
    if(hiveConf != null && hiveConfBackup != null) {
      FileOutputStream fos = new FileOutputStream(new File(hiveConf.getHiveSiteLocation().toURI()));
      fos.write(hiveConfBackup);
      fos.close();
    }
  }
}
