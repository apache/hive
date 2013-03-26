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
package org.apache.hcatalog.common;

import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hcatalog.common.HCatUtil;

public class TestHCatUtil extends TestCase{


  public void testFsPermissionOperation(){

    HashMap<String,Integer> permsCode = new HashMap<String,Integer>();

    for (int i = 0; i < 8; i++){
      for (int j = 0; j < 8; j++){
        for (int k = 0; k < 8; k++){
          StringBuilder sb = new StringBuilder();
          sb.append("0");
          sb.append(i);
          sb.append(j);
          sb.append(k);
          Integer code = (((i*8)+j)*8)+k;
          String perms = (new FsPermission(Short.decode(sb.toString()))).toString();
          if (permsCode.containsKey(perms)){
            assertEquals("permissions(" + perms + ") mapped to multiple codes",code,permsCode.get(perms));
          }
          permsCode.put(perms, code);
          assertFsPermissionTransformationIsGood(perms);
        }
      }
    }
  }

  private void assertFsPermissionTransformationIsGood(String perms) {
    assertEquals(perms,FsPermission.valueOf("-"+perms).toString());
  }

  public void testValidateMorePermissive(){
    assertConsistentFsPermissionBehaviour(FsAction.ALL,true,true,true,true,true,true,true,true);
    assertConsistentFsPermissionBehaviour(FsAction.READ,false,true,false,true,false,false,false,false);
    assertConsistentFsPermissionBehaviour(FsAction.WRITE,false,true,false,false,true,false,false,false);
    assertConsistentFsPermissionBehaviour(FsAction.EXECUTE,false,true,true,false,false,false,false,false);
    assertConsistentFsPermissionBehaviour(FsAction.READ_EXECUTE,false,true,true,true,false,true,false,false);
    assertConsistentFsPermissionBehaviour(FsAction.READ_WRITE,false,true,false,true,true,false,true,false);
    assertConsistentFsPermissionBehaviour(FsAction.WRITE_EXECUTE,false,true,true,false,true,false,false,true);
    assertConsistentFsPermissionBehaviour(FsAction.NONE,false,true,false,false,false,false,false,false);
  }


  private void assertConsistentFsPermissionBehaviour(
      FsAction base, boolean versusAll, boolean versusNone,
      boolean versusX, boolean versusR, boolean versusW,
      boolean versusRX, boolean versusRW,  boolean versusWX){

    assertTrue(versusAll == HCatUtil.validateMorePermissive(base, FsAction.ALL));
    assertTrue(versusX == HCatUtil.validateMorePermissive(base, FsAction.EXECUTE));
    assertTrue(versusNone == HCatUtil.validateMorePermissive(base, FsAction.NONE));
    assertTrue(versusR == HCatUtil.validateMorePermissive(base, FsAction.READ));
    assertTrue(versusRX == HCatUtil.validateMorePermissive(base, FsAction.READ_EXECUTE));
    assertTrue(versusRW == HCatUtil.validateMorePermissive(base, FsAction.READ_WRITE));
    assertTrue(versusW == HCatUtil.validateMorePermissive(base, FsAction.WRITE));
    assertTrue(versusWX == HCatUtil.validateMorePermissive(base, FsAction.WRITE_EXECUTE));
  }

  public void testExecutePermissionsCheck(){
    assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.ALL));
    assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.NONE));
    assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.EXECUTE));
    assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.READ_EXECUTE));
    assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.WRITE_EXECUTE));

    assertFalse(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.READ));
    assertFalse(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.WRITE));
    assertFalse(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.READ_WRITE));

  }

}
