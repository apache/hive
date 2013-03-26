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

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

public class AuthUtils {

  /**
   * @param path non-null
   * @param action non-null
   * @param conf
   * @throws SemanticException
   * @throws HCatException
   *
   * This method validates only for existing path. If path doesn't exist
   * there is nothing to validate. So, make sure that path passed in is non-null.
   */

  @SuppressWarnings("deprecation")
  public static void authorize(final Path path, final FsAction action, final Configuration conf) throws SemanticException, HCatException{

    if(path == null) {
      throw new HCatException(ErrorType.ERROR_INTERNAL_EXCEPTION);
    }
    final FileStatus stat;

    try {
      stat = path.getFileSystem(conf).getFileStatus(path);
    } catch (FileNotFoundException fnfe){
      // File named by path doesn't exist; nothing to validate.
      return;
    }
    catch (AccessControlException ace) {
      throw new HCatException(ErrorType.ERROR_ACCESS_CONTROL, ace);
    } catch (org.apache.hadoop.fs.permission.AccessControlException ace){
      // Older hadoop version will throw this @deprecated Exception.
      throw new HCatException(ErrorType.ERROR_ACCESS_CONTROL, ace);
    } catch (IOException ioe){
      throw new SemanticException(ioe);
    }

    final UserGroupInformation ugi;
    try {
      ugi = ShimLoader.getHadoopShims().getUGIForConf(conf);
    } catch (LoginException le) {
      throw new HCatException(ErrorType.ERROR_ACCESS_CONTROL,le);
    } catch (IOException ioe) {
      throw new SemanticException(ioe);
    }

    final FsPermission dirPerms = stat.getPermission();

    final String user = HiveConf.getBoolVar(conf, ConfVars.METASTORE_USE_THRIFT_SASL) ?
                          ugi.getShortUserName() : ugi.getUserName();
    final String grp = stat.getGroup();
    if(user.equals(stat.getOwner())){
      if(dirPerms.getUserAction().implies(action)){
        return;
      }
      throw new HCatException(ErrorType.ERROR_ACCESS_CONTROL);
    }
    if(ArrayUtils.contains(ugi.getGroupNames(), grp)){
      if(dirPerms.getGroupAction().implies(action)){
        return;
      }
      throw new HCatException(ErrorType.ERROR_ACCESS_CONTROL);

    }
    if(dirPerms.getOtherAction().implies(action)){
      return;
    }
    throw new HCatException(ErrorType.ERROR_ACCESS_CONTROL);


  }
}
