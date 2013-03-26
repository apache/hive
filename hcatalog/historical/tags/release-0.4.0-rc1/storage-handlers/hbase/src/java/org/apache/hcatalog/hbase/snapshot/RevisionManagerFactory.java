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
package org.apache.hcatalog.hbase.snapshot;

import java.io.IOException;
import java.util.Properties;

public class RevisionManagerFactory {

   /**
    * Gets an instance of revision manager.
    *
    * @param properties The properties required to created the revision manager.
    * @return the revision manager An instance of revision manager.
    * @throws IOException Signals that an I/O exception has occurred.
    */
   public static RevisionManager getRevisionManager(Properties properties) throws IOException{

        RevisionManager revisionMgr;
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        if (classLoader == null) {
            classLoader = RevisionManagerFactory.class.getClassLoader();
        }
        String className = properties.getProperty(
                RevisionManager.REVISION_MGR_IMPL_CLASS,
                ZKBasedRevisionManager.class.getName());
        try {

            @SuppressWarnings("unchecked")
            Class<? extends RevisionManager> revisionMgrClass = (Class<? extends RevisionManager>) Class
                    .forName(className, true , classLoader);
            revisionMgr = (RevisionManager) revisionMgrClass.newInstance();
            revisionMgr.initialize(properties);
        } catch (ClassNotFoundException e) {
            throw new IOException(
                    "The implementation class of revision manager not found.",
                    e);
        } catch (InstantiationException e) {
            throw new IOException(
                    "Exception encountered during instantiating revision manager implementation.",
                    e);
        } catch (IllegalAccessException e) {
            throw new IOException(
                    "IllegalAccessException encountered during instantiating revision manager implementation.",
                    e);
        } catch (IllegalArgumentException e) {
            throw new IOException(
                    "IllegalArgumentException encountered during instantiating revision manager implementation.",
                    e);
        }
        return revisionMgr;
    }

}
