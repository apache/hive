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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

/**
 * Utility to instantiate the revision manager (not a true factory actually).
 * Depends on HBase configuration to resolve ZooKeeper connection (when ZK is used).
 */
public class RevisionManagerFactory {

  public static final String REVISION_MGR_IMPL_CLASS = "revision.manager.impl.class";

  /**
   * Gets an instance of revision manager.
   *
   * @param properties The properties required to created the revision manager.
   * @return the revision manager An instance of revision manager.
   * @throws IOException Signals that an I/O exception has occurred.
   */
   private static RevisionManager getRevisionManager(String className, Properties properties) throws IOException{

        RevisionManager revisionMgr;
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        if (classLoader == null) {
            classLoader = RevisionManagerFactory.class.getClassLoader();
        }
        try {
            Class<? extends RevisionManager> revisionMgrClass = Class
                    .forName(className, true , classLoader).asSubclass(RevisionManager.class);
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

   /**
    * Internally used by endpoint implementation to instantiate from different configuration setting.
    * @param className
    * @param conf
    * @return
    * @throws IOException
    */
   static RevisionManager getOpenedRevisionManager(String className, Configuration conf) throws IOException {

       Properties properties = new Properties();
       String zkHostList = conf.get(HConstants.ZOOKEEPER_QUORUM);
       int port = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT,
               HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);

       if (zkHostList != null) {
           String[] splits = zkHostList.split(",");
           StringBuffer sb = new StringBuffer();
           for (String split : splits) {
               sb.append(split);
               sb.append(':');
               sb.append(port);
               sb.append(',');
           }

           sb.deleteCharAt(sb.length() - 1);
           properties.put(ZKBasedRevisionManager.HOSTLIST, sb.toString());
       }
       String dataDir = conf.get(ZKBasedRevisionManager.DATADIR);
       if (dataDir != null) {
           properties.put(ZKBasedRevisionManager.DATADIR, dataDir);
       }
       RevisionManager revisionMgr = RevisionManagerFactory
               .getRevisionManager(className, properties);
       if (revisionMgr instanceof Configurable) {
         ((Configurable)revisionMgr).setConf(conf);
       }
       revisionMgr.open();
       return revisionMgr;
   }

   /**
    * Gets an instance of revision manager which is opened.
    * The revision manager implementation can be specified as {@link #REVISION_MGR_IMPL_CLASS},
    * default is {@link ZKBasedRevisionManager}.
    * @param hbaseConf The HBase configuration.
    * @return RevisionManager An instance of revision manager.
    * @throws IOException
    */
   public static RevisionManager getOpenedRevisionManager(Configuration hbaseConf) throws IOException {
     String className = hbaseConf.get(RevisionManagerFactory.REVISION_MGR_IMPL_CLASS,
         ZKBasedRevisionManager.class.getName());
     return getOpenedRevisionManager(className, hbaseConf);
   }

}
