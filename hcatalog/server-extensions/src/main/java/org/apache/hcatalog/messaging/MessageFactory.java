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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.messaging;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hcatalog.messaging.json.JSONMessageFactory;

/**
 * Abstract Factory for the construction of HCatalog message instances.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.messaging.MessageFactory} instead
 */
public abstract class MessageFactory {

  private static MessageFactory instance = new JSONMessageFactory();

  protected static final HiveConf hiveConf = new HiveConf();
  static {
    hiveConf.addResource("hive-site.xml");
  }

  private static final String CONF_LABEL_HCAT_MESSAGE_FACTORY_IMPL_PREFIX = "hcatalog.message.factory.impl.";
  private static final String CONF_LABEL_HCAT_MESSAGE_FORMAT = "hcatalog.message.format";
  private static final String HCAT_MESSAGE_FORMAT = hiveConf.get(CONF_LABEL_HCAT_MESSAGE_FORMAT, "json");
  private static final String DEFAULT_MESSAGE_FACTORY_IMPL = "org.apache.hcatalog.messaging.json.JSONMessageFactory";
  private static final String HCAT_MESSAGE_FACTORY_IMPL = hiveConf.get(CONF_LABEL_HCAT_MESSAGE_FACTORY_IMPL_PREFIX
                                     + HCAT_MESSAGE_FORMAT,
                                     DEFAULT_MESSAGE_FACTORY_IMPL);

  protected static final String HCAT_SERVER_URL = hiveConf.get(HiveConf.ConfVars.METASTOREURIS.name(), "");
  protected static final String HCAT_SERVICE_PRINCIPAL = hiveConf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.name(), "");

  /**
   * Getter for MessageFactory instance.
   */
  public static MessageFactory getInstance() {
    if (instance == null) {
      instance = getInstance(HCAT_MESSAGE_FACTORY_IMPL);
    }
    return instance;
  }

  private static MessageFactory getInstance(String className) {
    try {
      return (MessageFactory)ReflectionUtils.newInstance(Class.forName(className), hiveConf);
    }
    catch (ClassNotFoundException classNotFound) {
      throw new IllegalStateException("Could not construct MessageFactory implementation: ", classNotFound);
    }
  }

  /**
   * Getter for MessageDeserializer, corresponding to the specified format and version.
   * @param format Serialization format for notifications.
   * @param version Version of serialization format (currently ignored.)
   * @return MessageDeserializer.
   */
  public static MessageDeserializer getDeserializer(String format,
                            String version) {
    return getInstance(hiveConf.get(CONF_LABEL_HCAT_MESSAGE_FACTORY_IMPL_PREFIX + format,
                    DEFAULT_MESSAGE_FACTORY_IMPL)).getDeserializer();
  }

  public abstract MessageDeserializer getDeserializer();

  /**
   * Getter for version-string, corresponding to all constructed messages.
   */
  public abstract String getVersion();

  /**
   * Getter for message-format.
   */
  public abstract String getMessageFormat();

  /**
   * Factory method for CreateDatabaseMessage.
   * @param db The Database being added.
   * @return CreateDatabaseMessage instance.
   */
  public abstract CreateDatabaseMessage buildCreateDatabaseMessage(Database db);

  /**
   * Factory method for DropDatabaseMessage.
   * @param db The Database being dropped.
   * @return DropDatabaseMessage instance.
   */
  public abstract DropDatabaseMessage buildDropDatabaseMessage(Database db);

  /**
   * Factory method for CreateTableMessage.
   * @param table The Table being created.
   * @return CreateTableMessage instance.
   */
  public abstract CreateTableMessage buildCreateTableMessage(Table table);

  /**
   * Factory method for DropTableMessage.
   * @param table The Table being dropped.
   * @return DropTableMessage instance.
   */
  public abstract DropTableMessage buildDropTableMessage(Table table);

  /**
   * Factory method for AddPartitionMessage.
   * @param table The Table to which the partition is added.
   * @param partition The Partition being added.
   * @return AddPartitionMessage instance.
   */
  public abstract AddPartitionMessage buildAddPartitionMessage(Table table, Partition partition);

  /**
   * Factory method for DropPartitionMessage.
   * @param table The Table from which the partition is dropped.
   * @param partition The Partition being dropped.
   * @return DropPartitionMessage instance.
   */
  public abstract DropPartitionMessage buildDropPartitionMessage(Table table, Partition partition);
}
