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
package org.apache.hive.hcatalog.api.repl;

import com.google.common.base.Function;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.messaging.MessageFactory;


/**
 * ReplicationTask captures the concept of what it'd take to replicate changes from
 * one warehouse to another given a notification event that captures what changed.
 */
@InterfaceStability.Evolving
public abstract class ReplicationTask {
  protected HCatNotificationEvent event;
  protected StagingDirectoryProvider srcStagingDirProvider = null;
  protected StagingDirectoryProvider dstStagingDirProvider = null;
  protected Function<String,String> tableNameMapping = null;
  protected Function<String,String> dbNameMapping = null;

  protected static MessageFactory messageFactory = MessageFactory.getInstance();
  private static Factory factoryInstance = null;
  private static String factoryClassName = null;

  public interface Factory {
    public ReplicationTask create(HCatClient client, HCatNotificationEvent event);
  }

  private static Factory getFactoryInstance(HCatClient client) {
    if (factoryInstance == null){
      createFactoryInstance(client);
    }
    return factoryInstance;
  }

  /**
   * Create factory instance for instantiating ReplicationTasks.
   *
   * The order precedence is as follows:
   *
   * a) If a factory has already been instantiated, and is valid, use it.
   * b) If a factoryClassName has been provided, through .resetFactory(), attempt to instantiate that.
   * c) If a hive.repl.task.factory has been set in the default hive conf, use that.
   * d) If none of the above methods work, instantiate an anoymous factory that will return an error
   *    whenever called, till a user calls resetFactory.
   */
  private synchronized static void createFactoryInstance(HCatClient client) {
    if (factoryInstance == null){
      // instantiate new factory instance only if current one is not valid.
      if (factoryClassName == null){
        // figure out which factory we're instantiating from HiveConf iff it's not been set on us directly.
        factoryClassName = client.getConfVal(HiveConf.ConfVars.HIVE_REPL_TASK_FACTORY.varname,"");
      }
      try {
        Class<? extends Factory> factoryClass = (Class<? extends Factory>) Class.forName(factoryClassName);
        factoryInstance = factoryClass.newInstance();
      } catch (Exception e) {
        factoryInstance = new Factory() {
            @Override
            public ReplicationTask create(HCatClient client, HCatNotificationEvent event) {
              throw new IllegalStateException("Error instantiating ReplicationTask.Factory " +
                  HiveConf.ConfVars.HIVE_REPL_TASK_FACTORY.varname+"="+factoryClassName +
                  ". Call resetFactory() if you need to reset to a valid one.");
            }
        };
      }
    }
  }

  /**
   * Package scoped method used for testing - allows resetting the ReplicationTaskFactory used
   * @param factoryClass The new ReplicationTaskFactory to use.
   */
  public static void resetFactory(Class<? extends Factory> factoryClass) {
    if (factoryClass != null){
      factoryClassName = factoryClass.getName();
    } else {
      factoryClassName = null;
    }
    factoryInstance = null;
  }

  /**
   * Factory method to return appropriate subtype of ReplicationTask for given event
   * @param event HCatEventMessage returned by the notification subsystem
   * @return corresponding ReplicationTask
   */
  public static ReplicationTask create(HCatClient client, HCatNotificationEvent event){
    if (event == null){
      throw new IllegalArgumentException("event should not be null");
    }
    return getFactoryInstance(client).create(client,event);
  }

  // Primary entry point is a factory method instead of ctor
  // to allow for future ctor mutabulity in design
  protected ReplicationTask(HCatNotificationEvent event) {
    this.event = event;
  }

  /**
   * Returns the event that this ReplicationTask is attempting to replicate
   * @return underlying event
   */
  public HCatNotificationEvent getEvent(){
    return this.event;
  }

  /**
   * Returns true if the replication task in question needs to create staging
   * directories to complete its operation. This will mean that you will need
   * to copy these directories over to the destination warehouse for each
   * source-destination warehouse pair.
   * If this is true, you will need to call .withSrcStagingDirProvider(...)
   * and .withDstStagingDirProvider(...) before this ReplicationTask is usable
   */
  public abstract boolean needsStagingDirs();

  /**
   * Returns true if this ReplicationTask is prepared with all info it needs, and is
   * ready to be used
   */
  public boolean isActionable(){
    if (! this.needsStagingDirs()) {
      return true;
    }
    if ((srcStagingDirProvider != null) && (dstStagingDirProvider != null)){
      return true;
    }
    return false;
  }

  /**
   * See {@link org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider}
   * @param srcStagingDirProvider Staging Directory Provider for the source warehouse
   * @return this
   */
  public ReplicationTask withSrcStagingDirProvider(StagingDirectoryProvider srcStagingDirProvider){
    this.srcStagingDirProvider = srcStagingDirProvider;
    return this;
  }

  /**
   * See {@link org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider}
   * @param dstStagingDirProvider Staging Directory Provider for the destination warehouse
   * @return this replication task
   */
  public ReplicationTask withDstStagingDirProvider(StagingDirectoryProvider dstStagingDirProvider){
    this.dstStagingDirProvider = dstStagingDirProvider;
    return this;
  }

  /**
   * Allows a user to specify a table name mapping, where the the function provided maps the name of
   * the table in the source warehouse to the name of the table in the dest warehouse. It is expected
   * that if the mapping does not exist, it should return the same name sent in. Or, if the function
   * throws an IllegalArgumentException as well, a ReplicationTask will use the same key sent in.
   * That way, the default will then be that the destination db name is the same as the src db name
   *
   * If you want to use a Map<String,String> mapping instead of a Function<String,String>,
   * simply call this function as .withTableNameMapping(ReplicationUtils.mapBasedFunction(tableMap))
   * @param tableNameMapping
   * @return this replication task
   */
  public ReplicationTask withTableNameMapping(Function<String,String> tableNameMapping){
    this.tableNameMapping = tableNameMapping;
    return this;
  }

  /**
   * Allows a user to specify a db name mapping, where the the function provided maps the name of
   * the db in the source warehouse to the name of the db in the dest warehouse. It is expected
   * that if the mapping does not exist, it should return the same name sent in. Or, if the function
   * throws an IllegalArgumentException as well, a ReplicationTask will use the same key sent in.
   * That way, the default will then be that the destination db name is the same as the src db name
   *
   * If you want to use a Map<String,String> mapping instead of a Function<String,String>,
   * simply call this function as .withDbNameMapping(ReplicationUtils.mapBasedFunction(dbMap))
   * @param dbNameMapping
   * @return this replication task
   */
  public ReplicationTask withDbNameMapping(Function<String,String> dbNameMapping){
    this.dbNameMapping = dbNameMapping;
    return this;
  }

  protected void verifyActionable() {
    if (!this.isActionable()){
      throw new IllegalStateException("actionable command on task called when ReplicationTask is still not actionable.");
    }
  }

  /**
   * Returns a Iterable<Command> to send to a hive driver on the source warehouse
   *
   * If you *need* a List<Command> instead, you can use guava's
   * ImmutableList.copyOf(iterable) or Lists.newArrayList(iterable) to
   * get the underlying list, but this defeats the purpose of making this
   * interface an Iterable rather than a List, since it is very likely
   * that the number of Commands returned here will cause your process
   * to run OOM.
   */
  abstract public Iterable<? extends Command> getSrcWhCommands();

  /**
   * Returns a Iterable<Command> to send to a hive driver on the source warehouse
   *
   * If you *need* a List<Command> instead, you can use guava's
   * ImmutableList.copyOf(iterable) or Lists.newArrayList(iterable) to
   * get the underlying list, but this defeats the purpose of making this
   * interface an Iterable rather than a List, since it is very likely
   * that the number of Commands returned here will cause your process
   * to run OOM.
   */
  abstract public Iterable<? extends Command> getDstWhCommands();

  protected void validateEventType(HCatNotificationEvent event, String allowedEventType) {
    if (event == null || !allowedEventType.equals(event.getEventType())){
      throw new IllegalStateException(this.getClass().getName() + " valid only for " +
          allowedEventType + " events.");
    }
  }
}

