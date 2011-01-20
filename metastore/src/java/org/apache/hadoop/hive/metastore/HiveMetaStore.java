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

package org.apache.hadoop.hive.metastore;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_COMMENT;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.validateName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.IndexAlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.hooks.JDOConnectionURLHook;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRole;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;

/**
 * TODO:pc remove application logic to a separate interface.
 */
public class HiveMetaStore extends ThriftHiveMetastore {
  public static final Log LOG = LogFactory.getLog(
    HiveMetaStore.class);

  private static HadoopThriftAuthBridge.Server saslServer;

  public static class HMSHandler extends FacebookBase implements
      ThriftHiveMetastore.Iface {
    public static final Log LOG = HiveMetaStore.LOG;
    private static boolean createDefaultDB = false;
    private String rawStoreClassName;
    private final HiveConf hiveConf; // stores datastore (jpox) properties,
                                     // right now they come from jpox.properties

    private Warehouse wh; // hdfs warehouse
    private final ThreadLocal<RawStore> threadLocalMS =
      new ThreadLocal<RawStore>() {
      @Override
      protected synchronized RawStore initialValue() {
        return null;
      }
    };

    // Thread local configuration is needed as many threads could make changes
    // to the conf using the connection hook
    private final ThreadLocal<Configuration> threadLocalConf =
      new ThreadLocal<Configuration>() {
      @Override
      protected synchronized Configuration initialValue() {
        return null;
      }
    };

    // The next serial number to be assigned
    private boolean checkForDefaultDb;
    private static int nextSerialNum = 0;
    private static ThreadLocal<Integer> threadLocalId = new ThreadLocal() {
      @Override
      protected synchronized Object initialValue() {
        return new Integer(nextSerialNum++);
      }
    };

    // Used for retrying JDO calls on datastore failures
    private int retryInterval = 0;
    private int retryLimit = 0;
    private JDOConnectionURLHook urlHook = null;
    private String urlHookClassName = "";

    public static Integer get() {
      return threadLocalId.get();
    }

    public HMSHandler(String name) throws MetaException {
      super(name);
      hiveConf = new HiveConf(this.getClass());
      init();
    }

    public HMSHandler(String name, HiveConf conf) throws MetaException {
      super(name);
      hiveConf = conf;
      init();
    }

    public HiveConf getHiveConf() {
      return hiveConf;
    }

    private ClassLoader classLoader;
    private AlterHandler alterHandler;
    {
      classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = Configuration.class.getClassLoader();
      }
    }

    private boolean init() throws MetaException {
      rawStoreClassName = hiveConf.get("hive.metastore.rawstore.impl");
      checkForDefaultDb = hiveConf.getBoolean(
          "hive.metastore.checkForDefaultDb", true);
      String alterHandlerName = hiveConf.get("hive.metastore.alter.impl",
          HiveAlterHandler.class.getName());
      alterHandler = (AlterHandler) ReflectionUtils.newInstance(getClass(
          alterHandlerName, AlterHandler.class), hiveConf);
      wh = new Warehouse(hiveConf);

      retryInterval = HiveConf.getIntVar(hiveConf,
          HiveConf.ConfVars.METASTOREINTERVAL);
      retryLimit = HiveConf.getIntVar(hiveConf,
          HiveConf.ConfVars.METASTOREATTEMPTS);
      // Using the hook on startup ensures that the hook always has priority
      // over settings in *.xml. We can use hiveConf as only a single thread
      // will be calling the constructor.
      updateConnectionURL(hiveConf, null);

      createDefaultDB();
      return true;
    }

    private String addPrefix(String s) {
      return threadLocalId.get() + ": " + s;
    }

    /**
     * A Command is a closure used to pass a block of code from individual
     * functions to executeWithRetry, which centralizes connection error
     * handling. Command is parameterized on the return type of the function.
     *
     * The general transformation is:
     *
     * From:
     * String foo(int a) throws ExceptionB {
     *   <block of code>
     * }
     *
     * To:
     * String foo(final int a) throws ExceptionB {
     *   String ret =  null;
     *   try {
     *     ret = executeWithRetry(new Command<Boolean>() {
     *       String run(RawStore ms) {
     *         <block of code>
     *       }
     *     }
     *   } catch (ExceptionB e) {
     *     throw e;
     *   } catch (Exception e) {
     *     // Since run is only supposed to throw ExceptionB it could only
     *     // be a runtime exception
     *     throw (RuntimeException)e;
     *   }
     * }
     *
     * The catch blocks are used to ensure that the exceptions thrown by the
     * <block of code> follow the function definition.
     */
    private static class Command<T> {
      T run(RawStore ms) throws Exception {
        return null;
      }
    }

    private <T> T executeWithRetry(Command<T> cmd) throws Exception {
      T ret = null;

      boolean gotNewConnectUrl = false;
      boolean reloadConf = HiveConf.getBoolVar(hiveConf,
          HiveConf.ConfVars.METASTOREFORCERELOADCONF);

      if (reloadConf) {
        updateConnectionURL(getConf(), null);
      }

      int retryCount = 0;
      Exception caughtException = null;
      while(true) {
        try {
          RawStore ms = getMS(reloadConf || gotNewConnectUrl);
          ret = cmd.run(ms);
          break;
        } catch (javax.jdo.JDOFatalDataStoreException e) {
          caughtException = e;
        } catch (javax.jdo.JDODataStoreException e) {
          caughtException = e;
        }

        if (retryCount >= retryLimit) {
          throw caughtException;
        }

        assert(retryInterval >= 0);
        retryCount++;
        LOG.error(
            String.format(
                "JDO datastore error. Retrying metastore command " +
                "after %d ms (attempt %d of %d)", retryInterval, retryCount, retryLimit));
        Thread.sleep(retryInterval);
        // If we have a connection error, the JDO connection URL hook might
        // provide us with a new URL to access the datastore.
        String lastUrl = getConnectionURL(getConf());
        gotNewConnectUrl = updateConnectionURL(getConf(), lastUrl);
      }
      return ret;
    }

    private Configuration getConf() {
      Configuration conf = threadLocalConf.get();
      if (conf == null) {
        conf = new Configuration(hiveConf);
        threadLocalConf.set(conf);
      }
      return conf;
    }

    /**
     * Get a cached RawStore.
     *
     * @return
     * @throws MetaException
     */
    private RawStore getMS(boolean reloadConf) throws MetaException {
      RawStore ms = threadLocalMS.get();
      if (ms == null) {
        LOG.info(addPrefix("Opening raw store with implemenation class:"
            + rawStoreClassName));
        ms = (RawStore) ReflectionUtils.newInstance(getClass(rawStoreClassName,
            RawStore.class), getConf());
        threadLocalMS.set(ms);
        ms = threadLocalMS.get();
      }

      if (reloadConf) {
        ms.setConf(getConf());
      }

      return ms;
    }

    /**
     * Updates the connection URL in hiveConf using the hook
     * @return true if a new connection URL was loaded into the thread local
     * configuration
     */
    private boolean updateConnectionURL(Configuration conf, String badUrl)
        throws MetaException {
      String connectUrl = null;
      String currentUrl = getConnectionURL(conf);
      try {
        // We always call init because the hook name in the configuration could
        // have changed.
        initConnectionUrlHook();
        if (urlHook != null) {
          if (badUrl != null) {
            urlHook.notifyBadConnectionUrl(badUrl);
          }
          connectUrl = urlHook.getJdoConnectionUrl(hiveConf);
        }
      } catch (Exception e) {
        LOG.error("Exception while getting connection URL from the hook: " +
            e);
      }

      if (connectUrl != null && !connectUrl.equals(currentUrl)) {
        LOG.error(addPrefix(
            String.format("Overriding %s with %s",
                HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(),
                connectUrl)));
        conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(),
            connectUrl);
        return true;
      }
      return false;
    }

    private static String getConnectionURL(Configuration conf) {
      return conf.get(
          HiveConf.ConfVars.METASTORECONNECTURLKEY.toString(),"");
    }

    // Multiple threads could try to initialize at the same time.
    synchronized private void initConnectionUrlHook()
        throws ClassNotFoundException {

      String className =
        hiveConf.get(HiveConf.ConfVars.METASTORECONNECTURLHOOK.toString(), "").trim();
      if (className.equals("")){
        urlHookClassName = "";
        urlHook = null;
        return;
      }
      boolean urlHookChanged = !urlHookClassName.equals(className);
      if (urlHook == null || urlHookChanged) {
        urlHookClassName = className.trim();

        Class <?> urlHookClass = Class.forName(urlHookClassName, true,
            JavaUtils.getClassLoader());
        urlHook = (JDOConnectionURLHook) ReflectionUtils.newInstance(urlHookClass, null);
      }
      return;
    }

    private void createDefaultDB_core(RawStore ms) throws MetaException, InvalidObjectException {
      try {
        ms.getDatabase(DEFAULT_DATABASE_NAME);
      } catch (NoSuchObjectException e) {
        ms.createDatabase(
            new Database(DEFAULT_DATABASE_NAME, DEFAULT_DATABASE_COMMENT,
                wh.getDefaultDatabasePath(DEFAULT_DATABASE_NAME).toString(), null));
      }
      HMSHandler.createDefaultDB = true;
    }
    /**
     * create default database if it doesn't exist
     *
     * @throws MetaException
     */
    private void createDefaultDB() throws MetaException {
      if (HMSHandler.createDefaultDB || !checkForDefaultDb) {
        return;
      }

      try {
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            createDefaultDB_core(ms);
            return Boolean.TRUE;
          }
        });
      } catch (InvalidObjectException e) {
        throw new MetaException(e.getMessage());
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }

    }

    private Class<?> getClass(String rawStoreClassName, Class<?> class1)
        throws MetaException {
      try {
        return Class.forName(rawStoreClassName, true, classLoader);
      } catch (ClassNotFoundException e) {
        throw new MetaException(rawStoreClassName + " class not found");
      }
    }

    private void logStartFunction(String m) {
      LOG.info(threadLocalId.get().toString() + ": " + m);
    }

    private void logStartTableFunction(String f, String db, String tbl) {
      LOG.info(threadLocalId.get().toString() + ": " + f + " : db=" + db + " tbl=" + tbl);
    }

    private void logStartPartitionFunction(String f, String db, String tbl, List<String> partVals) {
      LOG.info(threadLocalId.get().toString() + ": " + f + " : db=" + db + " tbl=" + tbl
          + "[" + join(partVals, ",") + "]");
    }

    @Override
    public fb_status getStatus() {
      return fb_status.ALIVE;
    }

    @Override
    public void shutdown() {
      logStartFunction("Shutting down the object store...");
      RawStore ms = threadLocalMS.get();
      if (ms != null) {
        ms.shutdown();
      }
      System.exit(0);
    }

    private void create_database_core(RawStore ms, final Database db)
        throws AlreadyExistsException, InvalidObjectException, MetaException {
      if (!validateName(db.getName())) {
        throw new InvalidObjectException(db.getName() + " is not a valid database name");
      }
      boolean success = false;
      try {
        ms.openTransaction();
        if (null == db.getLocationUri()) {
          db.setLocationUri(wh.getDefaultDatabasePath(db.getName()).toString());
        }
        ms.createDatabase(db);
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else {
          wh.mkdirs(new Path(db.getLocationUri()));
        }
      }
    }

    public void create_database(final Database db)
        throws AlreadyExistsException, InvalidObjectException, MetaException {
      incrementCounter("create_database");
      logStartFunction("create_database: "
                       + db.getName() + " "
                       + db.getLocationUri() + " "
                       + db.getDescription());
      try {
        try {
          if(null != get_database(db.getName())) {
            throw new AlreadyExistsException("Database " + db.getName() + " already exists");
          }
        } catch (NoSuchObjectException e) {
          // expected
        }
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            create_database_core(ms, db);
            return Boolean.TRUE;
          }
        });
      } catch (AlreadyExistsException e) {
        throw e;
      } catch (InvalidObjectException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
    }

    public Database get_database(final String name) throws NoSuchObjectException,
        MetaException {
      incrementCounter("get_database");
      logStartFunction("get_database: " + name);

      Database db = null;
      try {
        db = executeWithRetry(new Command<Database>() {
          @Override
          Database run(RawStore ms) throws Exception {
            return ms.getDatabase(name);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return db;
    }

    public void alter_database(final String dbName, final Database db)
        throws NoSuchObjectException, TException, MetaException {
      incrementCounter("alter_database");
      logStartFunction("alter_database" + dbName);
      try {
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            return ms.alterDatabase(dbName, db);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException) e;
      }
    }

    private void drop_database_core(RawStore ms,
        final String name, final boolean deleteData)
        throws NoSuchObjectException, InvalidOperationException, MetaException {
      boolean success = false;
      Database db = null;
      try {
        ms.openTransaction();
        db = ms.getDatabase(name);
        if (!get_all_tables(db.getName()).isEmpty()) {
          throw new InvalidOperationException("Database " + db.getName() + " is not empty");
        }
        if (ms.dropDatabase(name)) {
          success = ms.commitTransaction();
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (deleteData) {
          wh.deleteDir(new Path(db.getLocationUri()), true);
          // it is not a terrible thing even if the data is not deleted
        }
      }
    }

    public void drop_database(final String dbName, final boolean deleteData)
        throws NoSuchObjectException, InvalidOperationException, MetaException {
      incrementCounter("drop_database");
      logStartFunction("drop_database: " + dbName);
      if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(dbName)) {
        throw new MetaException("Can not drop default database");
      }

      try {
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            drop_database_core(ms, dbName, deleteData);
            return Boolean.TRUE;
          }
        });
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (InvalidOperationException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
    }

    public List<String> get_databases(final String pattern) throws MetaException {
      incrementCounter("get_databases");
      logStartFunction("get_databases: " + pattern);

      List<String> ret = null;
      try {
        ret = executeWithRetry(new Command<List<String>>() {
          @Override
          List<String> run(RawStore ms) throws Exception {
            return ms.getDatabases(pattern);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    public List<String> get_all_databases() throws MetaException {
      incrementCounter("get_all_databases");
      logStartFunction("get_all_databases");

      List<String> ret = null;
      try {
        ret = executeWithRetry(new Command<List<String>>() {
          @Override
          List<String> run(RawStore ms) throws Exception {
            return ms.getAllDatabases();
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    private void create_type_core(final RawStore ms, final Type type)
    throws AlreadyExistsException, MetaException, InvalidObjectException {
      if (!MetaStoreUtils.validateName(type.getName())) {
        throw new InvalidObjectException("Invalid type name");
      }

      boolean success = false;
      try {
        ms.openTransaction();
        if (is_type_exists(ms, type.getName())) {
          throw new AlreadyExistsException("Type " + type.getName() + " already exists");
        }
        ms.createType(type);
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
      }
    }

    public boolean create_type(final Type type) throws AlreadyExistsException,
        MetaException, InvalidObjectException {
      incrementCounter("create_type");
      logStartFunction("create_type: " + type.getName());
      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            create_type_core(ms, type);
            return Boolean.TRUE;
          }
        });
      } catch (AlreadyExistsException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (InvalidObjectException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }

      return ret.booleanValue();
    }

    public Type get_type(final String name) throws MetaException, NoSuchObjectException {
      incrementCounter("get_type");
      logStartFunction("get_type: " + name);

      Type ret;
      try {
        ret = executeWithRetry(new Command<Type>() {
          @Override
          Type run(RawStore ms) throws Exception {
            Type type = ms.getType(name);
            if (null == type) {
              throw new NoSuchObjectException("Type \"" + name + "\" not found.");
            }
            return type;
          }
        });
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    private boolean is_type_exists(RawStore ms, String typeName)
        throws MetaException {
      return (ms.getType(typeName) != null);
    }

    private void drop_type_core(final RawStore ms, String typeName)
        throws NoSuchObjectException, MetaException {
      boolean success = false;
      try {
        ms.openTransaction();
        // drop any partitions
        if (!is_type_exists(ms, typeName)) {
          throw new NoSuchObjectException(typeName + " doesn't exist");
        }
        if (!ms.dropType(typeName)) {
          throw new MetaException("Unable to drop type " + typeName);
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
      }
    }


    public boolean drop_type(final String name) throws MetaException {
      incrementCounter("drop_type");
      logStartFunction("drop_type: " + name);

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            // TODO:pc validate that there are no types that refer to this
            return ms.dropType(name);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    public Map<String, Type> get_type_all(String name) throws MetaException {
      incrementCounter("get_type_all");
      // TODO Auto-generated method stub
      logStartFunction("get_type_all: " + name);
      throw new MetaException("Not yet implemented");
    }

    private void create_table_core(final RawStore ms, final Table tbl)
        throws AlreadyExistsException, MetaException, InvalidObjectException {

      if (!MetaStoreUtils.validateName(tbl.getTableName())
          || !MetaStoreUtils.validateColNames(tbl.getSd().getCols())
          || (tbl.getPartitionKeys() != null && !MetaStoreUtils
              .validateColNames(tbl.getPartitionKeys()))) {
        throw new InvalidObjectException(tbl.getTableName()
            + " is not a valid object name");
      }

      Path tblPath = null;
      boolean success = false, madeDir = false;
      try {
        ms.openTransaction();

        // get_table checks whether database exists, it should be moved here
        if (is_table_exists(ms, tbl.getDbName(), tbl.getTableName())) {
          throw new AlreadyExistsException("Table " + tbl.getTableName()
              + " already exists");
        }

        if (!TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
          if (tbl.getSd().getLocation() == null
            || tbl.getSd().getLocation().isEmpty()) {
            tblPath = wh.getDefaultTablePath(
              tbl.getDbName(), tbl.getTableName());
          } else {
            if (!isExternal(tbl) && !MetaStoreUtils.isNonNativeTable(tbl)) {
              LOG.warn("Location: " + tbl.getSd().getLocation()
                + " specified for non-external table:" + tbl.getTableName());
            }
            tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
          }
          tbl.getSd().setLocation(tblPath.toString());
        }

        if (tblPath != null) {
          if (!wh.isDir(tblPath)) {
            if (!wh.mkdirs(tblPath)) {
              throw new MetaException(tblPath
                  + " is not a directory or unable to create one");
            }
            madeDir = true;
          }
        }

        // set create time
        long time = System.currentTimeMillis() / 1000;
        tbl.setCreateTime((int) time);
        if (tbl.getParameters() == null ||
            tbl.getParameters().get(Constants.DDL_TIME) == null) {
          tbl.putToParameters(Constants.DDL_TIME, Long.toString(time));
        }
        ms.createTable(tbl);
        success = ms.commitTransaction();

      } finally {
        if (!success) {
          ms.rollbackTransaction();
          if (madeDir) {
            wh.deleteDir(tblPath, true);
          }
        }
      }
    }

    public void create_table(final Table tbl) throws AlreadyExistsException,
        MetaException, InvalidObjectException {
      incrementCounter("create_table");
      logStartFunction("create_table: db=" + tbl.getDbName() + " tbl="
          + tbl.getTableName());
      try {
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            create_table_core(ms, tbl);
            return Boolean.TRUE;
          }
        });
      } catch (AlreadyExistsException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (InvalidObjectException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
    }

    private boolean is_table_exists(RawStore ms, String dbname, String name)
        throws MetaException {
      return (ms.getTable(dbname, name) != null);
    }

    private void drop_table_core(final RawStore ms, final String dbname,
        final String name, final boolean deleteData)
        throws NoSuchObjectException, MetaException {

      boolean success = false;
      boolean isExternal = false;
      Path tblPath = null;
      Table tbl = null;
      isExternal = false;
      boolean isIndexTable = false;
      try {
        ms.openTransaction();
        // drop any partitions
        tbl = get_table(dbname, name);
        if (tbl == null) {
          throw new NoSuchObjectException(name + " doesn't exist");
        }
        if (tbl.getSd() == null) {
          throw new MetaException("Table metadata is corrupted");
        }

        isIndexTable = isIndexTable(tbl);
        if (isIndexTable) {
          throw new RuntimeException(
              "The table " + name + " is an index table. Please do drop index instead.");
        }

        if (!isIndexTable) {
          try {
            List<Index> indexes = ms.getIndexes(dbname, name, Short.MAX_VALUE);
            while(indexes != null && indexes.size()>0) {
              for (Index idx : indexes) {
                this.drop_index_by_name(dbname, name, idx.getIndexName(), true);
              }
              indexes = ms.getIndexes(dbname, name, Short.MAX_VALUE);
            }
          } catch (TException e) {
            throw new MetaException(e.getMessage());
          }
        }
        isExternal = isExternal(tbl);
        if (tbl.getSd().getLocation() != null) {
          tblPath = new Path(tbl.getSd().getLocation());
        }

        if (!ms.dropTable(dbname, name)) {
          throw new MetaException("Unable to drop table");
        }
        tbl = null; // table collections disappear after dropping
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (deleteData && (tblPath != null) && !isExternal) {
          wh.deleteDir(tblPath, true);
          // ok even if the data is not deleted
        }
      }
    }

    public void drop_table(final String dbname, final String name, final boolean deleteData)
        throws NoSuchObjectException, MetaException {
      incrementCounter("drop_table");
      logStartTableFunction("drop_table", dbname, name);

      try {
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            drop_table_core(ms, dbname, name, deleteData);
            return Boolean.TRUE;
          }
        });
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }

    }

    /**
     * Is this an external table?
     *
     * @param table
     *          Check if this table is external.
     * @return True if the table is external, otherwise false.
     */
    private boolean isExternal(Table table) {
      return MetaStoreUtils.isExternalTable(table);
    }

    private boolean isIndexTable (Table table) {
      return MetaStoreUtils.isIndexTable(table);
    }

    public Table get_table(final String dbname, final String name) throws MetaException,
        NoSuchObjectException {
      Table t = null;
      incrementCounter("get_table");
      logStartTableFunction("get_table", dbname, name);
      try {
        t = executeWithRetry(new Command<Table>() {
          @Override
          Table run(RawStore ms) throws Exception {
            Table t = ms.getTable(dbname, name);
            if (t == null) {
              throw new NoSuchObjectException(dbname + "." + name
                  + " table not found");
            }
            return t;
          }
        });
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return t;
    }

    public boolean set_table_parameters(String dbname, String name,
        Map<String, String> params) throws NoSuchObjectException, MetaException {
      incrementCounter("set_table_parameters");
      logStartTableFunction("set_table_parameters", dbname, name);
      // TODO Auto-generated method stub
      return false;
    }

    private Partition append_partition_common(RawStore ms, String dbName, String tableName,
        List<String> part_vals) throws InvalidObjectException,
        AlreadyExistsException, MetaException {

      Partition part = new Partition();
      boolean success = false, madeDir = false;
      Path partLocation = null;
      try {
        ms.openTransaction();
        part = new Partition();
        part.setDbName(dbName);
        part.setTableName(tableName);
        part.setValues(part_vals);

        Table tbl = ms.getTable(part.getDbName(), part.getTableName());
        if (tbl == null) {
          throw new InvalidObjectException(
              "Unable to add partition because table or database do not exist");
        }

        part.setSd(tbl.getSd());
        partLocation = new Path(tbl.getSd().getLocation(), Warehouse
            .makePartName(tbl.getPartitionKeys(), part_vals));
        part.getSd().setLocation(partLocation.toString());

        Partition old_part = null;
        try {
          old_part = get_partition(part.getDbName(), part
            .getTableName(), part.getValues());
        } catch (NoSuchObjectException e) {
          // this means there is no existing partition
          old_part = null;
        }
        if (old_part != null) {
          throw new AlreadyExistsException("Partition already exists:" + part);
        }

        if (!wh.isDir(partLocation)) {
          if (!wh.mkdirs(partLocation)) {
            throw new MetaException(partLocation
                + " is not a directory or unable to create one");
          }
          madeDir = true;
        }

        // set create time
        long time = System.currentTimeMillis() / 1000;
        part.setCreateTime((int) time);
        part.putToParameters(Constants.DDL_TIME, Long.toString(time));

        success = ms.addPartition(part);
        if (success) {
          success = ms.commitTransaction();
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
          if (madeDir) {
            wh.deleteDir(partLocation, true);
          }
        }
      }
      return part;
    }

    public Partition append_partition(final String dbName, final String tableName,
        final List<String> part_vals) throws InvalidObjectException,
        AlreadyExistsException, MetaException {
      incrementCounter("append_partition");
      logStartPartitionFunction("append_partition", dbName, tableName, part_vals);
      if (LOG.isDebugEnabled()) {
        for (String part : part_vals) {
          LOG.debug(part);
        }
      }

      Partition ret = null;
      try {
        ret = executeWithRetry(new Command<Partition>() {
          @Override
          Partition run(RawStore ms) throws Exception {
            return append_partition_common(ms, dbName, tableName, part_vals);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (InvalidObjectException e) {
        throw e;
      } catch (AlreadyExistsException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    private int add_partitions_core(final RawStore ms, final List<Partition> parts)
        throws MetaException, InvalidObjectException, AlreadyExistsException {
      String db = parts.get(0).getDbName();
      String tbl = parts.get(0).getTableName();
      logStartTableFunction("add_partitions", db, tbl);
      boolean success = false;
      try {
        ms.openTransaction();
        for (Partition part : parts) {
          add_partition(part);
        }
        success = true;
        ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
      }
      return parts.size();
    }

    public int add_partitions(final List<Partition> parts) throws MetaException,
        InvalidObjectException, AlreadyExistsException {
      incrementCounter("add_partition");
      if (parts.size() == 0) {
        return 0;
      }

      Integer ret = null;
      try {
        ret = executeWithRetry(new Command<Integer>() {
          @Override
          Integer run(RawStore ms) throws Exception {
            int ret = add_partitions_core(ms, parts);
            return Integer.valueOf(ret);
          }
        });
      } catch (InvalidObjectException e) {
        throw e;
      } catch (AlreadyExistsException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    private Partition add_partition_core(final RawStore ms, final Partition part)
        throws InvalidObjectException, AlreadyExistsException, MetaException {
      boolean success = false, madeDir = false;
      Path partLocation = null;
      try {
        ms.openTransaction();
        Partition old_part = null;
        try {
          old_part = get_partition(part.getDbName(), part
            .getTableName(), part.getValues());
        } catch(NoSuchObjectException e) {
          // this means there is no existing partition
          old_part = null;
        }
        if (old_part != null) {
          throw new AlreadyExistsException("Partition already exists:" + part);
        }
        Table tbl = ms.getTable(part.getDbName(), part.getTableName());
        if (tbl == null) {
          throw new InvalidObjectException(
              "Unable to add partition because table or database do not exist");
        }

        String partLocationStr = part.getSd().getLocation();
        if (partLocationStr == null || partLocationStr.isEmpty()) {
          // set default location if not specified
          partLocation = new Path(tbl.getSd().getLocation(), Warehouse
              .makePartName(tbl.getPartitionKeys(), part.getValues()));

        } else {
          partLocation = wh.getDnsPath(new Path(partLocationStr));
        }

        part.getSd().setLocation(partLocation.toString());

        // Check to see if the directory already exists before calling mkdirs()
        // because if the file system is read-only, mkdirs will throw an
        // exception even if the directory already exists.
        if (!wh.isDir(partLocation)) {
          if (!wh.mkdirs(partLocation)) {
            throw new MetaException(partLocation
                + " is not a directory or unable to create one");
          }
          madeDir = true;
        }

        // set create time
        long time = System.currentTimeMillis() / 1000;
        part.setCreateTime((int) time);
        if (part.getParameters() == null ||
            part.getParameters().get(Constants.DDL_TIME) == null) {
          part.putToParameters(Constants.DDL_TIME, Long.toString(time));
        }
        success = ms.addPartition(part) && ms.commitTransaction();

      } finally {
        if (!success) {
          ms.rollbackTransaction();
          if (madeDir) {
            wh.deleteDir(partLocation, true);
          }
        }
      }
      return part;
    }

    public Partition add_partition(final Partition part)
        throws InvalidObjectException, AlreadyExistsException, MetaException {
      incrementCounter("add_partition");
      logStartTableFunction("add_partition", part.getDbName(), part.getTableName());

      Partition ret = null;
      try {
        ret = executeWithRetry(new Command<Partition>() {
          @Override
          Partition run(RawStore ms) throws Exception {
            return add_partition_core(ms, part);
          }
        });
      } catch (InvalidObjectException e) {
        throw e;
      } catch (AlreadyExistsException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;

    }

    private boolean drop_partition_common(RawStore ms, String db_name, String tbl_name,
        List<String> part_vals, final boolean deleteData)
    throws MetaException, NoSuchObjectException {

      boolean success = false;
      Path partPath = null;
      Table tbl = null;
      Partition part = null;
      boolean isArchived = false;
      Path archiveParentDir = null;

      try {
        ms.openTransaction();
        part = get_partition(db_name, tbl_name, part_vals);

        if (part == null) {
          throw new NoSuchObjectException("Partition doesn't exist. "
              + part_vals);
        }

        isArchived = MetaStoreUtils.isArchived(part);
        if (isArchived) {
          archiveParentDir = MetaStoreUtils.getOriginalLocation(part);
        }
        if (part.getSd() == null || part.getSd().getLocation() == null) {
          throw new MetaException("Partition metadata is corrupted");
        }
        if (!ms.dropPartition(db_name, tbl_name, part_vals)) {
          throw new MetaException("Unable to drop partition");
        }
        success = ms.commitTransaction();
        partPath = new Path(part.getSd().getLocation());
        tbl = get_table(db_name, tbl_name);
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (deleteData && ((partPath != null) || (archiveParentDir != null))) {
          if (tbl != null && !isExternal(tbl)) {
            // Archived partitions have har:/to_har_file as their location.
            // The original directory was saved in params
            if (isArchived) {
              assert(archiveParentDir != null);
              wh.deleteDir(archiveParentDir, true);
            } else {
              assert(partPath != null);
              wh.deleteDir(partPath, true);
            }
            // ok even if the data is not deleted
          }
        }
      }
      return true;
    }
    public boolean drop_partition(final String db_name, final String tbl_name,
        final List<String> part_vals, final boolean deleteData)
        throws NoSuchObjectException, MetaException, TException {
      incrementCounter("drop_partition");
      logStartPartitionFunction("drop_partition", db_name, tbl_name, part_vals);
      LOG.info("Partition values:" + part_vals);

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            return Boolean.valueOf(
                drop_partition_common(ms, db_name, tbl_name, part_vals, deleteData));
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret.booleanValue();

    }

    public Partition get_partition(final String db_name, final String tbl_name,
        final List<String> part_vals) throws MetaException, NoSuchObjectException {
      incrementCounter("get_partition");
      logStartPartitionFunction("get_partition", db_name, tbl_name, part_vals);

      Partition ret = null;
      try {
        ret = executeWithRetry(new Command<Partition>() {
          @Override
          Partition run(RawStore ms) throws Exception {
            return ms.getPartition(db_name, tbl_name, part_vals);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }
    
    @Override
    public Partition get_partition_with_auth(final String db_name,
        final String tbl_name, final List<String> part_vals,
        final String user_name, final List<String> group_names)
        throws MetaException, NoSuchObjectException, TException {
      incrementCounter("get_partition_with_auth");
      logStartPartitionFunction("get_partition_with_auth", db_name, tbl_name,
          part_vals);

      Partition ret = null;
      try {
        ret = executeWithRetry(new Command<Partition>() {
          @Override
          Partition run(RawStore ms) throws Exception {
            return ms.getPartitionWithAuth(db_name, tbl_name, part_vals,
                user_name, group_names);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (Exception e) {
        assert (e instanceof RuntimeException);
        throw (RuntimeException) e;
      }
      return ret;
    }

    public List<Partition> get_partitions(final String db_name, final String tbl_name,
        final short max_parts) throws NoSuchObjectException, MetaException {
      incrementCounter("get_partitions");
      logStartTableFunction("get_partitions", db_name, tbl_name);

      List<Partition> ret = null;
      try {
        ret = executeWithRetry(new Command<List<Partition>>() {
          @Override
          List<Partition> run(RawStore ms) throws Exception {
            return ms.getPartitions(db_name, tbl_name, max_parts);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;

    }
    
    @Override
    public List<Partition> get_partitions_with_auth(final String dbName,
        final String tblName, final short maxParts, final String userName,
        final List<String> groupNames) throws NoSuchObjectException,
        MetaException, TException {
      incrementCounter("get_partitions_with_auth");
      logStartTableFunction("get_partitions_with_auth", dbName, tblName);

      List<Partition> ret = null;
      try {
        ret = executeWithRetry(new Command<List<Partition>>() {
          @Override
          List<Partition> run(RawStore ms) throws Exception {
            return ms.getPartitionsWithAuth(dbName, tblName, maxParts,
                userName, groupNames);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (Exception e) {
        assert (e instanceof RuntimeException);
        throw (RuntimeException) e;
      }
      return ret;

    }

    public List<String> get_partition_names(final String db_name, final String tbl_name,
        final short max_parts) throws MetaException {
      incrementCounter("get_partition_names");
      logStartTableFunction("get_partition_names", db_name, tbl_name);

      List<String> ret = null;
      try {
        ret = executeWithRetry(new Command<List<String>>() {
          @Override
          List<String> run(RawStore ms) throws Exception {
            return ms.listPartitionNames(db_name, tbl_name, max_parts);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    private void alter_partition_core(final RawStore ms, final String db_name,
        final String tbl_name, final Partition new_part)
        throws InvalidOperationException, MetaException, TException {
      try {
        // Set DDL time to now if not specified
        if (new_part.getParameters() == null ||
            new_part.getParameters().get(Constants.DDL_TIME) == null ||
            Integer.parseInt(new_part.getParameters().get(Constants.DDL_TIME)) == 0) {
          new_part.putToParameters(Constants.DDL_TIME, Long.toString(System
              .currentTimeMillis() / 1000));
        }
        ms.alterPartition(db_name, tbl_name, new_part);
      } catch (InvalidObjectException e) {
        throw new InvalidOperationException("alter is not possible");
      }
    }

    public void alter_partition(final String db_name, final String tbl_name,
        final Partition new_part) throws InvalidOperationException, MetaException,
        TException {
      incrementCounter("alter_partition");
      logStartTableFunction("alter_partition", db_name, tbl_name);
      LOG.info("Partition values:" + new_part.getValues());

      try {
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            alter_partition_core(ms, db_name, tbl_name, new_part);
            return Boolean.TRUE;
          }
        });
      } catch (InvalidOperationException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return;
    }

    public boolean create_index(Index index_def)
        throws IndexAlreadyExistsException, MetaException {
      incrementCounter("create_index");
      // TODO Auto-generated method stub
      throw new MetaException("Not yet implemented");
    }

    public void alter_index(final String dbname, final String base_table_name, final String index_name, final Index newIndex)
        throws InvalidOperationException, MetaException {
      incrementCounter("alter_index");
      logStartFunction("alter_index: db=" + dbname + " base_tbl=" + base_table_name
          + " idx=" + index_name + " newidx=" + newIndex.getIndexName());
      newIndex.putToParameters(Constants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));

      try {
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            ms.alterIndex(dbname, base_table_name, index_name, newIndex);
            return Boolean.TRUE;
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (InvalidOperationException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return;
    }

    public String getVersion() throws TException {
      incrementCounter("getVersion");
      logStartFunction("getVersion");
      return "3.0";
    }

    public void alter_table(final String dbname, final String name, final Table newTable)
        throws InvalidOperationException, MetaException {
      incrementCounter("alter_table");
      logStartFunction("alter_table: db=" + dbname + " tbl=" + name
          + " newtbl=" + newTable.getTableName());

      // Update the time if it hasn't been specified.
      if (newTable.getParameters() == null ||
          newTable.getParameters().get(Constants.DDL_TIME) == null) {
        newTable.putToParameters(Constants.DDL_TIME, Long.toString(System
            .currentTimeMillis() / 1000));
      }

      try {
        executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            alterHandler.alterTable(ms, wh, dbname, name, newTable);
            return Boolean.TRUE;
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (InvalidOperationException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
    }

    public List<String> get_tables(final String dbname, final String pattern)
        throws MetaException {
      incrementCounter("get_tables");
      logStartFunction("get_tables: db=" + dbname + " pat=" + pattern);

      List<String> ret;
      try {
        ret = executeWithRetry(new Command<List<String>>() {
          @Override
          List<String> run(RawStore ms) throws Exception {
            return ms.getTables(dbname, pattern);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    public List<String> get_all_tables(final String dbname) throws MetaException {
      incrementCounter("get_all_tables");
      logStartFunction("get_all_tables: db=" + dbname);

      List<String> ret;
      try {
        ret = executeWithRetry(new Command<List<String>>() {
          @Override
          List<String> run(RawStore ms) throws Exception {
            return ms.getAllTables(dbname);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    public List<FieldSchema> get_fields(String db, String tableName)
        throws MetaException, UnknownTableException, UnknownDBException {
      incrementCounter("get_fields");
      logStartFunction("get_fields: db=" + db + "tbl=" + tableName);
      String[] names = tableName.split("\\.");
      String base_table_name = names[0];

      Table tbl;
      try {
        tbl = get_table(db, base_table_name);
      } catch (NoSuchObjectException e) {
        throw new UnknownTableException(e.getMessage());
      }
      boolean getColsFromSerDe = SerDeUtils.shouldGetColsFromSerDe(
        tbl.getSd().getSerdeInfo().getSerializationLib());
      if (!getColsFromSerDe) {
        return tbl.getSd().getCols();
      } else {
        try {
          Deserializer s = MetaStoreUtils.getDeserializer(hiveConf, tbl);
          return MetaStoreUtils.getFieldsFromDeserializer(tableName, s);
        } catch (SerDeException e) {
          StringUtils.stringifyException(e);
          throw new MetaException(e.getMessage());
        }
      }
    }

    /**
     * Return the schema of the table. This function includes partition columns
     * in addition to the regular columns.
     *
     * @param db
     *          Name of the database
     * @param tableName
     *          Name of the table
     * @return List of columns, each column is a FieldSchema structure
     * @throws MetaException
     * @throws UnknownTableException
     * @throws UnknownDBException
     */
    public List<FieldSchema> get_schema(String db, String tableName)
        throws MetaException, UnknownTableException, UnknownDBException {
      incrementCounter("get_schema");
      logStartFunction("get_schema: db=" + db + "tbl=" + tableName);
      String[] names = tableName.split("\\.");
      String base_table_name = names[0];

      Table tbl;
      try {
        tbl = get_table(db, base_table_name);
      } catch (NoSuchObjectException e) {
        throw new UnknownTableException(e.getMessage());
      }
      List<FieldSchema> fieldSchemas = get_fields(db, base_table_name);

      if (tbl == null || fieldSchemas == null) {
        throw new UnknownTableException(tableName + " doesn't exist");
      }

      if (tbl.getPartitionKeys() != null) {
        // Combine the column field schemas and the partition keys to create the
        // whole schema
        fieldSchemas.addAll(tbl.getPartitionKeys());
      }
      return fieldSchemas;
    }

    public String getCpuProfile(int profileDurationInSec) throws TException {
      return "";
    }

    /**
     * Returns the value of the given configuration variable name. If the
     * configuration variable with the given name doesn't exist, or if there
     * were an exception thrown while retrieving the variable, or if name is
     * null, defaultValue is returned.
     */
    public String get_config_value(String name, String defaultValue)
        throws TException, ConfigValSecurityException {
      incrementCounter("get_config_value");
      logStartFunction("get_config_value: name=" + name + " defaultValue="
          + defaultValue);
      if (name == null) {
        return defaultValue;
      }
      // Allow only keys that start with hive.*, hdfs.*, mapred.* for security
      // i.e. don't allow access to db password
      if (!Pattern.matches("(hive|hdfs|mapred).*", name)) {
        throw new ConfigValSecurityException("For security reasons, the "
            + "config key " + name + " cannot be accessed");
      }

      String toReturn = defaultValue;
      try {
        toReturn = hiveConf.get(name, defaultValue);
      } catch (RuntimeException e) {
        LOG.error(threadLocalId.get().toString() + ": "
            + "RuntimeException thrown in get_config_value - msg: "
            + e.getMessage() + " cause: " + e.getCause());
      }
      return toReturn;
    }

    private List<String> getPartValsFromName(RawStore ms, String dbName, String tblName,
        String partName) throws MetaException, InvalidObjectException {
      // Unescape the partition name
      LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);

      // getPartition expects partition values in a list. use info from the
      // table to put the partition column values in order
      Table t = ms.getTable(dbName, tblName);
      if (t == null) {
        throw new InvalidObjectException(dbName + "." + tblName
            + " table not found");
      }

      List<String> partVals = new ArrayList<String>();
      for(FieldSchema field : t.getPartitionKeys()) {
        String key = field.getName();
        String val = hm.get(key);
        if (val == null) {
          throw new InvalidObjectException("incomplete partition name - missing " + key);
        }
        partVals.add(val);
      }
      return partVals;
    }

    private Partition get_partition_by_name_core(final RawStore ms, final String db_name,
        final String tbl_name, final String part_name)
        throws MetaException, NoSuchObjectException, TException {
      List<String> partVals = null;
      try {
        partVals = getPartValsFromName(ms, db_name, tbl_name, part_name);
      } catch (InvalidObjectException e) {
        throw new NoSuchObjectException(e.getMessage());
      }
      Partition p = ms.getPartition(db_name, tbl_name, partVals);

      if (p == null) {
        throw new NoSuchObjectException(db_name + "." + tbl_name
            + " partition (" + part_name + ") not found");
      }
      return p;
    }

    public Partition get_partition_by_name(final String db_name,final String tbl_name,
        final String part_name) throws MetaException, NoSuchObjectException, TException {

      incrementCounter("get_partition_by_name");
      logStartFunction("get_partition_by_name: db=" + db_name + " tbl="
          + tbl_name + " part=" + part_name);

      Partition ret = null;

      try {
        ret = executeWithRetry(new Command<Partition>() {
          @Override
          Partition run(RawStore ms) throws Exception {
            return get_partition_by_name_core(ms, db_name, tbl_name, part_name);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    public Partition append_partition_by_name(final String db_name, final String tbl_name,
        final String part_name) throws InvalidObjectException,
        AlreadyExistsException, MetaException, TException {
      incrementCounter("append_partition_by_name");
      logStartFunction("append_partition_by_name: db=" + db_name + " tbl="
          + tbl_name + " part=" + part_name);

      Partition ret = null;
      try {
        ret = executeWithRetry(new Command<Partition>() {
          @Override
          Partition run(RawStore ms) throws Exception {
            List<String> partVals = getPartValsFromName(ms, db_name, tbl_name, part_name);
            return append_partition_common(ms, db_name, tbl_name, partVals);
          }
        });
      } catch (InvalidObjectException e) {
        throw e;
      } catch (AlreadyExistsException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    private boolean drop_partition_by_name_core(final RawStore ms,
        final String db_name, final String tbl_name, final String part_name,
        final boolean deleteData) throws NoSuchObjectException,
        MetaException, TException {

      List<String> partVals = null;
      try {
        partVals = getPartValsFromName(ms, db_name, tbl_name, part_name);
      } catch (InvalidObjectException e) {
        throw new NoSuchObjectException(e.getMessage());
      }

      return drop_partition_common(ms, db_name, tbl_name, partVals, deleteData);
    }

    @Override
    public boolean drop_partition_by_name(final String db_name, final String tbl_name,
        final String part_name, final boolean deleteData) throws NoSuchObjectException,
        MetaException, TException {
      incrementCounter("drop_partition_by_name");
      logStartFunction("drop_partition_by_name: db=" + db_name + " tbl="
          + tbl_name + " part=" + part_name);

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            return drop_partition_by_name_core(ms, db_name, tbl_name,
                part_name, deleteData);
          }
        });
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }

      return ret.booleanValue();
    }

    @Override
    public List<Partition> get_partitions_ps(final String db_name,
        final String tbl_name, final List<String> part_vals,
        final short max_parts) throws MetaException, TException {
      incrementCounter("get_partitions_ps");
      logStartPartitionFunction("get_partitions_ps", db_name, tbl_name,
          part_vals);

      return this.get_partitions_ps_with_auth(db_name, tbl_name, part_vals,
          max_parts, null, null);
    }
    
    @Override
    public List<Partition> get_partitions_ps_with_auth(final String db_name,
        final String tbl_name, final List<String> part_vals,
        final short max_parts, final String userName,
        final List<String> groupNames) throws MetaException, TException {
      incrementCounter("get_partitions_ps");
      logStartPartitionFunction("get_partitions_ps", db_name, tbl_name,
          part_vals);
      List<Partition> parts = null;
      List<Partition> matchingParts = new ArrayList<Partition>();

      // This gets all the partitions and then filters based on the specified
      // criteria. An alternative approach would be to get all the partition
      // names, do the filtering on the names, and get the partition for each
      // of the names. that match.

      try {
         parts = get_partitions(db_name, tbl_name, (short) -1);
      } catch (NoSuchObjectException e) {
        throw new MetaException(e.getMessage());
      }

      for (Partition p : parts) {
        if (MetaStoreUtils.pvalMatches(part_vals, p.getValues())) {
          matchingParts.add(p);
        }
      }

      return matchingParts;
    }

    @Override
    public List<String> get_partition_names_ps(final String db_name,
        final String tbl_name, final List<String> part_vals, final short max_parts)
        throws MetaException, TException {
      incrementCounter("get_partition_names_ps");
      logStartPartitionFunction("get_partitions_names_ps", db_name, tbl_name, part_vals);
      Table t;
      try {
        t = get_table(db_name, tbl_name);
      } catch (NoSuchObjectException e) {
        throw new MetaException(e.getMessage());
      }

     List<String> partNames = get_partition_names(db_name, tbl_name, max_parts);
     List<String> filteredPartNames = new ArrayList<String>();

      for(String name : partNames) {
        LinkedHashMap<String, String> spec = Warehouse.makeSpecFromName(name);
        List<String> vals = new ArrayList<String>();
        // Since we are iterating through a LinkedHashMap, iteration should
        // return the partition values in the correct order for comparison.
        for (String val : spec.values()) {
          vals.add(val);
        }
        if (MetaStoreUtils.pvalMatches(part_vals, vals)) {
          filteredPartNames.add(name);
        }
      }

      return filteredPartNames;
    }

    @Override
    public List<String> partition_name_to_vals(String part_name)
        throws MetaException, TException {
      if (part_name.length() == 0) {
        return new ArrayList<String>();
      }
      LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(part_name);
      List<String> part_vals = new ArrayList<String>();
      part_vals.addAll(map.values());
      return part_vals;
    }

    @Override
    public Map<String, String> partition_name_to_spec(String part_name) throws MetaException,
        TException {
      if (part_name.length() == 0) {
        return new HashMap<String, String>();
      }
      return Warehouse.makeSpecFromName(part_name);
    }

    @Override
    public Index add_index(final Index newIndex, final Table indexTable) throws InvalidObjectException,
        AlreadyExistsException, MetaException, TException {
      incrementCounter("add_partition");
      logStartFunction("add_index: db=" + newIndex.getDbName() + " tbl="
          + newIndex.getOrigTableName() + " index=" + newIndex.getIndexName());
      Index ret = null;
      try {
        ret = executeWithRetry(new Command<Index>() {
          @Override
          Index run(RawStore ms) throws Exception {
            return add_index_core(ms, newIndex, indexTable);
          }
        });
      } catch (InvalidObjectException e) {
        throw e;
      } catch (AlreadyExistsException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    private Index add_index_core(final RawStore ms, final Index index, final Table indexTable)
        throws InvalidObjectException, AlreadyExistsException, MetaException {

      boolean success = false, indexTableCreated = false;

      try {
        ms.openTransaction();
        Index old_index = null;
        try {
          old_index = get_index_by_name(index.getDbName(), index
              .getOrigTableName(), index.getIndexName());
        } catch (Exception e) {
        }
        if (old_index != null) {
          throw new AlreadyExistsException("Index already exists:" + index);
        }
        Table origTbl = ms.getTable(index.getDbName(), index.getOrigTableName());
        if (origTbl == null) {
          throw new InvalidObjectException(
              "Unable to add index because database or the orginal table do not exist");
        }

        // set create time
        long time = System.currentTimeMillis() / 1000;
        Table indexTbl = indexTable;
        if (indexTbl != null) {
          try {
            indexTbl = ms.getTable(index.getDbName(), index.getIndexTableName());
          } catch (Exception e) {
          }
          if (indexTbl != null) {
            throw new InvalidObjectException(
                "Unable to add index because index table already exists");
          }
          this.create_table(indexTable);
          indexTableCreated = true;
        }

        index.setCreateTime((int) time);
        index.putToParameters(Constants.DDL_TIME, Long.toString(time));

        ms.addIndex(index);
        success = ms.commitTransaction();
        return index;
      } finally {
        if (!success) {
          if (indexTableCreated) {
            try {
              this.drop_table(index.getDbName(), index.getIndexTableName(), false);
            } catch (Exception e) {
            }
          }
          ms.rollbackTransaction();
        }
      }
    }

    @Override
    public boolean drop_index_by_name(final String dbName, final String tblName,
        final String indexName, final boolean deleteData) throws NoSuchObjectException,
        MetaException, TException {
      incrementCounter("drop_index_by_name");
      logStartFunction("drop_index_by_name: db=" + dbName + " tbl="
          + tblName + " index=" + indexName);

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            return drop_index_by_name_core(ms, dbName, tblName,
                indexName, deleteData);
          }
        });
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (MetaException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }

      return ret.booleanValue();
    }

    private boolean drop_index_by_name_core(final RawStore ms,
        final String dbName, final String tblName,
        final String indexName, final boolean deleteData) throws NoSuchObjectException,
        MetaException, TException {

      boolean success = false;
      Path tblPath = null;
      try {
        ms.openTransaction();

        //drop the underlying index table
        Index index = get_index_by_name(dbName, tblName, indexName);
        if (index == null) {
          throw new NoSuchObjectException(indexName + " doesn't exist");
        }
        ms.dropIndex(dbName, tblName, indexName);

        String idxTblName = index.getIndexTableName();
        if (idxTblName != null) {
          Table tbl = null;
          tbl = this.get_table(dbName, idxTblName);
          if (tbl.getSd() == null) {
            throw new MetaException("Table metadata is corrupted");
          }

          if (tbl.getSd().getLocation() != null) {
            tblPath = new Path(tbl.getSd().getLocation());
          }
          if (!ms.dropTable(dbName, idxTblName)) {
            throw new MetaException("Unable to drop underlying data table "
                + idxTblName + " for index " + idxTblName);
          }
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
          return false;
        } else if (deleteData && tblPath != null) {
          wh.deleteDir(tblPath, true);
          // ok even if the data is not deleted
        }
      }
      return true;
    }

    @Override
    public Index get_index_by_name(final String dbName, final String tblName,
        final String indexName) throws MetaException, NoSuchObjectException,
        TException {

      incrementCounter("get_index_by_name");
      logStartFunction("get_index_by_name: db=" + dbName + " tbl="
          + tblName + " index=" + indexName);

      Index ret = null;

      try {
        ret = executeWithRetry(new Command<Index>() {
          @Override
          Index run(RawStore ms) throws Exception {
            return get_index_by_name_core(ms, dbName, tblName, indexName);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (TException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    private Index get_index_by_name_core(final RawStore ms, final String db_name,
        final String tbl_name, final String index_name)
        throws MetaException, NoSuchObjectException, TException {
      Index index = ms.getIndex(db_name, tbl_name, index_name);

      if (index == null) {
        throw new NoSuchObjectException(db_name + "." + tbl_name
            + " index=" + index_name + " not found");
      }
      return index;
    }

    @Override
    public List<String> get_index_names(final String dbName, final String tblName,
        final short maxIndexes) throws MetaException, TException {
      incrementCounter("get_index_names");
      logStartTableFunction("get_index_names", dbName, tblName);

      List<String> ret = null;
      try {
        ret = executeWithRetry(new Command<List<String>>() {
          @Override
          List<String> run(RawStore ms) throws Exception {
            return ms.listIndexNames(dbName, tblName, maxIndexes);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }
    
    @Override
    public List<Index> get_indexes(final String dbName, final String tblName,
        final short maxIndexes) throws NoSuchObjectException, MetaException,
        TException {
      incrementCounter("get_indexes");
      logStartTableFunction("get_indexes", dbName, tblName);

      List<Index> ret = null;
      try {
        ret = executeWithRetry(new Command<List<Index>>() {
          @Override
          List<Index> run(RawStore ms) throws Exception {
            return ms.getIndexes(dbName, tblName, maxIndexes);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    @Override
    public List<Partition> get_partitions_by_filter(final String dbName,
        final String tblName, final String filter, final short maxParts)
        throws MetaException, NoSuchObjectException, TException {
      incrementCounter("get_partitions_by_filter");
      logStartTableFunction("get_partitions_by_filter", dbName, tblName);

      List<Partition> ret = null;
      try {
        ret = executeWithRetry(new Command<List<Partition>>() {
          @Override
          List<Partition> run(RawStore ms) throws Exception {
            return ms.getPartitionsByFilter(dbName, tblName, filter, maxParts);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (NoSuchObjectException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
        String userName, List<String> groupNames) throws MetaException,
        TException {
      if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
        String partName = getPartName(hiveObject);
        return this.get_column_privilege_set(hiveObject.getDbName(), hiveObject
            .getObjectName(), partName, hiveObject.getColumnName(), userName,
            groupNames);
      } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
        String partName = getPartName(hiveObject);
        return this.get_partition_privilege_set(hiveObject.getDbName(),
            hiveObject.getObjectName(), partName, userName, groupNames);
      } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
        return this.get_db_privilege_set(hiveObject.getDbName(), userName,
            groupNames);
      } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
        return this.get_table_privilege_set(hiveObject.getDbName(), hiveObject
            .getObjectName(), userName, groupNames);
      }
      return null;
    }

    private String getPartName(HiveObjectRef hiveObject) throws MetaException {
      String partName = null;
      List<String> partValue = hiveObject.getPartValues();
      if (partValue != null && partValue.size() > 0) {
        try {
          Table table = get_table(hiveObject.getDbName(), hiveObject
              .getObjectName());
          partName = Warehouse
              .makePartName(table.getPartitionKeys(), partValue);
        } catch (NoSuchObjectException e) {
          throw new MetaException(e.getMessage());
        }
      }
      return partName;
    }
    
    public PrincipalPrivilegeSet get_column_privilege_set(final String dbName,
        final String tableName, final String partName, final String columnName,
        final String userName, final List<String> groupNames) throws MetaException,
        TException {
      incrementCounter("get_column_privilege_set");

      PrincipalPrivilegeSet ret = null;
      try {
        ret = executeWithRetry(new Command<PrincipalPrivilegeSet>() {
          @Override
          PrincipalPrivilegeSet run(RawStore ms) throws Exception {
            return ms.getColumnPrivilegeSet(dbName, tableName, partName, columnName, userName, groupNames);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    public PrincipalPrivilegeSet get_db_privilege_set(final String dbName,
        final String userName, final List<String> groupNames) throws MetaException,
        TException {
      incrementCounter("get_db_privilege_set");

      PrincipalPrivilegeSet ret = null;
      try {
        ret = executeWithRetry(new Command<PrincipalPrivilegeSet>() {
          @Override
          PrincipalPrivilegeSet run(RawStore ms) throws Exception {
            return ms.getDBPrivilegeSet(dbName, userName, groupNames);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    public PrincipalPrivilegeSet get_partition_privilege_set(
        final String dbName, final String tableName, final String partName,
        final String userName, final List<String> groupNames)
        throws MetaException, TException {
      incrementCounter("get_partition_privilege_set");

      PrincipalPrivilegeSet ret = null;
      try {
        ret = executeWithRetry(new Command<PrincipalPrivilegeSet>() {
          @Override
          PrincipalPrivilegeSet run(RawStore ms) throws Exception {
            return ms.getPartitionPrivilegeSet(dbName, tableName, partName,
                userName, groupNames);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    public PrincipalPrivilegeSet get_table_privilege_set(final String dbName,
        final String tableName, final String userName,
        final List<String> groupNames) throws MetaException, TException {
      incrementCounter("get_table_privilege_set");

      PrincipalPrivilegeSet ret = null;
      try {
        ret = executeWithRetry(new Command<PrincipalPrivilegeSet>() {
          @Override
          PrincipalPrivilegeSet run(RawStore ms) throws Exception {
            return ms.getTablePrivilegeSet(dbName, tableName, userName,
                groupNames);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public boolean grant_role(final String roleName,
        final String userName, final PrincipalType principalType,
        final String grantor, final PrincipalType grantorType, final boolean grantOption)
        throws MetaException, TException {
      incrementCounter("add_role_member");

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            Role role = ms.getRole(roleName);
            return ms.grantRole(role, userName, principalType, grantor, grantorType, grantOption);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }
    
    public List<Role> list_roles(final String principalName,
        final PrincipalType principalType) throws MetaException, TException {
      incrementCounter("list_roles");

      List<Role> ret = null;
      try {
        ret = executeWithRetry(new Command<List<Role>>() {
          @Override
          List<Role> run(RawStore ms) throws Exception {
            List<Role> result = new ArrayList<Role>();
            List<MRoleMap> roleMap = ms.listRoles(principalName,principalType);
            if (roleMap!=null) {
              for (MRoleMap role : roleMap) {
                MRole r = role.getRole();
                result.add(new Role(r.getRoleName(), r
                    .getCreateTime(), r.getOwnerName()));
              }
            }
            return result;
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public boolean create_role(final Role role)
        throws MetaException, TException {
      incrementCounter("create_role");

      Boolean ret = null;
      try {
        
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            return ms.addRole(role.getRoleName(), role.getOwnerName());
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public boolean drop_role(final String roleName)
        throws MetaException, TException {
      incrementCounter("drop_role");

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            return ms.removeRole(roleName);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }
    
    @Override
    public List<String> get_role_names() throws MetaException, TException {
      incrementCounter("get_role_names");

      List<String> ret = null;
      try {
        ret = executeWithRetry(new Command<List<String>>() {
          @Override
          List<String> run(RawStore ms) throws Exception {
            return ms.listRoleNames();
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        assert(e instanceof RuntimeException);
        throw (RuntimeException)e;
      }
      return ret;
    }

    @Override
    public boolean grant_privileges(final PrivilegeBag privileges) throws MetaException,
        TException {
      incrementCounter("grant_privileges");

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            return ms.grantPrivileges(privileges);
          }
        });
      } catch (MetaException e) {
        e.printStackTrace();
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public boolean revoke_role(final String roleName, final String userName,
        final PrincipalType principalType) throws MetaException, TException {
      incrementCounter("remove_role_member");

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            Role mRole = ms.getRole(roleName);
            return ms.revokeRole(mRole, userName, principalType);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public boolean revoke_privileges(final PrivilegeBag privileges)
        throws MetaException, TException {
      incrementCounter("revoke_privileges");

      Boolean ret = null;
      try {
        ret = executeWithRetry(new Command<Boolean>() {
          @Override
          Boolean run(RawStore ms) throws Exception {
            return ms.revokePrivileges(privileges);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    public PrincipalPrivilegeSet get_user_privilege_set(final String userName,
        final List<String> groupNames) throws MetaException, TException {
      incrementCounter("get_user_privilege_set");

      PrincipalPrivilegeSet ret = null;
      try {
        ret = executeWithRetry(new Command<PrincipalPrivilegeSet>() {
          @Override
          PrincipalPrivilegeSet run(RawStore ms) throws Exception {
            return ms.getUserPrivilegeSet(userName, groupNames);
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }
    
    public PrincipalType getPrincipalType (String principalType) {
      return PrincipalType.valueOf(principalType);
    }
    
    @Override
    public List<HiveObjectPrivilege> list_privileges(String principalName,
        PrincipalType principalType, HiveObjectRef hiveObject)
        throws MetaException, TException {
      if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
        return this.list_global_privileges(principalName, principalType);
      } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
        return this.list_db_privileges(principalName, principalType, hiveObject
            .getDbName());
      } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
        return this.list_table_privileges(principalName, principalType,
            hiveObject.getDbName(), hiveObject.getObjectName());
      } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
        return this.list_partition_privileges(principalName, principalType,
            hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject
                .getPartValues());
      } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
        return this.list_column_privileges(principalName, principalType,
            hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject
                .getPartValues(), hiveObject.getColumnName());
      }
      return null;
    }

    public List<HiveObjectPrivilege> list_column_privileges(
        final String principalName, final PrincipalType principalType,
        final String dbName, final String tableName, final List<String> partValues,
        final String columnName) throws MetaException, TException {
      incrementCounter("list_security_column_grant");

      List<HiveObjectPrivilege> ret = null;
      try {
        ret = executeWithRetry(new Command<List<HiveObjectPrivilege>>() {
          @Override
          List<HiveObjectPrivilege> run(RawStore ms) throws Exception {
            String partName = null;
            if (partValues != null && partValues.size()>0) {
              Table tbl = get_table(dbName, tableName);
              partName = Warehouse.makePartName(tbl.getPartitionKeys(), partValues);
            }
            
            List<HiveObjectPrivilege> result = null;
            
            if (partName != null) {
              Partition part = null;
              part = get_partition_by_name(dbName, tableName, partName);
              List<MPartitionColumnPrivilege> mPartitionCols = ms.listPrincipalPartitionColumnGrants(principalName,
                  principalType, dbName, tableName, partName, columnName);
              if (mPartitionCols.size() > 0) {
                result = new ArrayList<HiveObjectPrivilege>();
                for (int i = 0; i < mPartitionCols.size(); i++) {
                  MPartitionColumnPrivilege sCol = mPartitionCols.get(i);
                  HiveObjectRef objectRef = new HiveObjectRef(
                      HiveObjectType.COLUMN, dbName, tableName,
                      part == null ? null : part.getValues(), sCol
                          .getColumnName());
                  HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
                      sCol.getPrincipalName(), principalType,
                      new PrivilegeGrantInfo(sCol.getPrivilege(), sCol
                          .getCreateTime(), sCol.getGrantor(), PrincipalType
                          .valueOf(sCol.getGrantorType()), sCol.getGrantOption()));
                  result.add(secObj);
                }
              }
            } else {
              List<MTableColumnPrivilege> mTableCols = ms
                  .listPrincipalTableColumnGrants(principalName, principalType,
                      dbName, tableName, columnName);
              if (mTableCols.size() > 0) {
                result = new ArrayList<HiveObjectPrivilege>();
                for (int i = 0; i < mTableCols.size(); i++) {
                  MTableColumnPrivilege sCol = mTableCols.get(i);
                  HiveObjectRef objectRef = new HiveObjectRef(
                      HiveObjectType.COLUMN, dbName, tableName, null, sCol
                          .getColumnName());
                  HiveObjectPrivilege secObj = new HiveObjectPrivilege(
                      objectRef, sCol.getPrincipalName(), principalType,
                      new PrivilegeGrantInfo(sCol.getPrivilege(), sCol
                          .getCreateTime(), sCol.getGrantor(), PrincipalType
                          .valueOf(sCol.getGrantorType()), sCol
                          .getGrantOption()));
                  result.add(secObj);
                }
              }
            }
             
            return result;
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    public List<HiveObjectPrivilege> list_db_privileges(final String principalName,
        final PrincipalType principalType, final String dbName)
        throws MetaException, TException {
      incrementCounter("list_security_db_grant");

      List<HiveObjectPrivilege> ret = null;
      try {
        ret = executeWithRetry(new Command<List<HiveObjectPrivilege>>() {
          @Override
          List<HiveObjectPrivilege> run(RawStore ms) throws Exception {
            List<MDBPrivilege> mDbs = ms.listPrincipalDBGrants(
                principalName, principalType, dbName);
            if (mDbs.size() > 0) {
              List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
              for (int i = 0; i < mDbs.size(); i++) {
                MDBPrivilege sDB = mDbs.get(i);
                HiveObjectRef objectRef = new HiveObjectRef(
                    HiveObjectType.DATABASE, dbName, null, null, null);
                HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
                    sDB.getPrincipalName(), principalType,
                    new PrivilegeGrantInfo(sDB.getPrivilege(), sDB
                        .getCreateTime(), sDB.getGrantor(), PrincipalType
                        .valueOf(sDB.getGrantorType()), sDB.getGrantOption()));
                result.add(secObj);
              }
              return result;
            }
            return null;
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    public List<HiveObjectPrivilege> list_partition_privileges(
        final String principalName, final PrincipalType principalType,
        final String dbName, final String tableName, final List<String> partValues)
        throws MetaException, TException {
      incrementCounter("list_security_partition_grant");

      List<HiveObjectPrivilege> ret = null;
      try {
        ret = executeWithRetry(new Command<List<HiveObjectPrivilege>>() {
          @Override
          List<HiveObjectPrivilege> run(RawStore ms) throws Exception {
            Table tbl = get_table(dbName, tableName);
            String partName = Warehouse.makePartName(tbl.getPartitionKeys(), partValues);
            List<MPartitionPrivilege> mParts = ms
                .listPrincipalPartitionGrants(principalName, principalType, dbName, tableName, partName);
            if (mParts.size() > 0) {
              List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
              for (int i = 0; i < mParts.size(); i++) {
                MPartitionPrivilege sPart = mParts.get(i);
                HiveObjectRef objectRef = new HiveObjectRef(
                    HiveObjectType.PARTITION, dbName, tableName, partValues,
                    null);
                HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
                    sPart.getPrincipalName(), principalType,
                    new PrivilegeGrantInfo(sPart.getPrivilege(), sPart
                        .getCreateTime(), sPart.getGrantor(), PrincipalType
                        .valueOf(sPart.getGrantorType()), sPart
                        .getGrantOption()));

                result.add(secObj);
              }
              return result;
            }
            return null;
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    public List<HiveObjectPrivilege> list_table_privileges(
        final String principalName, final PrincipalType principalType,
        final String dbName, final String tableName) throws MetaException,
        TException {
      incrementCounter("list_security_table_grant");

      List<HiveObjectPrivilege> ret = null;
      try {
        ret = executeWithRetry(new Command<List<HiveObjectPrivilege>>() {
          @Override
          List<HiveObjectPrivilege> run(RawStore ms) throws Exception {
            List<MTablePrivilege> mTbls = ms
                .listAllTableGrants(principalName, principalType, dbName, tableName);
            if (mTbls.size() > 0) {
              List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
              for (int i = 0; i < mTbls.size(); i++) {
                MTablePrivilege sTbl = mTbls.get(i);
                HiveObjectRef objectRef = new HiveObjectRef(
                    HiveObjectType.TABLE, dbName, tableName, null,null);
                HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
                    sTbl.getPrincipalName(), principalType,
                    new PrivilegeGrantInfo(sTbl.getPrivilege(), sTbl.getCreateTime(), sTbl
                        .getGrantor(), PrincipalType.valueOf(sTbl
                        .getGrantorType()), sTbl.getGrantOption()));
                result.add(secObj);
              }
              return result;
            }
            return null;
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    public List<HiveObjectPrivilege> list_global_privileges(
        final String principalName, final PrincipalType principalType)
        throws MetaException, TException {
      incrementCounter("list_security_user_grant");

      List<HiveObjectPrivilege> ret = null;
      try {
        ret = executeWithRetry(new Command<List<HiveObjectPrivilege>>() {
          @Override
          List<HiveObjectPrivilege> run(RawStore ms) throws Exception {
            List<MGlobalPrivilege> mUsers = ms.listPrincipalGlobalGrants(
                principalName, principalType);
            if (mUsers.size() > 0) {
              List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
              for (int i = 0; i < mUsers.size(); i++) {
                MGlobalPrivilege sUsr = mUsers.get(i);
                HiveObjectRef objectRef = new HiveObjectRef(
                    HiveObjectType.GLOBAL, null, null, null, null);
                HiveObjectPrivilege secUser = new HiveObjectPrivilege(
                    objectRef, sUsr.getPrincipalName(), principalType,
                    new PrivilegeGrantInfo(sUsr.getPrivilege(), sUsr
                        .getCreateTime(), sUsr.getGrantor(), PrincipalType
                        .valueOf(sUsr.getGrantorType()), sUsr.getGrantOption()));
                result.add(secUser);
              }
              return result;
            }
            return null;
          }
        });
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }
    @Override
    public void cancel_delegation_token(String token_str_form)
    throws MetaException, TException {
      incrementCounter("cancel_delegation_token");
      logStartFunction("cancel_delegation_token");
      try {
        HiveMetaStore.cancelDelegationToken(token_str_form);
      } catch(IOException e) {
        throw new MetaException(e.getMessage());
      }

    }

    @Override
    public String get_delegation_token_with_signature(
        String renewer_kerberos_principal_name,
        String token_signature) throws MetaException, TException {
      incrementCounter("get_delegation_token_with_signature");
      logStartFunction("get_delegation_token_with_signature");
      try {
        return
        HiveMetaStore.getDelegationToken(renewer_kerberos_principal_name,
            token_signature);
      } catch(IOException e) {
        throw new MetaException(e.getMessage());
      }
    }

    @Override
    public long renew_delegation_token(String token_str_form)
    throws MetaException, TException {
      incrementCounter("renew_delegation_token");
      logStartFunction("renew_delegation_token");
      try {
        return HiveMetaStore.renewDelegationToken(token_str_form);
      } catch(IOException e) {
        throw new MetaException(e.getMessage());
      }
    }

    @Override
    public String get_delegation_token(String renewer_kerberos_principal_name)
    throws MetaException, TException {
      incrementCounter("get_delegation_token_with_signature");
      logStartFunction("get_delegation_token_with_signature");
      try {
        return
        HiveMetaStore.getDelegationToken(renewer_kerberos_principal_name);
      } catch(IOException e) {
        throw new MetaException(e.getMessage());
      }
    }

  }

  /**
   * Discard a current delegation token.
   * @param tokenStrForm the token in string form
   */
  public static void cancelDelegationToken(String tokenStrForm
  ) throws IOException {
    saslServer.cancelDelegationToken(tokenStrForm);
  }
  /**
   * Get a new delegation token.
   * @param renewer the designated renewer
   * @param token_signature an identifier that is set as the service on the generated token
   */
  public static String getDelegationToken(String renewer,String token_signature
  )throws IOException {
    return saslServer.getDelegationToken(renewer, token_signature);
  }

  /**
   * Get a new delegation token.
   * @param renewer the designated renewer
   */
  public static String getDelegationToken(String renewer)throws IOException {
    return saslServer.getDelegationToken(renewer);
  }
  /**
   * Renew a delegation token to extend its lifetime.
   * @param tokenStrForm the token in string form
   */
  public static long renewDelegationToken(String tokenStrForm
  ) throws IOException {
    return saslServer.renewDelegationToken(tokenStrForm);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    int port = 9083;

    if (args.length > 0) {
      port = new Integer(args[0]);
    }
    try {
      startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());
    } catch (Throwable t) {
      HMSHandler.LOG
       .error("Metastore Thrift Server threw an exception. Exiting...");
      System.exit(1);
    }
  }

  /**
   * Start Metastore based on a passed {@link HadoopThriftAuthBridge}
   * @param port
   * @param bridge
   * @throws Throwable
   */
  public static void startMetaStore(int port, HadoopThriftAuthBridge bridge)
  throws Throwable {
    try {

      HMSHandler handler = new HMSHandler("new db based metaserver");
      HiveConf conf = handler.getHiveConf();

      // Server will create new threads up to max as necessary. After an idle
      // period, it will destory threads to keep the number of threads in the
      // pool to min.
      int minWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMINTHREADS);
      int maxWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMAXTHREADS);
      boolean tcpKeepAlive = conf.getBoolVar(HiveConf.ConfVars.METASTORE_TCP_KEEP_ALIVE);
      boolean useSasl = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);

      TServerTransport serverTransport = tcpKeepAlive ?
          new TServerSocketKeepAlive(port) : new TServerSocket(port);

      TProcessor processor = new ThriftHiveMetastore.Processor(handler);
      TTransportFactory transFactory;
      if (useSasl) {
         saslServer = bridge.createServer(
           conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE),
           conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL));

        // start delegation token manager
        saslServer.startDelegationTokenSecretManager(conf);
        transFactory = saslServer.createTransportFactory();
        processor = saslServer.wrapProcessor(processor);
      } else {
        transFactory = new TTransportFactory();
      }

      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      options.minWorkerThreads = minWorkerThreads;
      options.maxWorkerThreads = maxWorkerThreads;
      TServer tServer = new TThreadPoolServer(processor, serverTransport,
          transFactory, transFactory,
          new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);
      HMSHandler.LOG.info("Started the new metaserver on port [" + port
          + "]...");
      HMSHandler.LOG.info("Options.minWorkerThreads = "
          + options.minWorkerThreads);
      HMSHandler.LOG.info("Options.maxWorkerThreads = "
          + options.maxWorkerThreads);
      HMSHandler.LOG.info("TCP keepalive = " + tcpKeepAlive);
      tServer.serve();
    } catch (Throwable x) {
      x.printStackTrace();
      HMSHandler.LOG.error(StringUtils.stringifyException(x));
      throw x;
    }
  }
}
