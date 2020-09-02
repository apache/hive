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

package org.apache.hadoop.hive.metastore;

import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Database product infered via JDBC.
 * This class is a singleton, which is instantiated the first time
 * method determineDatabaseProduct is invoked.
 * Tests that need to create multiple instances can use the reset method
 * */
public class DatabaseProduct implements Configurable {
  static final private Logger LOG = LoggerFactory.getLogger(DatabaseProduct.class.getName());

  public static enum ProductId {DERBY, MYSQL, POSTGRES, ORACLE, SQLSERVER, OTHER};

  private Configuration conf;
  public ProductId pid;
  
  private static DatabaseProduct theDatabaseProduct;
  
  /**
   * Private constructor for singleton class
   * @param id
   */
  private DatabaseProduct(ProductId id) {
    pid = id;
  }
  
  public static final String DERBY_NAME = "derby";
  public static final String SQL_SERVER_NAME = "microsoft sql server";
  public static final String MYSQL_NAME = "mysql";
  public static final String POSTGRESQL_NAME = "postgresql";
  public static final String ORACLE_NAME = "oracle";
  
  /**
   * Determine the database product type
   * @param productName string to defer database connection
   * @param conf Configuration object for hive-site.xml or metastore-site.xml
   * @return database product type
   */
  public static DatabaseProduct determineDatabaseProduct(String productName, Configuration conf) {
    ProductId id;
    
    productName = productName.toLowerCase();

    if (productName.contains(DERBY_NAME)) {
      id = ProductId.DERBY;
    } else if (productName.contains(SQL_SERVER_NAME)) {
      id = ProductId.SQLSERVER;
    } else if (productName.contains(MYSQL_NAME)) {
      id = ProductId.MYSQL;
    } else if (productName.contains(ORACLE_NAME)) {
      id = ProductId.ORACLE;
    } else if (productName.contains(POSTGRESQL_NAME)) {
      id = ProductId.POSTGRES;
    } else {
      id = ProductId.OTHER;
    }

    // If the singleton instance exists, ensure it is consistent  
    if (theDatabaseProduct != null) {
        if (theDatabaseProduct.pid != id) {
            throw new RuntimeException(String.format("Unexpected mismatched database products. Expected=%s. Got=%s",
                    theDatabaseProduct.pid.name(),id.name()));
        }
        return theDatabaseProduct;
    }

    if (conf == null) {
        // TODO: how to get the right conf object for hive-site.xml or metastore-site.xml?
        conf = new Configuration();
    }

    boolean isExternal = conf.getBoolean("metastore.use.custom.database.product", false) ||
            conf.getBoolean("hive.metastore.use.custom.database.product", false);

    DatabaseProduct databaseProduct = null;
    if (isExternal) {

      String className = conf.get("metastore.custom.database.product.classname");
      
      try {
        databaseProduct = (DatabaseProduct)
            ReflectionUtils.newInstance(Class.forName(className), conf);
      }catch (Exception e) {
        LOG.warn("Unable to instantiate custom database product. Reverting to default", e);
      }
    }

    if (databaseProduct == null) {
      databaseProduct = new DatabaseProduct(id);
    }

    databaseProduct.setConf(conf);
    return databaseProduct;
  }

  public boolean isDeadlock(SQLException e) {
    return e instanceof SQLTransactionRollbackException
        || ((isMYSQL() || isPOSTGRES() || isSQLSERVER())
            && "40001".equals(e.getSQLState()))
        || (isPOSTGRES() && "40P01".equals(e.getSQLState()))
        || (isORACLE() && (e.getMessage() != null && (e.getMessage().contains("deadlock detected")
            || e.getMessage().contains("can't serialize access for this transaction"))));
  }

  /**
   * Whether the RDBMS has restrictions on IN list size (explicit, or poor perf-based).
   */
  public boolean needsInBatching() {
    return isORACLE() || isSQLSERVER();
  }

  /**
   * Whether the RDBMS has a bug in join and filter operation order described in DERBY-6358.
   */
  public boolean hasJoinOperationOrderBug() {
    return isDERBY() || isORACLE() || isPOSTGRES();
  }

  public String getHiveSchemaPostfix() {
    switch (pid) {
    case SQLSERVER:
      return "mssql";
    case DERBY:
    case MYSQL:
    case POSTGRES:
    case ORACLE:
      return pid.name().toLowerCase();
    case OTHER:
    default:
      return null;
    }
  }

  public final boolean isDERBY() {
  	return pid == ProductId.DERBY;
  }

  public final boolean isMYSQL() {
  	return pid == ProductId.MYSQL;
  }

  public final boolean isORACLE() {
  	return pid == ProductId.ORACLE;
  }

  public final boolean isSQLSERVER() {
  	return pid == ProductId.SQLSERVER;
  }

  public final boolean isPOSTGRES() {
  	return pid == ProductId.POSTGRES;
  }

  public final boolean isOTHER() {
  	return pid == ProductId.OTHER;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
    
  @Override
  public void setConf(Configuration c) {
    conf = c;
  }

  public static void reset() {
    theDatabaseProduct = null;
  }
}
