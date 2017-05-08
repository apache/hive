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
package org.apache.hive.testutils.jdbc;


import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;


public class HiveBurnInClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  //default 80k (runs slightly over 1 day long)
  private final static int NUM_QUERY_ITERATIONS = 80000;

  /**
   * Creates 2 tables to query from
   *
   * @param num
   */
  public static void createTables(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    String tableName = "table1";
    String sql = "drop table if exists " + tableName;
    executeQuery(stmt, sql, false);

    sql = "create table " + tableName + " (key int, value string)";
    executeQuery(stmt, sql, false);

    // load data into table
    // NOTE: filepath has to be local to the hive server
    String filepath = "./examples/files/kv1.txt";
    sql = "load data local inpath '" + filepath + "' into table " + tableName;
    executeQuery(stmt, sql, false);

    tableName = "table2";
    sql = "drop table if exists " + tableName;
    executeQuery(stmt, sql, false);
    sql = "create table " + tableName + " (key int, value string)";
    executeQuery(stmt, sql, false);

    filepath = "./examples/files/kv2.txt";
    sql = "load data local inpath '" + filepath + "' into table " + tableName;
    executeQuery(stmt, sql, false);
  }

  private static void executeQuery(Statement stmt, String sql, boolean resultSet) throws SQLException{

    System.out.println("Running: " + sql);
    long startTime = System.currentTimeMillis();
    if (resultSet){
      ResultSet res = stmt.executeQuery(sql);
      while (res.next()){
        res.getString(1);
      }
    }
    else {
      stmt.execute(sql);
    }
    long endTime = System.currentTimeMillis();
    long msElapsedTime = endTime - startTime;
    System.out.printf("Time taken for query = %d ms \n", msElapsedTime);
  }

  /**
   * @param con
   * @param numberOfQueryIterations
   * @throws SQLException
   */
  public static void runQueries(Connection con, int numberOfQueryIterations) throws SQLException {
    Statement stmt = con.createStatement();

    for (int i = 0; i < numberOfQueryIterations; i++) {
      System.out.println("Iteration #" + i);

      // select query
      String sql = "from table1 SELECT * group by table1.key order by table1.key desc";
      executeQuery(stmt, sql, true);

      // count query
      sql = "select count(*) from table1";
      executeQuery(stmt, sql, true);

      // join with group-by, having, order-by
      sql = "select t1.key,count(t1.key) as cnt from table1 t1"
              + " join table2 t2 on (t1.key = t2.key) group by t1.key having cnt > 5 order by cnt desc";
      executeQuery(stmt, sql, true);

      // full outer join
      sql = "select table1.value, table2.value from table1 full outer join table2 "
              + "on (table1.key = table2.key)";
      executeQuery(stmt, sql, true);
    }

  }

  /**
   * @param args
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    Class.forName(driverName);
    int numberOfQueryIterations = NUM_QUERY_ITERATIONS;

    if (args.length > 0) {
      numberOfQueryIterations = Integer.parseInt(args[0]);
    }
    if (numberOfQueryIterations < 0) {
      numberOfQueryIterations = NUM_QUERY_ITERATIONS;
    }
    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
    createTables(con);
    runQueries(con, numberOfQueryIterations);
  }
}
