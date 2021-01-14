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
package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Parser tests special character in table names.
 */
public class TestSpecialCharacterInTableNamesQuotes {
  private static Configuration conf;

  private ParseDriver parseDriver;

  @BeforeClass
  public static void initialize() {
    conf = new Configuration();
    conf.set("hive.support.quoted.identifiers", "standard");
  }

  @Before
  public void setup() throws SemanticException, IOException {
    parseDriver = new ParseDriver();
  }

  private ASTNode parse(String query) throws ParseException {
    return (ASTNode) parseDriver.parse(query, conf).getTree().getChild(0);
  }

  @Test
  public void testCreateDatabase() throws ParseException {
    parse("create database \"db~!@#$%^&*(),<>\"");
    parse("use \"db~!@#$%^&*(),<>\"");
  }

  @Test
  public void testCreateTable() throws ParseException {
    parse("create table \"c/b/o_t1\"(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE");
    parse("create table \"//cbo_t2\"(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE");
    parse("create table \"cbo_/t3////\"(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE");

    parse("CREATE TABLE \"p/a/r/t\"(\n" +
        "    p_partkey INT,\n" +
        "    p_name STRING,\n" +
        "    p_mfgr STRING,\n" +
        "    p_brand STRING,\n" +
        "    p_type STRING,\n" +
        "    p_size INT,\n" +
        "    p_container STRING,\n" +
        "    p_retailprice DOUBLE,\n" +
        "    p_comment STRING\n" +
        ")");

    parse("CREATE TABLE \"line/item\" (L_ORDERKEY      INT,\n" +
        "                                L_PARTKEY       INT,\n" +
        "                                L_SUPPKEY       INT,\n" +
        "                                L_LINENUMBER    INT,\n" +
        "                                L_QUANTITY      DOUBLE,\n" +
        "                                L_EXTENDEDPRICE DOUBLE,\n" +
        "                                L_DISCOUNT      DOUBLE,\n" +
        "                                L_TAX           DOUBLE,\n" +
        "                                L_RETURNFLAG    STRING,\n" +
        "                                L_LINESTATUS    STRING,\n" +
        "                                l_shipdate      STRING,\n" +
        "                                L_COMMITDATE    STRING,\n" +
        "                                L_RECEIPTDATE   STRING,\n" +
        "                                L_SHIPINSTRUCT  STRING,\n" +
        "                                L_SHIPMODE      STRING,\n" +
        "                                L_COMMENT       STRING)\n" +
        "ROW FORMAT DELIMITED\n" +
        "FIELDS TERMINATED BY '|'");

    parse("create table \"src/_/cbo\" as select * from default.src");
  }

  @Test
  public void testLoadData() throws ParseException {
    parse("load data local inpath '../../data/files/cbo_t1.txt' into table \"c/b/o_t1\" partition (dt='2014')");
    parse("load data local inpath '../../data/files/cbo_t2.txt' into table \"//cbo_t2\" partition (dt='2014')");
    parse("load data local inpath '../../data/files/cbo_t3.txt' into table \"cbo_/t3////\"");
    parse("LOAD DATA LOCAL INPATH '../../data/files/part_tiny.txt' overwrite into table \"p/a/r/t\"");
    parse("LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE \"line/item\"");
  }

  @Test
  public void testAnalyzeTable() throws ParseException {
    parse("analyze table \"c/b/o_t1\" partition (dt) compute statistics");
    parse("analyze table \"c/b/o_t1\" compute statistics for columns key, value, c_int, c_float, c_boolean");
    parse("analyze table \"//cbo_t2\" partition (dt) compute statistics");
    parse("analyze table \"//cbo_t2\" compute statistics for columns key, value, c_int, c_float, c_boolean");
    parse("analyze table \"cbo_/t3////\" compute statistics");
    parse("analyze table \"cbo_/t3////\" compute statistics for columns key, value, c_int, c_float, c_boolean");
    parse("analyze table \"src/_/cbo\" compute statistics");
    parse("analyze table \"src/_/cbo\" compute statistics for columns");
    parse("analyze table \"p/a/r/t\" compute statistics");
    parse("analyze table \"p/a/r/t\" compute statistics for columns");
    parse("analyze table \"line/item\" compute statistics");
    parse("analyze table \"line/item\" compute statistics for columns");
  }

  @Test
  public void testSelect() throws ParseException {
    parse("select key, (c_int+1)+2 as x, sum(c_int) from \"c/b/o_t1\" group by c_float, \"c/b/o_t1\".c_int, key");
    parse("select x, y, count(*) from (select key, (c_int+c_float+1+2) as x, sum(c_int) as y from \"c/b/o_t1\" group by c_float, \"c/b/o_t1\".c_int, key) R group by y, x");
    parse("select \"cbo_/t3////\".c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0) group by c_float, \"c/b/o_t1\".c_int, key order by a) \"c/b/o_t1\" join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key order by q/10 desc, r asc) \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q >= 0) and (b > 0 or c_int >= 0) group by \"cbo_/t3////\".c_int, c order by \"cbo_/t3////\".c_int+c desc, c");
    parse("select \"cbo_/t3////\".c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)  group by c_float, \"c/b/o_t1\".c_int, key having \"c/b/o_t1\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b % c asc, b desc) \"c/b/o_t1\" left outer join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key  having \"//cbo_t2\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) \"//cbo_t2\" on \"c/b/o_t1\".a=p left outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q >= 0) and (b > 0 or c_int >= 0) group by \"cbo_/t3////\".c_int, c  having \"cbo_/t3////\".c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0  order by \"cbo_/t3////\".c_int % c asc, \"cbo_/t3////\".c_int desc");
    parse("select \"cbo_/t3////\".c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)  group by c_float, \"c/b/o_t1\".c_int, key having \"c/b/o_t1\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b+c, a desc) \"c/b/o_t1\" right outer join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key having \"//cbo_t2\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) \"//cbo_t2\" on \"c/b/o_t1\".a=p right outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q >= 2) and (b > 0 or c_int >= 0) group by \"cbo_/t3////\".c_int, c");
    parse("select \"cbo_/t3////\".c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)  group by c_float, \"c/b/o_t1\".c_int, key having \"c/b/o_t1\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by c+a desc) \"c/b/o_t1\" full outer join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key having \"//cbo_t2\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by p+q desc, r asc) \"//cbo_t2\" on \"c/b/o_t1\".a=p full outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q >= 0) and (b > 0 or c_int >= 0) group by \"cbo_/t3////\".c_int, c having \"cbo_/t3////\".c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0 order by \"cbo_/t3////\".c_int");
    parse("select \"cbo_/t3////\".c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)  group by c_float, \"c/b/o_t1\".c_int, key having \"c/b/o_t1\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) \"c/b/o_t1\" join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key having \"//cbo_t2\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0) \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q >= 0) and (b > 0 or c_int >= 0) group by \"cbo_/t3////\".c_int, c");
  }

  @Test
  public void testTestGroupByIsEmptyAndThereIsNoOtherColsInAggr() throws ParseException {
    parse("select unionsrc.key FROM (select 'tst1' as key, count(1) as value from default.src) unionsrc");
    parse("select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from default.src) unionsrc");

    parse("select unionsrc.key FROM (select 'max' as key, max(c_int) as value from \"cbo_/t3////\" s1\n" +
        "\n" +
        "UNION  ALL\n" +
        "\n" +
        "    select 'min' as key,  min(c_int) as value from \"cbo_/t3////\" s2\n" +
        "\n" +
        "    UNION ALL\n" +
        "\n" +
        "        select 'avg' as key,  avg(c_int) as value from \"cbo_/t3////\" s3) unionsrc order by unionsrc.key");

    parse("select unionsrc.key, unionsrc.value FROM (select 'max' as key, max(c_int) as value from \"cbo_/t3////\" s1\n" +
        "\n" +
        "UNION  ALL\n" +
        "\n" +
        "    select 'min' as key,  min(c_int) as value from \"cbo_/t3////\" s2\n" +
        "\n" +
        "    UNION ALL\n" +
        "\n" +
        "        select 'avg' as key,  avg(c_int) as value from \"cbo_/t3////\" s3) unionsrc order by unionsrc.key");

    parse("select unionsrc.key, count(1) FROM (select 'max' as key, max(c_int) as value from \"cbo_/t3////\" s1\n" +
        "\n" +
        "    UNION  ALL\n" +
        "\n" +
        "        select 'min' as key,  min(c_int) as value from \"cbo_/t3////\" s2\n" +
        "\n" +
        "    UNION ALL\n" +
        "\n" +
        "        select 'avg' as key,  avg(c_int) as value from \"cbo_/t3////\" s3) unionsrc group by unionsrc.key order by unionsrc.key");
  }

  @Test
  public void testSelectJoinTS() throws ParseException {
    parse("select \"c/b/o_t1\".c_int, \"//cbo_t2\".c_int from \"c/b/o_t1\" join             \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key");
    parse("select \"c/b/o_t1\".key from \"c/b/o_t1\" join \"cbo_/t3////\"");
    parse("select \"c/b/o_t1\".key from \"c/b/o_t1\" join \"cbo_/t3////\" where \"c/b/o_t1\".key=\"cbo_/t3////\".key and \"c/b/o_t1\".key >= 1");
    parse("select \"c/b/o_t1\".c_int, \"//cbo_t2\".c_int from \"c/b/o_t1\" left outer join  \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key");
    parse("select \"c/b/o_t1\".c_int, \"//cbo_t2\".c_int from \"c/b/o_t1\" right outer join \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key");
    parse("select \"c/b/o_t1\".c_int, \"//cbo_t2\".c_int from \"c/b/o_t1\" full outer join  \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key");
    parse("select b, \"c/b/o_t1\".c, \"//cbo_t2\".p, q, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\") \"c/b/o_t1\" join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\") \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key");
    parse("select key, \"c/b/o_t1\".c_int, \"//cbo_t2\".p, q from \"c/b/o_t1\" join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\") \"//cbo_t2\" on \"c/b/o_t1\".key=p join (select key as a, c_int as b, \"cbo_/t3////\".c_float as c from \"cbo_/t3////\")\"cbo_/t3////\" on \"c/b/o_t1\".key=a");
    parse("select a, \"c/b/o_t1\".b, key, \"//cbo_t2\".c_int, \"cbo_/t3////\".p from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\") \"c/b/o_t1\" join \"//cbo_t2\"  on \"c/b/o_t1\".a=key join (select key as p, c_int as q, \"cbo_/t3////\".c_float as r from \"cbo_/t3////\")\"cbo_/t3////\" on \"c/b/o_t1\".a=\"cbo_/t3////\".p");
    parse("select b, \"c/b/o_t1\".c, \"//cbo_t2\".c_int, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\") \"c/b/o_t1\" join \"//cbo_t2\" on \"c/b/o_t1\".a=\"//cbo_t2\".key join \"cbo_/t3////\" on \"c/b/o_t1\".a=\"cbo_/t3////\".key");
    parse("select \"cbo_/t3////\".c_int, b, \"//cbo_t2\".c_int, \"c/b/o_t1\".c from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\") \"c/b/o_t1\" join \"//cbo_t2\" on \"c/b/o_t1\".a=\"//cbo_t2\".key join \"cbo_/t3////\" on \"c/b/o_t1\".a=\"cbo_/t3////\".key");
    parse("select b, \"c/b/o_t1\".c, \"//cbo_t2\".p, q, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\") \"c/b/o_t1\" left outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\") \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key");
    parse("select key, \"c/b/o_t1\".c_int, \"//cbo_t2\".p, q from \"c/b/o_t1\" join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\") \"//cbo_t2\" on \"c/b/o_t1\".key=p left outer join (select key as a, c_int as b, \"cbo_/t3////\".c_float as c from \"cbo_/t3////\")\"cbo_/t3////\" on \"c/b/o_t1\".key=a");
    parse("select b, \"c/b/o_t1\".c, \"//cbo_t2\".p, q, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\") \"c/b/o_t1\" right outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\") \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key");
    parse("select key, \"c/b/o_t1\".c_int, \"//cbo_t2\".p, q from \"c/b/o_t1\" join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\") \"//cbo_t2\" on \"c/b/o_t1\".key=p right outer join (select key as a, c_int as b, \"cbo_/t3////\".c_float as c from \"cbo_/t3////\")\"cbo_/t3////\" on \"c/b/o_t1\".key=a");
    parse("select b, \"c/b/o_t1\".c, \"//cbo_t2\".p, q, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\") \"c/b/o_t1\" full outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\") \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key");
    parse("select key, \"c/b/o_t1\".c_int, \"//cbo_t2\".p, q from \"c/b/o_t1\" join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\") \"//cbo_t2\" on \"c/b/o_t1\".key=p full outer join (select key as a, c_int as b, \"cbo_/t3////\".c_float as c from \"cbo_/t3////\")\"cbo_/t3////\" on \"c/b/o_t1\".key=a");
  }

  @Test
  public void testSelectJoinFILTS() throws ParseException {
    parse("select \"c/b/o_t1\".c_int, \"//cbo_t2\".c_int from \"c/b/o_t1\" join \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key where (\"c/b/o_t1\".c_int + \"//cbo_t2\".c_int == 2) and (\"c/b/o_t1\".c_int > 0 or \"//cbo_t2\".c_float >= 0)");
    parse("select \"c/b/o_t1\".c_int, \"//cbo_t2\".c_int from \"c/b/o_t1\" left outer join  \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key where (\"c/b/o_t1\".c_int + \"//cbo_t2\".c_int == 2) and (\"c/b/o_t1\".c_int > 0 or \"//cbo_t2\".c_float >= 0)");
    parse("select \"c/b/o_t1\".c_int, \"//cbo_t2\".c_int from \"c/b/o_t1\" right outer join \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key where (\"c/b/o_t1\".c_int + \"//cbo_t2\".c_int == 2) and (\"c/b/o_t1\".c_int > 0 or \"//cbo_t2\".c_float >= 0)");
    parse("select \"c/b/o_t1\".c_int, \"//cbo_t2\".c_int from \"c/b/o_t1\" full outer join  \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key where (\"c/b/o_t1\".c_int + \"//cbo_t2\".c_int == 2) and (\"c/b/o_t1\".c_int > 0 or \"//cbo_t2\".c_float >= 0)");
    parse("select b, \"c/b/o_t1\".c, \"//cbo_t2\".p, q, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or \"//cbo_t2\".q >= 0)");
    parse("select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" left outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)");
    parse("select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" right outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)");
    parse("select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" full outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" full outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" left outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p left outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" left outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p right outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" left outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p full outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" right outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p right outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" right outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p left outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" right outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p full outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" full outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p full outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" full outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p left outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select q, b, \"//cbo_t2\".p, \"c/b/o_t1\".c, \"cbo_/t3////\".c_int from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" full outer join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p right outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q == 2) and (b > 0 or c_int >= 0)) R where  (q + 1 = 2) and (R.b > 0 or c_int >= 0)");
  }

  @Test
  public void testSelectTSJoinFilGBGBHavingLimit() throws ParseException {
    parse("select key, (c_int+1)+2 as x, sum(c_int) from \"c/b/o_t1\" group by c_float, \"c/b/o_t1\".c_int, key order by x limit 1");
    parse("select x, y, count(*) from (select key, (c_int+c_float+1+2) as x, sum(c_int) as y from \"c/b/o_t1\" group by c_float, \"c/b/o_t1\".c_int, key) R group by y, x order by x,y limit 1");
    parse("select key from(select key from (select key from \"c/b/o_t1\" limit 5)\"//cbo_t2\"  limit 5)\"cbo_/t3////\"  limit 5");
    parse("select key, c_int from(select key, c_int from (select key, c_int from \"c/b/o_t1\" order by c_int limit 5)\"c/b/o_t1\"  order by c_int limit 5)\"//cbo_t2\"  order by c_int limit 5");
    parse("select \"cbo_/t3////\".c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0) group by c_float, \"c/b/o_t1\".c_int, key order by a limit 5) \"c/b/o_t1\" join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key order by q/10 desc, r asc limit 5) \"//cbo_t2\" on \"c/b/o_t1\".a=p join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q >= 0) and (b > 0 or c_int >= 0) group by \"cbo_/t3////\".c_int, c order by \"cbo_/t3////\".c_int+c desc, c limit 5");
    parse("select \"cbo_/t3////\".c_int, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)  group by c_float, \"c/b/o_t1\".c_int, key having \"c/b/o_t1\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by b % c asc, b desc limit 5) \"c/b/o_t1\" left outer join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key  having \"//cbo_t2\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 limit 5) \"//cbo_t2\" on \"c/b/o_t1\".a=p left outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"//cbo_t2\".q >= 0) and (b > 0 or c_int >= 0) group by \"cbo_/t3////\".c_int, c  having \"cbo_/t3////\".c_int > 0 and (c_int >=1 or c >= 1) and (c_int + c) >= 0  order by \"cbo_/t3////\".c_int % c asc, \"cbo_/t3////\".c_int, c desc limit 5");
  }

  @Test
  public void testSemiJoin() throws ParseException {
    parse("select \"c/b/o_t1\".c_int           from \"c/b/o_t1\" left semi join   \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key");
    parse("select \"c/b/o_t1\".c_int           from \"c/b/o_t1\" left semi join   \"//cbo_t2\" on \"c/b/o_t1\".key=\"//cbo_t2\".key where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)");
    parse("select * from (select c, b, a from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" left semi join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p left semi join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + 1 == 2) and (b > 0 or c >= 0)) R where  (b + 1 = 2) and (R.b > 0 or c >= 0)");
    parse("select * from (select \"cbo_/t3////\".c_int, \"c/b/o_t1\".c, b from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 = 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" left semi join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p left outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + \"cbo_/t3////\".c_int  == 2) and (b > 0 or c_int >= 0)) R where  (R.c_int + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select c_int, b, \"c/b/o_t1\".c from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" left semi join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p right outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + 1 == 2) and (b > 0 or c_int >= 0)) R where  (c + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select * from (select c_int, b, \"c/b/o_t1\".c from (select key as a, c_int as b, \"c/b/o_t1\".c_float as c from \"c/b/o_t1\"  where (\"c/b/o_t1\".c_int + 1 == 2) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)) \"c/b/o_t1\" left semi join (select \"//cbo_t2\".key as p, \"//cbo_t2\".c_int as q, c_float as r from \"//cbo_t2\"  where (\"//cbo_t2\".c_int + 1 == 2) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)) \"//cbo_t2\" on \"c/b/o_t1\".a=p full outer join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + 1 == 2) and (b > 0 or c_int >= 0)) R where  (c + 1 = 2) and (R.b > 0 or c_int >= 0)");
    parse("select a, c, count(*) from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)  group by c_float, \"c/b/o_t1\".c_int, key having \"c/b/o_t1\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc) \"c/b/o_t1\" left semi join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key having \"//cbo_t2\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p) \"//cbo_t2\" on \"c/b/o_t1\".a=p left semi join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a");
    parse("select a, c, count(*)  from (select key as a, c_int+1 as b, sum(c_int) as c from \"c/b/o_t1\" where (\"c/b/o_t1\".c_int + 1 >= 0) and (\"c/b/o_t1\".c_int > 0 or \"c/b/o_t1\".c_float >= 0)  group by c_float, \"c/b/o_t1\".c_int, key having \"c/b/o_t1\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc limit 5) \"c/b/o_t1\" left semi join (select key as p, c_int+1 as q, sum(c_int) as r from \"//cbo_t2\" where (\"//cbo_t2\".c_int + 1 >= 0) and (\"//cbo_t2\".c_int > 0 or \"//cbo_t2\".c_float >= 0)  group by c_float, \"//cbo_t2\".c_int, key having \"//cbo_t2\".c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p limit 5) \"//cbo_t2\" on \"c/b/o_t1\".a=p left semi join \"cbo_/t3////\" on \"c/b/o_t1\".a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a");
  }

  @Test
  public void testSelectTs() throws ParseException {
    parse("select * from \"c/b/o_t1\"");
    parse("select * from \"c/b/o_t1\" as \"c/b/o_t1\"");
    parse("select * from \"c/b/o_t1\" as \"//cbo_t2\"");
    parse("select \"c/b/o_t1\".key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from \"c/b/o_t1\"");
    parse("select * from \"c/b/o_t1\" where (((key=1) and (c_float=10)) and (c_int=20))");
  }

  @Test
  public void testSelectFilTs() throws ParseException {
    parse("select * from \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0");
    parse("select * from \"c/b/o_t1\" as \"c/b/o_t1\"  where \"c/b/o_t1\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100");
    parse("select * from \"c/b/o_t1\" as \"//cbo_t2\" where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100");
    parse("select \"//cbo_t2\".key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from \"c/b/o_t1\" as \"//cbo_t2\"  where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100");
  }

  @Test
  public void testSelectSelectTsFil() throws ParseException {
    parse("select * from (select * from \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0) as \"c/b/o_t1\"");
    parse("select * from (select * from \"c/b/o_t1\" as \"c/b/o_t1\"  where \"c/b/o_t1\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"c/b/o_t1\"");
    parse("select * from (select * from \"c/b/o_t1\" as \"//cbo_t2\" where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"c/b/o_t1\"");
    parse("select * from (select \"//cbo_t2\".key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from \"c/b/o_t1\" as \"//cbo_t2\"  where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"c/b/o_t1\"");
    parse("select * from (select * from \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0) as \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0");
    parse("select * from (select * from \"c/b/o_t1\" as \"c/b/o_t1\"  where \"c/b/o_t1\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"c/b/o_t1\"  where \"c/b/o_t1\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100");
    parse("select * from (select * from \"c/b/o_t1\" as \"//cbo_t2\" where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"//cbo_t2\" where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100");
    parse("select * from (select \"//cbo_t2\".key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from \"c/b/o_t1\" as \"//cbo_t2\"  where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0 and y+c_int >= 0 or x <= 100");
    parse("select \"c/b/o_t1\".c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0) as \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0");
    parse("select \"//cbo_t2\".c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0) as \"//cbo_t2\" where \"//cbo_t2\".c_int >= 0");
    parse("select * from (select * from \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0) as \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0");
    parse("select * from (select * from \"c/b/o_t1\" as \"c/b/o_t1\"  where \"c/b/o_t1\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"c/b/o_t1\"  where \"c/b/o_t1\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100");
    parse("select * from (select * from \"c/b/o_t1\" as \"//cbo_t2\" where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"//cbo_t2\" where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100");
    parse("select * from (select \"//cbo_t2\".key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from \"c/b/o_t1\" as \"//cbo_t2\"  where \"//cbo_t2\".c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0 and y+c_int >= 0 or x <= 100");
    parse("select \"c/b/o_t1\".c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0) as \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0");
    parse("select \"//cbo_t2\".c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from \"c/b/o_t1\" where \"c/b/o_t1\".c_int >= 0) as \"//cbo_t2\" where \"//cbo_t2\".c_int >= 0");
  }

  @Test
  public void testNullExprInSelectList() throws ParseException {
    parse("select null from \"cbo_/t3////\"");
  }

  @Test
  public void testUnaryOperator() throws ParseException {
    parse("select key from \"c/b/o_t1\" where c_int = -6  or c_int = +6");
  }

  @Test
  public void testQueryReferencingOnlyPartitionColumns() throws ParseException {
    parse("select count(\"c/b/o_t1\".dt) from \"c/b/o_t1\" join \"//cbo_t2\" on \"c/b/o_t1\".dt  = \"//cbo_t2\".dt  where \"c/b/o_t1\".dt = '2014' ");
  }

  @Test
  public void testGetStatsWithEmptyPartitionList() throws ParseException {
    parse("select \"c/b/o_t1\".value from \"c/b/o_t1\" join \"//cbo_t2\" on \"c/b/o_t1\".key = \"//cbo_t2\".key where \"c/b/o_t1\".dt = '10' and \"c/b/o_t1\".c_boolean = true");
  }

  @Test
  public void testSubQueriesNotExistsDistinctCorr() throws ParseException {
    parse("select * \n" +
        "\n" +
        "from \"src/_/cbo\" b \n" +
        "\n" +
        "where not exists \n" +
        "\n" +
        "  (select distinct a.key \n" +
        "\n" +
        "  from \"src/_/cbo\" a \n" +
        "\n" +
        "  where b.value = a.value and a.value > 'val_2'\n" +
        "\n" +
        "  )\n" +
        "\n");
  }

  @Test
  public void testSubQueriesNotExistsNoAggCorrHaving() throws ParseException {
    parse("select * \n" +
        "\n" +
        "from \"src/_/cbo\" b \n" +
        "\n" +
        "group by key, value\n" +
        "\n" +
        "having not exists \n" +
        "\n" +
        "  (select a.key \n" +
        "\n" +
        "  from \"src/_/cbo\" a \n" +
        "\n" +
        "  where b.value = a.value  and a.key = b.key and a.value > 'val_12'\n" +
        "\n" +
        "  )\n" +
        "\n");
  }

  @Test
  public void testSubQueryInFrom() throws ParseException {
    parse("select * \n" +
        "\n" +
        "from (select * \n" +
        "\n" +
        "      from \"src/_/cbo\" b \n" +
        "\n" +
        "      where exists \n" +
        "\n" +
        "          (select a.key \n" +
        "\n" +
        "          from \"src/_/cbo\" a \n" +
        "\n" +
        "          where b.value = a.value  and a.key = b.key and a.value > 'val_9')\n" +
        "\n" +
        "     ) a\n" +
        "\n");
  }

  @Test
  public void testSubQueryIn() throws ParseException {
    // from, having
    parse("select *\n" +
        "\n" +
        "from (select b.key, count(*) \n" +
        "\n" +
        "  from \"src/_/cbo\" b \n" +
        "\n" +
        "  group by b.key\n" +
        "\n" +
        "  having exists \n" +
        "\n" +
        "    (select a.key \n" +
        "\n" +
        "    from \"src/_/cbo\" a \n" +
        "\n" +
        "    where a.key = b.key and a.value > 'val_9'\n" +
        "\n" +
        "    )\n" +
        "\n" +
        ") a\n" +
        "\n");

    // non agg, non corr
    parse("select * \n" +
        "\n" +
        "from \"src/_/cbo\" \n" +
        "\n" +
        "where \"src/_/cbo\".key in (select key from \"src/_/cbo\" s1 where s1.key > '9') order by key\n" +
        "\n");

    // distinct, corr
    parse("select * \n" +
        "\n" +
        "from \"src/_/cbo\" b \n" +
        "\n" +
        "where b.key in\n" +
        "\n" +
        "        (select distinct a.key \n" +
        "\n" +
        "         from \"src/_/cbo\" a \n" +
        "\n" +
        "         where b.value = a.value and a.key > '9'\n" +
        "\n" +
        "        ) order by b.key\n" +
        "\n");

    // non agg, corr, with join in Parent Query
    parse("select p.p_partkey, li.l_suppkey \n" +
        "\n" +
        "from (select distinct l_partkey as p_partkey from \"line/item\") p join \"line/item\" li on p.p_partkey = li.l_partkey \n" +
        "\n" +
        "where li.l_linenumber = 1 and\n" +
        "\n" +
        " li.l_orderkey in (select l_orderkey from \"line/item\" where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)\n" +
        "\n" +
        " order by p.p_partkey\n" +
        "\n");

    // where and having
    parse("select key, value, count(*) \n" +
        "\n" +
        "from \"src/_/cbo\" b\n" +
        "\n" +
        "where b.key in (select key from \"src/_/cbo\" where \"src/_/cbo\".key > '8')\n" +
        "\n" +
        "group by key, value\n" +
        "\n" +
        "having count(*) in (select count(*) from \"src/_/cbo\" s1 where s1.key > '9' group by s1.key ) order by key\n" +
        "\n");

    // non agg, non corr, windowing
    parse("select p_mfgr, p_name, avg(p_size) \n" +
        "\n" +
        "from \"p/a/r/t\" \n" +
        "\n" +
        "group by p_mfgr, p_name\n" +
        "\n" +
        "having p_name in \n" +
        "\n" +
        "  (select first_value(p_name) over(partition by p_mfgr order by p_size) from \"p/a/r/t\") order by p_mfgr\n");
  }

  @Test
  public void testSubQueriesNotIn() throws ParseException {
    // non agg, non corr
    parse("select * \n" +
        "\n" +
        "from \"src/_/cbo\" \n" +
        "\n" +
        "where \"src/_/cbo\".key not in  \n" +
        "\n" +
        "  ( select key  from \"src/_/cbo\" s1 \n" +
        "\n" +
        "    where s1.key > '2'\n" +
        "\n" +
        "  ) order by key\n" +
        "\n");

    // non agg, corr
    parse("select p_mfgr, b.p_name, p_size \n" +
        "\n" +
        "from \"p/a/r/t\" b \n" +
        "\n" +
        "where b.p_name not in \n" +
        "\n" +
        "  (select p_name \n" +
        "\n" +
        "  from (select p_mfgr, p_name, p_size as r from \"p/a/r/t\") a \n" +
        "\n" +
        "  where r < 10 and b.p_mfgr = a.p_mfgr \n" +
        "\n" +
        "  ) order by p_mfgr,p_size");

    // agg, non corr
    parse("" +
        "select p_name, p_size \n" +
        "\n" +
        "from \n" +
        "\n" +
        "\"p/a/r/t\" where \"p/a/r/t\".p_size not in \n" +
        "\n" +
        "  (select avg(p_size) \n" +
        "\n" +
        "  from (select p_size from \"p/a/r/t\") a \n" +
        "\n" +
        "  where p_size < 10\n" +
        "\n" +
        "  ) order by p_name\n");

    // agg, corr
    parse("" +
        "select p_mfgr, p_name, p_size \n" +
        "\n" +
        "from \"p/a/r/t\" b where b.p_size not in \n" +
        "\n" +
        "  (select min(p_size) \n" +
        "\n" +
        "  from (select p_mfgr, p_size from \"p/a/r/t\") a \n" +
        "\n" +
        "  where p_size < 10 and b.p_mfgr = a.p_mfgr\n" +
        "\n" +
        "  ) order by  p_name\n");

    // non agg, non corr, Group By in Parent Query
    parse("select li.l_partkey, count(*) \n" +
        "\n" +
        "from \"line/item\" li \n" +
        "\n" +
        "where li.l_linenumber = 1 and \n" +
        "\n" +
        "  li.l_orderkey not in (select l_orderkey from \"line/item\" where l_shipmode = 'AIR') \n" +
        "\n" +
        "group by li.l_partkey order by li.l_partkey\n");

    // non agg, corr, having
    parse("select b.p_mfgr, min(p_retailprice) \n" +
        "\n" +
        "from \"p/a/r/t\" b \n" +
        "\n" +
        "group by b.p_mfgr\n" +
        "\n" +
        "having b.p_mfgr not in \n" +
        "\n" +
        "  (select p_mfgr \n" +
        "\n" +
        "  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from \"p/a/r/t\" group by p_mfgr) a \n" +
        "\n" +
        "  where min(p_retailprice) = l and r - l > 600\n" +
        "\n" +
        "  )\n" +
        "\n" +
        "  order by b.p_mfgr\n");

    // agg, non corr, having
    parse("select b.p_mfgr, min(p_retailprice) \n" +
        "\n" +
        "from \"p/a/r/t\" b \n" +
        "\n" +
        "group by b.p_mfgr\n" +
        "\n" +
        "having b.p_mfgr not in \n" +
        "\n" +
        "  (select p_mfgr \n" +
        "\n" +
        "  from \"p/a/r/t\" a\n" +
        "\n" +
        "  group by p_mfgr\n" +
        "\n" +
        "  having max(p_retailprice) - min(p_retailprice) > 600\n" +
        "\n" +
        "  )\n" +
        "\n" +
        "  order by b.p_mfgr  \n");
  }

  @Test
  public void testUDF_UDAF() throws ParseException {
    parse("select count(*), count(c_int), sum(c_int), avg(c_int), max(c_int), min(c_int) from \"c/b/o_t1\"");
    parse("select count(*), count(c_int) as a, sum(c_int), avg(c_int), max(c_int), min(c_int), case c_int when 0  then 1 when 1 then 2 else 3 end, sum(case c_int when 0  then 1 when 1 then 2 else 3 end) from \"c/b/o_t1\" group by c_int order by a");
    parse("select * from (select count(*) as a, count(distinct c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f from \"c/b/o_t1\") \"c/b/o_t1\"");
    parse("select * from (select count(*) as a, count(distinct c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f, case c_int when 0  then 1 when 1 then 2 else 3 end as g, sum(case c_int when 0  then 1 when 1 then 2 else 3 end) as h from \"c/b/o_t1\" group by c_int) \"c/b/o_t1\" order by a");
    parse("select f,a,e,b from (select count(*) as a, count(c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f from \"c/b/o_t1\") \"c/b/o_t1\"");
    parse("select f,a,e,b from (select count(*) as a, count(distinct c_int) as b, sum(distinct c_int) as c, avg(distinct c_int) as d, max(distinct c_int) as e, min(distinct c_int) as f from \"c/b/o_t1\") \"c/b/o_t1\"");
    parse("select key,count(c_int) as a, avg(c_float) from \"c/b/o_t1\" group by key order by a");
    parse("select count(distinct c_int) as a, avg(c_float) from \"c/b/o_t1\" group by c_float order by a");
    parse("select count(distinct c_int) as a, avg(c_float) from \"c/b/o_t1\" group by c_int order by a");
    parse("select count(distinct c_int) as a, avg(c_float) from \"c/b/o_t1\" group by c_float, c_int order by a");
  }

  @Test
  public void testUnionAll() throws ParseException {
    parse("select * from (select * from \"c/b/o_t1\" order by key, c_boolean, value, dt)a union all select * from (select * from \"//cbo_t2\" order by key, c_boolean, value, dt)b");
    parse("select key from (select key, c_int from (select * from \"c/b/o_t1\" union all select * from \"//cbo_t2\" where \"//cbo_t2\".key >=0)r1 union all select key, c_int from \"cbo_/t3////\")r2 where key >=0 order by key");
    parse("select r2.key from (select key, c_int from (select key, c_int from \"c/b/o_t1\" union all select key, c_int from \"cbo_/t3////\" )r1 union all select key, c_int from \"cbo_/t3////\")r2 join   (select key, c_int from (select * from \"c/b/o_t1\" union all select * from \"//cbo_t2\" where \"//cbo_t2\".key >=0)r1 union all select key, c_int from \"cbo_/t3////\")r3 on r2.key=r3.key where r3.key >=0 order by r2.key");
  }

  @Test
  public void testCreateView() throws ParseException {
    parse("create view v1_n7 as select c_int, value, c_boolean, dt from \"c/b/o_t1\"");
    parse("create view v2_n2 as select c_int, value from \"//cbo_t2\"");
    parse("create view v3_n0 as select v1_n7.value val from v1_n7 join \"c/b/o_t1\" on v1_n7.c_boolean = \"c/b/o_t1\".c_boolean");
  }

  @Test
  public void testWithClause() throws ParseException {
    parse("with q1 as ( select key from \"c/b/o_t1\" where key = '1')\n" +
        "select count(*) from q1");
    parse("with q1 as ( select key,c_int from \"c/b/o_t1\"  where key = '1')\n" +
        "select * from q1");
    parse("with q1 as ( select \"c/b/o_t1\".c_int c_int from q2 join \"c/b/o_t1\" where q2.c_int = \"c/b/o_t1\".c_int  and \"c/b/o_t1\".dt='2014'),\n" +
        "q2 as ( select c_int,c_boolean from v1_n7  where value = '1' or dt = '14')\n" +
        "select count(*) from q1 join q2 join v4_n0 on q1.c_int = q2.c_int and v4_n0.c_int = q2.c_int");
  }

  @Test
  public void testWindowingFunctions() throws ParseException {
    parse("select count(c_int) over() from \"c/b/o_t1\"");
    parse("select count(c_int) over(partition by c_float order by key), sum(c_float) over(partition by c_float order by key), max(c_int) over(partition by c_float order by key), min(c_int) over(partition by c_float order by key), row_number() over(partition by c_float order by key) as rn, rank() over(partition by c_float order by key), dense_rank() over(partition by c_float order by key), round(percent_rank() over(partition by c_float order by key), 2), lead(c_int, 2, c_int) over(partition by c_float order by key), lag(c_float, 2, c_float) over(partition by c_float order by key) from \"c/b/o_t1\" order by rn");
    parse("select * from (select count(c_int) over(partition by c_float order by key), sum(c_float) over(partition by c_float order by key), max(c_int) over(partition by c_float order by key), min(c_int) over(partition by c_float order by key), row_number() over(partition by c_float order by key) as rn, rank() over(partition by c_float order by key), dense_rank() over(partition by c_float order by key), round(percent_rank() over(partition by c_float order by key),2), lead(c_int, 2, c_int) over(partition by c_float   order by key  ), lag(c_float, 2, c_float) over(partition by c_float   order by key) from \"c/b/o_t1\" order by rn) \"c/b/o_t1\"");
    parse("select x from (select count(c_int) over() as x, sum(c_float) over() from \"c/b/o_t1\") \"c/b/o_t1\"");
    parse("select 1+sum(c_int) over() from \"c/b/o_t1\"");
    parse("select sum(c_int)+sum(sum(c_int)) over() from \"c/b/o_t1\"");
    parse("select * from (select max(c_int) over (partition by key order by value Rows UNBOUNDED PRECEDING), min(c_int) over (partition by key order by value rows current row), count(c_int) over(partition by key order by value ROWS 1 PRECEDING), avg(value) over (partition by key order by value Rows between unbounded preceding and unbounded following), sum(value) over (partition by key order by value rows between unbounded preceding and current row), avg(c_float) over (partition by key order by value Rows between 1 preceding and unbounded following), sum(c_float) over (partition by key order by value rows between 1 preceding and current row), max(c_float) over (partition by key order by value rows between 1 preceding and unbounded following), min(c_float) over (partition by key order by value rows between 1 preceding and 1 following) from \"c/b/o_t1\") \"c/b/o_t1\"");
    parse("select i, a, h, b, c, d, e, f, g, a as x, a +1 as y from (select max(c_int) over (partition by key order by value range UNBOUNDED PRECEDING) a, min(c_int) over (partition by key order by value range current row) b, count(c_int) over(partition by key order by value range 1 PRECEDING) c, avg(value) over (partition by key order by value range between unbounded preceding and unbounded following) d, sum(value) over (partition by key order by value range between unbounded preceding and current row) e, avg(c_float) over (partition by key order by value range between 1 preceding and unbounded following) f, sum(c_float) over (partition by key order by value range between 1 preceding and current row) g, max(c_float) over (partition by key order by value range between 1 preceding and unbounded following) h, min(c_float) over (partition by key order by value range between 1 preceding and 1 following) i from \"c/b/o_t1\") \"c/b/o_t1\"");
    parse("select *, rank() over(partition by key order by value) as rr from default.src1");
    parse("select *, rank() over(partition by key order by value) from default.src1");
  }

  @Test
  public void testInsert() throws ParseException {
    parse("insert into table \"src/_/cbo\" select * from default.src");
    parse("insert overwrite table \"src/_/cbo\" select * from default.src");
    parse("insert into \"t//\" values(1)");
    parse("insert into \"t//\" values(null)");
  }

  @Test
  public void testDropTable() throws ParseException {
    parse("drop table \"t//\"");
  }

  @Test
  public void testExplain() throws ParseException {
    parse("explain select * from \"t//\"");
  }

  @Test
  public void testDropDatabase() throws ParseException {
    parse("drop database \"db~!@#$%^&*(),<>\" cascade");
  }
}
