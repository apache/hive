/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.jdbc;

import java.util.ArrayList;
import java.util.List;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;

public class LlapInputFormat<V extends WritableComparable> implements InputFormat<NullWritable, V> {

  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
  private String url;  // "jdbc:hive2://localhost:10000/default"
  private String user; // "hive",
  private String pwd;  // ""
  private String query;

  public final String URL_KEY = "llap.if.hs2.connection";
  public final String QUERY_KEY = "llap.if.query";
  public final String USER_KEY = "llap.if.user";
  public final String PWD_KEY = "llap.if.pwd";

  private Connection con;
  private Statement stmt;

  public LlapInputFormat(String url, String user, String pwd, String query) {
    this.url = url;
    this.user = user;
    this.pwd = pwd;
    this.query = query;
  }

  public LlapInputFormat() {}


  @Override
  public RecordReader<NullWritable, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    LlapInputSplit llapSplit = (LlapInputSplit) split;
    return llapSplit.getInputFormat().getRecordReader(llapSplit.getSplit(), job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<InputSplit> ins = new ArrayList<InputSplit>();

    if (url == null) url = job.get(URL_KEY);
    if (query == null) query = job.get(QUERY_KEY);
    if (user == null) user = job.get(USER_KEY);
    if (pwd == null) pwd = job.get(PWD_KEY);

    if (url == null || query == null) {
      throw new IllegalStateException();
    }

    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    try {
      con = DriverManager.getConnection(url,user,pwd);
      stmt = con.createStatement();
      String sql = "select r.if_class as ic, r.split_class as sc, r.split as s from (select explode(get_splits(\""+query+"\","+numSplits+")) as r) t";
      ResultSet res = stmt.executeQuery(sql);
      while (res.next()) {
        // deserialize split
        DataInput in = new DataInputStream(res.getBinaryStream(3));
        InputSplitWithLocationInfo is = (InputSplitWithLocationInfo)Class.forName(res.getString(2)).newInstance();
        is.readFields(in);
        ins.add(new LlapInputSplit(is, res.getString(1)));
      }

      res.close();
      stmt.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
    return ins.toArray(new InputSplit[ins.size()]);
  }

  public void close() {
    try {
      con.close();
    } catch (Exception e) {
      // ignore
    }
  }
}
