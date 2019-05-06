/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.log;

import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.log.syslog.SyslogInputFormat;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestSyslogInputFormat {

  @Test
  public void testTimestampFilePruningAllBefore() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().between("ts", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2018-01-03 10:00:00.000"), Timestamp.valueOf("2019-01-03 10:00:00.000")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-10_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList();
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampFilePruningAllAfter() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().between("ts", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2020-01-03 10:00:00.000"), Timestamp.valueOf("2020-01-03 10:00:00.000")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-10_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList();
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampFilePruningSimple() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().between("ts", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2019-01-03 10:15:00.000"), Timestamp.valueOf("2019-01-03 11:15:00.000")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-00_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList(fs2, fs3, fs4, fs5)
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampFilePruningTimeSlice() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().between("ts", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2019-01-03 10:16:00.000"), Timestamp.valueOf("2019-01-03 11:16:00.000")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-10_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList(fs2, fs3, fs4, fs5)
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampFilePruningPredicateWithSeconds() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().between("ts", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2019-01-03 10:15:03.600"), Timestamp.valueOf("2019-01-03 11:15:04.320")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-00_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList(fs2, fs3, fs4, fs5)
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampFilePruningPredicateWithSecondsFlipRange() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().between("ts", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2019-01-03 11:15:04.320"), Timestamp.valueOf("2019-01-03 10:15:03.600")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-00_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList();
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampNoFilePruningUnsupportedColumnName() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().between("ts1", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2019-01-03 11:15:04.320"), Timestamp.valueOf("2019-01-03 10:15:03.600")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-10_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6)
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampNoFilePruningUnsupportedExpression() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().equals("ts", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2019-01-03 11:15:04.320")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-10_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6)
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampNoFilePruningUnsupportedMultiExpression() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().startOr()
        .equals("ts", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2019-01-03 11:15:04.320"))
        .equals("ts", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2020-01-03 11:15:04.320"))
        .end()
        .build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-10_0.log.gz"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_0.log.gz"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15_1.log.gz"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35_0.log.gz"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15_0.log.gz"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35_0.log.gz"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6)
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }

  @Test
  public void testTimestampNoFilePruningUnsupportedFilename() {
    TimeZone defaultTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      SearchArgument sarg = SearchArgumentFactory.newBuilder().between("ts", PredicateLeaf.Type.TIMESTAMP,
        Timestamp.valueOf("2019-01-03 10:15:03.600"), Timestamp.valueOf("2019-01-03 11:15:04.320")).build();
      SyslogInputFormat syslogInputFormat = new SyslogInputFormat();
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-10"));
      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15"));
      FileStatus fs3 = new FileStatus();
      fs3.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-15"));
      FileStatus fs4 = new FileStatus();
      fs4.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-10-35"));
      FileStatus fs5 = new FileStatus();
      fs5.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-15"));
      FileStatus fs6 = new FileStatus();
      fs6.setPath(new Path("s3a://bucket/logs/dt=2019-01-01/ns=foo/app=hs2/2019-01-03-11-35"));
      List<FileStatus> inputFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6);
      List<String> expectedFiles = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6)
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      List<FileStatus> selectedFiles = syslogInputFormat.pruneFiles(sarg, 300, inputFiles);
      List<String> gotFiles = selectedFiles
        .stream()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
      Assert.assertEquals(expectedFiles, gotFiles);
    } finally {
      TimeZone.setDefault(defaultTZ);
    }
  }
}
