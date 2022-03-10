/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidInputFormat.DeltaMetaData;
import org.apache.hadoop.hive.ql.io.HdfsUtils.HdfsFileStatusWithoutId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestAcidInputFormat {

  @Mock
  private DataInput mockDataInput;
  
  @Test
  public void testDeltaMetaDataReadFieldsNoStatementIds() throws Exception {
    when(mockDataInput.readLong()).thenReturn(1L, 2L);
    when(mockDataInput.readInt()).thenReturn(0, 0);

    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData();
    deltaMetaData.readFields(mockDataInput);

    verify(mockDataInput, times(2)).readInt();
    assertThat(deltaMetaData.getMinWriteId(), is(1L));
    assertThat(deltaMetaData.getMaxWriteId(), is(2L));
    assertThat(deltaMetaData.getStmtIds().isEmpty(), is(true));
  }

  @Test
  public void testDeltaMetaDataReadFieldsWithStatementIds() throws Exception {
    when(mockDataInput.readLong()).thenReturn(1L, 2L);
    when(mockDataInput.readInt()).thenReturn(2, 100, 101, 0);

    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData();
    deltaMetaData.readFields(mockDataInput);

    verify(mockDataInput, times(4)).readInt();
    assertThat(deltaMetaData.getMinWriteId(), is(1L));
    assertThat(deltaMetaData.getMaxWriteId(), is(2L));
    assertThat(deltaMetaData.getStmtIds().size(), is(2));
    assertThat(deltaMetaData.getStmtIds().get(0), is(100));
    assertThat(deltaMetaData.getStmtIds().get(1), is(101));
  }

  @Test
  public void testDeltaMetaConstructWithState() throws Exception {
    DeltaMetaData deltaMetaData = new AcidInputFormat
        .DeltaMetaData(2000L, 2001L, Arrays.asList(97, 98, 99), 0, null);

    assertThat(deltaMetaData.getMinWriteId(), is(2000L));
    assertThat(deltaMetaData.getMaxWriteId(), is(2001L));
    assertThat(deltaMetaData.getStmtIds().size(), is(3));
    assertThat(deltaMetaData.getStmtIds().get(0), is(97));
    assertThat(deltaMetaData.getStmtIds().get(1), is(98));
    assertThat(deltaMetaData.getStmtIds().get(2), is(99));
  }

  @Test
  public void testDeltaMetaWithFile() throws Exception {
    FileStatus fs = new FileStatus(200, false, 100, 100, 100, new Path("mypath"));
    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData(2000L, 2001L, new ArrayList<>(), 0,
        Collections.singletonList(new AcidInputFormat.DeltaFileMetaData(new HdfsFileStatusWithoutId(fs), null, 1)));

    assertEquals(2000L, deltaMetaData.getMinWriteId());
    assertEquals(2001L, deltaMetaData.getMaxWriteId());
    assertEquals(0, deltaMetaData.getStmtIds().size());

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    deltaMetaData.write(new DataOutputStream(byteArrayOutputStream));

    byte[] bytes = byteArrayOutputStream.toByteArray();
    DeltaMetaData copy = new DeltaMetaData();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

    assertEquals(2000L, copy.getMinWriteId());
    assertEquals(2001L, copy.getMaxWriteId());
    assertEquals(0, copy.getStmtIds().size());
    AcidInputFormat.DeltaFileMetaData fileMetaData = copy.getDeltaFiles().get(0);
    Object fileId = fileMetaData.getFileId(new Path("deleteDelta"), 1, new HiveConf());

    Assert.assertTrue(fileId instanceof SyntheticFileId);
    assertEquals(100, ((SyntheticFileId)fileId).getModTime());
    assertEquals(200, ((SyntheticFileId)fileId).getLength());

    String fileName = fileMetaData.getPath(new Path("deleteDelta"), 1).getName();
    Assert.assertEquals("bucket_00001", fileName);
  }

  @Test
  public void testDeltaMetaWithHdfsFileId() throws Exception {
    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData(2000L, 2001L, new ArrayList<>(), 0,
        Collections.singletonList(new AcidInputFormat.DeltaFileMetaData(100, 200, null, 123L, null,1)));

    assertEquals(2000L, deltaMetaData.getMinWriteId());
    assertEquals(2001L, deltaMetaData.getMaxWriteId());
    assertEquals(0, deltaMetaData.getStmtIds().size());

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    deltaMetaData.write(new DataOutputStream(byteArrayOutputStream));

    byte[] bytes = byteArrayOutputStream.toByteArray();
    DeltaMetaData copy = new DeltaMetaData();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

    assertEquals(2000L, copy.getMinWriteId());
    assertEquals(2001L, copy.getMaxWriteId());
    assertEquals(0, copy.getStmtIds().size());
    AcidInputFormat.DeltaFileMetaData fileMetaData = copy.getDeltaFiles().get(0);

    Object fileId = fileMetaData.getFileId(new Path("deleteDelta"), 1, new HiveConf());
    Assert.assertTrue(fileId instanceof Long);
    long fId = (Long)fileId;
    assertEquals(123L, fId);

    String fileName = fileMetaData.getPath(new Path("deleteDelta"), 1).getName();
    Assert.assertEquals("bucket_00001", fileName);
  }
  @Test
  public void testDeltaMetaWithAttemptId() throws Exception {
    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData(2000L, 2001L, new ArrayList<>(), 0,
        Collections.singletonList(new AcidInputFormat.DeltaFileMetaData(100, 200, 123, null, null, 1)));

    assertEquals(2000L, deltaMetaData.getMinWriteId());
    assertEquals(2001L, deltaMetaData.getMaxWriteId());
    assertEquals(0, deltaMetaData.getStmtIds().size());

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    deltaMetaData.write(new DataOutputStream(byteArrayOutputStream));

    byte[] bytes = byteArrayOutputStream.toByteArray();
    DeltaMetaData copy = new DeltaMetaData();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

    assertEquals(2000L, copy.getMinWriteId());
    assertEquals(2001L, copy.getMaxWriteId());
    assertEquals(0, copy.getStmtIds().size());
    AcidInputFormat.DeltaFileMetaData fileMetaData = copy.getDeltaFiles().get(0);
    Object fileId = fileMetaData.getFileId(new Path("deleteDelta"), 1, new HiveConf());

    Assert.assertTrue(fileId instanceof SyntheticFileId);
    assertEquals(100, ((SyntheticFileId)fileId).getModTime());
    assertEquals(200, ((SyntheticFileId)fileId).getLength());

    String fileName = fileMetaData.getPath(new Path("deleteDelta"), 1).getName();
    Assert.assertEquals("bucket_00001_123", fileName);
  }

  @Test
  public void testDeltaMetaWithFileMultiStatement() throws Exception {
    FileStatus fs = new FileStatus(200, false, 100, 100, 100, new Path("mypath"));
    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData(2000L, 2001L, Arrays.asList(97, 98, 99), 0,
        Collections.singletonList(new AcidInputFormat.DeltaFileMetaData(new HdfsFileStatusWithoutId(fs), 97, 1)));

    assertEquals(2000L, deltaMetaData.getMinWriteId());
    assertEquals(2001L, deltaMetaData.getMaxWriteId());
    assertEquals(3, deltaMetaData.getStmtIds().size());

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    deltaMetaData.write(new DataOutputStream(byteArrayOutputStream));

    byte[] bytes = byteArrayOutputStream.toByteArray();
    DeltaMetaData copy = new DeltaMetaData();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));

    assertEquals(2000L, copy.getMinWriteId());
    assertEquals(2001L, copy.getMaxWriteId());
    assertEquals(3, copy.getStmtIds().size());
    Object fileId = copy.getDeltaFiles().get(0).getFileId(new Path("deleteDelta"), 1, new HiveConf());
    Assert.assertTrue(fileId instanceof SyntheticFileId);

    assertEquals(100, ((SyntheticFileId)fileId).getModTime());
    assertEquals(200, ((SyntheticFileId)fileId).getLength());
    assertEquals(1, copy.getDeltaFilesForStmtId(97).size());
    assertEquals(0, copy.getDeltaFilesForStmtId(99).size());
  }

  @Test
  public void testDeltaMetaDataReadFieldsWithStatementIdsResetsState() throws Exception {
    when(mockDataInput.readLong()).thenReturn(1L, 2L);
    when(mockDataInput.readInt()).thenReturn(2, 100, 101, 0);

    List<Integer> statementIds = new ArrayList<>();
    statementIds.add(97);
    statementIds.add(98);
    statementIds.add(99);
    DeltaMetaData deltaMetaData = new AcidInputFormat
        .DeltaMetaData(2000L, 2001L, statementIds, 0, null);
    deltaMetaData.readFields(mockDataInput);

    verify(mockDataInput, times(4)).readInt();
    assertThat(deltaMetaData.getMinWriteId(), is(1L));
    assertThat(deltaMetaData.getMaxWriteId(), is(2L));
    assertThat(deltaMetaData.getStmtIds().size(), is(2));
    assertThat(deltaMetaData.getStmtIds().get(0), is(100));
    assertThat(deltaMetaData.getStmtIds().get(1), is(101));
  }

}
