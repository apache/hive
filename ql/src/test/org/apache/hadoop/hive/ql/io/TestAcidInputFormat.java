package org.apache.hadoop.hive.ql.io;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataInput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.io.AcidInputFormat.DeltaMetaData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestAcidInputFormat {

  @Mock
  private DataInput mockDataInput;

  @Test
  public void testDeltaMetaDataReadFieldsNoStatementIds() throws Exception {
    when(mockDataInput.readLong()).thenReturn(1L, 2L);
    when(mockDataInput.readInt()).thenReturn(0);

    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData();
    deltaMetaData.readFields(mockDataInput);

    verify(mockDataInput, times(1)).readInt();
    assertThat(deltaMetaData.getMinTxnId(), is(1L));
    assertThat(deltaMetaData.getMaxTxnId(), is(2L));
    assertThat(deltaMetaData.getStmtIds().isEmpty(), is(true));
  }

  @Test
  public void testDeltaMetaDataReadFieldsWithStatementIds() throws Exception {
    when(mockDataInput.readLong()).thenReturn(1L, 2L);
    when(mockDataInput.readInt()).thenReturn(2, 100, 101);

    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData();
    deltaMetaData.readFields(mockDataInput);

    verify(mockDataInput, times(3)).readInt();
    assertThat(deltaMetaData.getMinTxnId(), is(1L));
    assertThat(deltaMetaData.getMaxTxnId(), is(2L));
    assertThat(deltaMetaData.getStmtIds().size(), is(2));
    assertThat(deltaMetaData.getStmtIds().get(0), is(100));
    assertThat(deltaMetaData.getStmtIds().get(1), is(101));
  }

  @Test
  public void testDeltaMetaConstructWithState() throws Exception {
    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData(2000L, 2001L, Arrays.asList(97, 98, 99));

    assertThat(deltaMetaData.getMinTxnId(), is(2000L));
    assertThat(deltaMetaData.getMaxTxnId(), is(2001L));
    assertThat(deltaMetaData.getStmtIds().size(), is(3));
    assertThat(deltaMetaData.getStmtIds().get(0), is(97));
    assertThat(deltaMetaData.getStmtIds().get(1), is(98));
    assertThat(deltaMetaData.getStmtIds().get(2), is(99));
  }

  @Test
  public void testDeltaMetaDataReadFieldsWithStatementIdsResetsState() throws Exception {
    when(mockDataInput.readLong()).thenReturn(1L, 2L);
    when(mockDataInput.readInt()).thenReturn(2, 100, 101);

    List<Integer> statementIds = new ArrayList<>();
    statementIds.add(97);
    statementIds.add(98);
    statementIds.add(99);
    DeltaMetaData deltaMetaData = new AcidInputFormat.DeltaMetaData(2000L, 2001L, statementIds);
    deltaMetaData.readFields(mockDataInput);

    verify(mockDataInput, times(3)).readInt();
    assertThat(deltaMetaData.getMinTxnId(), is(1L));
    assertThat(deltaMetaData.getMaxTxnId(), is(2L));
    assertThat(deltaMetaData.getStmtIds().size(), is(2));
    assertThat(deltaMetaData.getStmtIds().get(0), is(100));
    assertThat(deltaMetaData.getStmtIds().get(1), is(101));
  }

}
