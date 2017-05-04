package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HiveWrapperTest {
  @Mock
  private HiveWrapper.Tuple.Function<ReplicationSpec> specFunction;
  @Mock
  private HiveWrapper.Tuple.Function<Table> tableFunction;

  @Test
  public void replicationIdIsRequestedBeforeObjectDefinition() throws HiveException {
    new HiveWrapper.Tuple<>(specFunction, tableFunction);
    InOrder inOrder = Mockito.inOrder(specFunction, tableFunction);
    inOrder.verify(specFunction).fromMetaStore();
    inOrder.verify(tableFunction).fromMetaStore();
  }
}