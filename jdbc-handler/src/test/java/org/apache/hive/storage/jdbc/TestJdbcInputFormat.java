/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DatabaseAccessorFactory.class)
public class TestJdbcInputFormat {

  @Mock
  private DatabaseAccessor mockDatabaseAccessor;


  @Test
  public void testSplitLogic_noSpillOver() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getTotalNumberOfRecords(any(Configuration.class))).thenReturn(15);

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    InputSplit[] splits = f.getSplits(conf, 3);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(3));

    assertThat(splits[0].getLength(), is(5L));
  }


  @Test
  public void testSplitLogic_withSpillOver() throws HiveJdbcDatabaseAccessException, IOException {
    PowerMockito.mockStatic(DatabaseAccessorFactory.class);
    BDDMockito.given(DatabaseAccessorFactory.getAccessor(any(Configuration.class))).willReturn(mockDatabaseAccessor);
    JdbcInputFormat f = new JdbcInputFormat();
    when(mockDatabaseAccessor.getTotalNumberOfRecords(any(Configuration.class))).thenReturn(15);

    JobConf conf = new JobConf();
    conf.set("mapred.input.dir", "/temp");
    InputSplit[] splits = f.getSplits(conf, 6);

    assertThat(splits, is(notNullValue()));
    assertThat(splits.length, is(6));

    for (int i = 0; i < 3; i++) {
      assertThat(splits[i].getLength(), is(3L));
    }

    for (int i = 3; i < 6; i++) {
      assertThat(splits[i].getLength(), is(2L));
    }
  }
}
