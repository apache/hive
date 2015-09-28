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

package org.apache.hadoop.hive.ql.io.avro;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.when;

import java.io.IOException;

public class TestAvroGenericRecordReader {

    @Mock private JobConf jobConf;
    @Mock private FileSplit emptyFileSplit;
    @Mock private Reporter reporter;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(emptyFileSplit.getLength()).thenReturn(0l);
    }

    @Test
    public void emptyFile() throws IOException
    {
        AvroGenericRecordReader reader = new AvroGenericRecordReader(jobConf, emptyFileSplit, reporter);

        //next() should always return false
        Assert.assertEquals(false, reader.next(null, null));

        //getPos() should always return 0
        Assert.assertEquals(0, reader.getPos());

        //close() should just do nothing
        reader.close();
    }
}
