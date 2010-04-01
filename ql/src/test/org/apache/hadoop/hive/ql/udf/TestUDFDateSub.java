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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

import java.util.TimeZone;

/**
 * JUnit test for UDFDateSub.
 */
public class TestUDFDateSub extends TestCase {

    /**
     * Verify if subtracting dates across a daylight savings time change
     * from daylight to standard time.  The timezone tested is west
     * coast US (PDT/PST) with a 1 hour shift back in time at 02:00 AM
     * on 2009-10-31.
     */
    public void testFallBack() throws Exception {
        // set the default time zone so that the dates cover 
        // the zone's daylight saving time adjustment (2009-10-31)
        // from daylight to standard time
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
        final IntWritable nmDays = new IntWritable(7);
        final Text dtStr = new Text("2009-11-02");

        final Text result = new UDFDateSub().evaluate(dtStr, nmDays);
        assertEquals("2009-10-26", result.toString());
    }

    /**
     * Verify if subtracting dates across a daylight savings time change
     * from standard to daylight time.  The timezone tested is west
     * coast US (PDT/PST) with a 1 hour shift forward in time at 02:00 AM
     * on 2010-03-14.
     */
    public void testSpringAhead() throws Exception {
        // set the default time zone so that the dates cover 
        // the zone's daylight saving time adjustment (2010-03-14)
        // from standard to daylight time
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
        final IntWritable nmDays = new IntWritable(7);
        final Text dtStr = new Text("2010-03-15");

        final Text result = new UDFDateSub().evaluate(dtStr, nmDays);
        assertEquals("2010-03-08", result.toString());
    }
}
