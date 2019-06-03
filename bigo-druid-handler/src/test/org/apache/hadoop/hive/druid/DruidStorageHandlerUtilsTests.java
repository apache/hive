package org.apache.hadoop.hive.druid;

import org.apache.druid.timeline.DataSegment;
import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.chrono.GregorianChronology;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tangyun@bigo.sg
 * @date 5/31/19 2:31 PM
 */
public class DruidStorageHandlerUtilsTests {

    @Test
    public void getIntervalsToOverWriteTest01() {

        List<DataSegment> dataSegments = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            DataSegment dataSegment = new DataSegment(
                    "ds",
                    new Interval(start + i * 100000, start + (i + 1) *100000).
                            withChronology(GregorianChronology.getInstance(DateTimeZone.UTC)),
                    "111111",
                    null,
                    null,
                    null,
                    null,
                    null,
                    10
            );
            dataSegments.add(dataSegment);
        }

        List<Interval> intervals =
                DruidStorageHandlerUtils.getIntervalsToOverWrite(dataSegments);
        for (Interval interval: intervals) {
            System.out.println(interval.getStart().toDateTime());
            System.out.println(interval.getEnd().toDateTime());
        }
    }

    @Test
    public void getIntervalsToOverWriteTest02() {

        List<DataSegment> dataSegments = new ArrayList<>();
        long start = 1559034000000L;
        for (int i = 0; i < 14; ++i) {
            DataSegment dataSegment = new DataSegment(
                    "ds",
                    new Interval(start + i * 3600000, start + (i + 1) * 3600000).
                            withChronology(GregorianChronology.getInstance(DateTimeZone.UTC)),
                    "111111",
                    null,
                    null,
                    null,
                    null,
                    null,
                    10
            );
            dataSegments.add(dataSegment);
        }

        List<Interval> intervals =
                DruidStorageHandlerUtils.getIntervalsToOverWrite(dataSegments);
        for (Interval interval: intervals) {
            System.out.println(interval.getStart().toDateTime());
            System.out.println(interval.getEnd().toDateTime());
        }
        System.out.println(dataSegments.get(dataSegments.size() - 1).getInterval().getStart());
        System.out.println(dataSegments.get(dataSegments.size() - 1).getInterval().getEnd());
    }

    @Test
    public void intervalTest() {

        Interval interval = new Interval(System.currentTimeMillis(), System.currentTimeMillis() + 1000000);
        System.out.println(interval);
        System.out.println(interval.getStart());
        System.out.println(interval.getEnd());
    }

    @Test
    public void test01() {

        String data = "data";
        Map<String, String> map = new HashMap<>();
        System.out.println(map.getClass());
    }

    @Test
    public void test02() {

        Interval interval = new Interval(1559034000000L, 1559037600000L);
        Interval interval1 = interval.withChronology(GregorianChronology.getInstance(DateTimeZone.UTC));
        System.out.println(interval);
        System.out.println(interval1);
    }
}
