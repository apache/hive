/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.timestamp;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import jodd.datetime.JDateTime;

/**
 * Utilities for converting from java.sql.Timestamp to parquet timestamp.
 * This utilizes the Jodd library.
 */
public class NanoTimeUtils {
   static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
   static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
   static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

   private static final ThreadLocal<Calendar> parquetGMTCalendar = new ThreadLocal<Calendar>();
   private static final ThreadLocal<Calendar> parquetLocalCalendar = new ThreadLocal<Calendar>();

   private static Calendar getGMTCalendar() {
     //Calendar.getInstance calculates the current-time needlessly, so cache an instance.
     if (parquetGMTCalendar.get() == null) {
       parquetGMTCalendar.set(Calendar.getInstance(TimeZone.getTimeZone("GMT")));
     }
     return parquetGMTCalendar.get();
   }

   private static Calendar getLocalCalendar() {
     if (parquetLocalCalendar.get() == null) {
       parquetLocalCalendar.set(Calendar.getInstance());
     }
     return parquetLocalCalendar.get();
   }

   private static Calendar getCalendar(boolean skipConversion) {
     return skipConversion ? getLocalCalendar() : getGMTCalendar();
   }

   public static NanoTime getNanoTime(Timestamp ts, boolean skipConversion) {

     Calendar calendar = getCalendar(skipConversion);
     calendar.setTime(ts);
     JDateTime jDateTime = new JDateTime(calendar.get(Calendar.YEAR),
       calendar.get(Calendar.MONTH) + 1,  //java calendar index starting at 1.
       calendar.get(Calendar.DAY_OF_MONTH));
     int days = jDateTime.getJulianDayNumber();

     long hour = calendar.get(Calendar.HOUR_OF_DAY);
     long minute = calendar.get(Calendar.MINUTE);
     long second = calendar.get(Calendar.SECOND);
     long nanos = ts.getNanos();
     long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE * minute +
         NANOS_PER_HOUR * hour;

     return new NanoTime(days, nanosOfDay);
   }

   public static Timestamp getTimestamp(NanoTime nt, boolean skipConversion) {
     int julianDay = nt.getJulianDay();
     long nanosOfDay = nt.getTimeOfDayNanos();

     JDateTime jDateTime = new JDateTime((double) julianDay);
     Calendar calendar = getCalendar(skipConversion);
     calendar.set(Calendar.YEAR, jDateTime.getYear());
     calendar.set(Calendar.MONTH, jDateTime.getMonth() - 1); //java calender index starting at 1.
     calendar.set(Calendar.DAY_OF_MONTH, jDateTime.getDay());

     long remainder = nanosOfDay;
     int hour = (int) (remainder / (NANOS_PER_HOUR));
     remainder = remainder % (NANOS_PER_HOUR);
     int minutes = (int) (remainder / (NANOS_PER_MINUTE));
     remainder = remainder % (NANOS_PER_MINUTE);
     int seconds = (int) (remainder / (NANOS_PER_SECOND));
     long nanos = remainder % NANOS_PER_SECOND;

     calendar.set(Calendar.HOUR_OF_DAY, hour);
     calendar.set(Calendar.MINUTE, minutes);
     calendar.set(Calendar.SECOND, seconds);
     Timestamp ts = new Timestamp(calendar.getTimeInMillis());
     ts.setNanos((int) nanos);
     return ts;
   }
}
