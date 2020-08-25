/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.DecodeTimestampOptions;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.orc.reader.ApacheHiveTimestampDecoder.decodeTimestamp;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestApacheHiveTimestampDecoder
{
    @Test
    public void testMicroseconds()
    {
        test(694310400, 7994, MICROSECONDS, parseTimestamp("2037-01-01T00:00:00", 999));
        test(-378691200, 1776, MICROSECONDS, parseTimestamp("2003-01-01T00:00:00", 0));
        test(-504921600, 7999999992L, MICROSECONDS, parseTimestamp("1999-01-01T00:00:00", 999999));
        test(-631152000, 5511111104L, MICROSECONDS, parseTimestamp("1995-01-01T00:00:00", 688888));
        test(-410227200, 15, MICROSECONDS, parseTimestamp("2002-01-01T00:00:00", 100000));
        test(-152582400, 72008, MICROSECONDS, parseTimestamp("2010-03-02T00:00:00", 9));
        test(-315532800, 17832, MICROSECONDS, parseTimestamp("2005-01-01T00:00:00", 2));

        test(-283996800, 7201624024L, MICROSECONDS, parseTimestamp("2006-01-01T00:00:00", 900203));
        test(-378691200, 6400000056L, MICROSECONDS, parseTimestamp("2003-01-01T00:00:00", 800000));
        test(-581130000, 5784806472L, MICROSECONDS, parseTimestamp("1996-08-01T23:00:00", 723100));
        test(-510105600, 6858725144L, MICROSECONDS, parseTimestamp("1998-11-02T00:00:00", 857340));
        test(-197168400, 0, MICROSECONDS, parseTimestamp("2008-10-01T23:00:00", 0));
    }

    @Test
    public void testMilliseconds()
    {
        test(694310400, 7994, MILLISECONDS, parseTimestamp("2037-01-01T00:00:00", 0));
        test(-378691200, 1776, MILLISECONDS, parseTimestamp("2003-01-01T00:00:00", 0));
        test(-504921600, 7999999992L, MILLISECONDS, parseTimestamp("1999-01-01T00:00:00", 999000));
        test(-631152000, 5511111104L, MILLISECONDS, parseTimestamp("1995-01-01T00:00:00", 688000));
        test(-410227200, 15, MILLISECONDS, parseTimestamp("2002-01-01T00:00:00", 100000));
        test(-152582400, 72008, MILLISECONDS, parseTimestamp("2010-03-02T00:00:00", 0));
        test(-315532800, 17832, MILLISECONDS, parseTimestamp("2005-01-01T00:00:00", 0));
        test(-283996800, 7201624024L, MILLISECONDS, parseTimestamp("2006-01-01T00:00:00", 900000));
        test(-378691200, 6400000056L, MILLISECONDS, parseTimestamp("2003-01-01T00:00:00", 800000));
        test(-581130000, 5784806472L, MILLISECONDS, parseTimestamp("1996-08-01T23:00:00", 723000));
        test(-510105600, 6858725144L, MILLISECONDS, parseTimestamp("1998-11-02T00:00:00", 857000));
        test(-197168400, 0, MILLISECONDS, parseTimestamp("2008-10-01T23:00:00", 0));
    }

    private static void test(long seconds, long nanos, TimeUnit unit, Timestamp expected)
    {
        long tsAsLong = decodeTimestamp(seconds, nanos, new DecodeTimestampOptions(UTC, unit));
        long unitsPerSec = unit.convert(1, TimeUnit.SECONDS);
        Timestamp ts = new Timestamp(1000 * (tsAsLong / unitsPerSec));
        ts.setNanos((int) NANOSECONDS.convert(tsAsLong % unitsPerSec, unit));
        assertEquals(ts, expected);
    }

    private static Timestamp parseTimestamp(String s, int micros)
    {
        Timestamp ts = new Timestamp(DateTime.parse(s, ISODateTimeFormat.dateTimeParser().withZoneUTC()).getMillis());
        ts.setNanos((int) TimeUnit.MICROSECONDS.toNanos(micros));
        return ts;
    }
}
