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
package com.facebook.presto.orc;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class DecodeTimestampOptions
{
    private static final int MILLIS_PER_SECOND = 1000;

    private final long timestampUnitsPerSecond;
    private final long nanosecondsPerTimeUnit;
    private final long baseTimestampInSeconds;

    public DecodeTimestampOptions(
            DateTimeZone hiveStorageTimeZone, TimeUnit timestampTimeUnit)
    {
        this.timestampUnitsPerSecond = requireNonNull(timestampTimeUnit).convert(1, TimeUnit.SECONDS);
        if (this.timestampUnitsPerSecond == 0) {
            throw new IllegalArgumentException(timestampTimeUnit + " should be <= second");
        }
        this.nanosecondsPerTimeUnit = TimeUnit.NANOSECONDS.convert(1, requireNonNull(timestampTimeUnit));
        if (this.nanosecondsPerTimeUnit == 0) {
            throw new IllegalArgumentException(timestampTimeUnit + " should be >= nanosecond");
        }
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / MILLIS_PER_SECOND;
    }

    public long timestampUnitsPerSecond()
    {
        return timestampUnitsPerSecond;
    }

    public long nanosecondsPerTimestampUnit()
    {
        return nanosecondsPerTimeUnit;
    }

    public long baseTimestampInSeconds()
    {
        return baseTimestampInSeconds;
    }
}
