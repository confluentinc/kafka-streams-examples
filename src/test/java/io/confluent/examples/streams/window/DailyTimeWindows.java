/*
 * Copyright Confluent Inc.
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
package io.confluent.examples.streams.window;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

//Implementation of a daily custom window starting at 6pm with a given timezone
public class DailyTimeWindows extends Windows<TimeWindow> {

    private final ZoneId zoneId;
    private long grace;

    public DailyTimeWindows(ZoneId zoneId, Duration grace) {
        this.zoneId = zoneId;
        this.grace = grace.toMillis();
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);

        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        ZonedDateTime startTime = zonedDateTime.getHour() >= 18 ? zonedDateTime.truncatedTo(ChronoUnit.DAYS).withHour(18) : zonedDateTime.truncatedTo(ChronoUnit.DAYS).minusDays(1).withHour(18);
        ZonedDateTime endTime = startTime.plusDays(1);

        final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
        windows.put(toEpochMilli(startTime), new TimeWindow(toEpochMilli(startTime), toEpochMilli(endTime)));
        return windows;
    }

    @Override
    public long maintainMs() {
        //By default the Window class have a maintainMs = 1 day
        //So we need to make sure retention is at least than size + grace period
        //Otherwise sanity check made by TimeWindowedKStreamImpl.materialize() method will throw an Exception

        //NOTE: that this could be done either in the window it self or by configuring retention on the Materialize by calling Materialize.withRetention(XXX)
        return size() + gracePeriodMs();
    }

    @Override
    public long size() {
        return Duration.ofDays(1).toMillis();
    }

    @Override
    public long gracePeriodMs() {
        return grace;
    }

    private long toEpochMilli(ZonedDateTime zonedDateTime) {
        return zonedDateTime.toInstant().toEpochMilli();
    }
}
