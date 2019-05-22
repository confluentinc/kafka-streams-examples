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

//Implementation of a daily custom window starting at given hour (like daily windows starting at 6pm) with a given timezone
public class DailyTimeWindows extends Windows<TimeWindow> {

    private final ZoneId zoneId;
    private final long grace;
    private final int startHour;

    public DailyTimeWindows(final ZoneId zoneId, final int startHour, final Duration grace) {
        this.zoneId = zoneId;
        this.grace = grace.toMillis();
        this.startHour = startHour;
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        final Instant instant = Instant.ofEpochMilli(timestamp);

        final ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        final ZonedDateTime startTime = zonedDateTime.getHour() >= startHour ? zonedDateTime.truncatedTo(ChronoUnit.DAYS).withHour(startHour) : zonedDateTime.truncatedTo(ChronoUnit.DAYS).minusDays(1).withHour(startHour);
        final ZonedDateTime endTime = startTime.plusDays(1);

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

    private long toEpochMilli(final ZonedDateTime zonedDateTime) {
        return zonedDateTime.toInstant().toEpochMilli();
    }
}
