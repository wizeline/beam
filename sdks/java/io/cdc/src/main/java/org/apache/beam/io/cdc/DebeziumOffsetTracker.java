/*
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
package org.apache.beam.io.cdc;

import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DebeziumOffsetTracker extends RestrictionTracker<DebeziumOffsetHolder, Map<String, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumOffsetTracker.class);

    private DebeziumOffsetHolder restriction;
    long toMillis = 60 * 1000;

    DebeziumOffsetTracker(DebeziumOffsetHolder holder) {
        this.restriction = holder;
    }

    @Override
    public boolean tryClaim(Map<String, Object> position) {
        LOG.info("-------------- Claiming {} used to have: {}", position, restriction.offset);
        DateTime currentTime = new DateTime();
        long elapsedTime = currentTime.getMillis() - KafkaSourceConsumerFn.startTime.getMillis();
        LOG.info("-------------- Time running: {} / {}", elapsedTime, (KafkaSourceConsumerFn.minutesToRun * toMillis));
        this.restriction = new DebeziumOffsetHolder(position, this.restriction.history);
        return elapsedTime < (KafkaSourceConsumerFn.minutesToRun * toMillis);
    }

    @Override
    public DebeziumOffsetHolder currentRestriction() {
        return restriction;
    }

    @Override
    public SplitResult<DebeziumOffsetHolder> trySplit(double fractionOfRemainder) {
        LOG.info("-------------- Trying to split: fractionOfRemainder={}", fractionOfRemainder);

        return SplitResult.of(new DebeziumOffsetHolder(null, null), restriction);
    }

    @Override
    public void checkDone() throws IllegalStateException {
    }

    @Override
    public IsBounded isBounded() {
        return IsBounded.BOUNDED;
    }
}
