/*
 * Copyright MapR Technologies, 2013
 *
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

package com.mapr.franz.stats;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps and reports statistics about the number of transactions and unique topics
 */
public class History {
    private final Logger log = LoggerFactory.getLogger(History.class);

    private final ScheduledExecutorService workers;

    private double t0 = System.nanoTime() / 1e9;

    private final int[] intervals;
    private final List<Multiset<String>> topicCounters = Collections.synchronizedList(Lists.<Multiset<String>>newArrayList());
    private final List<AtomicInteger> messageCounters = Collections.synchronizedList(Lists.<AtomicInteger>newArrayList());
    private List<Listener> listeners = Lists.newArrayList();

    public History(int... intervals) {
        this.intervals = intervals;

        workers = Executors.newScheduledThreadPool(2);
        int i = 0;
        for (int interval : intervals) {
            topicCounters.add(ConcurrentHashMultiset.<String>create());
            messageCounters.add(new AtomicInteger());

            workers.scheduleAtFixedRate(new Report(i++), 0, interval, TimeUnit.SECONDS);
        }
    }

    public History logTicks() {
        addListener(new LogListener());
        return this;
    }

    public History addListener(Listener listener) {
        listeners.add(listener);
        return this;
    }

    public void message(String topic) {
        for (int i = 0; i < intervals.length; i++) {
            topicCounters.get(i).add(topic);
            messageCounters.get(i).incrementAndGet();
        }
    }

    private class Report implements Runnable {
        private int index;

        public Report(int index) {
            this.index = index;
        }

        @Override
        public void run() {
            double t = System.nanoTime() / 1e9 - t0;
            int interval = intervals[index];
            int uniques = topicCounters.get(index).elementSet().size();
            int messages = messageCounters.get(index).get();
            for (Listener listener : listeners) {
                listener.tick(t, interval, uniques, messages);
            }
            topicCounters.get(index).clear();
            messageCounters.get(index).set(0);
        }
    }

    public class LogListener implements Listener {
        @Override
        public void tick(double t, int interval, int uniqueTopics, int messages) {
            log.warn(String.format("%.1f\t%d\t%d\t%d", t, interval, uniqueTopics, messages));
        }
    }

    public static interface Listener {
        public void tick(double t, int interval, int uniqueTopics, int messages);
    }
}
