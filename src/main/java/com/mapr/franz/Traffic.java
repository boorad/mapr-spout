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

package com.mapr.franz;

import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.mapr.franz.catcher.Client;
import com.mapr.franz.stats.History;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.jet.random.Exponential;
import org.apache.mahout.math.random.ChineseRestaurant;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;

/**
 * Generates traffic with strong peak rates for a variety of topics.
 */
public class Traffic {
    private static Logger log = LoggerFactory.getLogger(Traffic.class);

    public static void main(String[] args) throws InterruptedException, IOException, ServiceException {
        Options options = parseOptions(args);
        log.warn("Options = {}", options.toString());

        double scale = Math.log(options.ratio) / 2;

        Client client = new Client(Collections.singleton(new PeerInfo(options.host, options.port)));

        History recorder = new History(1, 10, 30).logTicks();

        Random gen = RandomUtils.getRandom();
        ChineseRestaurant topicSampler = new ChineseRestaurant(options.alpha);
        Exponential interval = new Exponential(1, gen);
        double t = 0;
        long t0 = System.nanoTime();
        long messageId = 0;
        while (messageId < options.max) {
            double rate = options.peak * Math.exp(-scale * (Math.cos(2 * Math.PI * t / options.period) + 1));
            double dt = interval.nextDouble() / rate;
            t += dt;
            double now = (System.nanoTime() - t0) / 1e9;
            if (t > now + 0.01) {
                double millis = Math.floor((t - now) * 1000);
                double nanos = Math.rint((t - now) * 1e9 - millis * 1e6);
                Thread.sleep((long) millis, (int) nanos);
            }

            String topic = "topic-" + topicSampler.sample();
            String message = "m-" + (++messageId);
            client.sendMessage(topic, message);
            recorder.message(topic);
            log.debug("Sent {} / {}", topic, message);
            if ((messageId % 10000) == 0) {
                log.warn("Sent {} messages", messageId);
            }
        }
    }

    private static Options parseOptions(String[] args) {
        Options opts = new Options();
        CmdLineParser parser = new CmdLineParser(opts);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace(System.err);
            parser.printUsage(System.err);
            System.exit(1);
        }
        return opts;
    }

    private static class Options {
        @Option(name = "-host", required = true, usage = "Name of the catcher server host to connect to.")
        String host;

        @Option(name = "-port", usage = "Port for the catcher server")
        int port = 5900;

        @Option(name = "-peak", usage = "The peak rate for transactions summed across all topics.  Current generator can only do about 5000 msg/s")
        double peak = 1000;

        @Option(name = "-ratio", usage = "The peak to valley ratio for transaction rate.  Default is 5")
        double ratio = 5;

        @Option(name = "-period", usage = "The periodicity of the traffic waves expressed in seconds per wave. Default is 60s")
        double period = 60;

        @Option(name = "-alpha", usage = "The topic generator parameter.  Bigger gives more topics. Default of 10 gives about 100 topics after about 100K messages")
        double alpha = 10;

        @Option(name = "-max", usage = "The maximum number of messages to send.  Default is 1 thousand.")
        public long max = 1000000;

        @Override
        public String toString() {
            return "Options{" +
                    "alpha=" + alpha +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", peak=" + peak +
                    ", ratio=" + ratio +
                    ", period=" + period +
                    ", max=" + max +
                    '}';
        }
    }
}
