/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.tools.api.RecordReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ConsoleProducer {
    public static void main(String[] args) {
        ConsoleProducer consoleProducer = new ConsoleProducer();
        consoleProducer.start(args);
    }

    void start(String[] args) {
        try {
            ConsoleProducerOptions opts = new ConsoleProducerOptions(args);
            RecordReader reader = messageReader(opts);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(opts.producerProps());

            Exit.addShutdownHook("producer-shutdown-hook", producer::close);

            loopReader(producer, reader, opts.sync());
        } catch (OptionException e) {
            System.err.println(e.getMessage());
            Exit.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            Exit.exit(1);
        }
        Exit.exit(0);
    }

    // Visible for testing
    RecordReader messageReader(ConsoleProducerOptions opts) throws Exception {
        Object objReader = Class.forName(opts.readerClass()).getDeclaredConstructor().newInstance();

        if (objReader instanceof RecordReader reader) {
            reader.configure(opts.readerProps());

            return reader;
        }

        throw new IllegalArgumentException("The reader must implement " + RecordReader.class.getName() + " interface");
    }

    // Visible for testing
    void loopReader(Producer<byte[], byte[]> producer, RecordReader reader, boolean sync) throws Exception {
        Iterator<ProducerRecord<byte[], byte[]>> iter = reader.readRecords(System.in);
        try {
            while (iter.hasNext()) {
                send(producer, iter.next(), sync);
            }
        } finally {
            reader.close();
        }
    }

    private void send(Producer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record, boolean sync) throws Exception {
        if (sync) {
            producer.send(record).get();
        } else {
            producer.send(record, new ErrorLoggingCallback(record.topic(), record.key(), record.value(), false));
        }
    }

    static final class ConsoleProducerOptions extends CommandDefaultOptions {
        private final OptionSpec<String> topicOpt;
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<Void> syncOpt;
        private final OptionSpec<String> compressionCodecOpt;
        private final OptionSpec<Integer> batchSizeOpt;
        private final OptionSpec<Integer> messageSendMaxRetriesOpt;
        private final OptionSpec<Long> retryBackoffMsOpt;
        private final OptionSpec<Long> sendTimeoutOpt;
        private final OptionSpec<String> requestRequiredAcksOpt;
        private final OptionSpec<Integer> requestTimeoutMsOpt;
        private final OptionSpec<Long> metadataExpiryMsOpt;
        private final OptionSpec<Long> maxBlockMsOpt;
        private final OptionSpec<Long> maxMemoryBytesOpt;
        private final OptionSpec<Integer> maxPartitionMemoryBytesOpt;
        private final OptionSpec<String> messageReaderOpt;
        private final OptionSpec<Integer> socketBufferSizeOpt;
        private final OptionSpec<String> propertyOpt;
        private final OptionSpec<String> readerConfigOpt;
        private final OptionSpec<String> producerPropertyOpt;
        private final OptionSpec<String> producerConfigOpt;

        public ConsoleProducerOptions(String[] args) {
            super(args);
            topicOpt = parser.accepts("topic", "REQUIRED: The topic name to produce messages to.")
                    .withRequiredArg()
                    .describedAs("topic")
                    .ofType(String.class);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to. The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                    .withRequiredArg()
                    .describedAs("server to connect to")
                    .ofType(String.class);
            syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.");
            compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', 'lz4', or 'zstd'." +
                            "If specified without value, then it defaults to 'gzip'")
                    .withOptionalArg()
                    .describedAs("compression-codec")
                    .ofType(String.class)
                    .defaultsTo(CompressionType.GZIP.name);
            batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously. " +
                            "please note that this option will be replaced if max-partition-memory-bytes is also set")
                    .withRequiredArg()
                    .describedAs("size")
                    .ofType(Integer.class)
                    .defaultsTo(16 * 1024);
            messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, " +
                            "and being unavailable transiently is just one of them. This property specifies the number of retries before the producer give up and drop this message. " +
                            "This is the option to control `retries` in producer configs.")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(3);
            retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. " +
                            "Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata. " +
                            "This is the option to control `retry.backoff.ms` in producer configs.")
                    .withRequiredArg()
                    .ofType(Long.class)
                    .defaultsTo(100L);
            sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
                            " a message will queue awaiting sufficient batch size. The value is given in ms. " +
                            "This is the option to control `linger.ms` in producer configs.")
                    .withRequiredArg()
                    .describedAs("timeout_ms")
                    .ofType(Long.class)
                    .defaultsTo(1000L);
            requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required `acks` of the producer requests")
                    .withRequiredArg()
                    .describedAs("request required acks")
                    .ofType(String.class)
                    .defaultsTo("-1");
            requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero.")
                    .withRequiredArg()
                    .describedAs("request timeout ms")
                    .ofType(Integer.class)
                    .defaultsTo(1500);
            metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
                            "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes. " +
                                    "This is the option to control `metadata.max.age.ms` in producer configs.")
                    .withRequiredArg()
                    .describedAs("metadata expiration interval")
                    .ofType(Long.class)
                    .defaultsTo(5 * 60 * 1000L);
            maxBlockMsOpt = parser.accepts("max-block-ms",
                            "The max time that the producer will block for during a send request.")
                    .withRequiredArg()
                    .describedAs("max block on send")
                    .ofType(Long.class)
                    .defaultsTo(60 * 1000L);
            maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
                            "The total memory used by the producer to buffer records waiting to be sent to the server. " +
                                    "This is the option to control `buffer.memory` in producer configs.")
                    .withRequiredArg()
                    .describedAs("total memory in bytes")
                    .ofType(Long.class)
                    .defaultsTo(32 * 1024 * 1024L);
            maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
                            "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
                                    "will attempt to optimistically group them together until this size is reached. " +
                                    "This is the option to control `batch.size` in producer configs.")
                    .withRequiredArg()
                    .describedAs("memory in bytes per partition")
                    .ofType(Integer.class)
                    .defaultsTo(16 * 1024);
            messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
                            "By default each line is read as a separate message.")
                    .withRequiredArg()
                    .describedAs("reader_class")
                    .ofType(String.class)
                    .defaultsTo(LineMessageReader.class.getName());
            socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size. " +
                            "This is the option to control `send.buffer.bytes` in producer configs.")
                    .withRequiredArg()
                    .describedAs("size")
                    .ofType(Integer.class)
                    .defaultsTo(1024 * 100);
            propertyOpt = parser.accepts("property",
                            "A mechanism to pass user-defined properties in the form key=value to the message reader. This allows custom configuration for a user-defined message reader." +
                                    "\nDefault properties include:" +
                                    "\n parse.key=false" +
                                    "\n parse.headers=false" +
                                    "\n ignore.error=false" +
                                    "\n key.separator=\\t" +
                                    "\n headers.delimiter=\\t" +
                                    "\n headers.separator=," +
                                    "\n headers.key.separator=:" +
                                    "\n null.marker=   When set, any fields (key, value and headers) equal to this will be replaced by null" +
                                    "\nDefault parsing pattern when:" +
                                    "\n parse.headers=true and parse.key=true:" +
                                    "\n  \"h1:v1,h2:v2...\\tkey\\tvalue\"" +
                                    "\n parse.key=true:" +
                                    "\n  \"key\\tvalue\"" +
                                    "\n parse.headers=true:" +
                                    "\n  \"h1:v1,h2:v2...\\tvalue\"")
                    .withRequiredArg()
                    .describedAs("prop")
                    .ofType(String.class);
            readerConfigOpt = parser.accepts("reader-config", "Config properties file for the message reader. Note that " + propertyOpt + " takes precedence over this config.")
                    .withRequiredArg()
                    .describedAs("config file")
                    .ofType(String.class);
            producerPropertyOpt = parser.accepts("producer-property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
                    .withRequiredArg()
                    .describedAs("producer_prop")
                    .ofType(String.class);
            producerConfigOpt = parser.accepts("producer.config", "Producer config properties file. Note that " + producerPropertyOpt + " takes precedence over this config.")
                    .withRequiredArg()
                    .describedAs("config file")
                    .ofType(String.class);

            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
            }

            checkArgs();
        }

        void checkArgs() {
            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to read data from standard input and publish it to Kafka.");

            CommandLineUtils.checkRequiredArgs(parser, options, topicOpt);

            try {
                ToolsUtils.validateBootstrapServer(options.valueOf(bootstrapServerOpt));
            } catch (IllegalArgumentException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
            }
        }

        boolean sync() {
            return options.has(syncOpt);
        }

        String readerClass() {
            return options.valueOf(messageReaderOpt);
        }

        Map<String, Object> readerProps() throws IOException {
            Map<String, Object> commandlineMap = new HashMap<>();
            CommandLineUtils.maybeMergeOption(options, commandlineMap, "topic", topicOpt);
            Map<String, Object> map = CommandLineUtils.mergeByPriority(options, readerConfigOpt, propertyOpt, commandlineMap);

            return map;
        }

        Map<String, Object> producerProps() throws IOException {
            Map<String, Object> commandlineMap = new HashMap<>();
            CommandLineUtils.maybeMergeOption(options, commandlineMap, BOOTSTRAP_SERVERS_CONFIG, bootstrapServerOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, COMPRESSION_TYPE_CONFIG, compressionCodecOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, LINGER_MS_CONFIG, sendTimeoutOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, ACKS_CONFIG, requestRequiredAcksOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMsOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, RETRIES_CONFIG, messageSendMaxRetriesOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, RETRY_BACKOFF_MS_CONFIG, retryBackoffMsOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, SEND_BUFFER_CONFIG, socketBufferSizeOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, BUFFER_MEMORY_CONFIG, maxMemoryBytesOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, BATCH_SIZE_CONFIG, batchSizeOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, BATCH_SIZE_CONFIG, maxPartitionMemoryBytesOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, METADATA_MAX_AGE_CONFIG, metadataExpiryMsOpt);
            CommandLineUtils.maybeMergeOption(options, commandlineMap, MAX_BLOCK_MS_CONFIG, maxBlockMsOpt);

            Map<String, Object> defaultMap = Map.of(
                BOOTSTRAP_SERVERS_CONFIG, CompressionType.NONE.name,
                CLIENT_ID_CONFIG, "console-producer",
                KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer",
                VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer"
            );

            Map<String, Object> map = CommandLineUtils.mergeByPriority(options, producerConfigOpt, producerPropertyOpt, commandlineMap, defaultMap);

            return map;
        }
    }
}
