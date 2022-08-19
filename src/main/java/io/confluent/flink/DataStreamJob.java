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

package io.confluent.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

import java.util.Properties;
import java.util.Map;
import ksql.users;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	final static String inputTopic = "input-topic-avro";
	final static String outputTopic = "output-topic";
	final static String bootstrapServers = "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092";
	final static String schemaRegistryUrl = "https://psrc-3w372.australia-southeast1.gcp.confluent.cloud";
	
	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */
		KafkaSource<users> source = KafkaSource.<users>builder()
		    .setBootstrapServers(bootstrapServers)
			.setProperty("security.protocol", "SASL_SSL")
			.setProperty("sasl.mechanism", "PLAIN")
			.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<api-key>\" password=\"<api-secret>\";")
			.setTopics(inputTopic)
		    .setGroupId("my-group")
		    .setStartingOffsets(OffsetsInitializer.earliest())
		    .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forSpecific(users.class, schemaRegistryUrl, Map.of("basic.auth.credentials.source", "USER_INFO", "schema.registry.basic.auth.user.info", "<api-key>:<api-secret>")))
		    .build();
		
		KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
			.setValueSerializationSchema(new SimpleStringSchema())
			.setTopic(outputTopic)
			.build();

		KafkaSink<String> sink = KafkaSink.<String>builder()
			.setBootstrapServers(bootstrapServers)
			.setProperty("security.protocol", "SASL_SSL")
			.setProperty("sasl.mechanism", "PLAIN")
			.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<api-key>\" password=\"<api-secret>\";")
			.setRecordSerializer(serializer)
			.build();

		DataStream<users> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		// Split up the lines in pairs (2-tuples) containing: (word,1)
		DataStream<String> counts = text.flatMap(new Tokenizer())
		// Group by the tuple field "0" and sum up tuple field "1"
			.keyBy(value -> value.f0)
			.sum(1)
			.flatMap(new Reducer());

		// Add the sink to so results
		// are written to the outputTopic
		counts.sinkTo(sink);

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}

	    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<users, Tuple2<String, Integer>> {

        @Override
        public void flatMap(users value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] tokens = value.getGender().toString().toLowerCase().split("\\W+");

            // Emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    // Implements a simple reducer using FlatMap to
    // reduce the Tuple2 into a single string for 
    // writing to kafka topics
    public static final class Reducer
            implements FlatMapFunction<Tuple2<String, Integer>, String> {

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<String> out) {
        	// Convert the pairs to a string
        	// for easy writing to Kafka Topic
        	String count = value.f0 + " " + value.f1;
        	out.collect(count);
        }
    }
}
