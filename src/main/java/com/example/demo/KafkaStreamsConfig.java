package com.example.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafkaStream.topicInput}")
	private String INPUT_TOPIC;

	@Value("${kafkaStream.topicOutput}")
	private String OUTPUT_TOPIC;

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kafkaStreamsConfig() {
		Map<String, Object> configProps = new HashMap<String, Object>();
		// list of host:port pairs used for establishing the initial connections to theKafka cluster
		configProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "mykafkaStream");
		configProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		configProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Stream processing is often stateful. When we want to save intermediate results, we need to specify the STATE_DIR_CONFIG parameter.
		//configProps.put(StreamsConfig.STATE_DIR_CONFIG, );
		  
		return new KafkaStreamsConfiguration(configProps);

	}
	
	
	@Bean
	public KStream<String, String> kStream(StreamsBuilder streamsBuilder){

		// Keep only sentence ended with *youcef
		//For every sentence sent to inputTopic, we want to split it into words and calculate the occurrence of every word.
		Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
		
		KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC,Consumed.with(Serdes.String(), Serdes.String()));
		
		stream.print(Printed.toSysOut());
	    stream.filter((key, value) -> ((String) value).endsWith("*youcef")).to(OUTPUT_TOPIC ,Produced.with(Serdes.String(), Serdes.String()));
	    return stream ; 
	}
}
