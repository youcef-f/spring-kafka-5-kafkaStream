package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.example.demo.entities.Greeting;

@Service
public class MyConsumer {

	/*
	  @Value("${kafka.topic}")
	  private  String TOPIC_NAME;
	 */

	private static final Logger logger = LoggerFactory.getLogger(MyConsumer.class);
	private static final String TOPIC_NAME = "mytopic";
	private static final String GREETING_TOPIC_NAME = "greetingTopic";
	private static final String STREAMKAFKA_TOPIC_OUTPUT = "my-spring-kafka-streams-output-topic";

	//@KafkaListener(groupId = "mykafkagroup", topics = TOPIC_NAME, properties = { "enable.auto.commit=true", "auto.commit.interval.ms=1000", "poll-interval=100"})
	@KafkaListener(topics = TOPIC_NAME, groupId = "group_id",containerFactory = "concurrentKafkaListenerContainerFactory")
	public void consumer(ConsumerRecord<?,?> record ) {
		logger.info(String.format("$$ -> Consuming message --> %s", record));
		  System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

	}
	
	
	@KafkaListener(topics = GREETING_TOPIC_NAME, groupId = "group_id",containerFactory = "greetingKafkaListenerContainerFactory")
	public void consumerGreeting(Greeting greeting) {
		logger.info(String.format("$$ -> Consuming message --> %s", greeting.getMsg()));
		  System.out.printf("message " + greeting.getMsg() + " name: " + greeting.getName()); 

	}
	
	
	@KafkaListener(topics = STREAMKAFKA_TOPIC_OUTPUT, groupId = "group_id",containerFactory = "concurrentKafkaListenerContainerFactory")
	public void consumerKafkaStreamfilter(ConsumerRecord<?,?> record) {
		  System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		 // System.out.println("message Filtrer " + record.value() ); 

	}
}
