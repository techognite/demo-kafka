package com.techognite.fourth;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerApp02 {
	public static void main(String[] args) throws InterruptedException {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("client.id", "client002");
		props.put("group.id", "demo");
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
		
		ArrayList<String> topics = new ArrayList<String>();
		topics.add("techognite");

		kafkaConsumer.subscribe(topics);
		try {
			while (true) {
				ConsumerRecords<String, String> records 
							= kafkaConsumer.poll(Duration.ofMillis(100l));
				
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("Topic Name: " + record.topic() + " Partition No: " + record.partition() +" Record:"+  record.value());
				}
			}
		} finally {
			kafkaConsumer.close();
		}
		
		
	}
}
