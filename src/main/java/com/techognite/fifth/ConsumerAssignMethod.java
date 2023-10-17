package com.techognite.fifth;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerAssignMethod {
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
		
		TopicPartition topicPartition = new TopicPartition("techognite", 0);
		
		List<TopicPartition> topicPartitions = new ArrayList<>();
		topicPartitions.add(topicPartition);

		kafkaConsumer.assign(topicPartitions);
		
		try {
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.topic() + ":" +  record.value());
				}
			}
		} finally {
			kafkaConsumer.close();
		}
	}
}
