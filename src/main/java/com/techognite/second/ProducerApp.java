package com.techognite.second;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerApp {

	public static void main(String[] args) throws InterruptedException {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
		String topic = "techognite";
		try {
			for (int i = 0; i < 500000; i++) {
				String message = "Message " + i;
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
				kafkaProducer.send(producerRecord);
			}
			System.out.println("All Message Sent");
			
		} finally {
			kafkaProducer.close();
		}
	}

}
