package com.combination.getMessage;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.combination.checkMessage.AppendMessageHere;
import com.getMessage5.postgres.GetStaticDataFromPostgres;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.in.encryption.Encryption;





public class getAllMessageFrom {


	private static KafkaConsumer<String, byte[]> COSUMER;
	

	public static void main(String[] args) {
		
		try {
			
			Properties props = new Properties();
			props.put("bootstrap.servers", "ip1:9092,ip2:9092,ip3:9092");
			props.put("zookeeper.connect", "ip1:2181,ip2:2181,ip3:2181");
			props.put("group.id", "0");
			props.put("zookeeper.session.timeout.ms", "50");
			// props.put("session.timeout.ms", "30000");
			props.put("auto.commit.interval.ms", "8000");
			// props.put("group.max.session.ms", "30000");
			props.put("enable.auto.commit", "true");
			props.put("max.poll.records", "2147483647");
			props.put("max.poll.interval.ms", Integer.MAX_VALUE);
			props.put("zookeeper.sync.time.ms", "250");
			// props.put("auto.offset.reset", "earliest");
			props.put("auto.offset.reset", "latest");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.ByteArrayDeserializer");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");
			props.put("metadata.broker.list", "ip1:9092,ip2:9092,ip3:9092");

			System.out.println(".......Kafka server connected:.....");

			COSUMER = new KafkaConsumer<>(props);

			COSUMER.subscribe(Arrays.asList("AllMessage"));
			Connection con = DriverManager.getConnection("jdbc:postgresql://ip:5432/" + "DatabaseName",
					"username", "password");
			System.out.println("<-----------Connection created----------->");
			while (true) {
				//Thread.sleep(100000);
				@SuppressWarnings("deprecation")
				ConsumerRecords<String, byte[]> recordss = COSUMER.poll(Integer.MAX_VALUE);
				for (ConsumerRecord<String, byte[]> record : recordss) {
				
					getMessageDecrypt(record.value(),con);

				}

				COSUMER.commitSync();
			}
		

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static <V> V getMessageDecrypt(final byte[] objectData,Connection con) throws IOException, InterruptedException, SQLException {
		int i;
		
		GetStaticDataFromPostgres callToPostgres=new GetStaticDataFromPostgres();
		String jsonFormData = new String(new Encryption().decrypt(objectData));
		JsonArray json = new JsonParser().parse(jsonFormData.toString()).getAsJsonArray();
		for( i=0;i<json.size();i++)
		{
		 callToPostgres.getStaticDataOntheBasisOfMmsiNumber(json.get(i).getAsJsonObject()
				 .get("Id").toString().substring(1, json.get(i).getAsJsonObject().get("Id").toString().length()-1),con,json.get(i).getAsJsonObject());
		}
		 System.out.println("this is a json array size:- "+i);
		@SuppressWarnings("unchecked")
		V objectData2 = (V) objectData;
		return objectData2;
	}



}
