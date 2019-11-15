package com.consumerclass.distinct.message;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.in.encryption.LANEncryption;
import com.put.manaltrack.tohbase.PutMTToHbase;
import com.put.remainsall.tohbase.PutRemainsAllTrackToHbase;

import ch.hsr.geohash.GeoHash;

public class OwnConsumerFromKafka_DistictMessage {

	private static KafkaConsumer<String, byte[]> COSUMER;
	public static PutRemainsAllTrackToHbase putallr = new PutRemainsAllTrackToHbase();

	public static void doAllPutOprationToHabse(JsonObject json) throws IOException {

		try {
			
			
				String key2 = null;
				String Manualkey = null;
				String Manualkey2 = null;
			
				String MessageType1 = json.get("messageType").toString();
				String MessageType = json.get("messageType").toString().substring(1,
						MessageType1.length() - 1);
				long data = 0;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String mmsi = null;
				String date = null;
				String year = null;
				String month = null;
				String geohash;
				String ManualGeohash;
				String key = null;
				String data1 = null;
				// System.out.println("\n\nThis is the message for you: - "+MessageType);
				SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				if (MessageType.equals("TRACK")) {
					data = Long.valueOf(json.get("Datetime").toString());
				} else {
					data = Long.valueOf(json.get("timeStamp").toString());
					data1 = sdf.format(data * 1000);
					//System.out.println("date time" + data1);
					String mmsi1 = json.get("Id").toString();
					mmsi = json.get("Id").toString().substring(1, mmsi1.length() - 1);

					date = s.format(data * 1000);
					key = mmsi + "=" + data1;
					System.out.println("\n this is a treack trajectory key for" + key);

					try {
						geohash = GeoHash.geoHashStringWithCharacterPrecision(
								Double.valueOf(json.get("latitude").toString()),
								Double.valueOf(json.get("latitude").toString()), 5);

						key2 = geohash + "="
								+ json.get("Id").toString()
										.substring(1,
												json.get("Id").toString().length() - 1)
										.toString()
								+ "=" + data1;
						System.out.println("this is a geohash key2" + key2);

					} catch (Exception e) {
					}
				}
				date = s.format(data * 1000);

				if (MessageType.equalsIgnoreCase("TRACK")) {

					Manualkey = json.get("Name").toString().substring(1,
							json.get("Name").toString().length() - 1) + "=" + data1;
					System.out.println("" + Manualkey);
					
					PutMTToHbase.manualTrackToHbase(json.get("latitude").toString(),
							json.get("longitude").toString(),
							json.get("variablenME").toString(),
							json.get("speed").toString(),
							json.get("Type").toString(),
							json.get("Name").toString(), Manualkey);

					// System.out.println("\nElement" + json.get(i) + "\ntotal size" + i + "\n");
					// Making geohas key
					
					ManualGeohash = GeoHash.geoHashStringWithCharacterPrecision(
							Double.valueOf(json.get("latitude").toString()),
							Double.valueOf(json.get("latitude").toString()), 5);
					Manualkey2 = ManualGeohash + "="
							+ json.get("Name").toString().substring(1,
									json.get("kName").toString().length() - 1)+"="+ data1;
					System.out.println("manual kay for geohash typ" + Manualkey2);
					PutMTToHbase.manualTrackToHbaseInGeohash(
							json.get("latitude").toString(),
							json.get("longitude").toString(),
							json.get("course").toString(),
							json.get("speed").toString(),
							json.get("type").toString(),
							json.get("trackName").toString(), Manualkey2);

				} else if (MessageType.substring(1, MessageType.length() - 1).equalsIgnoreCase("Link2_GISMessage")) {
					System.out.println("this is a naval track");
				}

				else {
					System.out.println(json.get("h").toString()+"  this is a h");
					PutRemainsAllTrackToHbase.PutHbaseRemainsTrack(
							json.get("latitude").toString(),
							json.get("longitude").toString(),
							json.get("c").toString(),
							json.get("s").toString(),
					         json.get("h").toString(),
							json.get("type").toString(),
							json.get("Id").toString().substring(1,
							json.get("Id").toString().length() - 1),
							json.getAsJsonObject("v").get("i").toString(),
							json.getAsJsonObject("v").get("w").toString(),
							json.getAsJsonObject("v").get("v").toString(),
							json.getAsJsonObject("v").get("c").toString(),
							json.getAsJsonObject("v").get("L").toString(),
							key);
					PutRemainsAllTrackToHbase.PutHbaseGeoHashTrack(
									json.get("latitude").toString(),
									json.get("longitude").toString(),
									json.get("c").toString(),
									json.get("s").toString(),
									json.get("h").toString(),
									json.get("type").toString(),
									json.get("Id").toString().substring(1,
									json.get("Id").toString().length() - 1),
									json.getAsJsonObject("v").get("i").toString(),
									json.getAsJsonObject("v").get("w").toString(),
									json.getAsJsonObject("v").get("v").toString(),
									json.getAsJsonObject("v").get("c").toString(),
									json.getAsJsonObject("v").get("L").toString(),
									key2);
					}

			

		} catch (Exception e) {
			//e.printStackTrace();
		}
		
	}

}
