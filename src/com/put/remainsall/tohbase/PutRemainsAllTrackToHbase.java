package com.put.remainsall.tohbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


import com.google.protobuf.ServiceException;
import com.hbase.connection.get.HbaseConnection;

public class PutRemainsAllTrackToHbase {
	
static HbaseConnection conection=new HbaseConnection();

	public static void PutHbaseRemainsTrack(String latitude, String longitude, String course, String speed,String Heading,
			String trackType, String trackId,String IMO,String width,String vesselName,String callsign,String Length, String key) throws IOException, ServiceException {
		
		TrackListImport listOfTrack=new TrackListImport();
		@SuppressWarnings("deprecation")
		HTable table = new HTable(conection.GetHbaseConnection(), "Track_Trajectorynew");
		Put p = new Put(Bytes.toBytes(key));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("I"), Bytes.toBytes(IMO));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("Width"), Bytes.toBytes(width));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("V"), Bytes.toBytes(vesselName));	
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("C"), Bytes.toBytes(callsign));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("Length"), Bytes.toBytes(Length));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("V"), Bytes.toBytes(trackType));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("M"), Bytes.toBytes(trackId));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("Latitude"), Bytes.toBytes(latitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("Longitude"), Bytes.toBytes(longitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("C"), Bytes.toBytes(course));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("S"), Bytes.toBytes(speed));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("H"), Bytes.toBytes(Heading));
		
		System.out.println("************************ DATA INSERTED INTO  TABLE 1**************************");
		//function call
		listOfTrack.ImportTrackList(id, key);
		table.put(p);	
		table.close();	
	}

	public static void PutHbaseGeoHashTrack(String latitude, String longitude, String course, String speed,
			String Heading,String trackType, String trackId,String IMO,String width,String vesselName,String callsign,String Length, String key2) throws IOException, ServiceException {
		
		HTable table = new HTable(conection.GetHbaseConnection(), "tablename");
		Put p = new Put(Bytes.toBytes(key2));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("I"), Bytes.toBytes(IMO));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("W"), Bytes.toBytes(width));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("V"), Bytes.toBytes(vesselName));	
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("C"), Bytes.toBytes(callsign));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("Length"), Bytes.toBytes(Length));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("V"), Bytes.toBytes(trackType));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("M"), Bytes.toBytes(trackId));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("Latitude"), Bytes.toBytes(latitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("Longitude"), Bytes.toBytes(longitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("C"), Bytes.toBytes(course));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("S"), Bytes.toBytes(speed));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("H"), Bytes.toBytes(Heading));
		table.put(p);
		System.out.println("************************ DATA INSERTED INTO  TABLE 3**************************");
		table.close();
		
	}


}
