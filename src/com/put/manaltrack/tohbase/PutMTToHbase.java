package com.put.manaltrack.tohbase;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;
import com.hbase.connection.get.HbaseConnection;

public class PutMTToHbase {
	static HbaseConnection connection=new HbaseConnection();
	@SuppressWarnings("deprecation")
	public static void manualTrackToHbase(String latitude, String longitude, String course, String speed,
			String trackType, String trackName,String key) throws IOException, ServiceException
	{
		HTable table = new HTable(connection.GetHbaseConnection(), "Tablename");                  //new to change without version table name
		Put p = new Put(Bytes.toBytes(key));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("V"), Bytes.toBytes(trackType));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("M"), Bytes.toBytes(trackName));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("Latitude"), Bytes.toBytes(latitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("Longitude"), Bytes.toBytes(longitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("C"), Bytes.toBytes(course));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("S"), Bytes.toBytes(speed));

		table.put(p);
		System.out.println("************************ _DATA INSERTED INTO  TABLE 5**************************");
		DesignationListImport.DesignationTohbase( trackName, key);
		table.close();
		
	}

	@SuppressWarnings("deprecation")
	public static void manualTrackToHbaseInGeohash(String latitude, String longitude, String course, String speed,
			String trackType, String trackName,String key) throws IOException, ServiceException {	
		HTable table = new HTable(connection.GetHbaseConnection(), "GeopocHbase");
		Put p = new Put(Bytes.toBytes(key));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("Type"), Bytes.toBytes(trackType));
		p.add(Bytes.toBytes("Static Data"), Bytes.toBytes("no"), Bytes.toBytes(trackName));

		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("LATITUDE"), Bytes.toBytes(latitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("LONGITUDE"), Bytes.toBytes(longitude));
		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("C"), Bytes.toBytes(course));

		p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("S"), Bytes.toBytes(speed));

		table.put(p);
		System.out.println("************************ DATA INSERTED INTO  TABLE 6**************************");
		table.close();
		
	}

}
