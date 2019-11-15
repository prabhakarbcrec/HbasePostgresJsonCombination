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

public class DesignationListImport {

	static HbaseConnection con = new HbaseConnection();

	@SuppressWarnings("deprecation")
	public static void DesignationTohbase(String trackName, String key2) throws IOException, ServiceException {

		@SuppressWarnings("deprecation")
		HTable table = new HTable(con.GetHbaseConnection(), "Track_Designation");
		if (GetTrackInfo(trackName)) {
			Put p = new Put(Bytes.toBytes(key2));

			p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("Designation"), Bytes.toBytes(trackName));

			table.put(p);
			System.out.println(
					"************************ DESIGNATION_LIST INSERTED INTO TRACKLISTTABLE 4 **************************");
			table.close();
		}
	}

	private static boolean GetTrackInfo(String trackName) throws IOException, ServiceException {

		@SuppressWarnings("deprecation")
		HTable table = new HTable(con.GetHbaseConnection(), "Track_Designation");
		Scan scan = new Scan();
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("Dynamic Data"),
				Bytes.toBytes("Designation"), CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(trackName)));
		scan.setFilter(filter);
		ResultScanner row = table.getScanner(scan);

		try {
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersions = row.iterator().next()
					.getMap();

		} catch (Exception e) {
			return true;

		}
		return false;

	}

}
