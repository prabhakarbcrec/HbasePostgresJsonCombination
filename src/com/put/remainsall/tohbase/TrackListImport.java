package com.put.remainsall.tohbase;

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

public class TrackListImport {

	public void ImportTrackList(String mmsi, String key) throws IOException, ServiceException {
		Configuration config = HBaseConfiguration.create();

		config.set("hbase.zookeeper.quorum", "ip1,ip2,ip3,ip4");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		@SuppressWarnings("deprecation")
		HTable table = new HTable(config, "Tablename");
		HBaseAdmin.checkHBaseAvailable(config);
		if (GetTrackInfo(mmsi)) {
			Put p = new Put(Bytes.toBytes(key));

			p.add(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("m"), Bytes.toBytes(mmsi));

			table.put(p);
			System.out.println(
					"************************  INSERTED INTO  TABLE 2 **************************");
			table.close();
		}
	}

	private static boolean GetTrackInfo(String m) throws IOException, ServiceException {
		Configuration config = HBaseConfiguration.create();

		config.set("hbase.zookeeper.quorum", "ip1,ip2,ip3,ip4");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		HTable table = new HTable(config, "tablename");

		HBaseAdmin.checkHBaseAvailable(config);

		Scan scan = new Scan();

		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("Dynamic Data"),
				Bytes.toBytes("m"), CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(mmsi)));
		scan.setFilter(filter);
		ResultScanner row = table.getScanner(scan);

		try {
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersions = row.iterator().next()
					.getMap();
//			 for(Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> entry:allVersions.entrySet())
//			 {
//			   System.out.println(new String(entry.getKey())); 
//				for(Entry<byte[],NavigableMap<Long,byte[]>> entry1:entry.getValue().entrySet())
//				{
//					int i=0;
//					for(Entry<Long,byte[]> entry2:entry1.getValue().entrySet())
//					{
//				System.out.println(new String(entry1.getKey())+"  "+"->"+new String(entry2.getValue()));
//					}
//				}
//	

			// }
		} catch (Exception e) {
			return false;

		}
		return true;
	}


}
