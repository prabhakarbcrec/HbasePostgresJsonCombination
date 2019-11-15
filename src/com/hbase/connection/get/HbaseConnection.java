package com.hbase.connection.get;

import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.protobuf.ServiceException;

public class HbaseConnection {
	static Configuration conn=null;
	public Configuration GetHbaseConnection() throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException
	{
		Configuration conn = HBaseConfiguration.create();

		conn.set("hbase.zookeeper.quorum", "ip1,ip2,ip3,ip4");
		conn.set("hbase.zookeeper.property.clientPort", "2181");
			HBaseAdmin.checkHBaseAvailable(conn);
		return conn;
		
	}

}
