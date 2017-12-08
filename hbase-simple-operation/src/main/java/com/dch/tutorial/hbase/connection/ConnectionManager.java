package com.dch.tutorial.hbase.connection;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.protobuf.ServiceException;

public class ConnectionManager {

	private static Connection connection;

	public static Connection getConnection() {
		try {
			if (connection == null || connection.isClosed()) {
				Configuration config = HBaseConfiguration.create();
				HBaseAdmin.checkHBaseAvailable(config);
				connection = ConnectionFactory.createConnection(config);
			}
		} catch (IOException | ServiceException e) {
			System.out.println("HBase is not running." + e.getMessage());
		}
		return connection;
	}
}
