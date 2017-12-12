package com.dch.tutorial.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dch.tutorial.hbase.connection.ConnectionManager;

/**
 * Basic HBase client operations.
 * 
 * @author David.Christianto
 */
public class HBaseClientOperations {

	private static final String TABLE_NAME = "user";

	/**
	 * Create the table with two column families.
	 * 
	 * @param admin
	 *            {@link Admin}
	 * @throws IOException
	 *             Error occurred when create the table.
	 */
	public void createTable(Admin admin) throws IOException {
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		tableDescriptor.addFamily(new HColumnDescriptor("name"));
		tableDescriptor.addFamily(new HColumnDescriptor("contactInfo"));
		admin.createTable(tableDescriptor);
	}

	/**
	 * Add each person to the table. <br/>
	 * Use the `name` column family for the name. <br/>
	 * Use the `contactInfo` column family for the email
	 * 
	 * @param table
	 *            {@link Table}
	 * @throws IOException
	 *             Error occurred when put data into table.
	 */
	public void put(Table table) throws IOException {
		String[][] users = { { "1", "Marcel", "Haddad", "marcel@fabrikam.com" },
				{ "2", "Franklin", "Holtz", "franklin@contoso.com" },
				{ "3", "Dwayne", "McKeeleen", "dwayne@fabrikam.com" },
				{ "4", "Raynaldi", "Schroeder", "raynaldi@contoso.com" },
				{ "5", "Rosalie", "Burton", "rosalie@fabrikam.com" },
				{ "6", "Gabriela", "Ingram", "gabriela@contoso.com" } };

		for (int i = 0; i < users.length; i++) {
			Put put = new Put(Bytes.toBytes(users[i][0]));
			put.addImmutable(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(users[i][1]));
			put.addImmutable(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(users[i][2]));
			put.addImmutable(Bytes.toBytes("contactInfo"), Bytes.toBytes("email"), Bytes.toBytes(users[i][3]));
			table.put(put);
		}
	}

	/**
	 * Method used to get value from the table by row, family, and qualifier.
	 * 
	 * @param table
	 *            The table.
	 * @param row
	 *            The row.
	 * @param family
	 *            The family.
	 * @param qualifier
	 *            The qualifier.
	 * @throws IOException
	 *             Error occurred when get data from the table.
	 */
	public void get(Table table, byte[] row, byte[] family, byte[] qualifier) throws IOException {
		System.out.println("\n*** GET ~fetching row " + Bytes.toString(row) + " in " + Bytes.toString(family) + ":"
				+ Bytes.toString(qualifier) + "~ ***");

		Get get = new Get(row);
		Result result = table.get(get);

		System.out.println("Fetched value: " + Bytes.toString(result.getValue(family, qualifier)));
	}

	/**
	 * Method used to scan all data in the table.
	 * 
	 * @param table
	 *            The table.
	 * @param family
	 *            The family.
	 * @param qualifier
	 *            The qualifier.
	 * @throws IOException
	 *             Error occurred when scanner from the table.
	 */
	public void scan(Table table, byte[] family, byte[] qualifier) throws IOException {
		System.out.println("\n*** SCAN ~fetching all data in " + Bytes.toString(family) + ":"
				+ Bytes.toString(qualifier) + "~ ***");

		Scan scan = new Scan();
		scan.addColumn(family, qualifier);

		try (ResultScanner scanner = table.getScanner(scan)) {
			for (Result result : scanner)
				System.out.println("Found row: " + Bytes.toString(result.getValue(family, qualifier)));
		}
	}

	/**
	 * Method used to scan with filters to fetch a row of which key is larger than
	 * the specified row.
	 * 
	 * @param table
	 *            The table.
	 * @param family
	 *            The family.
	 * @param qualifier
	 *            The qualifier.
	 * @param value
	 *            Value to compare column values against.
	 * @throws IOException
	 *             Error occurred when filter from the table.
	 */
	public void filters(Table table, byte[] family, byte[] qualifier, String value) throws IOException {
		System.out.println("\n*** FILTERS ~scanning with filters to fetch a row of which " + Bytes.toString(family)
				+ ":" + Bytes.toString(qualifier) + " is equal with " + value + "~ ***");

		RegexStringComparator filterValue = new RegexStringComparator(value);
		SingleColumnValueFilter filter = new SingleColumnValueFilter(family, qualifier, CompareOp.EQUAL, filterValue);

		Scan scan = new Scan();
		scan.setFilter(filter);

		try (ResultScanner scanner = table.getScanner(scan)) {
			for (Result result : scanner)
				System.out.println("Filter " + scan.getFilter() + " matched row: " + result);
		}
		System.out.println();
	}

	/**
	 * Method used to delete the specified table.
	 * 
	 * @param admin
	 *            {@link Admin}
	 * @param tableName
	 *            Table name to delete.
	 * @throws IOException
	 */
	public void deleteTable(Admin admin, TableName tableName) throws IOException {
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}

	public static void main(String... args) {
		try {
			HBaseClientOperations clientOperations = new HBaseClientOperations();
			Connection connection = ConnectionManager.getConnection();

			TableName tableName = TableName.valueOf(TABLE_NAME);
			if (!connection.getAdmin().tableExists(tableName)) {
				clientOperations.createTable(connection.getAdmin());
				clientOperations.put(connection.getTable(tableName));
			}

			Table table = connection.getTable(tableName);
			clientOperations.get(table, Bytes.toBytes("1"), Bytes.toBytes("name"), Bytes.toBytes("first"));
			clientOperations.scan(table, Bytes.toBytes("name"), Bytes.toBytes("last"));
			clientOperations.filters(table, Bytes.toBytes("contactInfo"), Bytes.toBytes("email"),
					"dwayne@fabrikam.com");
			clientOperations.deleteTable(connection.getAdmin(), tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
