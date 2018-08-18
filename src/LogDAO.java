

package com.ef;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import com.ef.Parser.logrecord;
import com.mysql.jdbc.ResultSet;

public class LogDAO {
	Connection con;
	ResultSet st;
	private String username;
	private String password;
	private String URL;

	public LogDAO(String address, String user, String pass)

	{

		System.out.println("You are in the Constructor for LogDAO");

		/*
		 * Constructor for initializing URL, username and password required to
		 * access the MYSQL Server Please adjust to your username and password.
		 */

		this.URL = "jdbc:mysql://localhost:3306/";
		this.username = "root";
		this.password = "";
	}

	public boolean insertData(HashMap<String, ArrayList<logrecord>> logs) {

		/*
		 * Stores the Log Data to DataBase
		 */

		try (Connection con = DriverManager.getConnection(URL, username, password)) {

			System.out.println("You are connected");
			Set<String> keys = logs.keySet();
			ArrayList<logrecord> records = new ArrayList<logrecord>();
			int batchcount = 0;
			Class.forName("com.mysql.jdbc.Driver");
			Statement stmt = con.createStatement();
			System.out.println("Creating statement...");
			stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS logs;");
			System.out.println("That database does exist ");
			stmt.executeUpdate("use logs");
			System.out.println("\n");

			String query = ("create table IF NOT EXISTS LogRecords( logtime timestamp(3),ip varbinary(16),request varchar(30),statusvalue smallint,useragent varchar(500),primary key(ip,logtime))");

			stmt.executeUpdate(query);
			System.out.println("Your create table LogRecords is " + "\n" + "\n" + query);
			System.out.println(" \n ");

			PreparedStatement ps = con.prepareStatement("INSERT IGNORE INTO LogRecords (logtime,ip,request,"
					+ "statusvalue,useragent)" + "values(?,INET_ATON(?),?,?,?)");

			con.setAutoCommit(false);

			System.out.println("Inserting into Database:" + LocalDateTime.now());

			for (String key : keys) {
				records = logs.get(key);
				for (logrecord t : records) {

					if (batchcount < 1000) {

						ps.setTimestamp(1, Timestamp.valueOf(t.getDt()));
						ps.setString(2, key);
						ps.setString(3, t.getRequest());
						ps.setInt(4, t.getStatus());
						ps.setString(5, t.getInfo());
						ps.addBatch();
						batchcount++;

					}

					else {

						ps.executeBatch();
						con.commit();
						batchcount = 0;
						ps.setTimestamp(1, Timestamp.valueOf(t.getDt()));
						ps.setString(2, key);
						ps.setString(3, t.getRequest());
						ps.setInt(4, t.getStatus());
						ps.setString(5, t.getInfo());
						ps.addBatch();
						batchcount++;

					}

				}

			}

			if (batchcount > 0) {

				ps.executeBatch();
				con.commit();
			}

			ps.close();

			System.out.println("Finished Inserting into Database:" + LocalDateTime.now());

		}

		catch (SQLException e) {

			e.printStackTrace();
			System.out.println("Your error code is " + e.getErrorCode());
			System.out.println("\n");

			System.out.println("Your message is " + e.getMessage());
			System.out.println("\n");
			return false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("\n");
		}

		return true;

	}

	public boolean insertIPs(HashMap<String, Long> logs) {

		/*
		 * Stores the IP's satisfying the given criteria into the DataBase
		 */

		try (Connection con = DriverManager.getConnection(URL, username, password)) {

			Set<String> keys = logs.keySet();
			int batchcount = 0;

			Statement st = con.createStatement();
			st.executeUpdate("use logs");
			System.out.println("\n");

			String me = ("create table IF NOT EXISTS IPLog(ip varbinary(16),reason varchar(35),primary key(ip))");

			System.out.println("Your create table IPLog " + "\n" + "\n" + me);

			System.out.println("\n");
			st.executeUpdate(me);

			System.out.println("\n");

			st.close();

			PreparedStatement ps = con.prepareStatement("INSERT IGNORE INTO IPLog(ip,reason) values(INET_ATON(?),?) ");

			con.setAutoCommit(false);

			System.out.println("Inserting IPS into Database:" + LocalDateTime.now());

			for (String key : keys) {

				if (batchcount < 10) {
					ps.setString(1, key);
					ps.setString(2, "This IP made " + logs.get(key) + " requests");
					ps.addBatch();
					batchcount++;

				}

				else {
					ps.executeBatch();
					con.commit();
					batchcount = 0;
					ps.setString(1, key);
					ps.setString(2, "This IP made " + logs.get(key) + " requests");
					ps.addBatch();
					batchcount++;

				}

			}

			if (batchcount > 0) {
				ps.executeBatch();
				con.commit();
			}

			ps.close();
			System.out.println("Finished Inserting IPS into Database:" + LocalDateTime.now());

		}

		catch (SQLException e) {

			e.printStackTrace();
			return false;
		}

		return true;
	}

}
