//https://github.com/JustDuck/ParseTest


package com.ef;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import com.ef.LogDAO;
import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.Set;

public class Parser {

	private static LocalDateTime startdate;

	private static LocalDateTime enddate;

	private static String duration;

	private static int threshold;

	private static HashMap<String, ArrayList<logrecord>> logmap;

	private static HashMap<String, Long> matchedip;

	private static String URL;

	private static String username;

	private static String password;

	private static LogDAO db;

	private static int linecount = 0;

	public static class logrecord {

		/*
		 * This class is used to store records mapping to each IP. For each IP,
		 * a corresponding ArrayList of logrecord is stored in the HashMap.
		 * 
		 */

		private LocalDateTime dt;

		private String request;

		private int status;

		private String info;

		public LocalDateTime getDt() {
			return dt;
		}

		public void setDt(LocalDateTime dt) {
			this.dt = dt;
		}

		public String getRequest() {
			return request;
		}

		public void setRequest(String request) {
			this.request = request;
		}

		public int getStatus() {
			return status;
		}

		public void setStatus(int status) {
			this.status = status;

		}

		public String getInfo() {
			return info;
		}

		public void setInfo(String info) {
			this.info = info;

		}

	}

	public static LocalDateTime setDate(String s) {

		/*
		 * This function sets the date in each log to a LocalDateTime Object.
		 */

		String[] dt = s.split(" ");

		String date = dt[0];
		String time = dt[1];

		String[] ymd = date.split("-");
		String[] hms = time.split(":");

		LocalDateTime datetime = LocalDateTime.of(Integer.parseInt(ymd[0]), Integer.parseInt(ymd[1]),
				Integer.parseInt(ymd[2]), Integer.parseInt(hms[0]), Integer.parseInt(hms[1]),
				Integer.parseInt(hms[2].substring(0, 2)),
				Integer.parseInt(hms[2].substring(3, hms[2].length()) + "000000"));

		return datetime;

	}

	private static void addrecord(String log) {

		/*
		 * This function takes each line from the file and stores the log to the
		 * HashMap
		 */

		logrecord record = new logrecord();

		String[] arr = log.split("\\|");

		record.setDt(setDate(arr[0]));
		record.setRequest(arr[2]);
		record.setStatus(Integer.parseInt(arr[3]));
		record.setInfo(arr[4]);

		if (logmap.containsKey(arr[1])) {

			logmap.get(arr[1]).add(record);

		}

		else {
			ArrayList<logrecord> t = new ArrayList<logrecord>();
			t.add(record);
			logmap.put(arr[1], t);

		}

	}

	private static LocalDateTime setstartDate(String dt) {

		/*
		 * This function sets the StartDate as given in the CommandLine
		 * argument.
		 */

		LocalDateTime ldt = null;
		String[] datetime = dt.split("\\.");

		String[] ymd = datetime[0].split("-");
		String[] hms = datetime[1].split(":");

		ldt = LocalDateTime.of(Integer.parseInt(ymd[0]), Integer.parseInt(ymd[1]), Integer.parseInt(ymd[2]),
				Integer.parseInt(hms[0]), Integer.parseInt(hms[1]), Integer.parseInt(hms[2]));

		return ldt;
	}

	private static void readingEntireFileWithoutLoop(String log) throws FileNotFoundException {

		/*
		 * This function reads the entire log file without looping. It uses the
		 * Scanner and delimiter of ",|\r\n" The output is the entire file on
		 * your screen so be aware it might take some amount of time to do that.
		 *
		 */

		try {
			File file = new File("X:\\access.log");
			Scanner sc = new Scanner(file);
			sc.useDelimiter(",|\r\n");
			System.out.println(sc.next());
			while (sc.hasNext()) {
				System.out.println(sc.next());
			}

			sc.close();// closing the scanner stream
		} catch (FileNotFoundException e) {

			System.out.println("Enter existing file name");

			e.printStackTrace();// System.exit(0);
		}
	}

	private static void loadTheFile(String name) throws ClassNotFoundException {

		/*
		 * This function loads the log file from your computer into a newly
		 * created database named boink and into a newly created table called
		 * LogRecords. Once that task is performed it prints out to the sceen
		 * all the records in that table. Note that the file is rather large
		 * with 116484 logs in it. So the outputting to your screen might take
		 * some time. Also, if you run this more than one, the records will
		 * increase. Lastly, if you do not wish to see the output, then comment
		 * out the while loop. Note: If you would like to add a record, you
		 * could create a text file and use that one in its place.
		 */

		String load = "LOAD DATA LOCAL INFILE 'X:/access.log/' REPLACE INTO TABLE `logrecords" + "`\n"
				+ "FIELDS TERMINATED BY \'|\'\n" + "ENCLOSED BY \'\"\'\n" + "ESCAPED BY \'\\\\\'\n"
				+ "LINES TERMINATED BY \'\\r\\n\'(\n" + "`startDate` ,\n" + "`IP` ,\n" + "`request` ,\n"
				+ "`threshold` ,\n" + "`useragent`\n" + ")";

		System.out.println("That load string is " + load);// 1

		try {
			Class.forName("com.mysql.jdbc.Driver");

			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/", "root", "");

			System.out.println("You are connected");
			System.out.println("\n ");
			Statement stmt = con.createStatement();
			stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS boink;");
			System.out.println("\n ");
			System.out.println("that database does exist ");
			System.out.println("\n ");
			stmt.executeUpdate("use boink;");

			stmt.close();

			Statement stmts = con.createStatement();

			stmts.executeUpdate("use boink;");

			String sql = "CREATE TABLE IF NOT EXISTS LogRecords " + "(startdate TIMESTAMP NOT NULL, "
					+ " IP VARCHAR(20) NOT NULL, " + " request VARCHAR(20) NOT NULL, " + " threshold int(50), "
					+ " useragent VARCHAR(255) NOT NULL)";

			stmts.executeUpdate(sql);// that's the table create
			System.out.println("Created table in given database...");

			System.out.println("The query is " + sql);
			System.out.println("\n ");
			stmts.close();

			String query = load;
			Statement stmt2 = con.createStatement();
			stmt2 = con.createStatement();
			stmt2.executeUpdate(query);// that's the load

			System.out.println("does this work ");
			System.out.println("\n ");

			stmt2.close();
			Statement stmt3 = con.createStatement();
			stmt3 = con.createStatement();
			ResultSet st = stmt3.executeQuery("select * from LogRecords");

			while (st.next())
				System.out.println(st.getInt(1) + " " + st.getString(2) + "  " + st.getString(3) + "  "
						+ st.getString(4) + "  " + st.getString(5));
			con.close();
		} catch (SQLException e) {
			System.out.println(e);
			e.printStackTrace();
			System.out.println(e.getMessage());

		}
	}

	private static void runQueries() throws SQLException {

		// This one is good so don't touch it
		/*
		 * This function performs the provided two queries. They were designated
		 * by the company for whom this code was created to test prospective
		 * candidates. They were:
		 * 
		 * (1) Write MySQL query to find IPs that mode more than a certain
		 * number of requests for a given time period.
		 * 
		 * (2) Write MySQL query to find requests made by a given IP.
		 * 
		 * Please note: The version of mysql that I have is: Ver 14.14 Distrib
		 * 5.1.41, for Win32 (ia32) As such functions such as ‘INET6_ATON and
		 * ‘INET6_NTOA’ do not work
		 */

		Connection con = null;
		try {

			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/logs", "root", "");

		} catch (SQLException e) {

			e.printStackTrace();
		}
		System.out.println("You are connected");
		System.out.println("\n ");
		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {

			e.printStackTrace();
		}
		stmt.executeUpdate("use logs;");

		String sql1 = "";

		sql1 = "SELECT inet_ntoa( ip ) AS IPAddress\n" + "FROM logrecords\n" + "WHERE logtime\n"
				+ "BETWEEN \'2017-01-01.13:00:00\'\n" + "AND \'2017-01-01.14:00:00\'\n" + "GROUP BY ip\n"
				+ "HAVING count( * ) >=100\n" + "LIMIT 0 , 30";

		ResultSet rs = stmt.executeQuery(sql1);
		System.out.println("\n ");
		System.out.println("The First Query is " + "\n" + "\n" + sql1);
		System.out.println("\n ");
		System.out.println("Your results from the 1st query are ");
		System.out.println("\n ");
		while (rs.next())
			System.out.println(rs.getString(1) + " ");

		System.out.println("\n ");

		String sql2 = "";

		sql2 = "SELECT inet_ntoa( ip ) AS IPAddress, logtime, request, statusvalue, useragent\n" + "FROM logrecords\n"
				+ "WHERE inet_ntoa( ip ) = \'192.168.11.231\'\n" + "LIMIT 0 , 30";

		rs = stmt.executeQuery(sql2);
		System.out.println("\n ");

		System.out.println("The Second Query is " + "\n" + "\n" + sql2);

		System.out.println("\n ");
		System.out.println("Your results from the 2nd query are ");
		System.out.println("\n ");
		while (rs.next())
			System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3) + " " + rs.getString(4)
					+ " " + rs.getString(5));
		System.out.println("\n ");

	}

	private static void readDatafromFile(String[] param) throws FileNotFoundException {

		/*
		 * This function opens the file, adds the record to HashMap, sets the
		 * startDate, EndDate required for processing the logs.
		 */

		try {

			File filepath = new File(param[0]);
			startdate = setstartDate(param[1]);
			duration = param[2];
			threshold = Integer.parseInt(param[3]);

			if (filepath.isFile()) {

				logmap = new HashMap<String, ArrayList<logrecord>>();
				String log;
				BufferedReader br = new BufferedReader(new FileReader(filepath));

				System.out.println("Start reading from file: " + LocalDateTime.now().toString());
				System.out.println("\n");
				while ((log = br.readLine()) != null) {

					linecount++;
					addrecord(log);

				}
				br.close();

			}

			else {
				System.out.println("File doesn't exist");
				System.exit(0);

			}

			System.out.println("End of reading from file: " + LocalDateTime.now().toString());
			System.out.println("\n");
			System.out.println("Number of logs: " + linecount);

			// which is 116484

			if (duration.toLowerCase().equals("hourly")) {

				enddate = startdate.plusHours(1);

			}

			if (duration.toLowerCase().equals("daily")) {
				enddate = startdate.plusDays(1);

			}

		}

		catch (NullPointerException nullp) {

			System.out.println("Enter file name");
			System.exit(1);

		} catch (FileNotFoundException e) {

			System.out.println("Enter existing file name");
			System.exit(0);

		} catch (IOException e) {

			System.out.println("An IO Exception Occured");
			System.exit(1);

			e.printStackTrace();
		}

	}
	private static void showSchemas(String schema)throws FileNotFoundException{
		/**
		 * This is for the future.
		 * The file I will be adding is the schema named "Schema.sql".
		 */
	}
	public static void readOneRow(String access) throws FileNotFoundException

	{
		try {

			File file = new File("X:\\access.txt");
			Scanner sc = new Scanner(file);
			sc.useDelimiter(",|\r\n");
			System.out.println(sc.next());
			while (sc.hasNext()) {
				System.out.println(sc.next());
			}

			sc.close();// closing the scanner stream
		} catch (FileNotFoundException e) {

			System.out.println("Enter existing file name");

			e.printStackTrace();// System.exit(0);
		}

	}

	private static void storeDatatoDB() {

		/*
		 * This function calls the corresponding method of DAO to store given
		 * logs to DataBase.
		 */

		if (logmap.isEmpty()) {
			System.out.println("No data found in the file");
			System.exit(0);
		}

		else {

			if (db.insertData(logmap))
				System.out.println("Insertion to DataBase Successfull");

			else {
				System.out.println("Insertion to DataBase UnSuccessfull");
				System.exit(1);
			}

		}

	}

	private static void findIPs() {

		/*
		 * This function selects the IP's based on the given StartDate and
		 * Duration
		 */

		if (logmap.isEmpty()) {
			System.out.println("No data found in the file");
			System.exit(0);
		}

		else {
			matchedip = new HashMap<String, Long>();
			Set<String> keys = logmap.keySet();
			ArrayList<logrecord> temp = new ArrayList<logrecord>();
			long count;
			System.out.println("\n");
			System.out.println("Start finding IP's: " + LocalDateTime.now().toString());
			System.out.println("\n");
			for (String i : keys) {

				temp = logmap.get(i);

				if ((count = temp.stream().map(m -> m.getDt()).filter(l -> {
					return (l.isAfter(startdate) || l.isEqual(startdate))
							&& (l.isBefore(enddate) || l.isEqual(enddate));
				}).count()) >= threshold) {
					matchedip.put(i, count);
					System.out.println("IP: " + i + " made " + count + " requests");

				}
			}
			System.out.println("\n");
			System.out.println("End of finding IP's: " + LocalDateTime.now().toString());
			System.out.println("\n");
			if (!matchedip.isEmpty()) {

				for (String ip : matchedip.keySet()) {

					System.out.println("IP: " + ip + " made " + matchedip.get(ip) + " Requests ");

				}
			}

		}
	}

	private static void storeIPstoDB() {

		/*
		 * This function calls the corresponding DAO object's method to store
		 * IP's satisfying the condition to DataBase.
		 */

		if (matchedip.isEmpty())
			System.out.println("No IPs found matching the given condition");

		else {

			if (db.insertIPs(matchedip))
				System.out.println("Matched IPs loaded into DataBase Successfully");

			else {
				System.out.println("Loading Matched IPs UnSuccessfull");
				System.exit(1);
			}

		}

	}

	public static void main(String[] execParameters) throws FileNotFoundException, SQLException, ClassNotFoundException

	{
		/*
		 * This is the entry point of the application. It does all the
		 * processing by calling other methods in the class.
		 * 
		 * The ones added are:
		 * 
		 * 'Q' = runQueries 'L' = loadTheFile 'R' = readingEntireFileWithoutLoop
		 * 
		 * I suggest that you point to wherever the log file is placed.
		 * 
		 * java -cp "parser.jar" com.ef.Parser X:/access.log 2017-01-01.00:00:00
		 * daily 250
		 * 
		 */

		// java -cp "parser.jar" com.ef.Parser X:/access.log 2017-01-01.15:00:00
		// daily 200

		if (execParameters.length != 4) {
			System.out.println("Enter required Parameters for execution");
			System.out.println("\n");
			return;

		}

		else {
			System.out.println("\n");
			System.out.println("Do You want to Store Data to your MySQl DataBase ?");

			System.out.println("Type 'Y' or 'N' or 'R' or 'L' or 'Q'");
			System.out.println("\n");
			Scanner s = new Scanner(System.in);
			String line;

			while (!((line = s.nextLine()).toLowerCase().equals("y") || line.toLowerCase().equals("n")
					|| line.toLowerCase().equals("r") || line.toLowerCase().equals("l")
					|| line.toLowerCase().equals("q")))
				;

			System.out.println("\n");

			switch (line.toLowerCase()) {

			case "y":

				System.out.println("Please Enter DataBase URL.");
				System.out.println("For MySQL. Just hit the enter button");
				System.out.println("\n");
				System.out.println("jdbc:mysql://localhost:3306 ");

				URL = s.nextLine().trim();
				System.out.println("Enter Username");

				username = s.nextLine().trim();
				System.out.println("\n");
				System.out.println("Enter Password");
				password = s.nextLine().trim();
				// setPassword(s.nextLine().trim());

				readDatafromFile(execParameters);
				db = new LogDAO(URL, username, "");
				storeDatatoDB();
				findIPs();
				storeIPstoDB();
				break;

			case "n":

				readDatafromFile(execParameters);
				findIPs();
				if (matchedip.isEmpty())
					System.out.println("No IPs found matching the given condition");
				break;

			case "l":

				loadTheFile("Boink");
				break;

			case "r":

				System.out.println("\n");
				try {
					readingEntireFileWithoutLoop("X:\\access.log");
				} catch (Exception e) {

					e.printStackTrace();
				}

				break;

			case "q":

				readOneRow("access.txt");
				System.out.println("\n");
				runQueries();

				break;

			}

			s.close();

		}

	}

}
