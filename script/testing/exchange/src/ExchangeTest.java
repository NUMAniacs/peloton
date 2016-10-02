//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_plan.h
//
// Identification: script/testing/jdbc/src/PelotonTest.java
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import java.sql.*;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

public class ExchangeTest {
    private final String url = "jdbc:postgresql://localhost:5432/";
    private final String username = "postgres";
    private final String pass = "postgres";

    private final String DROP = "DROP TABLE IF EXISTS A;";
    private final String DDL = "CREATE TABLE A (id INT PRIMARY KEY, name TEXT);";

    public final static String[] nameTokens = { "BAR", "OUGHT", "ABLE", "PRI",
      "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };
    
    private final Random rand;
    
    private final String TEMPLATE_FOR_BATCH_INSERT = "INSERT INTO A VALUES (?,?);";

    private final String SEQSCAN = "SELECT * FROM A";

    private final Connection conn;
    
    private static int numRows = 10;

    public ExchangeTest() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conn = this.makeConnection();
        rand = new Random();
        return;
    }

    private Connection makeConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(url, username, pass);
        return conn;
    }

    public void Close() throws SQLException {
        conn.close();
    }

    /**
    * Drop if exists and create testing database
    *
    * @throws SQLException
    */
    public void Init() throws SQLException {
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        stmt.execute(DROP);
        stmt.execute(DDL);
    }

    public void ShowTable() throws SQLException {
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();

        stmt.execute(SEQSCAN);

        ResultSet rs = stmt.getResultSet();

        ResultSetMetaData rsmd = rs.getMetaData();

        if (rsmd.getColumnCount() != 2) {
          throw new SQLException("Table should have 2 columns");
        } else if (rs.next()) {
          throw new SQLException("No rows should be returned");
        }
        System.out.println("Test db created.");
    }

    public void Batch_Insert() throws SQLException{
    	int[] res;
    	PreparedStatement stmt = conn.prepareStatement(TEMPLATE_FOR_BATCH_INSERT);
    	conn.setAutoCommit(false);
    
    	String name1 = nameTokens[rand.nextInt(nameTokens.length)];
    	String name2 = nameTokens[rand.nextInt(nameTokens.length)];
	    for(int i=1; i <= numRows ;i++){
	      stmt.setInt(1,i);
	      stmt.setString(2, name1+name2);
	      stmt.addBatch();
	    }
	    
	    System.out.println("Batch Being Launched");

		try{
			res = stmt.executeBatch();
		}catch(SQLException e){
			e.printStackTrace();
			throw e.getNextException();
		}

		System.out.println("Batch Executed");

		for(int i=0; i < res.length; i++){
			System.out.println(res[i]);
			if (res[i] < 0) {
				throw new SQLException("Query "+ (i+1) +" returned " + res[i]);
			}
		}
		conn.commit();
    }

  
    static public void main(String[] args) throws Exception {
        Options options = new Options();

        Option rows = new Option("r", "rows", true, "number of input rows");
        options.addOption(rows);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(ExchangeTest.class.getName(), options);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("rows")){
        	numRows = Integer.parseInt(cmd.getOptionValue("rows"));
        }

        System.out.println("Number of rows in Exchange Test:" + numRows);
        ExchangeTest et = new ExchangeTest();
        et.Init();
        et.Batch_Insert();
        et.Close();
    }
}
