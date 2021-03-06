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
import java.lang.reflect.Method;
import java.util.List;
import java.util.LinkedList;

public class ExchangeTest {
  private static boolean isCount; // are we running count * queries?
  private final String url = "jdbc:postgresql://localhost:54321/";
  private final String username = "postgres";
  private final String pass = "postgres";

  private final String DROP = "DROP TABLE IF EXISTS A;";
  private final String DDL = 
      "CREATE TABLE A (id INT PRIMARY KEY, name TEXT, extra_id INT, single INT);";

  public final static String[] nameTokens = { "BAR", "OUGHT", "ABLE", "PRI",
    "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };

  private final Random rand;

  private final String TEMPLATE_FOR_BATCH_INSERT = "INSERT INTO A VALUES (?,?,?,?);";

  private final String SEQSCAN = "SELECT * FROM A";

  private final String NON_KEY_SCAN_10 = "SELECT * FROM A WHERE name = ?";

  private final String NON_KEY_SCAN_1 = "SELECT * FROM A WHERE extra_id = ?";

  private final String NON_KEY_SCAN_50 = "SELECT * FROM A WHERE extra_id >= ?";

  private final String SINGLE_RESULT = "SELECT * FROM A WHERE single = 10";

  private final String COUNT_SEQSCAN = "SELECT COUNT(*) FROM A";

  private final String COUNT_NON_KEY_SCAN_10 = "SELECT COUNT(*) FROM A WHERE name = ?";

  private final String COUNT_NON_KEY_SCAN_1 = "SELECT COUNT(*) FROM A WHERE extra_id = ?";

  private final String COUNT_NON_KEY_SCAN_50 = "SELECT COUNT(*) FROM A WHERE extra_id >= ?";

  private final String COUNT_SINGLE_RESULT = "SELECT COUNT(*) FROM A WHERE single = 10";

  private List<Double> outputBuffer;

  private final Connection conn;

  private static final int BATCH_SIZE = 10000;

  private static int numRows;

  private static final int NUM_COLS = 4;

  public ExchangeTest() throws SQLException {
    try {
        Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
        e.printStackTrace();
    }
    conn = this.makeConnection();
    rand = new Random();
    outputBuffer = new LinkedList<Double>();
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

  public void BatchInsert() throws SQLException{
    int[] res;
    int insertCount = 0, numInsertions, key;
    int numBatches = (numRows + BATCH_SIZE - 1)/BATCH_SIZE;
    String name1, name2;
    PreparedStatement stmt = conn.prepareStatement(TEMPLATE_FOR_BATCH_INSERT);
    conn.setAutoCommit(false);

    for (int i=1; i<=numBatches; i++) {
      
      numInsertions = (i==numBatches) ? numRows - insertCount : BATCH_SIZE;
      for(int j=1; j <= numInsertions; j++) {
        key = j+insertCount;
        stmt.setInt(1, key);
        stmt.setString(2, nameTokens[j%nameTokens.length]);
        stmt.setInt(3, key%100);
        stmt.setInt(4, key);
        stmt.addBatch();
      }
      
      try{
        res = stmt.executeBatch();
      }catch(SQLException e){
        e.printStackTrace();
        throw e.getNextException();
      }

      for(int k=0; k < res.length; k++){
        if (res[k] < 0) {
          throw new SQLException("Query "+ (k+1) +" returned " + res[k]);
        }
      }
      
      insertCount += res.length;
      System.out.println("Inserted " + insertCount + 
          " rows out of " + numRows + " rows.");
      stmt.clearBatch();
    }
    
    conn.commit();
  }

  public void SeqScan() throws SQLException {
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();
    int rowCtr = 0;

    if (isCount) {
      stmt.execute(COUNT_SEQSCAN);
    } else {
      stmt.execute(SEQSCAN);
    }

    ResultSet rs = stmt.getResultSet();

    if (isCount) {
      if (rs.next()) {
        int count = rs.getInt(1);
        if (count != numRows) {
          throw new SQLException("Incorrect number of rows returned:" + count
              + " Expected:" + numRows);
        }
      } else {
        throw new SQLException("No rows returned from the table");
      }
    } else {
      ResultSetMetaData rsmd = rs.getMetaData();

      if (rsmd.getColumnCount() != NUM_COLS)
        throw new SQLException("Table should have "+ NUM_COLS +" columns");

      while(rs.next())
          rowCtr++;

      if (rowCtr != numRows) {
          throw new SQLException("Insufficient rows returned:" +
              rowCtr +"/" + numRows);
      }
    }
    System.out.println("Sequential Scan successful");
  }

  public void Selectivity1TupleScan() throws SQLException {
    int rowCtr = 0;
    PreparedStatement pstmt;
    conn.setAutoCommit(true);

    if (isCount) {
      pstmt = conn.prepareStatement(COUNT_SINGLE_RESULT);
    } else {
      pstmt = conn.prepareStatement(SINGLE_RESULT);
    }
    
    pstmt.execute();

    ResultSet rs = pstmt.getResultSet();

    if (isCount) {
      if (rs.next()) {
        int count = rs.getInt(1);
        if (count != 1) {
          throw new SQLException("Incorrect number of rows returned:" + count
              + " Expected:" + 1);
        }
      } else {
        throw new SQLException("No rows returned from the table");
      }
    } else {
      ResultSetMetaData rsmd = rs.getMetaData();

      if (rsmd.getColumnCount() != NUM_COLS)
        throw new SQLException("Table should have "+ NUM_COLS + " columns");

      while(rs.next())
        rowCtr++;

      if (rowCtr != 1)
        throw new SQLException("Incorrect rows returned:" + rowCtr +"/" + 1);
    }

    System.out.println("Selectivity1Tuple Scan successful");
  }

  public void Selectivity1Scan() throws SQLException {
    int rowCtr = 0;
    PreparedStatement pstmt;
    conn.setAutoCommit(true);
    if (isCount) {
      pstmt = conn.prepareStatement(COUNT_NON_KEY_SCAN_1);
    } else {
      pstmt = conn.prepareStatement(NON_KEY_SCAN_1);
    }
    pstmt.setInt(1, 1);
    pstmt.execute();

    ResultSet rs = pstmt.getResultSet();

    if (isCount) {
      if (rs.next()) {
        int count = rs.getInt(1);
        if (count != numRows/100) {
          throw new SQLException("Incorrect number of rows returned:" + count
              + " Expected:" + numRows/100);
        }
      } else {
        throw new SQLException("No rows returned from the table");
      }
    } else {
      ResultSetMetaData rsmd = rs.getMetaData();

      if (rsmd.getColumnCount() != NUM_COLS)
        throw new SQLException("Table should have "+ NUM_COLS +" columns");

      while(rs.next())
        rowCtr++;

      if (rowCtr != numRows/100) {
        throw new SQLException("Insufficient rows returned:" +
                                      rowCtr +"/" + numRows/100);
      }
    }

    System.out.println("Selectivity1 Scan successful");
  }


  public void Selectivity10Scan() throws SQLException {
    int rowCtr = 0;
    PreparedStatement pstmt;
    conn.setAutoCommit(true);

    if (isCount) {
      pstmt = conn.prepareStatement(COUNT_NON_KEY_SCAN_10);
    } else {
      pstmt = conn.prepareStatement(NON_KEY_SCAN_10);
    }

    pstmt.setString(1, nameTokens[1]);
    pstmt.execute();

    ResultSet rs = pstmt.getResultSet();

    if (isCount) {
      if (rs.next()) {
        int count = rs.getInt(1);
        if (count != numRows/10) {
          throw new SQLException("Incorrect number of rows returned:" + count
             + " Expected:" + numRows/10);
        }
      } else {
        throw new SQLException("No rows returned from the table");
      }
    } else {
      ResultSetMetaData rsmd = rs.getMetaData();

      if (rsmd.getColumnCount() != NUM_COLS)
        throw new SQLException("Table should have "+ NUM_COLS +" columns");

      while(rs.next())
          rowCtr++;

      if (rowCtr != numRows/10)
          throw new SQLException("Insufficient rows returned:" +
              rowCtr +"/" + numRows/10);
    }
    
    System.out.println("Selectivity10 Scan successful");
  }

  public void Selectivity50Scan() throws SQLException {
    int rowCtr = 0;
    PreparedStatement pstmt;
    conn.setAutoCommit(true);

    if (isCount) {
      pstmt = conn.prepareStatement(COUNT_NON_KEY_SCAN_50);
    } else {
      pstmt = conn.prepareStatement(NON_KEY_SCAN_50);
    }

    pstmt.setInt(1, 50);
    pstmt.execute();

    ResultSet rs = pstmt.getResultSet();

    if (isCount) {
      if (rs.next()) {
        int count = rs.getInt(1);
        if (count != numRows/2) {
          throw new SQLException("Incorrect number of rows returned:" + count
              + " Expected:" + numRows/2);
        }
      } else {
        throw new SQLException("No rows returned from the table");
      }
    } else {
      ResultSetMetaData rsmd = rs.getMetaData();

      if (rsmd.getColumnCount() != NUM_COLS)
      throw new SQLException("Table should have "+ NUM_COLS +" columns");

      while(rs.next())
        rowCtr++;

      if (rowCtr != numRows/2)
        throw new SQLException("Insufficient rows returned:" +
            rowCtr +"/" + numRows/2);
    }

    System.out.println("Selectivity50 Scan successful");
  }

  public void TimeAndExecuteQuery(Object obj, Method method) throws Exception {
    long startTime, endTime;
    startTime = System.nanoTime();
    method.invoke(obj);
    endTime = System.nanoTime();
    outputBuffer.add(((double) endTime - startTime)/Math.pow(10, 6));
  }

  public void PrintTimes() {
    for (double ele : outputBuffer) {
      System.out.print(ele + "\t");
    }
    System.out.println();
  }

  public static void main(String[] args) throws Exception {
    boolean isCreate, isExecute, isLoad;
    Options options = new Options();
    long startTime, endTime;
    Class[] parameterTypes = new Class[0];

    // load CLI options
    Option rows = new Option("r", "rows", true,
      "Required: number of input rows");
    Option count = new Option("s", "count", true,
      "Toggles normal SELECT queries and COUNT(*) queries (Default: true)");
    rows.setRequired(true);
    Option create = new Option("c", "create", true,
      "Create a new table (true or false)");
    Option load = new Option("l", "load", true,
      "Load values into the table (true or false)");
    Option execute = new Option("e", "execute", true,
      "Execute queries on the table (true or false)");

    options.addOption(rows);
    options.addOption(count);
    options.addOption(create);
    options.addOption(load);
    options.addOption(execute);

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

    isCount = Boolean.parseBoolean(cmd.getOptionValue("count", "true"));
    
    isCreate = Boolean.parseBoolean(cmd.getOptionValue("create", "true"));
    isLoad = Boolean.parseBoolean(cmd.getOptionValue("load", "true"));
    isExecute = Boolean.parseBoolean(cmd.getOptionValue("execute", "true"));

    // select the tests that will be run
    Method test1 = ExchangeTest.class.getMethod("Selectivity1TupleScan", 
                                                parameterTypes);
    Method test2 = ExchangeTest.class.getMethod("Selectivity1Scan", 
                                                parameterTypes);
    Method test3 = ExchangeTest.class.getMethod("Selectivity10Scan",
                                                parameterTypes);
    Method test4 = ExchangeTest.class.getMethod("Selectivity50Scan",
                                                parameterTypes);

    ExchangeTest et = new ExchangeTest();
    if (isCreate) {
      et.Init();
      System.out.println("Completed Init");
    }
    if (isLoad) {
      et.BatchInsert();
      System.out.println("Completed Batch Insert");
    }
    if (isExecute) {
      et.TimeAndExecuteQuery(et, test1);
      et.TimeAndExecuteQuery(et, test2);
      et.TimeAndExecuteQuery(et, test3);
      et.TimeAndExecuteQuery(et, test4);
      et.PrintTimes();
    }

    et.Close();
  }
}
