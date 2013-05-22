package ch.dsd.profiling.eavprofiling;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

/*
Profiling Parameters:
- EAV vs. condensed layout
- Table Size
- Number of IO operations
- Number of Attributes accessed
 */

public class Main {

	static final int[] tableSizes = {1000};
	static final int[] batchSizes = {100};
	static final int[] attrs = {0,1,2,3,4,5};
	static final int testRuns = 3;
	static final int cols = 23;

	private String dbName = "eavprofiling";
	private String userName = "user1";
	private static final String framework = "embedded";
	private static final String driver = "org.apache.dergy.jdbc.EmbeddedDriver";
	private static final String protocol = "jdbc:derby:";



	private Connection con;

	private void connect() throws SQLException {
		Properties props = new Properties();
		props.put("user", "user1");
		props.put("password", "user1");
		con = DriverManager.getConnection(protocol + dbName + ";create=true", props);
		con.setAutoCommit(false);
	}

	private double[] getRandomValues( int n ) {
		double[] res = new double[n];
		for( int i = 0; i < n; i++ ) {
			res[i] = Math.random();
		}
		return res;
	}

	private int getRandomId( int n ) {
		double rnd = Math.random();
		return (int) (rnd*((double)n));
	}

	private void printTestResults(
		String test, int testrun, int tableSize, int batchSize, double avg, double stdd ) {
		System.out.println(String.format(
			"Performed Test: %s, testrun: %d, tableSize: %d, batchSize: %d, avg: %f, stdd: %f",
			test, testrun, tableSize, batchSize, avg, stdd
		));
	}

	public void go(String[] args) {
		DeltaSequence deltaSeq = new DeltaSequence();
		IDBOperations[] schemas = new IDBOperations[2];
		schemas[0] = new STDTable();
		schemas[1] = new EAVTable();
		int lastSize = 0;

		try {
			connect();

			for(IDBOperations schema: schemas) {
				schema.setConnection(con);
				schema.createTable(cols, attrs);
			}

			/* We assume ascending table sizes, so let's sort first. */
			Arrays.sort(tableSizes);
			/* Iterate over table sized */
			for( int tableSize: tableSizes ) {
				for( IDBOperations schema: schemas ) {
					/* Fill in required vectors */
					for( int i = 0; i < (tableSize - lastSize); i++ ) {
						schema.insertVec(getRandomValues(cols));
					}
					schema.commit();
					/* Perform tests */
					for( int batchSize: batchSizes ) {
						for( int r = 0; r < testRuns; r++ ) {
							/* Perform random access */
							for( int i = 0; i < batchSize; i++ ) {
								final int id = getRandomId(tableSize);
								deltaSeq.start();
								schema.getVals(id);
								deltaSeq.stop();
							}
							printTestResults("Random Access", r, tableSize, batchSize,
								deltaSeq.getAverage(), deltaSeq.getStdDev());
							deltaSeq.clear();
						} /* testrun */

						for( int r = 0; r < testRuns; r++ ) {
							/* Get multiple vectors */
							deltaSeq.start();
							final double[][] res = schema.getRange(0, batchSize);
							System.out.println("Retrieved " + res.length + " values.");
							deltaSeq.stop();
						} /* testrun */

						printTestResults("Batch access ", testRuns, tableSize, batchSize,
							deltaSeq.getAverage(), deltaSeq.getStdDev());
					} /* batch Size */
				} /* schema */
			} /* table Size */

			if (framework.equals("embedded"))
			{
				try {
					DriverManager.getConnection("jdbc:derby:;shutdown=true");
				}
				catch (SQLException se)
				{
					if (( (se.getErrorCode() == 50000)
						&& ("XJ015".equals(se.getSQLState()) ))) {
						System.out.println("Derby shut down normally");
					} else {
						System.err.println("Derby did not shut down normally");
						printSQLException(se);
					}
				}
			}


		} catch (SQLException sqlExc) {
			sqlExc.printStackTrace(System.out);
		} finally {
			try {
				/* dispose statements */
				for( IDBOperations schema: schemas ) {
					schema.dispose();
				}

				/* close connection */
				con.close();
			} catch (SQLException sqlExc ) {
				sqlExc.printStackTrace(System.err);
			}
		}
	}


	/**
	 * Prints details of an SQLException chain to <code>System.err</code>.
	 * Details included are SQL State, Error code, Exception message.
	 *
	 * @param e the SQLException from which to print details.
	 */
	public static void printSQLException(SQLException e)
	{
		// Unwraps the entire exception chain to unveil the real cause of the
		// Exception.
		while (e != null)
		{
			System.err.println("\n----- SQLException -----");
			System.err.println("  SQL State:  " + e.getSQLState());
			System.err.println("  Error Code: " + e.getErrorCode());
			System.err.println("  Message:    " + e.getMessage());
			// for stack traces, refer to derby.log or uncomment this:
			//e.printStackTrace(System.err);
			e = e.getNextException();
		}
	}

	public static void main(String[] args) {
		new Main().go(args);
	}

}
