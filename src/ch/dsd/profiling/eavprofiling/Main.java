package ch.dsd.profiling.eavprofiling;

/*
Disclaimer: This code is for testing purposes only.
 */

import java.sql.*;
import java.util.*;

/*
Profiling Parameters:
- EAV vs. condensed layout
- Table Size
- Number of IO operations
- Number of Attributes accessed
 */

public class Main {

	static final int[] tableSizes = {1000, 10000, 100000, 1000000};
	static final int[] batchSizes = {100, 200};
	static final int[] attrs = {0,1,2,3,4,5};
	static final int testRuns = 3;
	static final int cols = 23;

	private String dbName = "derbyDB";
	private String userName = "user1";
	private static final String framework = "embedded";
	private static final String driver = "org.apache.derby.jdbc.EmbeddedDriver";
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
		int testrun, int tableSize, int batchSize, double avg, double stdd ) {
		System.out.println(String.format(
			"\t\t testrun: %d, tableSize: %d, batchSize: %d, avg: %f, stdd: %f",
			testrun, tableSize, batchSize, avg, stdd
		));
	}

	public void go(String[] args) {
		DeltaSequence deltaSeq = new DeltaSequence();
		IDBOperations[] schemas = new IDBOperations[2];
		schemas[0] = new STDTable();
		schemas[1] = new EAVTable();
		int lastSize = 0;
		List<double[]> res = new ArrayList<double[]>();
		// HashMap<IDBOperations, ArrayList<double[]>> verificationData = new HashMap<IDBOperations, ArrayList<double[]>>();

		try {
			loadDriver();
			connect();

			for(IDBOperations schema: schemas) {
				schema.setConnection(con);
				schema.createTable(cols, attrs);
				// verificationData.put(schema, new ArrayList<double[]>());
			}

			/* We assume ascending table sizes, so let's sort first. */
			Arrays.sort(tableSizes);
			/* Iterate over table sized */
			for( int tableSize: tableSizes ) {
				for( IDBOperations schema: schemas ) {
					System.out.println("================");
					System.out.println(schema.getName());
					System.out.println("================");
					// final List<double[]> verificationVectors = verificationData.get(schema);

					/* Fill in required vectors */
					System.out.print("\tFilling in table...");
					final DeltaSequence fillSeq = new DeltaSequence();
					for( int i = 0; i < (tableSize - lastSize); i++ ) {
						final double[] vec = getRandomValues(cols);
						// verificationVectors.add(vec);
						fillSeq.start();
						schema.insertVec(vec);
						fillSeq.stop();
					}
					printTestResults(0, tableSize-lastSize, 0, fillSeq.getAverage(), fillSeq.getStdDev());
					System.out.println("committing...");
					schema.commit();
					System.out.println("done.");
					/* Perform tests */
					for( int batchSize: batchSizes ) {

						System.out.println("\t= Random Access Test =");
						for( int r = 0; r < testRuns; r++ ) {
							/* Perform random access */
							for( int i = 0; i < batchSize; i++ ) {
								final int id = getRandomId(tableSize);
								deltaSeq.start();
								schema.getVals(id);
								deltaSeq.stop();
							}
							printTestResults(r, tableSize, batchSize,
								deltaSeq.getAverage(), deltaSeq.getStdDev());
							deltaSeq.clear();
						} /* testrun */

						System.out.println("\t= Batch Test =");
						for( int r = 0; r < testRuns; r++ ) {
							/* Get multiple vectors */
							deltaSeq.start();
							res = schema.getRange(0, batchSize);
							deltaSeq.stop();
						} /* testrun */

						printTestResults(testRuns, tableSize, batchSize,
							deltaSeq.getAverage(), deltaSeq.getStdDev());

						/* Verify batch result
						boolean verifySuccess = true;
						if( res != null ) {
							int sz = res.size();
							for( int i = 0; i < sz; i++ ) {
								double[] vecRes = res.get(i);
								double[] vecVer = verificationVectors.get(i);
								for( int j = 0; j < vecRes.length; j++ ) {
							    if(!verifySuccess) break;
									if( vecRes[j] != vecVer[attrs[j]] ) {
										verifySuccess = false;
										break;
									}
								}
							}
							if(!verifySuccess)
								System.out.println("Verification failed!");
						} */
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
				if( con != null ) con.close();
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

	private void loadDriver() {
        /*
         *  The JDBC driver is loaded by loading its class.
         *  If you are using JDBC 4.0 (Java SE 6) or newer, JDBC drivers may
         *  be automatically loaded, making this code optional.
         *
         *  In an embedded environment, this will also start up the Derby
         *  engine (though not any databases), since it is not already
         *  running. In a client environment, the Derby engine is being run
         *  by the network server framework.
         *
         *  In an embedded environment, any static Derby system properties
         *  must be set before loading the driver to take effect.
         */
		try {
			Class.forName(driver).newInstance();
			System.out.println("Loaded the appropriate driver");
		} catch (ClassNotFoundException cnfe) {
			System.err.println("\nUnable to load the JDBC driver " + driver);
			System.err.println("Please check your CLASSPATH.");
			cnfe.printStackTrace(System.err);
		} catch (InstantiationException ie) {
			System.err.println(
				"\nUnable to instantiate the JDBC driver " + driver);
			ie.printStackTrace(System.err);
		} catch (IllegalAccessException iae) {
			System.err.println(
				"\nNot allowed to access the JDBC driver " + driver);
			iae.printStackTrace(System.err);
		}
	}

	public static void main(String[] args) {
		new Main().go(args);
	}

}
