package ch.dsd.profiling.eavprofiling;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: dsd
 * Date: 5/21/13
 * Time: 10:15 AM
 */
public interface IDBOperations {

	public void setConnection( Connection con ) throws SQLException;
	public void createTable( int cols, int[] attrs ) throws SQLException;
	public void insertVec( double[] vals ) throws SQLException;
	public double[] getVals( int idx ) throws SQLException;
	public double[][] getRange( int fromIdx, int toIdx ) throws SQLException;
	public double[][] getFullTable() throws SQLException;
	public void commit() throws SQLException;
	public void dispose() throws SQLException;
}
