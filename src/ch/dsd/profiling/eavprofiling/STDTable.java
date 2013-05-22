package ch.dsd.profiling.eavprofiling;

import java.sql.*;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: dsd
 * Date: 5/21/13
 * Time: 10:17 AM
 */
public class STDTable implements IDBOperations {
	public static final String SQL_STATE_NOT_EXIST = "42Y55";
	private static final String SQL_TABLE_NAME = "vectors_std";
	private static final String SQL_COL_PREFIX = "C";
	private static final String SQL_DROP_TABLE = "DROP TABLE " + SQL_TABLE_NAME;
	private static final String SQL_KEY_NAME = "vector_id";
	private static final String SQL_CREATE_TABLE =
		"CREATE TABLE " + SQL_TABLE_NAME + "("+SQL_KEY_NAME+" PRIMARY KEY GENERATING AS IDENTITY, %s);";
	private static final String SQL_SELECT_FROM_WHERE =
		"SELECT %s FROM " + SQL_TABLE_NAME + " WHERE %s;";
	private static final String SQL_SELECT_FROM =
		"SELECT %s FROM " + SQL_TABLE_NAME;
	private static final String SQL_INSERT_INTO_VALUES =
		"INSERT INTO " + SQL_TABLE_NAME + " (%s) VALUES (%s);";

	private int cols;
	private int[] attrs;

	private Connection con;
	private PreparedStatement psGetValues;
	private PreparedStatement psInsertValues;
	private PreparedStatement psRangeValues;
	private PreparedStatement psFullTable;

	@Override
	public void setConnection(Connection con) {
		this.con = con;
	}

	@Override
	public void createTable(int cols, int[] attrs) throws SQLException {
		Statement s;
		this.cols = cols;
		this.attrs = attrs;
		s = this.con.createStatement();
		try {
			s.execute(String.format(SQL_DROP_TABLE));
		} catch (SQLException sqlExc) {
			if (!sqlExc.getSQLState().equals(SQL_STATE_NOT_EXIST))
				throw sqlExc;
		}
		s.execute(String.format(SQL_CREATE_TABLE, getColumnHeader()));
		prepareStatements();
		con.setAutoCommit(false);
		commit();
		s.close();
	}

	@Override
	public void insertVec(double[] vals) throws SQLException {
		for( int i = 1; i < this.cols+1; i++ ) {
			psInsertValues.setDouble(i, vals[i-1]);
		}
		psInsertValues.executeUpdate();
	}

	@Override
	public double[] getVals(int idx) throws SQLException {
		ResultSet rs;
		psGetValues.setInt(1, idx);
		rs = psGetValues.executeQuery();
		if( rs.next() ) {
			final double[] res = new double[attrs.length];
			for( int i = 0; i < attrs.length; i++ ) {
				res[i] = rs.getDouble(i+1);
			}
			return res;
		}
		rs.close();
		return null;
	}

	@Override
	public double[][] getRange(int fromIdx, int toIdx) throws SQLException {
		ResultSet rs;
		final ArrayList<double[]> res = new ArrayList<double[]>();
		psRangeValues.setInt(1, fromIdx);
		psRangeValues.setInt(2, toIdx);
		rs = psRangeValues.executeQuery();
		while( rs.next() ) {
			final double[] vals = new double[this.attrs.length];
			for( int i = 0; i < attrs.length; i++ ) {
				vals[i] = rs.getDouble(i+1);
			}
			res.add(vals);
		}
		rs.close();
		return (double[][])res.toArray();
	}

	@Override
	public double[][] getFullTable() throws SQLException {
		ResultSet rs;
		final ArrayList<double[]> res = new ArrayList<double[]>();
		rs = psFullTable.executeQuery();
		while( rs.next() ) {
			final double[] vals = new double[this.attrs.length];
			for( int i = 0; i < attrs.length; i++ ) {
				vals[i] = rs.getDouble(i+1);
			}
			res.add(vals);
		}
		rs.close();
		return (double[][])res.toArray();
	}

	@Override
	public void commit() throws SQLException {
		con.commit();
	}

	@Override
	public void dispose() throws SQLException {
		closePrepStatements();
	}

	private String getColumnNamesFromAttrs(int[] attrs) {
		final StringBuilder sb = new StringBuilder();
		if( attrs != null && attrs.length > 0 ) {
			sb.append("C");
			sb.append(attrs[0]);
			for(int i = 1; i < attrs.length; i++) {
				sb.append(", ");
				sb.append(getColName(attrs[i]));
			}
		}
		return sb.toString();
	}

	private String getColumnNames() {
		int[] idcs = new int[this.cols];
		for( int i = 0; i < cols; i++ )
			idcs[i] = i;
		return getColumnNamesFromAttrs(idcs);
	}

	private String getColName( int idx ) {
		return SQL_COL_PREFIX + idx;
	}

	private String getColumnHeader() {
		StringBuilder sb = new StringBuilder();
		if(cols > 0)
			sb.append(getColName(0));
		for (int i = 1; i < cols; i++) {
			sb.append(getColName(i));
		}
		return sb.toString();
	}

	private void closePrepStatements() throws SQLException {
		PreparedStatement[] psArray = {psGetValues, psInsertValues, psRangeValues, psFullTable};
		for( PreparedStatement ps: psArray ) {
			if( ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}

	private void prepareStatements() throws SQLException {
		closePrepStatements();

		psGetValues = con.prepareStatement(String.format(
			SQL_SELECT_FROM_WHERE, getColumnNamesFromAttrs(this.attrs), SQL_KEY_NAME + "=?"));
		psInsertValues = con.prepareStatement(String.format(
			SQL_INSERT_INTO_VALUES, getColumnNames(), getColumnPlaceholders(this.cols))
		);
		psRangeValues = con.prepareStatement(String.format(
			SQL_SELECT_FROM_WHERE, getColumnNamesFromAttrs(this.attrs), SQL_KEY_NAME + ">=? AND " + SQL_KEY_NAME +"=<?"
		));
		psFullTable = con.prepareStatement(String.format(
			SQL_SELECT_FROM, getColumnNamesFromAttrs(this.attrs)
		));
	}

	private String getColumnPlaceholders(int n) {
		StringBuilder sb = new StringBuilder();
		if( n > 0 ) {
			sb.append("?");
			for( int i = 1; i < n; i++ ) {
				sb.append(",?");
			}
		}
		return sb.toString();
	}
}
