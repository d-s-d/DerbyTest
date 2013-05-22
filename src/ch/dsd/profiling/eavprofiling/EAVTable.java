package ch.dsd.profiling.eavprofiling;

import java.sql.*;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: dsd
 * Date: 5/21/13
 * Time: 11:16 AM
 */
public class EAVTable implements IDBOperations {
	public static final String SQL_STATE_NOT_EXIST = "42Y55";
	private static final String SQL_TABLE_NAME = "vectors_eav";
	private static final String SQL_DROP_TABLE = "DROP TABLE " + SQL_TABLE_NAME;
	private static final String SQL_KEY_NAME = "vector_id";
	private static final String SQL_ATTR_NAME = "attribute_id";
	private static final String SQL_VALUE_NAME = "value";
	private static final String SQL_CREATE_TABLE =
		String.format("CREATE TABLE %s (%s INT INDEXED, %s SMALLINT, %s DOUBLE);", SQL_TABLE_NAME, SQL_KEY_NAME,
			SQL_ATTR_NAME, SQL_VALUE_NAME);

	private static final String SQL_SELECT_FROM_WHERE_ORDER_ATTR =

		"SELECT " + SQL_VALUE_NAME + " FROM " + SQL_TABLE_NAME + " WHERE " + SQL_KEY_NAME + "=? AND " +
			SQL_ATTR_NAME + " IN (%s) ORDER BY " + SQL_ATTR_NAME + ";";

	private static final String SQL_SELECT_FROM_WHERE_ORDER_VECTOR_ATTR =
		"SELECT " + SQL_VALUE_NAME + " FROM " + SQL_TABLE_NAME + " WHERE %s AND " + SQL_ATTR_NAME + " IN (%s) ORDER BY " +
			SQL_KEY_NAME + ", " + SQL_ATTR_NAME + ";";

	private static final String SQL_SELECT_FROM_ORDER_VECTOR_ATTR =
		String.format("SELECT %s FROM %s WHERE %s IN (%%s) ORDER BY %s, %s;", SQL_VALUE_NAME,
			SQL_TABLE_NAME, SQL_ATTR_NAME, SQL_KEY_NAME, SQL_ATTR_NAME);

	private static final String SQL_INSERT_INTO_VALUES =
		String.format("INSERT INTO %s VALUES (?,?,?);", SQL_TABLE_NAME);

	private static final String SQL_CREATE_INDEX =
		"CREATE INDEX vectorAttrIdx ON " + SQL_TABLE_NAME + " ( " + SQL_KEY_NAME + " ASC, " + SQL_ATTR_NAME + " ASC);";

	private int[] attrs;
	private int cols;
	private Connection con;
	private PreparedStatement psGetValues;
	private PreparedStatement psInsertValues;
	private PreparedStatement psRangeValues;
	private PreparedStatement psFullTable;

	private int currentId;

	@Override
	public void setConnection(Connection con) throws SQLException {
		this.con = con;
		this.con.setAutoCommit(false);
	}

	@Override
	public void createTable(int cols, int[] attrs) throws SQLException {
		Statement s, sIdx;
		this.cols = cols;
		this.attrs = attrs;
		s = this.con.createStatement();
		sIdx = this.con.createStatement();
		try {
			s.execute(SQL_DROP_TABLE);
			con.commit();
		} catch (SQLException sqlExc) {
			if (!sqlExc.getSQLState().equals(SQL_STATE_NOT_EXIST))
				throw sqlExc;
		}
		s.execute(SQL_CREATE_TABLE);
		sIdx.execute(SQL_CREATE_INDEX);
		con.commit();
		prepareStatements();

		currentId = 0;
	}

	@Override
	public void insertVec(double[] vals) throws SQLException {
		for( int i = 0; i < cols; i++ ) {
			// using local state to ensure database consistency is a sin, I know.
			psInsertValues.setInt(1, currentId++);
			psInsertValues.setShort(2, (short) i);
			psInsertValues.setDouble(3, vals[i]);
			psInsertValues.addBatch();
		}
		psInsertValues.executeBatch();
	}

	@Override
	public double[] getVals(int idx) throws SQLException {
		double[] res = new double[attrs.length];
		int i = 0;
		psGetValues.setInt(1, idx);
		final ResultSet rs = psGetValues.executeQuery();
		while( rs.next() ) {
			res[i] = rs.getDouble(1);
			i++;
		}
		return res;
	}

	private double[][] getMultipleValues(PreparedStatement ps) throws SQLException {
		ResultSet rs;
		int i = 0, l = attrs.length;
		ArrayList<double[]> res = new ArrayList<double[]>();
		double[] curVec;

		rs = ps.executeQuery();
		curVec = new double[attrs.length];
		while( rs.next() ) {
			curVec[i % l] = rs.getDouble(1);
			if( i % l == l-1 ) {
				res.add(curVec);
				curVec = new double[attrs.length];
			}
			i++;
		}
		return (double[][]) res.toArray();
	}

	@Override
	public double[][] getRange(int fromIdx, int toIdx) throws SQLException {
		psRangeValues.setInt(1, fromIdx);
		psRangeValues.setInt(2, toIdx);
		return getMultipleValues(psRangeValues);
	}

	@Override
	public double[][] getFullTable() throws SQLException {
		return getMultipleValues(psFullTable);
	}

	@Override
	public void commit() throws SQLException {
		con.commit();
	}

	@Override
	public void dispose() throws SQLException {
		closePrepStatements();
	}

	private void closePrepStatements() throws SQLException {
		// close all prepared statements
		PreparedStatement[] psArray = {psGetValues, psInsertValues, psRangeValues, psFullTable};
		for( PreparedStatement ps: psArray ) {
			if( ps != null && !ps.isClosed()) {
				ps.close();
			}
		}
	}

	private void prepareStatements() throws SQLException {
		closePrepStatements();

		this.psGetValues = con.prepareStatement(String.format(
			SQL_SELECT_FROM_WHERE_ORDER_ATTR, getAttributeList(this.attrs)
		));

		this.psRangeValues = con.prepareStatement(String.format(
			SQL_SELECT_FROM_WHERE_ORDER_VECTOR_ATTR, SQL_KEY_NAME + ">=? AND " + SQL_KEY_NAME + "<=?",
			getAttributeList(this.attrs)
		));

		this.psFullTable = con.prepareStatement(String.format(
			 SQL_SELECT_FROM_ORDER_VECTOR_ATTR, getAttributeList(this.attrs)
		));

		this.psInsertValues = con.prepareStatement(SQL_INSERT_INTO_VALUES);

	}

	private String getAttributeList(int[] attrs) {
		StringBuilder sb = new StringBuilder();

		if( attrs.length > 0 ) {
			sb.append(attrs[0]);
			for( int i = 1; i < attrs.length; i++ ) {
				sb.append(",");
				sb.append(attrs[i]);
			}
		}
		return sb.toString();
	}
}
