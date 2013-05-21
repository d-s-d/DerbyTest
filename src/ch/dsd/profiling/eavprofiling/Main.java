package ch.dsd.profiling.eavprofiling;

import java.sql.*;

/*
Profiling Parameters:
- EAV vs. condensed layout
- Table Size
- Number of IO operations
- Read, Write, Interleaved
- Number of Attributes accessed
 */

abstract class IDBOperations {
  protected Connection con = null;
  protected int nCols;
  protected int nRows;
  protected int[] attrs;
  protected static final String SQL_SELECT_FROM_WHERE = "SELECT %s FROM %s WHERE %s;";
  protected static final String SQL_UPDATE_SET_WHERE = "UPDATE %s SET %s WHERE %s;";
	protected static final String SQL_INSERT_INTO_VALUES = "INSERT INTO %s VALUES (%s)";
  public static final String SQL_TABLE_NAME = "vectors";
  public static final String SQL_ID_NAME = "vector_id";
  public static final String SQL_DROP_TABLE = "DROP TABLE %s;";
  public static final String SQL_CREATE_TABLE = "CREATE TABLE %s;";
  public static final String SQL_COL_PREFIX = "C";
  public static final String SQL_STATE_NOT_EXIST = "42Y55";



  public IDBOperations( Connection con, int nCols ) {
    this.con = con;
    this.nCols = nCols;
  }

  public void createTable(int[] attrs) throws SQLException {
    Statement s;
    s = this.con.createStatement();
    try {
	    s.execute(String.format(SQL_DROP_TABLE, SQL_TABLE_NAME));
    } catch (SQLException sqlExc) {
	    if (!sqlExc.getSQLState().equals(SQL_STATE_NOT_EXIST))
		    throw sqlExc;
    }
    s.execute(String.format(SQL_CREATE_TABLE, getTableHeader()));
    setAttrs(attrs);
  }

  public void populateTable(int nRows) throws Exception {
    for( int i = 0; i < nRows; i++ ) {
      insert( randomDoubleValues( nCols ) );
    }
    this.nRows = nRows;
  }

  protected void setAttrs( int[] attrs ) throws SQLException {
    this.attrs = attrs;
  }

  abstract public double[] getValues(int idx) throws SQLException;

  abstract public void writeValues(int idx, double[] vals) throws SQLException;

  abstract public void insert(double[]vals) throws Exception;

  protected double[] randomDoubleValues( int size ) {
      double[] res = new double[size];
      for( int i = 0; i < size; i++ ) {
          res[i] = Math.random();
      }
      return res;
  }

	abstract protected String getTableHeader();
}

class CondensedTable extends IDBOperations {

	private static final String SQL_PRIMARY_KEY_ATTRIBUTES = "PRIMARY KEY GENERATING AS IDENTITY";
  private PreparedStatement psGetValues;
  private PreparedStatement psUpdateValues;
	private PreparedStatement psInsertValues;

  public CondensedTable(Connection con, int nCols) {
      super(con, nCols);
  }


  @Override
  protected void setAttrs( int[] attrs ) throws SQLException {
    StringBuilder sb = new StringBuilder();
    super.setAttrs(attrs);

    if( attrs.length > 0 ) {
        sb.append(getColName(attrs[0]));
        sb.append("=?");
    }

    for( int attr: attrs ) {
        sb.append(",");
        sb.append(getColName(attr));
        sb.append("=?");
    }

    if(!psGetValues.isClosed()) psGetValues.close();
    psGetValues = con.prepareStatement(
        String.format(SQL_SELECT_FROM_WHERE, getColumnNamesFromAttrs(), SQL_TABLE_NAME, SQL_ID_NAME + "=?;"));
    if(!psUpdateValues.isClosed()) psUpdateValues.close();
    psUpdateValues = con.prepareStatement(
      String.format(SQL_UPDATE_SET_WHERE, SQL_TABLE_NAME, sb.toString(), SQL_ID_NAME+"=?;"));
    if(!psInsertValues.isClosed()) psInsertValues.close();
		psInsertValues = con.prepareStatement(
				String.format(SQL_INSERT_INTO_VALUES, SQL_TABLE_NAME, getColumnPlaceholders(nCols))
			);

  }

  @Override
  public double[] getValues(int idx) throws SQLException {
      ResultSet rs;
      psGetValues.setInt(0, idx);
      rs = psGetValues.executeQuery();
      if( rs.next() ) {
          final double[] res = new double[attrs.length];
          for( int i = 0; i < attrs.length; i++ ) {
              res[i] = rs.getDouble(i+1);
          }
          return res;
      }
      return null;
  }

  @Override
  public void writeValues(int idx, double[] vals) throws SQLException {
      for(int i = 0; i < vals.length; i++ ) {
          psUpdateValues.setDouble(i+1, vals[i]);
      }
      psUpdateValues.setInt(vals.length, idx);
      psUpdateValues.executeUpdate();
  }

  @Override
  public void insert(double[] vals) throws Exception {
	  if( vals.length == nCols ) {
		  for (int i = 0; i < vals.length; i++) {
			  psInsertValues.setDouble(i, vals[i]);
		  }
		  psInsertValues.executeUpdate();
	  } else
		  throw new Exception("insert(): Wrong number of values.");
  }

  private String getColumnNamesFromAttrs() {
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

  private String getColumnPlaceholders(int n) {
      StringBuilder sb = new StringBuilder("(");
      if( n > 0 ) {
          sb.append("?");
          for( int i = 1; i < n; i++ ) {
              sb.append(",?");
          }
      }
      sb.append(")");
      return sb.toString();
  }

	@Override
	protected String getTableHeader() {
		final StringBuilder sb = new StringBuilder();
		sb.append(SQL_TABLE_NAME);
		sb.append("(");
		sb.append(SQL_ID_NAME);
		sb.append(" PRIMARY KEY GENERATING AS IDENTITY");
		for( int i = 0; i < nCols; i++ ) {
			sb.append(", C");
			sb.append(i);
			sb.append(" DOUBLE");
		}
		sb.append(")");
		return sb.toString();
	}

	protected String getColName( int idx ) {
		return SQL_COL_PREFIX + idx;
	}

}

class EAV extends IDBOperations {

	public EAV(Connection con, int nCols) {
		super(con, nCols);
	}



	private String getAttributeList(int[] attrs) {
		StringBuilder sb = new StringBuilder();

		if( attrs.length > 0 ) {
			sb.append("(");
			sb.append(attrs[0]);
		}
		for(int attr: attrs) {
			sb.append(",");
			sb.append(attr);
		}
		return sb.toString();
	}

	@Override
	protected String getTableHeader() {
		return SQL_TABLE_NAME +
			"( " + SQL_ID_NAME + "INT UNIQUE, attribute INT, value DOUBLE)";
	}
}

public class Main {

    public static void main(String[] args) {
	// write your code here
    }
}
