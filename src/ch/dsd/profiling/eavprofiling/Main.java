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
    public static final String SQL_TABLE_NAME = "vectors";
    public static final String SQL_ID_NAME = "vector_id";
    public static final String SQL_DROP_TABLE = "DROP TABLE %s;";
    public static final String SQL_CREATE_TABLE = "CREATE TABLE %s ( int "
    + SQL_ID_NAME + " PRIMARY KEY GENERATING AS IDENTITY, %s);";
    public static final String SQL_COL_PREFIX = "C";
    public static final String SQL_STATE_NOT_EXIST = "42Y55";



    public IDBOperations( Connection con, int nCols ) {
        this.con = con;
        this.nCols = nCols;
    }

    public void createTable(int[] attrs) throws SQLException {
    }

    public void populateTable(int nRows) throws SQLException {
        for( int i = 0; i < nRows; i++ ) {
            insert( randomDoubleValues( nCols ));
        }
        this.nRows = nRows;
    }

    protected void setAttrs( int[] attrs ) throws SQLException {
        this.attrs = attrs;
    }

    public double[] getValues(int idx) throws SQLException {
        return null;
    }

    public void writeValues(int idx, double[] vals) throws SQLException {

    }

    public void insert(double[]vals) throws SQLException {

    }

    protected double[] randomDoubleValues( int size ) {
        double[] res = new double[size];
        for( int i = 0; i < size; i++ ) {
            res[i] = Math.random();
        }
        return res;
    }

    protected String getTableHeader() {
        final StringBuilder sb = new StringBuilder();
        if( nCols > 0 ) {
            sb.append("C0 DOUBLE");
            for( int i = 1; i < nCols; i++ ) {
                sb.append(", C");
                sb.append(i);
                sb.append(" DOUBLE");
            }
        }
        return sb.toString();
    }

    protected String getColName( int idx ) {
        return SQL_COL_PREFIX + idx;
    }
}

class CondensedTable extends IDBOperations {
    private PreparedStatement psGetValues;
    private PreparedStatement psUpdateValues;

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

    }

    @Override
    public void createTable( int[] attrs ) throws SQLException {
        Statement s;
        s = this.con.createStatement();
        try {
            s.execute(String.format(SQL_DROP_TABLE, SQL_TABLE_NAME));
        } catch (SQLException sqlExc) {
            if (!sqlExc.getSQLState().equals(SQL_STATE_NOT_EXIST))
                throw sqlExc;
        }
        s.execute(String.format(SQL_CREATE_TABLE, SQL_TABLE_NAME, getTableHeader()));
        setAttrs(attrs);
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
    public void insert(double[] vals) {
        //To change body of implemented methods use File | Settings | File Templates.
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

}

class EAV {

}

public class Main {

    public static void main(String[] args) {
	// write your code here
    }
}
