package chan.pcific.stmtmppr;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

class SqlTypesUtil {

	final static int[] SUPPORTED_SQL_TYPES = new int[]{
		Types.VARCHAR,
		Types.CHAR,
		Types.BIGINT,
		Types.INTEGER,
		Types.SMALLINT,
		Types.TINYINT,
		Types.TIMESTAMP,
		Types.DATE,
		Types.DECIMAL,
		Types.NUMERIC,
		Types.FLOAT,
		Types.REAL,
		Types.DOUBLE,
	};
	final static int[] NOT_SUPPORTED_SQL_TYPES  = new int[]{
		Types.NULL,
		Types.BLOB,
		Types.CLOB,
		Types.LONGVARCHAR,
	};
	
	static boolean isSupportedSqlType(int columnType){
		return SqlTypesUtil.isInit(SqlTypesUtil.SUPPORTED_SQL_TYPES, columnType);
	}
	static boolean isNotSupportedSqlType(int columnType){
		return SqlTypesUtil.isInit(SqlTypesUtil.NOT_SUPPORTED_SQL_TYPES, columnType);
	}
	private static boolean isInit(int[] types, int type){
		for(int i=0;i<types.length;i++){
			if(type == types[i])
				return true;
		}
		return false;
	}
	static Object getResultSetParamJavaObject(ResultSet rset, int index, int sqlType) throws SQLException{
		Object r = null;
		switch (sqlType) {
		case Types.VARCHAR:
			return rset.getString(index);
		case Types.CHAR:
			return rset.getString(index);
		case Types.NULL:
			throw new SQLException("Not Supported SqlType " + getSqlTypeName(sqlType));
		case Types.BIGINT:
			r = new Long(rset.getLong(index));
			if(rset.wasNull())
				return null;
			else
				return r;
		case Types.INTEGER:
			r = new Integer(rset.getInt(index));
			if(rset.wasNull())
				return null;
			else
				return r;
		case Types.SMALLINT:
			r = new Integer(rset.getInt(index));
			if(rset.wasNull())
				return null;
			else
				return r;
		case Types.TINYINT:
			r = new Integer(rset.getInt(index));
			if(rset.wasNull())
				return null;
			else
				return r;
		case Types.TIMESTAMP:
			r = rset.getTimestamp(index);
			if(r == null)
				return null;
			else
				return new Date(((Timestamp)r).getTime());
		case Types.DATE:
			r = rset.getDate(index);
			if(r == null)
				return null;
			else
				return new Date(((java.sql.Date)r).getTime());
		case Types.DECIMAL:
			return rset.getBigDecimal(index);
		case Types.NUMERIC:
			return rset.getBigDecimal(index);
		case Types.FLOAT:
			r = new Float(rset.getFloat(index));
			if(rset.wasNull())
				return null;
			else
				return r;
		case Types.REAL:
			r = new Float(rset.getFloat(index));
			if(rset.wasNull())
				return null;
			else
				return r;
		case Types.DOUBLE:
			r = new Float(rset.getFloat(index));
			if(rset.wasNull())
				return null;
			else
				return r;
		case Types.BLOB:
			throw new SQLException("Not Supported SqlType " + getSqlTypeName(sqlType));
		case Types.CLOB:
			throw new SQLException("Not Supported SqlType " + getSqlTypeName(sqlType));
		case Types.LONGVARCHAR:
			throw new SQLException("Not Supported SqlType " + getSqlTypeName(sqlType));
		default:
			return rset.getString(index);
		}	
	}
	static void setPreparedStatementParamJavaObject(PreparedStatement stmt, int index, Object javaObject) throws SQLException{
		try{
			if(javaObject instanceof String){
				stmt.setString(index, (String)javaObject);
			}else if(javaObject instanceof Long){
				stmt.setObject(index, javaObject, Types.BIGINT);
			}else if(javaObject instanceof Integer){
				stmt.setObject(index, javaObject, Types.INTEGER);
			}else if(javaObject instanceof Date){
				Date d = (Date)javaObject;
				stmt.setTimestamp(index, new java.sql.Timestamp(d.getTime()));
			}else if(javaObject instanceof Float){
				stmt.setObject(index, javaObject, Types.FLOAT);
			}else if(javaObject instanceof Double){
				stmt.setObject(index, new Float((Double)javaObject), Types.FLOAT);
			}else if(javaObject instanceof Boolean){
				// TODO QUICK FIXED 
				stmt.setString(index, ((Boolean)javaObject).toString());
			}else if(javaObject instanceof String[]){
				String[] lines = (String[])javaObject ;
				StringBuilder sbuff = new StringBuilder();
				for(int i=0;i<lines.length;i++){
					sbuff.append(lines[i]);
					if(i != lines.length - 1)
						sbuff.append(" \n");
				}
				stmt.setString(index, sbuff.toString());

			}else if(javaObject instanceof BigDecimal){
				stmt.setBigDecimal(index, ((BigDecimal)javaObject));
			}else if(javaObject == null){
					stmt.setObject(index, null);
			}else{
				Logger.warn("TypesUtil", "Not Supported Java Type. %s", javaObject.getClass().getName());
				stmt.setString(index, javaObject.toString());
			}
		}catch(SQLException sqle){
			Logger.error("SQLException", "index %s javaObject %s",  index, javaObject);
			throw sqle;
		}
	}
	
	static String getSqlTypeName(int type) {
	    switch (type) {
	    case Types.BIT:
	        return "BIT";
	    case Types.TINYINT:
	        return "TINYINT";
	    case Types.SMALLINT:
	        return "SMALLINT";
	    case Types.INTEGER:
	        return "INTEGER";
	    case Types.BIGINT:
	        return "BIGINT";
	    case Types.FLOAT:
	        return "FLOAT";
	    case Types.REAL:
	        return "REAL";
	    case Types.DOUBLE:
	        return "DOUBLE";
	    case Types.NUMERIC:
	        return "NUMERIC";
	    case Types.DECIMAL:
	        return "DECIMAL";
	    case Types.CHAR:
	        return "CHAR";
	    case Types.VARCHAR:
	        return "VARCHAR";
	    case Types.LONGVARCHAR:
	        return "LONGVARCHAR";
	    case Types.DATE:
	        return "DATE";
	    case Types.TIME:
	        return "TIME";
	    case Types.TIMESTAMP:
	        return "TIMESTAMP";
	    case Types.BINARY:
	        return "BINARY";
	    case Types.VARBINARY:
	        return "VARBINARY";
	    case Types.LONGVARBINARY:
	        return "LONGVARBINARY";
	    case Types.NULL:
	        return "NULL";
	    case Types.OTHER:
	        return "OTHER";
	    case Types.JAVA_OBJECT:
	        return "JAVA_OBJECT";
	    case Types.DISTINCT:
	        return "DISTINCT";
	    case Types.STRUCT:
	        return "STRUCT";
	    case Types.ARRAY:
	        return "ARRAY";
	    case Types.BLOB:
	        return "BLOB";
	    case Types.CLOB:
	        return "CLOB";
	    case Types.REF:
	        return "REF";
	    case Types.DATALINK:
	        return "DATALINK";
	    case Types.BOOLEAN:
	        return "BOOLEAN";
	    case Types.ROWID:
	        return "ROWID";
	    case Types.NCHAR:
	        return "NCHAR";
	    case Types.NVARCHAR:
	        return "NVARCHAR";
	    case Types.LONGNVARCHAR:
	        return "LONGNVARCHAR";
	    case Types.NCLOB:
	        return "NCLOB";
	    case Types.SQLXML:
	        return "SQLXML";
	    }
	    return "?";
	}
	
	
}
