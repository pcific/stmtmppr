package chan.pcific.stmtmppr;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

class MpprUtil {
	
	final static String NULL_PARAM_VALUE = "0";
	final static boolean STMT_MPPR_DEBUG     = Boolean.parseBoolean(System.getProperty("cohmon.stmtmppr.debug", "false"));
	final static boolean MISSING_PARAM_DEBUG = Boolean.parseBoolean(System.getProperty("cohmon.stmtmppr.missing-param.debug", "false"));
	
	private static void logErrorSQLException(StmtMetaData stmtMeta, LinkedHashMap<String, Object> params){
		Logger.error("SQLException", "\n--------\n%s\n--------\n%s\n--------\nparams %s \n",  stmtMeta.getSql(), stmtMeta.getSqlPrepared(),  params);
	}
	
	private static void logDebugStmt(StmtMetaData stmtMeta, LinkedHashMap<String, Object> params){
		if(STMT_MPPR_DEBUG){
			System.out.println(String.format(" [D] stmtId %s\n--------\n%s\n--------\n%s\nparams %s \n", stmtMeta.getId(), stmtMeta.getSql(), stmtMeta.getSqlPrepared(), params ));
		}
	}
	private static void logDebugMissingParam(String paramName, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params){
		if(!(MISSING_PARAM_DEBUG)){
			Logger.warn("StmtMapper", "params are expected but missing %s", paramName);
		}else{
			Logger.warn("StmtMapper", "params are expected but missing %s stmtId %s\n--------\n%s\n--------\n%s\nparams %s \n", paramName, stmtMeta.getId(), stmtMeta.getSql(), stmtMeta.getSqlPrepared(), params );
		}
	}
	
	static int doSelectResultCount(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		int result = 0;
		
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try{
			ParamMetaData paramMeta = stmtMeta.getParamMetaData();
			stmt = conn.prepareStatement(stmtMeta.getSqlPrepared());
			for(int i=0;i<paramMeta.size();i++){
				if(params==null)
					params = new LinkedHashMap<String, Object>();
				String paramName = paramMeta.getParamName(i+1);
				Object paramValue = params.get(paramName);
				if(paramValue==null){
					logDebugMissingParam(paramName, stmtMeta, params);
					paramValue = NULL_PARAM_VALUE;
				}
				SqlTypesUtil.setPreparedStatementParamJavaObject(stmt, (i+1), paramValue);
			}
			rset = stmt.executeQuery();
			while(rset.next())
				result++;
			return result;
		}catch(SQLException sqle){
//			sqle.printStackTrace();
			logErrorSQLException(stmtMeta, params);
			throw sqle;
		}finally{
			try{if(rset!=null){rset.close();rset=null;}}catch(Exception ig){}
			try{if(stmt!=null){stmt.close();stmt=null;}}catch(Exception ig){}
			logDebugStmt(stmtMeta, params);
		}
	}
	
	@Deprecated
	static String doSelectResultString(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		return doSelectResultText(conn, stmtMeta, params);
	}
	
	static String doSelectResultText(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		String result = null;
		
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try{
			ParamMetaData paramMeta = stmtMeta.getParamMetaData();
			stmt = conn.prepareStatement(stmtMeta.getSqlPrepared());
			
			for(int i=0;i<paramMeta.size();i++){
				if(params==null)
					params = new LinkedHashMap<String, Object>();
				String paramName = paramMeta.getParamName(i+1);
				Object paramValue = params.get(paramName);
				if(paramValue==null){
					logDebugMissingParam(paramName, stmtMeta, params);
					paramValue = NULL_PARAM_VALUE;
				}
				SqlTypesUtil.setPreparedStatementParamJavaObject(stmt, (i+1), paramValue);
			}
			rset = stmt.executeQuery();
			if(rset.next()){
				result = rset.getString(1);
			}
			if(result==null)
				return null;
			return new String(result).trim();
		}catch(SQLException sqle){
//			sqle.printStackTrace();
			logErrorSQLException(stmtMeta, params);
			throw sqle;
		}finally{
			try{if(rset!=null){rset.close();rset=null;}}catch(Exception ig){}
			try{if(stmt!=null){stmt.close();stmt=null;}}catch(Exception ig){}
			logDebugStmt(stmtMeta, params);
		}
		
	}
	
	static LinkedHashMap<String, Object> doSelect(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();
		
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try{
			ParamMetaData paramMeta = stmtMeta.getParamMetaData();
			stmt = conn.prepareStatement(stmtMeta.getSqlPrepared());
			for(int i=0;i<paramMeta.size();i++){
				if(params==null)
					params = new LinkedHashMap<String, Object>();
				String paramName = paramMeta.getParamName(i+1);
				Object paramValue = params.get(paramName);
				if(paramValue==null){
					logDebugMissingParam(paramName, stmtMeta, params);
					paramValue = NULL_PARAM_VALUE;
				}
				SqlTypesUtil.setPreparedStatementParamJavaObject(stmt, (i+1), paramValue);
			}
			rset = stmt.executeQuery();
			RsetMetaData rsetMeta = stmtMeta.getRsetMetaData(rset);
			if(rset.next()){
				for(int i=0;i<rsetMeta.size();i++){
					ColMetaData colMeta = rsetMeta.getColMetaData(i+1);
					String columnName = colMeta.getColumnName();
					int columnType = colMeta.getColumnType();
					Object columnValue = SqlTypesUtil.getResultSetParamJavaObject(rset, (i+1), columnType);
					result.put(columnName, columnValue);
				}
			}
			return result;
		}catch(SQLException sqle){
//			sqle.printStackTrace();
			logErrorSQLException(stmtMeta, params);
			throw sqle;
		}finally{
			try{if(rset!=null){rset.close();rset=null;}}catch(Exception ig){}
			try{if(stmt!=null){stmt.close();stmt=null;}}catch(Exception ig){}
			logDebugStmt(stmtMeta, params);
		}
	}
	
	static List<LinkedHashMap<String, Object>> doSelects(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		List<LinkedHashMap<String, Object>> results = new ArrayList<LinkedHashMap<String,Object>>();
		
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try{
			ParamMetaData paramMeta = stmtMeta.getParamMetaData();
			stmt = conn.prepareStatement(stmtMeta.getSqlPrepared());
			for(int i=0;i<paramMeta.size();i++){
				if(params==null)
					params = new LinkedHashMap<String, Object>();
				String paramName = paramMeta.getParamName(i+1);
				Object paramValue = params.get(paramName);
				if(paramValue==null){
					logDebugMissingParam(paramName, stmtMeta, params);
					paramValue = NULL_PARAM_VALUE;
				}
				SqlTypesUtil.setPreparedStatementParamJavaObject(stmt, (i+1), paramValue);
			}
			rset = stmt.executeQuery();
			RsetMetaData rsetMeta = stmtMeta.getRsetMetaData(rset);
			while(rset.next()){
				LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();
				for(int i=0;i<rsetMeta.size();i++){
					ColMetaData colMeta = rsetMeta.getColMetaData(i+1);
					String columnName = colMeta.getColumnName();
					int columnType = colMeta.getColumnType();
					Object columnValue = SqlTypesUtil.getResultSetParamJavaObject(rset, (i+1), columnType);
					result.put(columnName, columnValue);
				}
				results.add(result);
			}
			return results;
		}catch(SQLException sqle){
//			sqle.printStackTrace();
			logErrorSQLException(stmtMeta, params);
			throw sqle;
		}finally{
			try{if(rset!=null){rset.close();rset=null;}}catch(Exception ig){}
			try{if(stmt!=null){stmt.close();stmt=null;}}catch(Exception ig){}
			logDebugStmt(stmtMeta, params);
		}
	}
	
	static LinkedHashMap<String, LinkedHashMap<String, Object>> doSelectsMap(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		LinkedHashMap<String, LinkedHashMap<String, Object>> results = new LinkedHashMap<String, LinkedHashMap<String,Object>>();
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try{
			ParamMetaData paramMeta = stmtMeta.getParamMetaData();
			stmt = conn.prepareStatement(stmtMeta.getSqlPrepared());
			for(int i=0;i<paramMeta.size();i++){
				if(params==null)
					params = new LinkedHashMap<String, Object>();
				String paramName = paramMeta.getParamName(i+1);
				Object paramValue = params.get(paramName);
				if(paramValue==null){
					logDebugMissingParam(paramName, stmtMeta, params);
					paramValue = NULL_PARAM_VALUE;
				}
				SqlTypesUtil.setPreparedStatementParamJavaObject(stmt, (i+1), paramValue);
			}
			rset = stmt.executeQuery();
			RsetMetaData rsetMeta = stmtMeta.getRsetMetaData(rset);
			while(rset.next()){
				String key = null;
				LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();
				key = rset.getObject(1).toString();
				for(int i=0;i<rsetMeta.size();i++){
					ColMetaData colMeta = rsetMeta.getColMetaData(i+1);
					String columnName = colMeta.getColumnName();
					int columnType = colMeta.getColumnType();
					Object columnValue = SqlTypesUtil.getResultSetParamJavaObject(rset, (i+1), columnType);
					result.put(columnName, columnValue);
				}
				results.put(key, result);
			}
			return results;
		}catch(SQLException sqle){
//			sqle.printStackTrace();
			logErrorSQLException(stmtMeta, params);
			throw sqle;
		}finally{
			try{if(rset!=null){rset.close();rset=null;}}catch(Exception ig){}
			try{if(stmt!=null){stmt.close();stmt=null;}}catch(Exception ig){}
			logDebugStmt(stmtMeta, params);
		}
	}
	
	static List<List<Object>> doSelectsList(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		List<List<Object>> results = new ArrayList<List<Object>>();
		
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try{
			ParamMetaData paramMeta = stmtMeta.getParamMetaData();
			stmt = conn.prepareStatement(stmtMeta.getSqlPrepared());
			for(int i=0;i<paramMeta.size();i++){
				if(params==null)
					params = new LinkedHashMap<String, Object>();
				String paramName = paramMeta.getParamName(i+1);
				Object paramValue = params.get(paramName);
				if(paramValue==null){
					logDebugMissingParam(paramName, stmtMeta, params);
					paramValue = NULL_PARAM_VALUE;
				}
				SqlTypesUtil.setPreparedStatementParamJavaObject(stmt, (i+1), paramValue);
			}
			rset = stmt.executeQuery();
			RsetMetaData rsetMeta = stmtMeta.getRsetMetaData(rset);
			while(rset.next()){
				List<Object> result = new ArrayList<Object>();
				for(int i=0;i<rsetMeta.size();i++){
					ColMetaData colMeta = rsetMeta.getColMetaData(i+1);
					int columnType = colMeta.getColumnType();
					Object columnValue = SqlTypesUtil.getResultSetParamJavaObject(rset, (i+1), columnType);
					result.add(columnValue);
				}
				results.add(result);
			}
			return results;
		}catch(SQLException sqle){
//			sqle.printStackTrace();
			logErrorSQLException(stmtMeta, params);
			throw sqle;
		}finally{
			try{if(rset!=null){rset.close();rset=null;}}catch(Exception ig){}
			try{if(stmt!=null){stmt.close();stmt=null;}}catch(Exception ig){}
			logDebugStmt(stmtMeta, params);
		}
	}
	
	static LinkedHashMap<String, String> doSelectsProps(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		LinkedHashMap<String, String> results = new LinkedHashMap<String,String>();
		
		PreparedStatement stmt = null;
		ResultSet rset = null;
		try{
			ParamMetaData paramMeta = stmtMeta.getParamMetaData();
			stmt = conn.prepareStatement(stmtMeta.getSqlPrepared());
			for(int i=0;i<paramMeta.size();i++){
				if(params==null)
					params = new LinkedHashMap<String, Object>();
				String paramName = paramMeta.getParamName(i+1);
				Object paramValue = params.get(paramName);
				if(paramValue==null){
					logDebugMissingParam(paramName, stmtMeta, params);
					paramValue = NULL_PARAM_VALUE;
				}
				SqlTypesUtil.setPreparedStatementParamJavaObject(stmt, (i+1), paramValue);
			}
			rset = stmt.executeQuery();
			while(rset.next()){
				String paramName  = rset.getString(1);
				String paramValue = rset.getString(2);
				results.put(paramName, paramValue);
			}
			return results;
		}catch(SQLException sqle){
//			sqle.printStackTrace();
			logErrorSQLException(stmtMeta, params);
			throw sqle;
		}finally{
			try{if(rset!=null){rset.close();rset=null;}}catch(Exception ig){}
			try{if(stmt!=null){stmt.close();stmt=null;}}catch(Exception ig){}
			logDebugStmt(stmtMeta, params);
		}
	}
	
	static int doUpdate(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		int result = 0;
		PreparedStatement stmt = null;
		try{
			ParamMetaData paramMeta = stmtMeta.getParamMetaData();
			stmt = conn.prepareStatement(stmtMeta.getSqlPrepared());
			for(int i=0;i<paramMeta.size();i++){
				if(params==null)
					params = new LinkedHashMap<String, Object>();
				String paramName = paramMeta.getParamName(i+1);
				Object paramValue = params.get(paramName);
				if(paramValue==null){
					logDebugMissingParam(paramName, stmtMeta, params);
					paramValue = NULL_PARAM_VALUE;
				}
				SqlTypesUtil.setPreparedStatementParamJavaObject(stmt, (i+1), paramValue);
			}
			result = stmt.executeUpdate();
			return result;
		}catch(SQLException sqle){
//			sqle.printStackTrace();
			logErrorSQLException(stmtMeta, params);
			throw sqle;
		}finally{
			try{if(stmt!=null){stmt.close();stmt=null;}}catch(Exception ig){}
			logDebugStmt(stmtMeta, params);
		}
	};
	
	static int doInsert(Connection conn, StmtMetaData stmtMeta, LinkedHashMap<String, Object> params) throws SQLException {
		return doUpdate(conn, stmtMeta, params);
	};
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
