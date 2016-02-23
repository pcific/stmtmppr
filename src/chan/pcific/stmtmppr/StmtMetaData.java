package chan.pcific.stmtmppr;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import chan.pcific.stmtmppr.xbean.XStatements;

public class StmtMetaData {
	private String id;
	private String sql;
	private String sqlPrepared;
	private ParamMetaData paramMetaData;
	private RsetMetaData rsetMetaData;
	
	public StmtMetaData(String id, String sql){
		super();
		this.id = id;
		this.sql = sql.trim();
	}
	public String getId() {
		return id;
	}
	public String getSql() {
		return sql;
	}
	public String getSqlPrepared() {
		if(sqlPrepared==null){
			this.prepareMetaData();
		}
		return sqlPrepared;
	}
	
	public ParamMetaData getParamMetaData() {
		if(paramMetaData==null){
			this.prepareMetaData();
		}
		return paramMetaData;
	}
	
	public RsetMetaData getRsetMetaData(ResultSet rset) {
		if(rsetMetaData==null){
			this.doRsetMetaData(rset);
		}
		return rsetMetaData;
	}
	
	public static LinkedHashMap<String, StmtMetaData> buildFromXStatements(XStatements xstmts){
		LinkedHashMap<String, StmtMetaData> map = new LinkedHashMap<String, StmtMetaData>(); 
		for(XStatements.XStatement xstmt:xstmts.getList()){
			String id = xstmt.getId();
			String sql = xstmt.getValue();
			map.put(id, new StmtMetaData(id, sql));
		}
		return map;
	}

	synchronized private void prepareMetaData(){
		if(this.sqlPrepared==null || this.paramMetaData==null){
			this.paramMetaData = ParamMetaData.buildFromSql(this.sql);
			this.sqlPrepared = this.sql.replaceAll(ParamMetaData.REGEX, "?");
		}
	}
	
	synchronized private void doRsetMetaData(ResultSet rset){
		if(this.rsetMetaData==null){
			this.rsetMetaData = RsetMetaData.buildFromResultSet(rset); 
		}
	}
	
	public int doSelectResultCount(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doSelectResultCount(conn, this, params);
	}
	
	@Deprecated
	public String doSelectResultString(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return doSelectResultText(conn, params);
	}
	
	public String doSelectResultText(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doSelectResultText(conn, this, params);
	}
	
	public LinkedHashMap<String, Object> doSelect(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doSelect(conn, this, params);
	}
	
	public List<LinkedHashMap<String, Object>> doSelects(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doSelects(conn, this, params);
	}
	
	public LinkedHashMap<String, LinkedHashMap<String, Object>> doSelectsMap(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doSelectsMap(conn, this, params);
	}
	
	public List<List<Object>> doSelectsList(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doSelectsList(conn, this, params);
	}
	
	public LinkedHashMap<String, String> doSelectsProps(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doSelectsProps(conn, this, params);
	}
	
	public int doUpdate(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doUpdate(conn, this, params);
	};
	
	public int doInsert(Connection conn, LinkedHashMap<String, Object> params) throws SQLException {
		return MpprUtil.doInsert(conn, this, params);
	};
	
}

class ParamMetaData {
	
	final static String REGEX = "\\$\\{[a-zA-Z0-9_]+\\}";

	private int size;
	private String[] params;
	ParamMetaData(List<String> params){
		super();
		this.size = params.size();
		this.params = params.toArray(new String[]{});
	}
	int size(){return this.size;}
	String getParamName(int i /* 1,2,... n */){
		return this.params[i-1];
	}
	
	static ParamMetaData buildFromSql(String sql){
		List<String> paramNames = new ArrayList<String>();
		Pattern p = Pattern.compile(REGEX);
		Matcher m = p.matcher(sql);
		while (m.find()) {
			String b = m.group();
			int start = m.start();
			int end = m.end();
			String b2 = b.substring(2, b.length()-1);
			paramNames.add(b2);
		}
		return new ParamMetaData(paramNames);
	}
}

class RsetMetaData {
	private int size;
	private ColMetaData[] colMetaDatas ;
	RsetMetaData(List<ColMetaData> colMetaDatas){
		super();
		this.size = colMetaDatas.size();
		this.colMetaDatas = colMetaDatas.toArray(new ColMetaData[]{});
	}
	int size(){return this.size;}
	ColMetaData getColMetaData(int i /* 1,2,... n */){
		return this.colMetaDatas[i-1];
	}
	
	static RsetMetaData buildFromResultSet(ResultSet rset){
		try {
			List<ColMetaData> colMetaDatas = new ArrayList<ColMetaData>();
			ResultSetMetaData rsetMeta = rset.getMetaData();
			int colCount = rsetMeta.getColumnCount();
			for(int i=0;i<colCount;i++){
				String columnName = rsetMeta.getColumnName(i+1);
				int columnType = rsetMeta.getColumnType(i+1);
				if(columnType == java.sql.Types.NULL){
					Logger.warn("RsetMetaData", "Not Supported SqlType %s. Skip building RsetMetaData ", SqlTypesUtil.getSqlTypeName(columnType));
					return null;
				}else if(SqlTypesUtil.isSupportedSqlType(columnType)){
					ColMetaData colMetaData = new ColMetaData(columnName, columnType);
					colMetaDatas.add(colMetaData);
				}else if(SqlTypesUtil.isNotSupportedSqlType(columnType)){
					Logger.error("RsetMetaData", "Not Supported SqlType %s.", SqlTypesUtil.getSqlTypeName(columnType));
					return null;
				}else{
					Logger.warn("RsetMetaData", "Not Supported SqlType %s. Assume String", SqlTypesUtil.getSqlTypeName(columnType));
					ColMetaData colMetaData = new ColMetaData(columnName, columnType);
					colMetaDatas.add(colMetaData);
				}
			}
			return new RsetMetaData(colMetaDatas);
		}catch(SQLException sqle) {
			Logger.error("RsetMetaData", "ResultSetMetaData Not Supported.", "");
			sqle.printStackTrace();
			return null;
		}
	}
}

class ColMetaData {

	private String columnName;
	private int columnType;
	
	public ColMetaData(String columnName, int columnType){
		super();
		this.columnName = columnName;
		this.columnType = columnType;
	}
	public String getColumnName() {
		return columnName;
	}
	public int getColumnType() {
		return columnType;
	}
	
}
