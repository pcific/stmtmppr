package chan.pcific.stmtmppr;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import chan.pcific.stmtmppr.xbean.XStatements;

public class StmtMapper {
	
	String resourcePath;
	private LinkedHashMap<String, StmtMetaData> stmts = new LinkedHashMap<String, StmtMetaData>(); 

	public StmtMapper(String resourcePath){
		super();
		this.resourcePath = resourcePath;
		if(this.stmts==null||this.stmts.size()==0){
			this.stmts = StmtMetaData.buildFromXStatements(XStatements.loadUp(resourcePath));
		}
	}
	
	public StmtMapper(StmtMetaData... stmts){
		super();
		this.stmts = new LinkedHashMap<String, StmtMetaData>();
		for(StmtMetaData stmt:stmts){
			String key = stmt.getId();
			this.stmts.put(key, stmt);
		}
	}
	
	public StmtMapper(List<StmtMetaData> stmts){
		super();
		this.stmts = new LinkedHashMap<String, StmtMetaData>();
		for(StmtMetaData stmt:stmts){
			String key = stmt.getId();
			this.stmts.put(key, stmt);
		}
	}
	
	public List<String> getIds(){
		return new ArrayList<String>(this.stmts.keySet());
	}
	
	public void addStmt(String id, StmtMetaData stmtMeta){
		this.stmts.put(id, stmtMeta);
	}
	
	public StmtMetaData getStmt(String id){
		return this.stmts.get(id);
	}

	public StmtMetaData getStmt(int i){
		return this.stmts.get(this.getIds().get(i));
	}
	
	public int doSelectResultCount(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doSelectResultCount(conn, stmtMeta, params);
	}
	
	@Deprecated
	public String doSelectResultString(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		return doSelectResultText(conn, stmtId, params);
	}
	
	public String doSelectResultText(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doSelectResultText(conn, stmtMeta, params);
	}
	
	public LinkedHashMap<String, Object> doSelect(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doSelect(conn, stmtMeta, params);
	}
	
	public List<LinkedHashMap<String, Object>> doSelects(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doSelects(conn, stmtMeta, params);
	}
	
	public LinkedHashMap<String, LinkedHashMap<String, Object>> doSelectsMap(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doSelectsMap(conn, stmtMeta, params);
	}
	
	public List<List<Object>> doSelectsList(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doSelectsList(conn, stmtMeta, params);
	}
	
	public LinkedHashMap<String, String> doSelectsProps(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doSelectsProps(conn, stmtMeta, params);
	}
	
	public int doUpdate(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doUpdate(conn, stmtMeta, params);
	};
	
	public int doInsert(Connection conn, String stmtId, LinkedHashMap<String, Object> params) throws SQLException {
		StmtMetaData stmtMeta = this.getStmt(stmtId);
		if(stmtMeta==null)
			throw new SQLException(String.format("not found StmtMeta %s", stmtId));
		return MpprUtil.doInsert(conn, stmtMeta, params);
	};
	
	
}
