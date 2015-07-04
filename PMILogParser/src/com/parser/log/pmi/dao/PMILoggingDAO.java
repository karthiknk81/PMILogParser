package com.parser.log.pmi.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class PMILoggingDAO {

	private final String USER="karthik";
	private final String PWD="passw0rd";
	private final String DRIVER="com.mysql.jdbc.Driver";
	private final String DB="perfmon_db";
	private final String CONN_STR="jdbc:mysql://localhost/"+DB+"?user="+USER+"&password="+PWD;
	private final String CS_TABLE_INSERT = "insert into CS_TABLE (TIMESTAMP,RESOURCE,TYPE,STAT_ID,ST,LST,COUNT) VALUES (?, ?,?,?,?,?,?);"; 
		

	private Connection connection = null;
	private PreparedStatement pStmt_CS = null;
	
	private boolean connect() {
		boolean retrun_status = false;
		try {
			
			Class.forName(DRIVER);
			connection = DriverManager.getConnection(CONN_STR);
			pStmt_CS = connection.prepareStatement(CS_TABLE_INSERT);
			retrun_status = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retrun_status;
	}
	
	
	private void close(){
		
		if(pStmt_CS!=null){try {pStmt_CS.close();} catch (Throwable e2) {}}
		if(connection!=null){try {connection.close();} catch (Throwable e2) {}}
		
	}
	
	
//	public boolean insertCSRecord (Timestamp tmStmp,String rsName,String statType, int statId, long sT, long lST, int ct){
	public boolean insertCSRecord (){
		boolean status = false;
		try {
			
			pStmt_CS.setString(1,"2014-11-07 12:03:23");
			pStmt_CS.setString(2, "test-rs");
			pStmt_CS.setString(3, "tes");
			pStmt_CS.setString(4, "2");
			pStmt_CS.setString(5, "123456789");
			pStmt_CS.setString(6, "123456789");
			pStmt_CS.setString(7, "6");
			
//			pStmt_CS.setTimestamp(1,tmStmp);
//			pStmt_CS.setString(2, rsName);
//			pStmt_CS.setString(3, statType);
//			pStmt_CS.setInt(4, statId);
//			pStmt_CS.setLong(5, sT);
//			pStmt_CS.setLong(6, lST);
//			pStmt_CS.setInt(7, ct);
			
			pStmt_CS.executeUpdate();
			
//			connection.commit();
			System.out.println("inserted.....");
		}catch(Exception e) {
			e.printStackTrace();
			status=false;
		}
		return status;
	}

	public static void main(String [] args)  throws Exception {
		
		PMILoggingDAO dao = new PMILoggingDAO();
		if (dao.connect()) {
			System.out.println("Connection established successfully ... ");
			dao.insertCSRecord();
			dao.close();
		}else {
			System.out.println("Fail to estabilish connection...");			
		}

		System.out.println("Terminated... ");
	}
}
