package com.parser.log.pmi.handler;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.parser.log.pmi.util.ToolSet;

public class JDBCConnectionMetricsExtractor extends DefaultHandler{

	//Constants declarations
	
	private final String USER="karthik";
	private final String PWD="passw0rd";
	private final String DRIVER="com.mysql.jdbc.Driver";
	private final String DB="perfmon_db";
	private final String CONN_STR="jdbc:mysql://localhost/"+DB+"?user="+USER+"&password="+PWD;
	private final String CS_TABLE_INSERT = "insert into CS_TABLE (TIMESTAMP,RESOURCE,TYPE,STAT_ID,ST,LST,COUNT) VALUES (?, ?,?,?,?,?,?);";
	private final String TS_TABLE_INSERT ="insert into TS_TABLE(TIMESTAMP,RESOURCE,TYPE,STAT_ID,sT,lST,ct,max,min,sOS,tot) values (?,?,?,?,?,?,?,?,?,?,?);";
	private final String RS_TABLE_INSERT ="insert into  RS_TABLE(TIMESTAMP,RESOURCE,TYPE,STAT_ID,sT,lST,lWM,hWM,current,int_val) values (?,?,?,?,?,?,?,?,?,?);";
	private final String BRS_TABLE_INSERT ="insert into BRS_TABLE (TIMESTAMP,RESOURCE,TYPE,STAT_ID,lWM,hWM,current,int_val,sT,lST,lB,uB) values (?,?,?,?,?,?,?,?,?,?,?,?);";
	
	
	private static final String ELEM_SNAPSHOT= "snapshot";
	private static final String ATT_TIME= "time";
	
	private static final String ELEM_STATS= "stats";
	private static final String ATT_NAME= "name";
	private static final String ATT_STATTYPE= "statType";
	private static final String ATT_TYPE= "type";
	
	private static final String VAL_STATTYPE="connectionPoolModule";
	private static final String VAL_TYPE ="COLLECTION";
	private static ArrayList<String> DATASOURCE_NAMES;
	
	//Variable Declaration
	private boolean isSnapshotElementStarted = false;
	private String snapshot_time;
	private boolean isJDBCConnectionElementStarted = false;
	private PrintStream out = null;
	private String statNameVal= null;
	private Connection connection = null;
	private PreparedStatement pStmt_CS = null;
	private PreparedStatement pStmt_TS = null;
	private PreparedStatement pStmt_RS = null;
	private PreparedStatement pStmt_BRS = null;
	
	//Constructor
	public JDBCConnectionMetricsExtractor(){
		out = System.out;
	}

	public JDBCConnectionMetricsExtractor (OutputStream argOut) throws Exception {
		out = new PrintStream(argOut);
		DATASOURCE_NAMES = new ArrayList<String>();
		DATASOURCE_NAMES.add("JDBC/PERFORMANCEDB");
	}
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attr) throws SAXException {
		
		if((!isSnapshotElementStarted) && (qName.equalsIgnoreCase(ELEM_SNAPSHOT))){
			snapshot_time = ToolSet.convertUnixToCST_V1(Long.parseLong(attr.getValue(ATT_TIME).substring(0,10)));
			isSnapshotElementStarted = true;
		}
		
		if(isSnapshotElementStarted && isJDBCConnectionElementStarted) {
			
			switch (qName) {
			case "CS":
				//				insert into CS_TABLE (TIMESTAMP,RESOURCE,TYPE,STAT_ID,ST,LST,COUNT) VALUES (?, ?,?,?,?,?,?);
				try {

					//Set parameter
					pStmt_CS.setString(1,snapshot_time);
					pStmt_CS.setString(2, statNameVal);
					pStmt_CS.setString(3, qName);
					pStmt_CS.setString(4, attr.getValue("id"));
					pStmt_CS.setString(5, attr.getValue("sT"));
					pStmt_CS.setString(6, attr.getValue("lST"));
					pStmt_CS.setString(7, attr.getValue("ct"));

					//Execute insert query
					pStmt_CS.executeUpdate();

				}catch (SQLException e){
					throw new SAXException (e);
				}
				break;
			case "TS":
				//				insert into TS_TABLE(TIMESTAMP,RESOURCE,TYPE,STAT_ID,sT,lST,ct,max,min,sOS,tot) values (?,?,?,?,?,?,?,?,?,?,?);
				try {

					//Set parameter
					pStmt_TS.setString(1,snapshot_time);
					pStmt_TS.setString(2, statNameVal);
					pStmt_TS.setString(3, qName);
					pStmt_TS.setString(4, attr.getValue("id"));
					pStmt_TS.setString(5, attr.getValue("sT"));
					pStmt_TS.setString(6, attr.getValue("lST"));
					pStmt_TS.setString(7, attr.getValue("ct"));

					pStmt_TS.setString(8, attr.getValue("max"));
					pStmt_TS.setString(9, attr.getValue("min"));
					pStmt_TS.setString(10, attr.getValue("sOS"));
					pStmt_TS.setString(11, attr.getValue("tot"));

					//Execute insert query
					pStmt_TS.executeUpdate();

				}catch (SQLException e){
					throw new SAXException (e);
				}
				break;

			case "RS":
				//				insert into  RS_TABLE(TIMESTAMP,RESOURCE,TYPE,STAT_ID,sT,lST,lWM,hWM,current,int_val) values (?,?,?,?,?,?,?,?,?,?);
				try {

					//Set parameter
					pStmt_RS.setString(1,snapshot_time);
					pStmt_RS.setString(2, statNameVal);
					pStmt_RS.setString(3, qName);
					pStmt_RS.setString(4, attr.getValue("id"));
					pStmt_RS.setString(5, attr.getValue("sT"));
					pStmt_RS.setString(6, attr.getValue("lST"));
					pStmt_RS.setString(7, attr.getValue("lWM"));

					pStmt_RS.setString(8, attr.getValue("hWM"));
					pStmt_RS.setString(9, attr.getValue("cur"));
					pStmt_RS.setString(10, attr.getValue("int"));


					//Execute insert query
					pStmt_RS.executeUpdate();

				}catch (SQLException e){
					throw new SAXException (e);
				}
				break;
			case "BRS":
				//				insert into BRS_TABLE (TIMESTAMP,RESOURCE,TYPE,STAT_ID,lWM,hWM,current,int_val,sT,lST,lB,uB) values (?,?,?,?,?,?,?,?,?,?,?,?);
				//				2015-06-22 04:58:38,jdbc/PerformanceDB,BRS,id=5,lWM=0,hWM=0,cur=0,int=0.0,sT=1434487385979,lST=1434967118701,lB=0,uB=200
				try {

					//Set parameter
					pStmt_BRS.setString(1,snapshot_time);
					pStmt_BRS.setString(2, statNameVal);
					pStmt_BRS.setString(3, qName);
					pStmt_BRS.setString(4, attr.getValue("id"));
					pStmt_BRS.setString(5, attr.getValue("lWM"));
					pStmt_BRS.setString(6, attr.getValue("hWM"));
					pStmt_BRS.setString(7, attr.getValue("cur"));
					pStmt_BRS.setString(8, attr.getValue("int"));
					pStmt_BRS.setString(9, attr.getValue("sT"));
					pStmt_BRS.setString(10, attr.getValue("lST"));
					pStmt_BRS.setString(11, attr.getValue("lB"));
					pStmt_BRS.setString(12, attr.getValue("uB"));

					//Execute insert query
					pStmt_BRS.executeUpdate();

				}catch (SQLException e){
					throw new SAXException (e);
				}
				break;


			}

			
			
			out.print(snapshot_time+","+ statNameVal+","+qName);
			short n =  (short) attr.getLength();
			for(short i =0; i<n; i++) {
				
				out.print(","+attr.getQName(i)+"="+attr.getValue(i));
			}
			out.print("\n");
		
		}else if(qName.equalsIgnoreCase(ELEM_STATS) && isJDBCConnectionElement(attr )){
			isJDBCConnectionElementStarted = true;
		}
		
		
	}
	
	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {

		if(qName.equalsIgnoreCase("snapshot")){
			isSnapshotElementStarted = false;
		}
		
		if(isJDBCConnectionElementStarted & qName.equalsIgnoreCase(ELEM_STATS)) {
			isJDBCConnectionElementStarted = false;
		}
		
	}
	
	
	public boolean isJDBCConnectionElement(Attributes argAttr) throws SAXException{
		boolean result = false;

		
		if(argAttr != null){
			if(	DATASOURCE_NAMES.contains(argAttr.getValue(ATT_NAME).toUpperCase()) &&
				argAttr.getValue(ATT_STATTYPE).equalsIgnoreCase(VAL_STATTYPE) &&
				argAttr.getValue(ATT_TYPE).equalsIgnoreCase(VAL_TYPE) ) {
				statNameVal = argAttr.getValue(ATT_NAME);
				result = true;
			}
		}else {
			throw new SAXException("NULL argument passed to the method.");
		}
		return result;
		
		}
	
	
	@Override
	public void startDocument() throws SAXException {
		this.connect();
	}
	
	@Override
	public void endDocument() throws SAXException {
		this.close();
	}
	
	
	private boolean connect() {
		boolean retrun_status = false;
		try {
			
			Class.forName(DRIVER);
			connection = DriverManager.getConnection(CONN_STR);
			pStmt_CS = connection.prepareStatement(CS_TABLE_INSERT);
			pStmt_TS = connection.prepareStatement(TS_TABLE_INSERT);
			pStmt_RS = connection.prepareStatement(RS_TABLE_INSERT);
			pStmt_BRS = connection.prepareStatement(BRS_TABLE_INSERT);
//			tuncateTables();
			retrun_status = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retrun_status;
	}
	
	private void close(){
		if(pStmt_CS!=null){try {pStmt_CS.close();} catch (Throwable e2) {}}
		if(pStmt_TS!=null){try {pStmt_TS.close();} catch (Throwable e2) {}}
		if(pStmt_RS!=null){try {pStmt_RS.close();} catch (Throwable e2) {}}
		if(pStmt_BRS!=null){try {pStmt_BRS.close();} catch (Throwable e2) {}}
		if(connection!=null){try {connection.close();} catch (Throwable e2) {}}
		
	}
	
	private void tuncateTables() throws SQLException{

		Statement stmt = null;
		try {
			stmt = connection.createStatement();
			stmt.execute("truncate table ts_table");
			stmt.execute("truncate table cs_table");
			stmt.execute("truncate table rs_table");
			stmt.execute("truncate table brs_table");
			stmt.execute("commit");
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
			
		}finally {
			if(stmt!=null){try {stmt.close();} catch (Throwable e2) {}}
		}
		
		
	}
	
}

