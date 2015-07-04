package com.parser.log.pmi.handler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import com.parser.log.pmi.TPVLogParser;
import com.parser.log.pmi.util.ToolSet;

import java.util.Properties;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.OutputStream;
/**
 *This handler class transform input TPV log file into CSV file which contains all the WebContainerStats
 */
public class WebContainerMetricsExtractor extends DefaultHandler {

	//Constants declarations
	private final String DRIVER=TPVLogParser.CONFIG.getProperty("odb.driver");
	private final String CONN_STR=TPVLogParser.CONFIG.getProperty("odb.conn.str");
	private final String CS_TABLE_INSERT = TPVLogParser.CONFIG.getProperty("cs.insert");
	private final String TS_TABLE_INSERT = TPVLogParser.CONFIG.getProperty("ts.insert");
	private final String RS_TABLE_INSERT = TPVLogParser.CONFIG.getProperty("rs.insert");
	private final String BRS_TABLE_INSERT = TPVLogParser.CONFIG.getProperty("brs.insert");
	
	
//	private static final String ELEM_SNAPSHOT= "snapshot";
//	private static final String ATT_TIME= "time";
//
//	private static final String ELEM_STATS= "stats";
//	private static final String ATT_NAME= "name";
//	private static final String ATT_STATTYPE= "statType";
//	private static final String ATT_TYPE= "type";
//
//	private static final String VAL_WEBCONTAINER="WebContainer";
//	private static final String VAL_THREADPOOLMODULE ="threadPoolModule";
//	private static final String VAL_COLLECTION ="COLLECTION";
	
	
	private static final String ELEM_SNAPSHOT= TPVLogParser.CONFIG.getProperty("ele.name.snapshot");
	private static final String ATT_TIME= TPVLogParser.CONFIG.getProperty("att.name.time");

	private static final String ELEM_STATS= TPVLogParser.CONFIG.getProperty("ele.name.stats");
	private static final String ATT_NAME= TPVLogParser.CONFIG.getProperty("att.name.name");
	private static final String ATT_STATTYPE= TPVLogParser.CONFIG.getProperty("att.name.stat_type");
	private static final String ATT_TYPE= TPVLogParser.CONFIG.getProperty("att.name.type");

	private static final String VAL_WEBCONTAINER=TPVLogParser.CONFIG.getProperty("wc.att.val.name");
	private static final String VAL_THREADPOOLMODULE =TPVLogParser.CONFIG.getProperty("wc.att.val.stat_type");
	private static final String VAL_COLLECTION =TPVLogParser.CONFIG.getProperty("wc.att.val.type");
	
	

	//Variables declaration
	private boolean isWebContainerElementStarted = false;
	private boolean isSnapshotElementStarted = false;
	private String snapshot_time; 
	private PrintStream out = null;
	private String statNameVal= null;
	private Connection connection = null;
	private PreparedStatement pStmt_CS = null;
	private PreparedStatement pStmt_TS = null;
	private PreparedStatement pStmt_RS = null;
	private PreparedStatement pStmt_BRS = null;
	
	@Override
	public void startDocument() throws SAXException {
		this.connect();
	}
	
	@Override
	public void endDocument() throws SAXException {
		this.close();
	}
	
	public WebContainerMetricsExtractor(){
//		out = System.out;
		
	}

	public WebContainerMetricsExtractor (OutputStream argOut) throws Exception {
		out = new PrintStream(argOut);
	}

	public void startElement (String uri, String localname,	String qName, 
			Attributes attr) throws SAXException{


		if((isSnapshotElementStarted!=true) && (qName.equalsIgnoreCase(ELEM_SNAPSHOT))){
			isSnapshotElementStarted = true;
			snapshot_time = ToolSet.convertUnixToCST_V1(Long.parseLong(attr.getValue(ATT_TIME).substring(0,10)));
		} 

		if(isSnapshotElementStarted && isWebContainerElementStarted) {
			
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

			out.println();
			out.print(snapshot_time+","+ statNameVal+","+qName);

			short n =  (short) attr.getLength();
			for(short i =0; i<n; i++) {
				out.print(","+attr.getQName(i)+"="+attr.getValue(i));
			}

		}else if(qName.equalsIgnoreCase(ELEM_STATS) && isWebContinerElement(attr )){
			isWebContainerElementStarted = true;
		}

	}

	public void endElement (String uri, String localName, String qName) 
			throws SAXException {
		if(qName.equalsIgnoreCase("snapshot")){
			isSnapshotElementStarted = false;
		}

		if(isWebContainerElementStarted & qName.equalsIgnoreCase(ELEM_STATS)) {
			isWebContainerElementStarted = false;
		}

	}

	public boolean isWebContinerElement(Attributes argAttr) throws SAXException{
		boolean result = false;


		if(argAttr != null){
			if(argAttr.getValue(ATT_NAME).equalsIgnoreCase(VAL_WEBCONTAINER) &&
					argAttr.getValue(ATT_STATTYPE).equalsIgnoreCase(VAL_THREADPOOLMODULE) &&
					argAttr.getValue(ATT_TYPE).equalsIgnoreCase(VAL_COLLECTION) ) {

				statNameVal = argAttr.getValue(ATT_NAME);
				result = true;
			}
		}else {
			throw new SAXException("NULL argument passed to isWebContainerElement() method.");
		}
		return result;

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
			tuncateTables();
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
