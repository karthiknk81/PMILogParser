package com.parser.log.pmi;

import java.io.BufferedInputStream;

import org.xml.sax.helpers.DefaultHandler;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

import javax.swing.JFileChooser;

import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.XMLReader;
import org.xml.sax.InputSource;

import com.parser.log.pmi.handler.*;

public class TPVLogParser {
	public static final Properties CONFIG = new Properties();
	public static void main(String[] args) throws Exception {
		
		loadConfiguration();
		String originalTPVLogFile = getInputFile();
		
		FileOutputStream outputWebContainerStats = new FileOutputStream(originalTPVLogFile+".webcontainer-stats.csv");
		FileOutputStream outputJDBCConnectionStats = new FileOutputStream(originalTPVLogFile+".jdbcconnection-stats.csv");

		try {
			TPVLogParser parser = new TPVLogParser();
			parser.transformCSV(new WebContainerMetricsExtractor(outputWebContainerStats), originalTPVLogFile);
			parser.transformCSV(new JDBCConnectionMetricsExtractor(outputJDBCConnectionStats), originalTPVLogFile);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (outputWebContainerStats != null){
					outputWebContainerStats.flush();
					outputWebContainerStats.close();
				}
				if (outputJDBCConnectionStats != null){	
					outputJDBCConnectionStats.flush();
					outputJDBCConnectionStats.close();
				}
			} catch (Throwable t) {
			}
			
			try {
				if (outputWebContainerStats != null)
					outputWebContainerStats.close();
				if (outputJDBCConnectionStats != null)
					outputJDBCConnectionStats.close();
			} catch (Throwable t) {
			}
		}

	}

	public void transformCSV(DefaultHandler argHdlr, String argInputXml)
			throws Exception {

		XMLReader xmlReader = null;
		xmlReader = XMLReaderFactory.createXMLReader();
		xmlReader.setContentHandler(argHdlr);
		xmlReader.parse(new InputSource(new BufferedInputStream(
				new FileInputStream(argInputXml))));

	}

	public static String getInputFile() throws Exception{
		JFileChooser chooser = new JFileChooser();
		chooser.setCurrentDirectory(new java.io.File("."));
		chooser.setDialogTitle("Choose TPV XML file ... ");
		//    chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		chooser.setAcceptAllFileFilterUsed(false);

		if (chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
			return chooser.getSelectedFile().toString();
		} else {
			throw new Exception("No Selection ");
		}
	}
	
	public static void loadConfiguration() throws Exception{
		FileInputStream in = new FileInputStream("./resource/config.properties");
		CONFIG.load(in);
		in.close();
		System.out.println("odb.driver = "+TPVLogParser.CONFIG.getProperty("odb.driver"));
		System.out.println("odb.conn.str = "+TPVLogParser.CONFIG.getProperty("odb.conn.str"));
	}
	
}
