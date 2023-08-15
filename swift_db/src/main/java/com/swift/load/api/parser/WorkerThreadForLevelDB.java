
package com.swift.load.api.parser;

import java.io.File;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
* The  program implements an application that
* runs multiple threads to load the data in to DB.
*
* @author  Shivakumar
* @version 1.0
* @since   2019-06-27 
*/
public class WorkerThreadForLevelDB implements Runnable {
	 List<File> listOfFiles;
	 List<String> listOfFilesToBeInserted;
	 String dumpFilePath;
	 String schemaFilePath;
	 String dbPath;
	 String filesToBeInserted;
	 String configDir;
	 String logPath ;
	 String cbpid;
	 private static final Logger LOG = LoggerFactory.getLogger(WorkerThreadForLevelDB.class);
	 
	WorkerThreadForLevelDB(List<File> listOfFiles, List<String> listOfFilesToBeInserted,
			String dumpFilePath,String schemaFilePath,String dbPath , String filesToBeInserted) {
		this.listOfFiles = listOfFiles;
		this.listOfFilesToBeInserted = listOfFilesToBeInserted;
		this.dumpFilePath=dumpFilePath;
		this.schemaFilePath=schemaFilePath;
		this.dbPath=dbPath;
		this.filesToBeInserted=filesToBeInserted;
	}
	

	@Override
	public void run() {
		for (File file : listOfFiles) {
			Collections.sort(listOfFilesToBeInserted);
			
			if (file.isFile() && file.length() > 0 && checkFilePresent(file)) {	
					try {			
						String entityName  = file.getName().replaceAll(".csv", "").replaceAll(".txt", "").replaceAll(".unl", "");
						InputFile2LevelDB obj = new InputFile2LevelDB(dumpFilePath, schemaFilePath, filesToBeInserted, dbPath);	
						obj.loadIntoLevelDB(entityName, file, dbPath);
						//InputFile2LevelDB2.loadIntoMapDB(entityName, file, InputFile2LevelDB2.mapdbPath)
						
					} catch (Exception e) {
						LOG.error("Exception",e);	
					}
			}

		}
	}

	private boolean checkFilePresent(File f) {
		boolean flag = false;
		String fName = f.getName();
		fName = fName.replace("tabledef_","");

		if(this.listOfFilesToBeInserted.contains(fName)){
			flag=true;
		}
		else{
			flag = false;
		}
		return flag;
	}
}
