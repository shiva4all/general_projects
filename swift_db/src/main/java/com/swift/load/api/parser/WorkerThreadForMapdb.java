
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
public class WorkerThreadForMapdb implements Runnable {
	 List<File> listOfFiles;
	 List<String> listOfFilesToBeInserted;
 private static final Logger LOG = LoggerFactory.getLogger(WorkerThreadForMapdb.class);
	WorkerThreadForMapdb(List<File> listOfFiles, List<String> listOfFilesToBeInserted) {
		this.listOfFiles = listOfFiles;
		this.listOfFilesToBeInserted = listOfFilesToBeInserted;
	
	}

	@Override
	public void run() {
		for (File file : listOfFiles) {
			Collections.sort(listOfFilesToBeInserted);
			
			if (file.isFile() && file.length() > 0 && (Collections.binarySearch(listOfFilesToBeInserted, file.getName())>=0)) {	
					try {
						InputFile2Mapdb.loadIntoMapDB(InputFile2Mapdb.mapDbDatabase,file.getName().replaceAll(".csv", "").replaceAll(".txt", ""),file);
					} catch (Exception e) {
						LOG.error("Exception",e);
					}

			}

		}
	}
}
