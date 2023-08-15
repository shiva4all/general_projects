package com.swift.load.api.custom.parser;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.collect.Lists;

public class MultiThreadedStreamer implements Runnable {




	public List<String> listOfLuw = new ArrayList<>();
	private MergedJsonLevelDB mergedJsonObject;
	private String generatedString;

	public String getGeneratedString() {
		return generatedString;
	}

	public void setGeneratedString(String generatedString) {
		this.generatedString = generatedString;
	}

	public MultiThreadedStreamer() {

	}

	private MultiThreadedStreamer(List<String> listOfLuw, MergedJsonLevelDB mergedJsonObject, String generatedString) {
		this.listOfLuw = listOfLuw;
		this.mergedJsonObject = mergedJsonObject;
		this.generatedString=generatedString;
	}

	private static final Logger logger = LoggerFactory.getLogger(MultiThreadedStreamer.class);

	private List<String> finalResponse = new ArrayList<>();

	public void routeRequest() {

		String home = "C:\\Users\\egjklol\\Desktop\\src\\";
		String files2BeLoaded = "SUBSCRIBER.csv,PRODUCT.csv,BUCKET_BALANCE.csv,BUCKET_COUNTER.csv";
		String masterFile = "SUBSCRIBER.csv";
		String dbpath = home + "/database/" + this.getGeneratedString();
		String configDir = home + "/config";
		MergedJsonLevelDB mergedJsonobject = new MergedJsonLevelDB(dbpath, configDir, masterFile, files2BeLoaded,
				"init_db");
		String mainTable = masterFile.substring(0, masterFile.lastIndexOf('.'));
		init(mergedJsonobject);
		List<String> completeKeyList = new ArrayList<>();
		completeKeyList.addAll(mergedJsonobject.getAllKeys(mainTable));
		int size = completeKeyList.size() / Integer.parseInt("8");
		executor(Lists.partition(completeKeyList, size));
		close(mergedJsonobject);
	}

	private void executor(List<List<String>> smallerKeyList) {
		// leveldb
		
		String home = "C:\\Users\\egjklol\\Desktop\\src\\";
		String dumpFilePath = home + "/Input";
		String schemFilePath = dumpFilePath + "/Schema";
		String files2BeLoaded = "SUBSCRIBER.csv,PRODUCT.csv,BUCKET_BALANCE.csv,BUCKET_COUNTER.csv";
		String masterFile = "SUBSCRIBER.csv";
		String dbpath = home + "/database/" + this.getGeneratedString();
		
		String configDir = home + "/config";



		ExecutorService executor = Executors.newFixedThreadPool(smallerKeyList.size());

		for (int i = 0; i < smallerKeyList.size(); i++) {
			String mainTable = masterFile.substring(0, masterFile.lastIndexOf('.'));
			MergedJsonLevelDB mergedJsonObj = new MergedJsonLevelDB(dbpath, configDir, masterFile, files2BeLoaded,
					"test_chulkId");
			mergedJsonObj.readConfiguration();
			MultiThreadedStreamer obj = new MultiThreadedStreamer(smallerKeyList.get(i), mergedJsonObj,this.getGeneratedString());
			executor.execute(obj);
		}
		executor.shutdown();
		while (!executor.isTerminated())
			;

	}

	@Override
	public void run() {
		for (String luwId : this.listOfLuw) {
			PayloadObj payloadObj = mergedJsonObject.formJson(luwId);
			System.out.println(payloadObj);
		}

	}

	private void init(MergedJsonLevelDB obj) {
		MergedJsonLevelDB.initializeDB();
	}

	private void close(MergedJsonLevelDB obj) {
		MergedJsonLevelDB.destroy();
	}
	
	public static void main(String args[]){
		MultiThreadedStreamer ms = new MultiThreadedStreamer();
		ms.setGeneratedString("test");
		ms.routeRequest();
	}

}
