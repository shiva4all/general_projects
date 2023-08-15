package com.swift.load.api.custom.parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.swift.load.api.db.ConfigureDB;
import com.swift.load.api.db.Database;
import com.swift.load.api.db.MessagingService;
import com.swift.load.api.rules.Evaluate;
import com.swift.load.api.rules.Filter;
import com.swift.load.api.rules.JoinCondition;
import com.google.common.collect.Lists;

public class CustomJsonMergeLevelDBLegacyExtractor implements Callable<List<PayloadObj>> {

	private static final Logger LOG = LoggerFactory.getLogger(CustomJsonMergeLevelDBLegacyExtractor.class);

	public static final String EX_OCCURED = " occured";
	
	private String dbDir;
	private String configDir;
	private String files2bLoaded;
	private String mainFile;
	private static final Map<String, List<Filter>> filterMap = new HashMap<>();
	private static final Map<String, List<JoinCondition>> joinMap = new HashMap<>();
	private List<String> listOfOtherTables = new ArrayList<>();
	private Map<String, List<Database>> databaseList = new HashMap<>();
	private List<String> mainTableKeys = new ArrayList<>();
	private ConfigureDB cfgDb;
	private static String chunkId;
	private String  fileLocationOfExclusionList;
	//private Set<String> exlusionList = new HashSet<>();
	private static Set<String> exlusionInclusionList = new HashSet<>();
	private static String command = "";

	private CustomJsonMergeLevelDBLegacyExtractor(String dbDir,String configDir, List<String> mainTableKeys, String mainFile, String files2bLoaded,
			Map<String, List<Database>> databaseList, List<String> listOfOtherTables) {
		try {
			this.dbDir=dbDir;
			this.configDir = configDir;
		
			this.mainTableKeys = mainTableKeys;
			this.files2bLoaded = files2bLoaded;
			this.mainFile = mainFile;
			this.databaseList = databaseList;
			this.listOfOtherTables = listOfOtherTables;
		
		} catch (Exception e) {
			LOG.error(e.getClass().getName() + EX_OCCURED, e);
			
		}
	}

	/*
	 * Constructor to perform JSON merge 
	 * @param dbDir specifies the dir name where DB is stored
	 * @param configDir specifies where the configuration has to be read from this directory contains join.csv and filter.csv
	 * @param mainFile specifies the main file to be used to merge
	 * @param files2bLoaded subtables to be used to merge
	 
	 */
	public CustomJsonMergeLevelDBLegacyExtractor(String dbDir,String configDir, String mainFile, String files2bLoaded, String chunkId) {
		try {
			this.dbDir=dbDir;
			this.configDir = configDir;
			this.files2bLoaded = files2bLoaded;
			this.mainFile = mainFile;
			this.chunkId = chunkId;
			
		} catch (Exception e) {
			LOG.error(e.getClass().getName() + EX_OCCURED, e);
			
		}
	}
	
	/*
	 * Constructor to perform JSON merge 
	 * @param dbDir specifies the dir name where DB is stored
	 * @param configDir specifies where the configuration has to be read from this directory contains join.csv and filter.csv
	 * @param mainFile specifies the main file to be used to merge
	 * @param files2bLoaded subtables to be used to merge
	 * @param fileLocationOfExclusionList list of luwids to be excluded
	 
	 */
	public CustomJsonMergeLevelDBLegacyExtractor(String dbDir,String configDir, String mainFile, String files2bLoaded, String chunkId,String fileLocationOfExclusionList, String command) {
		try {
			this.dbDir=dbDir;
			this.configDir = configDir;
			this.files2bLoaded = files2bLoaded;
			this.mainFile = mainFile;
			this.chunkId = chunkId;
			this.fileLocationOfExclusionList= fileLocationOfExclusionList;
			this.exlusionInclusionList=getexlusionInclusionList();
			this.command = command;
			
		} catch (Exception e) {
			LOG.error(e.getClass().getName() + EX_OCCURED, e);
			
		}
	}
	
	/*
	 * Method to read exclusion list to file and store in Hashset
	 * 
	 */

	private Set<String> getexlusionInclusionList() {
		Set<String> set = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(this.fileLocationOfExclusionList))) {
			String line ;
			while((line=br.readLine())!=null){
				set.add(line);
			}
		} catch (IOException e) {
			LOG.error("Error reading exclusion list: ", e);
		}
		return set;
	}

	/*
	 * Method to excute the json merge form the configuraion provided in the constructor
	 */
	public List<PayloadObj> mergeJsonFiles() {
		final String tableName = mainFile.replaceAll(".csv", "").replaceAll(".txt", "");
		String[] array = files2bLoaded.split(",");
		readConfiguration();
		for (String str : array) {
			str = str.replaceAll(".csv", "").replaceAll(".txt", "");
			if (!str.equals(tableName)) {
				listOfOtherTables.add(str);
			}
		}

		databaseList = initializeDB();
		List<List<String>> smallerKeyLists = getSmallerList(databaseList.get(tableName));
		return executor(smallerKeyLists);
	}
	
	public void close() {
		joinMap.clear();
		filterMap.clear();
		exlusionInclusionList.clear();
		command = "";
	}

	private List<PayloadObj> executor(List<List<String>> smallerKeyList) {
		List<PayloadObj> rsult = new ArrayList<>();
		// Get ExecutorService from Executors utility class, thread pool size is
		// 10
		ExecutorService executor = Executors.newFixedThreadPool(smallerKeyList.size());
		// create a list to hold the Future object associated with Callable
		List<Future<List<PayloadObj>>> list = new ArrayList<>();
		// Create MyCallable instance

		for (int i = 0; i < smallerKeyList.size(); i++) {
			// submit Callable tasks to be executed by thread pool
			Callable<List<PayloadObj>> callable = new CustomJsonMergeLevelDBLegacyExtractor(dbDir,configDir, smallerKeyList.get(i),
					this.mainFile.replaceAll(".csv", ""), this.files2bLoaded, this.databaseList,
					this.listOfOtherTables);
			Future<List<PayloadObj>> future = executor.submit(callable);
			// add Future to the list, we can get return value using Future
			list.add(future);
		}
		for (Future<List<PayloadObj>> fut : list) {
			try {
				List<PayloadObj> listOfData = fut.get();
				if (listOfData != null) {
					rsult.addAll(listOfData);
				}

			} catch (InterruptedException e) {
				LOG.error("InterruptedException",e);
				
				Thread.currentThread().interrupt();
				// throw new InterruptedException()
			} catch (ExecutionException e) {
				LOG.error("ExecutionException",e);
				
			}

		}
		// shut down the executor service now
		executor.shutdown();
		this.cfgDb.destroy();
		
		return rsult;
	}

	@Override
	public List<PayloadObj> call() throws Exception {
		return formJson();
	}

	private List<PayloadObj> formJson() {

		
		List<PayloadObj> jsonList = new ArrayList<>();
		for (String key : mainTableKeys) {
			/*
			 * if(exlusionInclusionList.contains(key)){ LOG.
			 * error("INC1001: Subscriber is been excluded since present in exclusion list:"
			 * +key); continue; }
			 */
			if (!exlusionInclusionList.isEmpty()) {
				if (exlusionInclusionList.contains(key) && command.equalsIgnoreCase("exclude")) {
					LOG.error("INC1001 : Subscriber is been excluded since present in exclusion list:" + key);
					continue;
				}
				if (!exlusionInclusionList.contains(key) && command.equalsIgnoreCase("include")) {
					LOG.error("INC1002 : Subscriber is been excluded since not present in inclusion list:" + key);
					continue;
				}
			}
			MessagingService msgService = new MessagingService(Thread.currentThread().getName());
			
			String mainJson = msgService.combineIntoOneJson(databaseList.get(this.mainFile), key);
			Evaluate evaluate = new Evaluate(filterMap.get(this.mainFile), mainJson, this.mainFile);
			
			JSONArray jsonArra = evaluate.evaluateFilterCondition();
			//Reject the json if filter condition on main table is successful
			if(jsonArra.length()==0){
				PayloadObj payload = new PayloadObj(null, "luw_id", key);
				jsonList.add(payload);
				continue;
			}
			JSONObject jsonObj = new JSONObject();
			mainJson = jsonObj.put(this.mainFile, jsonArra).toString();
			Map<String, String> mapOfTableData = new HashMap<>();
			if (mainJson == null) {
				return new ArrayList<>();
			}
			mapOfTableData.put(this.mainFile, mainJson);
			mainJson = mainJson.substring(1, mainJson.length() - 1);
			String luwId = key;
			
			mainJson = "{\"chunk_id\":".concat("\"").concat(chunkId).concat("\",").concat("\"luw_id\":").concat("\"").concat(luwId).concat("\",").concat("\"BODY\":{")
					.concat(mainJson);
					
			// Get all children Json
			// integer index := 0
			for (String subTableName : listOfOtherTables) {
				try {
					String subJson = getSubTableJson(msgService, mapOfTableData, subTableName);
					if (subJson != null && subJson.length() > 0) {
						Evaluate evaluatesubtable = new Evaluate(filterMap.get(subTableName), subJson, subTableName);
						JSONArray jsonArra1 = evaluatesubtable.evaluateFilterCondition();
						JSONObject jsonObj1 = new JSONObject();
						subJson = jsonObj1.put(subTableName, jsonArra1).toString();
						mapOfTableData.put(subTableName, subJson);
						subJson = subJson.substring(1, subJson.length() - 1);
						mainJson = new StringBuilder(mainJson).append(",").append(subJson).toString();
					}
				} catch (Exception e) {
					LOG.error("formJson" , e);
					
				}
			}
			mainJson = mainJson + "}}";
			PayloadObj payloadObj = new PayloadObj(mainJson, "luw_id", luwId);
			jsonList.add(payloadObj);

		}
		return jsonList;
	}

	private String getSubTableJson(MessagingService msgService, Map<String, String> mapOfTableData,
			String subTableName) {
		List<String> keylist = getKeyListToSearch(mapOfTableData, subTableName);
		String subJson;
		JSONArray finalArray = new JSONArray();
		for (String key2 : keylist) {
			
			String tmpString = msgService.combineIntoOneJson(databaseList.get(subTableName), key2);
			if (tmpString != null && tmpString.length() > 0) {
				JSONObject jsonObjTmp = new JSONObject(tmpString);

				JSONArray jsonArray2 = (JSONArray) jsonObjTmp.get(subTableName);
				for (int index = 0; index < jsonArray2.length(); index++) {
					finalArray.put((JSONObject) jsonArray2.get(index));
				}
			}

		}
		subJson = new JSONObject().put(subTableName, finalArray).toString();
		return subJson;
	}

	private List<String> getKeyListToSearch(Map<String, String> mapOfTableData, String subTableName) {
		List<JoinCondition> listOfMainTables = joinMap.get(subTableName);
		List<String> keyList = new ArrayList<>();
		for (JoinCondition jcObj : listOfMainTables) {
			String entity = jcObj.getMaintableToBeSearched();
			String key2Search = jcObj.getColumnToBesearched();

			getKeyList(mapOfTableData, keyList, entity, key2Search);
		}
		return keyList;
	}

	private void getKeyList(Map<String, String> mapOfTableData, List<String> keyList, String entity,
			String key2Search) {
		JSONObject jObj = new JSONObject(mapOfTableData.get(entity));
		JSONArray jarr = (JSONArray) jObj.get(entity);
		for (int i = 0; i < jarr.length(); i++) {
			JSONObject j1Obj = (JSONObject) jarr.get(i);
			if (key2Search.contains(",")) {
				String tmp[] = key2Search.split(",", -4);
				StringBuilder finalKey = new StringBuilder();
				int cnt = 0;
				for (String key : tmp) {
					if (cnt == 0) {
						finalKey.append(key);
					} else {
						finalKey = finalKey .append( "_" ).append(key);
					}
					cnt++;
				}
				keyList.add((String) j1Obj.get(finalKey.toString()));
			} else {
				keyList.add((String) j1Obj.get(key2Search));
			}

		}
	}

	private Map<String, List<Database>> initializeDB() {
		this.cfgDb = new ConfigureDB(this.dbDir);
		cfgDb.initialize();
		return cfgDb.getDbConfigMap();
	}

	private List<List<String>> getSmallerList(List<Database> databaseList) {
		MessagingService msgService = new MessagingService("123");
		Set<String> keys = msgService.getAllKeys(databaseList);
		List<String> keyList = new ArrayList<>();
		keyList.addAll(keys);
		int size = keyList.size() / (Runtime.getRuntime().availableProcessors() - 1);
		if (size == 0)
			size = 1;
		return Lists.partition(keyList, size);
	}

	private void readConfiguration() {
		readFilterConfiguraion();
		readJoinConfiguration();
	}

	private void readJoinConfiguration() {
		try (BufferedReader br = new BufferedReader(new FileReader(configDir + "/join.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String tmp[] = line.split(",", -5);
				if (joinMap.containsKey(tmp[0])) {
					List<JoinCondition> joinList = joinMap.get(tmp[0]);
					String tmp2[] = tmp[1].split("\\.");
					JoinCondition join = new JoinCondition(tmp2[0], tmp2[1], tmp[0]);
					joinList.add(join);
					joinMap.put(tmp[0], joinList);
				} else {
					List<JoinCondition> joinList = new ArrayList<>();
					String tmp2[] = tmp[1].split("\\.");
					JoinCondition join = new JoinCondition(tmp2[0], tmp2[1], tmp[0]);
					joinList.add(join);
					joinMap.put(tmp[0], joinList);
				}
			}
		} catch (IOException e) {
			LOG.error("IOException 2",e);
		}

	}

	private void readFilterConfiguraion() {
		try (BufferedReader br = new BufferedReader(new FileReader(configDir + "/filter.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String tmp[] = line.split(",", -5);
				if (filterMap.containsKey(tmp[0])) {
					List<Filter> filterList = filterMap.get(tmp[0]);
					Filter filter = new Filter(tmp[0], tmp[1], tmp[2], tmp[3], tmp[4], tmp[5]);
					filterList.add(filter);
					filterMap.put(tmp[0], filterList);
				} else {
					List<Filter> filterList = new ArrayList<>();
					Filter filter = new Filter(tmp[0], tmp[1], tmp[2], tmp[3], tmp[4], tmp[5]);
					filterList.add(filter);
					filterMap.put(tmp[0], filterList);
				}
			}
		} catch (IOException e) {
			LOG.error("IOException",e);
		}
	}

	public static void main(String args[]) {
		
		String home = "C:/Partha/CS2BEAM/source_code/SOMIG-277/src";
		String dbDir= home+"/database";
		String configDir= home+"/config";
		String mainFile = "SUBSCRIBER.csv";
		String files2bLoaded = "SUBSCRIBER.csv,PRODUCT.csv,BUCKET_BALANCE.csv,BUCKET_COUNTER.csv";
		
		CustomJsonMergeLevelDBLegacyExtractor obj = new CustomJsonMergeLevelDBLegacyExtractor(dbDir,configDir, mainFile, files2bLoaded, "test_chulkId");
		List<PayloadObj> list = obj.mergeJsonFiles();
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter( "C:/Partha/CS2BEAM/source_code/SOMIG-277/src/output/InputJson.txt"))) {
			for (PayloadObj data : list) {
				bw.write("luw_id == " + data.getPropertiesMap().get("luw_id") + "\nPayload == " + data.getJsonPayload() + "\n");
			}
			
		} catch (IOException e) {
			LOG.error("IOException",e);

		}

	}

}
