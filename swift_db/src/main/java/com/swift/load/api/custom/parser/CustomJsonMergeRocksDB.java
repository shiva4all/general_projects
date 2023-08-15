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
import java.util.Random;
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

public class CustomJsonMergeRocksDB implements Callable<List<String>> {

	private static final Logger LOG = LoggerFactory.getLogger(CustomJsonMergeRocksDB.class);

	public static final String EX_OCCURED = " occured";

	private String dbDir;
	private String configDir;
	private static String workingDir;
	private String files2bLoaded;
	private String mainFile;
	private String fileLocationOfExclusionList;
	private Set<String> exlusionList = new HashSet<>();
	private static final Random uniqueFileIndex = new Random();
	private static final Map<String, List<Filter>> filterMap = new HashMap<>();
	private static final Map<String, List<JoinCondition>> joinMap = new HashMap<>();
	private List<String> listOfOtherTables = new ArrayList<>();
	private Map<String, List<Database>> databaseList = new HashMap<>();
	private List<String> mainTableKeys = new ArrayList<>();
	ConfigureDB cfgDb;

	private CustomJsonMergeRocksDB(String dbDir, String configDir, List<String> mainTableKeys, String mainFile,
			String files2bLoaded, Map<String, List<Database>> databaseList, List<String> listOfOtherTables) {

		this.dbDir = dbDir;
		this.configDir = configDir;
		this.mainTableKeys = mainTableKeys;
		this.files2bLoaded = files2bLoaded;
		this.mainFile = mainFile;
		this.databaseList = databaseList;
		this.listOfOtherTables = listOfOtherTables;

	}

	public CustomJsonMergeRocksDB(String dbDir, String configDir, String mainFile, String files2bLoaded,
			String workingDir1, String fileLocationOfExclusionList) {

		this.dbDir = dbDir;
		this.configDir = configDir;
		this.files2bLoaded = files2bLoaded;
		this.mainFile = mainFile;
		workingDir = workingDir1;
		this.fileLocationOfExclusionList = fileLocationOfExclusionList;
		if (fileLocationOfExclusionList != null && fileLocationOfExclusionList.length() > 0)
			exlusionList = getExclusionList();

	}

	/*
	 * Constructor to perform JSON merge
	 * 
	 * @param dbDir specifies the dir name where DB is stored
	 * 
	 * @param configDir specifies where the configuration has to be read from this
	 * directory contains join.csv and filter.csv
	 * 
	 * @param mainFile specifies the main file to be used to merge
	 * 
	 * @param files2bLoaded subtables to be used to merge
	 * 
	 */
	public CustomJsonMergeRocksDB(String dbDir, String configDir, String mainFile, String files2bLoaded) {

		this.dbDir = dbDir;
		this.configDir = configDir;
		this.files2bLoaded = files2bLoaded;
		this.mainFile = mainFile;

	}

	/*
	 * Constructor to perform JSON merge
	 * 
	 * @param dbDir specifies the dir name where DB is stored
	 * 
	 * @param configDir specifies where the configuration has to be read from this
	 * directory contains join.csv and filter.csv
	 * 
	 * @param mainFile specifies the main file to be used to merge
	 * 
	 * @param files2bLoaded subtables to be used to merge
	 * 
	 * @param fileLocationOfExclusionList contains the excution list filename
	 * 
	 */
	public CustomJsonMergeRocksDB(String dbDir, String configDir, String mainFile, String files2bLoaded,
			String fileLocationOfExclusionList) {

		this.dbDir = dbDir;
		this.configDir = configDir;
		this.files2bLoaded = files2bLoaded;
		this.mainFile = mainFile;
		this.fileLocationOfExclusionList = fileLocationOfExclusionList;
		exlusionList = getExclusionList();

	}

	/*
	 * Method to read exclusion list to file and store in Hashset
	 * 
	 */

	private Set<String> getExclusionList() {
		Set<String> set = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(this.fileLocationOfExclusionList))) {
			String line;
			while ((line = br.readLine()) != null) {
				set.add(line);
			}
		} catch (IOException e) {
			LOG.error("Error reading exclusion list: ", e);
		}
		return set;
	}

	/*
	 * Method to excute the json merge form the configuraion provided in the
	 * constructor
	 */
	public List<String> mergeJsonFiles() {
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

		//System.out.println(tableName);
		List<List<String>> smallerKeyLists = getSmallerList(databaseList.get(tableName));
		// System.out.println("SIZE:::::::::"+smallerKeyLists.size())
		return executor(smallerKeyLists);
	}

	private List<String> executor(List<List<String>> smallerKeyList) {
		List<String> rsult = new ArrayList<>();
		// Get ExecutorService from Executors utility class, thread pool size is
		// 10
		ExecutorService executor = Executors.newFixedThreadPool(smallerKeyList.size());
		// create a list to hold the Future object associated with Callable
		List<Future<List<String>>> list = new ArrayList<>();
		// Create MyCallable instance

		for (int i = 0; i < smallerKeyList.size(); i++) {
			// submit Callable tasks to be executed by thread pool
			Callable<List<String>> callable = new CustomJsonMergeRocksDB(dbDir, configDir, smallerKeyList.get(i),
					this.mainFile.replaceAll(".csv", ""), this.files2bLoaded, this.databaseList,
					this.listOfOtherTables);
			Future<List<String>> future = executor.submit(callable);
			// add Future to the list, we can get return value using Future
			list.add(future);
		}
		for (Future<List<String>> fut : list) {
			try {
				List<String> listOfData = fut.get();
				if (listOfData != null) {
					rsult.addAll(listOfData);
				}

			} catch (InterruptedException e) {
				LOG.error("InterruptedException", e);
				// e.printStackTrace()

				Thread.currentThread().interrupt();
				// throw new InterruptedException()
			} catch (ExecutionException e) {
				LOG.error("ExecutionException", e);
				// .printStackTrace()

			}

		}
		// shut down the executor service now
		executor.shutdown();
		this.cfgDb.destroy();
		// System.out.println("ALL PROCESS ARE COMPLETED")
		return rsult;
	}

	@Override
	public List<String> call() throws Exception {
		return formJson();
	}

	private List<String> formJson() {
		List<String> jsonList = new ArrayList<>();
		try (BufferedWriter bw = new BufferedWriter(
				new FileWriter(workingDir + "/Subscriber_" + uniqueFileIndex.nextInt(1000000) + ".csv"))) {
			for (String key : mainTableKeys) {
				if (exlusionList.contains(key)) {
					LOG.error("INC1001: Subscriber is been excluded since present in exclusion list:" + key);
					continue;
				}
				MessagingService msgService = new MessagingService(Thread.currentThread().getName());
				String mainJson = msgService.combineIntoOneJson(databaseList.get(this.mainFile), key);
				//System.out.println("Main JSON :::"+mainJson);
				Evaluate evaluate = new Evaluate(filterMap.get(this.mainFile), mainJson, this.mainFile);
				JSONArray jsonArra;
				
				jsonArra = evaluate.evaluateFilterCondition();
				JSONObject jsonObj = new JSONObject();
				mainJson = jsonObj.put(this.mainFile, jsonArra).toString();

				Map<String, String> mapOfTableData = new HashMap<>();
				if (mainJson == null) {
					return new ArrayList<>();
				}
				mapOfTableData.put(this.mainFile, mainJson);
				mainJson = mainJson.substring(1, mainJson.length() - 1);

				mainJson = "{\"luw_id\":".concat("\"").concat(key).concat("\",").concat("\"BODY\":{").concat(mainJson);
				// Get all children Json
				// integer index := 0
				for (String subTableName : listOfOtherTables) {
					try {
						String subJson = getSubTableJson(msgService, mapOfTableData, subTableName);
						if (subJson != null && subJson.length() > 0) {

							Evaluate evaluatesubtable = new Evaluate(filterMap.get(subTableName), subJson,
									subTableName);
							JSONArray jsonArra1 = evaluatesubtable.evaluateFilterCondition();
							JSONObject jsonObj1 = new JSONObject();
							subJson = jsonObj1.put(subTableName, jsonArra1).toString();

							mapOfTableData.put(subTableName, subJson);
							subJson = subJson.substring(1, subJson.length() - 1);
							mainJson = new StringBuilder(mainJson).append(",").append(subJson).toString();
						}
					} catch (Exception e) {
						LOG.error("Exception", e);
						// e.printStackTrace()

					}
				}
				mainJson = mainJson + "}}";
				jsonList.add(key);
				bw.write(mainJson.concat("\n"));

			}
			bw.flush();
			bw.close();

		} catch (IOException e) {
			LOG.error("Exception", e);
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
						finalKey = finalKey.append("_").append(key);
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
		this.cfgDb = new ConfigureDB(this.dbDir,"ROCKSDB");
		cfgDb.initialize();
		return cfgDb.getDbConfigMap();
	}

	private List<List<String>> getSmallerList(List<Database> databaseList) {
		MessagingService msgService = new MessagingService("123");
		
		Set<String> keys = msgService.getAllKeys(databaseList);
		List<String> keyList = new ArrayList<>();
		
		keyList.addAll(keys);
		int size = keyList.size() / (Runtime.getRuntime().availableProcessors());
		System.out.println();
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
			LOG.error("IOException2", e);
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
			LOG.error("IOException", e);
		}
	}

	public static void main(String args[]) {
		String home = "C:\\Users\\egjklol\\Desktop\\Test\\";
		String dbDir = home + "/database/";
		String configDir = home + "/Scheme/";
		String mainFile = "INFILE_OPTIVA_RPP_Product.csv";
		String files2bLoaded = "INFILE_OPTIVA_RPP_Product.csv,INFILE_OPTIVA_RPPQoS_Product.csv";
	
		
		//CustomJsonMergeLevelDB obj = new CustomJsonMergeLevelDB(dbDir, configDir, mainFile, files2bLoaded,
		//		home + "/Working/", "");
		CustomJsonMergeRocksDB obj = new CustomJsonMergeRocksDB(dbDir, configDir, mainFile, files2bLoaded,
				home + "/Working/", "");
		List<String> list = obj.mergeJsonFiles();

		try (BufferedWriter bw = new BufferedWriter(new FileWriter(home + "/Working/Success.txt"))) {
			for (String data : list) {
				bw.write(data + "\n");
			}
			// System.out.println("end : " + new Date())
		} catch (IOException e) {
			LOG.error("IOException", e);

		}

	}

}
