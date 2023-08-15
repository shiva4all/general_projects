package com.swift.load.api.custom.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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


public class MergedJsonLevelDB  {

	
	public static final String EX_OCCURED = " occured";
	public static final String LUWID = " luw_id";
	
	private static String dbDir;
	private String configDir;
	private String files2bLoaded;
	private String mainFile;
	private  static Map<String, List<Filter>> filterMap = new HashMap<>();
	private static Map<String, List<JoinCondition>> joinMap = new HashMap<>();
	private List<String> listOfOtherTables = new ArrayList<>();
	public static  Map<String, List<Database>> databaseList = new HashMap<>();
	private String  fileLocationOfExclusionList;
	private Set<String> exlusionList = new HashSet<>();
	private static final Logger LOG = LoggerFactory.getLogger(MergedJsonLevelDB.class);
	
	public static ConfigureDB cfgDb;
	private static String chunkId;

	/*
	 * Constructor to perform JSON merge
	 * 
	 * @param dbDir specifies the dir name where DB is stored
	 * 
	 * @param configDir specifies where the configuration has to be read from
	 * this directory contains join.csv and filter.csv
	 * 
	 * @param mainFile specifies the main file to be used to merge
	 * 
	 * @param files2bLoaded subtables to be used to merge
	 * 
	 */
	public MergedJsonLevelDB(String dbDir, String configDir, String mainFile, String files2bLoaded,
			String chunkId) {
		try {
			this.dbDir = dbDir;
			this.configDir = configDir;
			this.files2bLoaded = files2bLoaded;
			this.mainFile = mainFile.substring(0,mainFile.lastIndexOf('.'));
			this.chunkId = chunkId;

		} catch (Exception e) {
			LOG.error(e.getClass().getName() + EX_OCCURED, e);

		}
	}
	
	/*
	 * Constructor to perform JSON merge
	 * 
	 * @param dbDir specifies the dir name where DB is stored
	 * 
	 * @param configDir specifies where the configuration has to be read from
	 * this directory contains join.csv and filter.csv
	 * 
	 * @param mainFile specifies the main file to be used to merge
	 * 
	 * @param files2bLoaded subtables to be used to merge
	 * 
	 * @param fileLocationOfExclusionList list of luwids to be excluded
	 * 
	 */
	public MergedJsonLevelDB(String dbDir, String configDir, String mainFile, String files2bLoaded,
			String chunkId,String fileLocationOfExclusionList) {
		try {
			this.dbDir = dbDir;
			this.configDir = configDir;
			this.files2bLoaded = files2bLoaded;
			this.mainFile = mainFile.substring(0,mainFile.lastIndexOf('.'));
			this.chunkId = chunkId;
			this.fileLocationOfExclusionList=fileLocationOfExclusionList;
			this.exlusionList=getExclusionList();

		} catch (Exception e) {
			LOG.error(e.getClass().getName() + EX_OCCURED, e);

		}
	}
	
	/*
	 * Method to read exclusion list to file and store in Hashset
	 * 
	 */

	private Set<String> getExclusionList() {
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

	public PayloadObj formJson(String key) {
		if(exlusionList.contains(key)){
			LOG.error("INC1001: Subscriber is been excluded since present in exclusion list:"+key);
			return  new PayloadObj("{}", LUWID, key);
		}

		MessagingService msgService = new MessagingService(Thread.currentThread().getName());
		String mainJson = msgService.combineIntoOneJson(databaseList.get(this.mainFile), key);
		Evaluate evaluate = new Evaluate(filterMap.get(this.mainFile), mainJson, this.mainFile);
		JSONArray jsonArra = evaluate.evaluateFilterCondition();
		if(jsonArra.length()==0){
			return new PayloadObj(null, LUWID, key);
			
		}
		JSONObject jsonObj = new JSONObject();
		mainJson = jsonObj.put(this.mainFile, jsonArra).toString();
		Map<String, String> mapOfTableData = new HashMap<>();
		if (mainJson == null) {
			return  new PayloadObj(mainJson, LUWID, key);
			

		}
		mapOfTableData.put(this.mainFile, mainJson);
		mainJson = mainJson.substring(1, mainJson.length() - 1);
		String luwId = key;

		mainJson = "{\"chunk_id\":".concat("\"").concat(chunkId).concat("\",").concat("\"luw_id\":").concat("\"")
				.concat(luwId).concat("\",").concat("\"BODY\":{").concat(mainJson);

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
				LOG.error("formJson" ,e);

			}
		}
		mainJson = mainJson + "}}";
		return new PayloadObj(mainJson, "luw_id", luwId);

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

	/*
	 * Method to initialize level db which is already created during
	 * InputFile2LevelDb operation
	 * 
	 * @returns Map with list of databases
	 * 
	 */
	public static void initializeDB() {
		cfgDb = new ConfigureDB(dbDir);
		cfgDb.initialize();
		databaseList= cfgDb.getDbConfigMap();
	}

	
	/*
	 * Method to read join and filter configuration
	 * 
	 */

	public void readConfiguration() {
		readFilterConfiguraion();
		readJoinConfiguration();
		prepareOtherTablesList();
	}

	private void prepareOtherTablesList() {
		String[] array = files2bLoaded.split(",");
		String mainTable = mainFile.replaceAll(".csv", "").replaceAll(".txt", "");
		for (String str : array) {
			str = str.replaceAll(".csv", "").replaceAll(".txt", "");
			if (!str.equals(mainTable)) {
				listOfOtherTables.add(str);
			}
		}
		
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
			LOG.error("IOException",e);
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
			LOG.error("IOException 2",e);
		}
	}

	/*
	 * Method to close db and destroy static vars
	 * 
	 */
	public static void destroy(){
		cfgDb.destroy();
		joinMap.clear();
		filterMap.clear();
	}
	
	/*
	 * Method which returns the all the keys of the main table
	 * 
	 * @return set of all keys from main table
	 */
	public Set<String> getAllKeys(String tableName) {
		Set<String> keyList = new HashSet<>();
		if (databaseList != null) {
			for (Database database : databaseList.get(tableName)) {
				keyList.addAll(database.getKeySet());
			}
		}
		return keyList;
		
	}
	

	public List<String> getListOfOtherTables() {
		return listOfOtherTables;
	}

	public void setListOfOtherTables(List<String> listOfOtherTables) {
		this.listOfOtherTables = listOfOtherTables;
	}

	public static void main(String args[]) {

		String home = "C:/Users/egjklol/Desktop/src/";
		String dbDir = home + "/database/test";
		String configDir = home + "/config";
		String mainFile = "SUBSCRIBER.csv";
		String files2bLoaded = "SUBSCRIBER.csv,PRODUCT.csv,BUCKET_BALANCE.csv,BUCKET_COUNTER.csv";

		MergedJsonLevelDB obj = new MergedJsonLevelDB(dbDir, configDir,
				mainFile, files2bLoaded, "test_chulkId");
		obj.initializeDB();
		obj.readConfiguration();
		String mainTable =  mainFile.substring(0,mainFile.lastIndexOf('.'));
		
		
		Set<String> mainTableKeys = obj.getAllKeys(mainTable);
		
		for (String key : mainTableKeys) {
			PayloadObj payload = obj.formJson(key);
			System.out.println(payload);
			
			
		}
		obj.destroy();

	}

}
