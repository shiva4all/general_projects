package com.swift.load.api.custom.parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class CustomJsonMerge {

	private static final Logger LOG = LoggerFactory.getLogger(CustomJsonMerge.class);
	public static final String DATE_19700101 = "01/01/1970";
	public static final String EX_OCCURED = " occured";

	protected static final Set<String> status4Msisdn = new HashSet<>();
	protected static final Set<String> prepaidMsisdn = new HashSet<>();
	protected static final Set<String> excludeMsisdn = new HashSet<>();

	private Random objGenerator = new Random();

	private CustomJsonMerge() {
		try {
			this.objGenerator = SecureRandom.getInstanceStrong();
		} catch (NoSuchAlgorithmException e) {
			LOG.error(e.getClass().getName() + EX_OCCURED, e);
		}
	}

	public void mergeJsonFiles(String mainFile, String files2bLoaded, String dbPath, String outputFile, String logpath,
			String excludeMsisdnFile) throws IOException {
		List<String> rejecteddata = null;
		String line = "";
		try (final DB mapDB = DBMaker.fileDB(new File(dbPath)).fileMmapEnable().closeOnJvmShutdown()
				.concurrencyScale(20).make();
				BufferedReader br = new BufferedReader(new FileReader(excludeMsisdnFile));
				GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(outputFile)));
				BufferedWriter jsonOutputWriter = new BufferedWriter(
						new OutputStreamWriter(zip, StandardCharsets.UTF_8))) {
			while ((line = br.readLine()) != null) {
				excludeMsisdn.add("47".concat(line));
			}
			final String tableName = mainFile.replaceAll(".csv", "").replaceAll(".txt", "");
			String[] array = files2bLoaded.split(",");
			List<String> listOfOtherTables = new ArrayList<>();
			for (String str : array) {
				str = str.replaceAll(".csv", "").replaceAll(".txt", "");
				if (!str.equals(tableName)) {
					listOfOtherTables.add(str);
				}
			}
			rejecteddata = processMainAndOtherTables(mapDB, tableName, listOfOtherTables, jsonOutputWriter);
		}
		if (!rejecteddata.isEmpty()) {
			try (BufferedWriter bw = new BufferedWriter(new FileWriter(logpath))) {
				for (String data : rejecteddata) {
					bw.write(data + "\n");
				}
				// bw.close()
			} catch (IOException e) {
				LOG.error(e.getClass().getName() + EX_OCCURED, e);
			}
		}
	}

	private List<String> processMainAndOtherTables(final DB mapDB, final String tableName,
			List<String> listOfOtherTables, BufferedWriter jsonOutputWriter) throws IOException {
		List<String> rejecteddata = new ArrayList<>();
		try (final HTreeMap<String, String> mainTable = mapDB.hashMap(tableName).keySerializer(Serializer.STRING)
				.valueSerializer(Serializer.STRING).createOrOpen()) {
			Set<String> mainTableKeys = mainTable.keySet();
			for (String key : mainTableKeys) {
				String mainJson = mainTable.get(key);
				boolean status4flag = mainJson.contains("\"STATUS\":\"4\"");
				status4flag = mainJson.contains("\"STATUS\":\"0\"");
				if (status4flag) {
					status4Msisdn.add(key);
				} else {
					prepaidMsisdn.add(key);
				}
			}
			for (String key : mainTableKeys) {
				String mainJson = mainTable.get(key);
				JSONObject mainJsonObject = new JSONObject(mainJson);
				JSONArray mainJsonArray = (JSONArray) mainJsonObject.get("TELENOR_SUBSCRIBER");
				mainJsonObject = mainJsonArray.getJSONObject(0);
				String status = mainJsonObject.getString("STATUS");
				String validdateBegin = mainJsonObject.getString("Validdatebegin");
				String validdateEnd = mainJsonObject.getString("ValiddateEnd");
				String activeBeginDate = mainJsonObject.getString("ACTIVE_BEGIN_DATE");
				String activeEndDate = mainJsonObject.getString("Active_End_Date");
				boolean reject = processOtherSubTablesRules(status, activeBeginDate, activeEndDate, validdateBegin,
						validdateEnd);
				mainJson += processOtherSubTablesFaf2(mapDB, key, status, reject, listOfOtherTables, jsonOutputWriter,
						rejecteddata);
			}
		}
		return rejecteddata;
	}

	private boolean processOtherSubTablesRules(String status, String activeBeginDate, String activeEndDate,
			String validdateBegin, String validdateEnd) {
		boolean reject = false;
		if (status.equals("2")) {
			reject = processOtherSubTablesRulesFor2(activeBeginDate, activeEndDate);
		} else if (status.equals("1")) {
			reject = processOtherSubTablesRulesFor1(activeBeginDate, activeEndDate, validdateBegin, validdateEnd);
		} else {
			reject = true;
		}
		return reject;
	}

	private boolean processOtherSubTablesRulesFor1(String activeBeginDate, String activeEndDate, String validdateBegin,
			String validdateEnd) {
		boolean reject = false;
		if (activeBeginDate != null && !activeBeginDate.equals(DATE_19700101) && activeBeginDate.length() > 0
				&& !activeBeginDate.equals("NaN")) {
			reject = true;
		} else if (validdateBegin != null && !validdateBegin.equals(DATE_19700101) && validdateBegin.length() > 0
				&& !validdateBegin.equals("NaN")) {
			reject = true;
		} else {
			reject = false;
		}
		if (activeEndDate != null && !activeEndDate.equals(DATE_19700101) && activeEndDate.length() > 0
				&& !activeEndDate.equals("NaN")) {
			// reject := reject && true
		} else if (validdateEnd != null && !validdateEnd.equals(DATE_19700101) && validdateEnd.length() > 0
				&& !validdateEnd.equals("NaN")) {
			// reject := reject && true
		} else {
			reject = false;
		}
		return reject;
	}

	private boolean processOtherSubTablesRulesFor2(String activeBeginDate, String activeEndDate) {
		boolean reject = false;
		if (activeBeginDate != null && !activeBeginDate.equals(DATE_19700101) && activeBeginDate.length() > 0
				&& !activeBeginDate.equals("NaN")) {
			reject = true;
		} else {
			reject = false;
		}
		if (activeEndDate != null && !activeEndDate.equals(DATE_19700101) && activeEndDate.length() > 0
				&& !activeEndDate.equals("NaN")) {
			// reject := reject && true
		} else {
			reject = false; // reject := reject && false
		}
		return reject;
	}

	private String processOtherSubTablesFaf2(final DB mapDB, String key, String status, boolean reject,
			List<String> listOfOtherTables, BufferedWriter jsonOutputWriter, List<String> rejecteddata)
			throws IOException {
		String mainJsonAdd = "";
		if (reject) {
			if (!excludeMsisdn.contains(key)) {
				if (!status.equals("4") && !status.equals("0")) {
					mainJsonAdd = processOtherAndPclSubTableFaf2(mapDB, mainJsonAdd, key, listOfOtherTables,
							jsonOutputWriter);
				} else {
					rejecteddata.add("INC01: MSISDN REJECTED DUE TO STATUS=4 or 0, MSISDN=" + key);
				}
			} else {
				rejecteddata.add("INC01: MSISDN REJECTED DUE TO EXCLUDE LIST, MSISDN=" + key);
			}
		} else {
			rejecteddata.add("INC01: MSISDN REJECTED DUE TO EXPIRED STATE, MSISDN=" + key);
		}
		return mainJsonAdd;
	}

	private String processOtherAndPclSubTableFaf2(final DB mapDB, String mainJsonArg, String key,
			List<String> listOfOtherTables, BufferedWriter jsonOutputWriter) throws IOException {
		String mainJson = mainJsonArg;
		mainJson = mainJson.substring(1, mainJson.length() - 1);
		Integer luwId = objGenerator.nextInt(10000000);
		mainJson = "{\"luw_id\":".concat("\"").concat(luwId.toString()).concat("\",").concat("\"BODY\":{")
				.concat(mainJson);
		// Get all children Json
		// integer index := 0
		Set<String> listpclname = new HashSet<>();
		for (String subTableName : listOfOtherTables) {
			mainJson = processOtherSubTableFaf2(mapDB, mainJson, listpclname, subTableName, key);
		}
		mainJson = mainJson + ",\"TELENOR_FAF_2\":[";
		Integer pclcounter = 0;
		for (String pclname : listpclname) {
			Object[] result = processPclSubTableFaf2(mapDB, mainJson, pclname, pclcounter);
			mainJson = (String) result[0];
			pclcounter = (Integer) result[1];
		}
		mainJson = mainJson + "]}}";
		jsonOutputWriter.write(mainJson);
		jsonOutputWriter.newLine();
		return mainJson;
	}

	private String processOtherSubTableFaf2(final DB mapDB, String mainJsonArg, Set<String> listpclname,
			String subTableName, String key) {
		String mainJson = mainJsonArg;
		if (!subTableName.equals("Telenor_Faf_2")) {
			try (HTreeMap<String, String> subTable = mapDB.hashMap(subTableName).keySerializer(Serializer.STRING)
					.valueSerializer(Serializer.STRING).createOrOpen()) {
				String subJson = subTable.get(key);
				String origsubJson = subJson;
				if (subTable.containsKey(key) && subJson != null && subJson.length() > 0) {
					subJson = subJson.substring(1, subJson.length() - 1);
					mainJson = new StringBuilder().append(mainJson).append(",").append(subJson).toString();
				}
				if (subTableName.equals("Telenor_Faf") && origsubJson != null && origsubJson.length() > 0) {
					JSONObject jsonObject = new JSONObject(origsubJson);
					JSONArray jsonarray = (JSONArray) jsonObject.get("TELENOR_FAF");
					for (int i = 0; i < jsonarray.length(); i++) {
						JSONObject jsonobject = jsonarray.getJSONObject(i);
						String pclname = jsonobject.getString("PCLNAME");
						listpclname.add(pclname);
					}
				}
			}
		}
		return mainJson;
	}

	private Object[] processPclSubTableFaf2(final DB mapDB, String mainJsonArg, String pclname, Integer pclcounterArg) {
		String mainJson = mainJsonArg;
		Integer pclcounter = pclcounterArg;
		try (HTreeMap<String, String> subTable = mapDB.hashMap("Telenor_Faf_2")
				.keySerializer((Serializer<String>) Serializer.STRING)
				.valueSerializer((Serializer<String>) Serializer.STRING).open()) {
			String subJson = subTable.get(pclname);
			JSONObject jsonObject = new JSONObject(subJson);
			JSONArray jsonarray = (JSONArray) jsonObject.get("TELENOR_FAF_2");
			int counter = 0;
			for (int i = 0; i < jsonarray.length(); i++) {
				JSONObject jobj = jsonarray.getJSONObject(i);
				if (!status4Msisdn.contains(jobj.get("ID"))) {
					String type = (String) jobj.get("Subscriber_type");
					if (("prepaid".equalsIgnoreCase(type) && prepaidMsisdn.contains(jobj.get("ID")))
							|| "postpaid".equalsIgnoreCase(type)) {
						String subjson = jobj.toString();
						if (counter == 0 && pclcounter == 0) {
							mainJson = new StringBuilder().append(mainJson).append(subjson).toString();
						} else {
							mainJson = new StringBuilder().append(mainJson).append(",").append(subjson).toString();
						}
						counter++;
						pclcounter++;
					}
				}
			}
		}
		return new Object[] { mainJson, pclcounter };
	}

	public void logOrphonData(String dbPath, String logpath) {
		final DB mapDB = DBMaker.fileDB(new File(dbPath)).fileMmapEnable().closeOnJvmShutdown().concurrencyScale(20)
				.make();
		try (final HTreeMap<String, String> subscriberTable = mapDB.hashMap("Telenor_Subscriber")
				.keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen()) {

			try (BufferedWriter bwOrphonOffer = new BufferedWriter(new FileWriter(logpath + "/Offers_Orphan.log"));
					final HTreeMap<String, String> offerTable = mapDB.hashMap("Telenor_Offers")
							.keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen()) {
				for (String key : offerTable.getKeys()) {
					if (!subscriberTable.containsKey(key)) {
						bwOrphonOffer.write(key + "\n");
					}
				}
			} catch (IOException e) {
				LOG.error(e.getClass().getName() + EX_OCCURED, e);
			}

			try (BufferedWriter bwDMO = new BufferedWriter(new FileWriter(logpath + "/DMO_Orphan.log"));
					final HTreeMap<String, String> dmotable = mapDB.hashMap("Telenor_DMO")
							.keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen()) {
				for (String key : dmotable.getKeys()) {
					if (!subscriberTable.containsKey(key)) {
						bwDMO.write(key + "\n");
					}
				}
			} catch (IOException e) {
				LOG.error(e.getClass().getName() + EX_OCCURED, e);
			}

			try (BufferedWriter bwFaf = new BufferedWriter(new FileWriter(logpath + "/Faf_Orphan.log"));
					final HTreeMap<String, String> faftable = mapDB.hashMap("Telenor_Faf")
							.keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen()) {
				for (String key : faftable.getKeys()) {
					if (!subscriberTable.containsKey(key)) {
						bwFaf.write(key + "\n");
					}
				}
			} catch (IOException e) {
				LOG.error(e.getClass().getName() + EX_OCCURED, e);
			}
			mapDB.close();
		}

	}

	public static void main(String[] args) {
		CustomJsonMerge obj = new CustomJsonMerge();
		try {
			obj.mergeJsonFiles("Telenor_Subscriber.txt",
					"Telenor_Subscriber.txt,Telenor_Offers.txt,Telenor_DMO.txt,Telenor_Faf.txt,Telenor_Faf_2.txt",
					"C:\\Users\\egjklol\\Projects\\TelenorNorway\\telenor_norway\\dev\\src\\database\\db_map",
					"C:\\Users\\egjklol\\Projects\\TelenorNorway\\telenor_norway\\dev\\src\\Working\\jsonInput.gz",
					"C:\\Users\\egjklol\\Projects\\TelenorNorway\\telenor_norway\\dev\\src\\logs\\Rejected.log",
					"C:/Users/egjklol/Projects/TelenorNorway/telenor_norway/dev/src/Input/exclude_msisdn.txt");
			// obj.logOrphonData(dbPath, logpath)
		} catch (IOException e) {
			LOG.error(e.getClass().getName() + EX_OCCURED, e);
		}

	}

}
