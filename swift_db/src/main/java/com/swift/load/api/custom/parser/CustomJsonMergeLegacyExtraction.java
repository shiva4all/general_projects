package com.swift.load.api.custom.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomJsonMergeLegacyExtraction {

	private static final Logger LOG = LoggerFactory.getLogger(CustomJsonMergeLegacyExtraction.class);

	public static final String EX_OCCURED = " occured";



	public List<PayloadObj> mergeJsonFiles(String mainFile, String files2bLoaded, String dbPath, String chunkId) throws IOException {

		List<PayloadObj> jsonList = Collections.synchronizedList(new ArrayList<PayloadObj>());

		final DB mapDB = DBMaker.fileDB(new File(dbPath)).fileMmapEnable().closeOnJvmShutdown().concurrencyScale(20)
				.make();

		final String tableName = mainFile.replaceAll(".csv", "").replaceAll(".txt", "");
		String[] array = files2bLoaded.split(",");
		List<String> listOfOtherTables = new ArrayList<>();
		for (String str : array) {
			str = str.replaceAll(".csv", "").replaceAll(".txt", "");
			if (!str.equals(tableName)) {
				listOfOtherTables.add(str);
			}
		}

		final HTreeMap<String, String> mainTable = mapDB.hashMap(tableName).keySerializer(Serializer.STRING)
				.valueSerializer(Serializer.STRING).createOrOpen();
		Set<String> mainTableKeys = mainTable.keySet();
		for (String key : mainTableKeys) {
			String mainJson = mainTable.get(key);
			mainJson = mainJson.substring(1, mainJson.length() - 1);
			String luwId = key;
			
			mainJson = "{\"chunk_id\":".concat("\"").concat(chunkId).concat("\",").concat("\"luw_id\":").concat("\"").concat(luwId).concat("\",").concat("\"BODY\":{")
					.concat(mainJson);
			// Get all children Json
			// integer index := 0
			for (String subTableName : listOfOtherTables) {

				try {

					HTreeMap<String, String> subTable = mapDB.hashMap(subTableName).keySerializer(Serializer.STRING)
							.valueSerializer(Serializer.STRING).createOrOpen();

					String subJson = subTable.get(key);
					if (subTable.containsKey(key) && subJson != null && subJson.length() > 0) {
						subJson = subJson.substring(1, subJson.length() - 1);
						mainJson = new StringBuilder(mainJson).append(",").append(subJson).toString();
					}
				} catch (Exception e) {
					LOG.error("Exception in CustomJsonMergeLegacyExtraction",e);
				}
			}
			mainJson = mainJson + "}}";

			PayloadObj payloadObj = new PayloadObj(mainJson, "luw_id", luwId);
			jsonList.add(payloadObj);

		}

		mapDB.close();
		return jsonList;
	}
}
