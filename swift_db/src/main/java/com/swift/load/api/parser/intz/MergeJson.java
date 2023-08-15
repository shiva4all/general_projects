package com.swift.load.api.parser.intz;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPOutputStream;


import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface MergeJson {
	static final Logger LOG = LoggerFactory.getLogger(MergeJson.class);
	public static final class CONST {
		private CONST() {
		}

		public static final Random objGenerator = new Random();
	}

	public default void mergeJsonFiles(String mainFile, String files2bLoaded, String dbPath, String outputFile)
			throws IOException {
		final DB mapDB = DBMaker.fileDB(new File(dbPath)).fileMmapEnable().closeOnJvmShutdown().concurrencyScale(20)
				.make();
		GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(outputFile)));
		try (BufferedWriter jsonOutputWriter = new BufferedWriter(
				new OutputStreamWriter(zip, StandardCharsets.UTF_8))) {
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
				Integer luwId = CONST.objGenerator.nextInt(10000000);
				mainJson = "{\"luw_id\":".concat("\"").concat(luwId.toString()).concat("\",").concat("\"BODY\":{")
						.concat(mainJson);
				// Get all children Json
				// integer index := 0
				for (String subTableName : listOfOtherTables) {
				//	try (HTreeMap<String, String> subTable = mapDB.hashMap(subTableName)
							//.keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen()) 
                    try{
                    	
                    	
                    	HTreeMap<String, String> subTable = mapDB.hashMap(subTableName)
								.keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen();
                    	
						String subJson = subTable.get(key);
						if (subTable.containsKey(key) && subJson != null && subJson.length() > 0)
						{
							subJson = subJson.substring(1, subJson.length() - 1);
							mainJson = new StringBuilder(mainJson).append(",").append(subJson).toString();
						}
					}
                    catch(Exception e)
					{
						LOG.error("Exception",e);
					}
				}
				mainJson = mainJson + "}}";
				jsonOutputWriter.write(mainJson);
				jsonOutputWriter.newLine();
			}

		}
		mapDB.close();
	}

}
