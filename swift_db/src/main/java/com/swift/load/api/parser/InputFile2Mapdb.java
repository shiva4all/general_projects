package com.swift.load.api.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.swift.load.api.custom.parser.CustomJsonMergeLegacyExtraction;
import com.swift.load.api.custom.parser.PayloadObj;
import com.google.common.collect.Lists;

public class InputFile2Mapdb {
	protected static DB mapDbDatabase;
	private String sdpId;
	protected static Map<String, Map<String, List<String>>> entityColumnsMap;
	private String dumpFilePath;
	private String schemaFilePath;
	private String mapdbPath;
	private static final Logger LOG = LoggerFactory.getLogger(InputFile2Mapdb.class);
	private static final String COLUMNS = "columns";
	private static final String INDEXES = "indexes";

	public InputFile2Mapdb(String dumpFilePath, String schemaFilePath, String mapdbPath, String sdpId) {
		this.sdpId = sdpId;
		this.dumpFilePath = dumpFilePath;
		this.schemaFilePath = schemaFilePath;
		this.mapdbPath = mapdbPath;
		createFileCacheMap(this.schemaFilePath, this.mapdbPath, this.sdpId);
	}

	private static void createFileCacheMap(String schemaFilePathArg, String mapdbPathArg, String sdpIdArg) {

		entityColumnsMap = parseSchemaCsvFiles(schemaFilePathArg);
		mapDbDatabase = DBMaker.fileDB(new File(mapdbPathArg + "/db_" + sdpIdArg)).fileMmapEnable().closeOnJvmShutdown()
				.concurrencyScale(20).make();
	}

	protected static void loadIntoMapDB(DB db, String entity, File fullFile) throws IOException {


		HTreeMap<String, String> hTree = db.hashMap(entity).keySerializer(Serializer.STRING)
				.valueSerializer(Serializer.STRING).createOrOpen();
		// LOG.info("Entity: "+entity)

		for (Map.Entry<String, Map<String, List<String>>> entry : entityColumnsMap.entrySet()) {

			Map<String, List<String>> map1 = entry.getValue();
			for (Map.Entry<String, List<String>> e2 : map1.entrySet()) {

				List<String> lst = e2.getValue();

			}
		}

		List<String> columnList = entityColumnsMap.get(entity).get(COLUMNS);

		List<String> indexList = entityColumnsMap.get(entity).get(INDEXES);

		String sep = entityColumnsMap.get(entity).get("seperator").get(0);
		// BufferedReader bufferedReader = new BufferedReader(new FileReader(fullFile))
		try (BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(fullFile), StandardCharsets.ISO_8859_1))) {
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				// LOG.info(line)
				String normalized = Normalizer.normalize(line, Normalizer.Form.NFD);
				String accentRemoved = normalized.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
				// LOG.info(accentRemoved)

				String[] arr = accentRemoved.split(sep, -30);


				String dataIndex = "";
				for (String index : indexList) {
					int indxNumber = columnList.indexOf(index);
					if (indxNumber > -1) {
						dataIndex = new StringBuilder().append(arr[indxNumber]).append(dataIndex).toString();
					}
				}
				// For Json
				if (hTree.containsKey(dataIndex)) {

					String temp = hTree.get(dataIndex);
					temp = temp.replace("]}", ",");
					String xml = temp + formJson(columnList, arr, entity) + "]}";
					hTree.put(dataIndex, xml);
				} else {

					hTree.put(dataIndex,
							"{" + '"' + entity.toUpperCase() + '"' + ":[" + formJson(columnList, arr, entity) + "]}");
				}
			}
		}

	}

	private static String formJson(List<String> columnList, String[] data, String entity) {

		if (columnList.size() < data.length) {
			LOG.error("Wrong format of file in sdp schema " +entity);
			// LOG.error(columnList + "----" + data[0])
		}
		StringBuilder sb = new StringBuilder("{");
		int index = 0;
		for (String column : columnList) {
			if (index < data.length) {
				sb.append('"').append(column).append('"').append(":").append('"').append(data[index]).append('"');
				if (index == data.length - 1) {
					// dont append here
				} else {
					sb.append(",");
				}
			}
			index++;

		}
		sb.append("}");
		return sb.toString();
	}

	private static Map<String, Map<String, List<String>>> parseSchemaCsvFiles(String schemaFilePathArg) {
		Map<String, Map<String, List<String>>> mapOfTablesAndColumns = new LinkedHashMap<>();
		File dir = new File(schemaFilePathArg);

		FilenameFilter filter = new MyFilenameFilter(new String[] { "tabledef" }, new String[] { ".csv" });
		List<File> list = Arrays.asList(dir.listFiles(filter));
		for (File file : list) {
			parseSchemaCsvFile(file, mapOfTablesAndColumns);
		}

		return mapOfTablesAndColumns;
	}

	private static void parseSchemaCsvFile(File fileArg,
			Map<String, Map<String, List<String>>> mapOfTablesAndColumnsArg) {
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(fileArg))) {
			String line = "";
			Map<String, List<String>> columnsAndIndexes = new HashMap<>();
			while ((line = bufferedReader.readLine()) != null) {
				parseSchemaCsvLine(line, columnsAndIndexes);
			}
			String tableName = fileArg.getName();
			mapOfTablesAndColumnsArg.put(tableName.replace(".csv", "").replace("tabledef_", ""), columnsAndIndexes);
		} catch (IOException e) {
			LOG.error(e.getClass().getName(), e);
		}
	}

	private static void parseSchemaCsvLine(String line, Map<String, List<String>> columnsAndIndexes) {
		if (line.startsWith("seperator;")) {
			String[] temp = line.split(";", -1);
			List<String> sep = new ArrayList<>();
			if (temp[1] != null && temp[1].length() > 0) {
				sep.add(temp[1]);
			} else {
				sep.add(";");
			}
			// LOG.info("SEPRAROTm "+sep)

			columnsAndIndexes.put("seperator", sep);
		}
		if (line.startsWith("c;")) {
			String[] temp = line.split(";");
			if (columnsAndIndexes.containsKey(COLUMNS)) {
				List<String> list1 = columnsAndIndexes.get(COLUMNS);
				list1.add(temp[1]);

				columnsAndIndexes.put(COLUMNS, list1);
			} else {
				List<String> list1 = new ArrayList<>();
				list1.add(temp[1]);
				columnsAndIndexes.put(COLUMNS, list1);
			}
		} else if (line.startsWith("i;")) {
			String[] temp = line.split(";");
			if (columnsAndIndexes.containsKey(INDEXES)) {
				List<String> list1 = columnsAndIndexes.get(INDEXES);
				list1.add(temp[1]);

				columnsAndIndexes.put(INDEXES, list1);
			} else {
				List<String> list1 = new ArrayList<>();
				list1.add(temp[1]);
				columnsAndIndexes.put(INDEXES, list1);
			}
		}
	}

	public Map<String, Map<String, List<String>>> parseSchemaCsvFiles() {
		return parseSchemaCsvFiles(this.schemaFilePath);
	}

	/*
	 * @Params
	 * 
	 */
	public static boolean execute(String dumpFilePath, String schemaFilePath, String filesToBeInserted,
			String mapdbPath, String sdpId) {

		InputFile2Mapdb obj = new InputFile2Mapdb(dumpFilePath, schemaFilePath, mapdbPath, sdpId);
		File folder = new File(obj.dumpFilePath);
		ArrayList<String> listOfFilesToBeInserted = new ArrayList<>(Arrays.asList(filesToBeInserted.split(",")));

		int cores = Runtime.getRuntime().availableProcessors();

		if (listOfFilesToBeInserted.size() <= cores) {
			if (listOfFilesToBeInserted.size() == 1) {
				cores = 2;

			} else {
				cores = listOfFilesToBeInserted.size();
			}
		}


		ExecutorService executor = Executors.newFixedThreadPool(cores - 1);
		FilenameFilter filter = new MyFilenameFilter(null, new String[] { ".csv", ".txt" });
		List<File> listOfFiles = new ArrayList<>(Arrays.asList(folder.listFiles(filter)));
		List<List<String>> smallerLists = Lists.partition(listOfFilesToBeInserted,
				listOfFilesToBeInserted.size() / (cores - 1));


		for (int i = 0; i < smallerLists.size(); i++) {

			Runnable worker = new WorkerThreadForMapdb(listOfFiles, smallerLists.get(i));
			executor.execute(worker);
		}
		executor.shutdown();
		while (!executor.isTerminated())
			;
		listOfFilesToBeInserted.clear();
		listOfFiles.clear();
		mapDbDatabase.commit();
		mapDbDatabase.close();
		smallerLists.clear();
		entityColumnsMap.clear();
		entityColumnsMap = null;
		return true;

	}

	public static void main(String[] args) {

		String home = "C:/Partha/CS2BEAM/extractorComponentLibrary/src";
		String dumpFilePath = home + "/Input";
		String schemFilePath = dumpFilePath + "/Schema";
		String files2BeLoaded = "SUBSCRIBER.csv,PRODUCT.csv,BUCKET_BALANCE.csv,BUCKET_COUNTER.csv";
		String generatedString = (UUID.randomUUID()).toString();
		String dbname = "database_" + generatedString;
		String dbpath = home + "/database";

		try {
			InputFile2Mapdb.execute(dumpFilePath, schemFilePath, files2BeLoaded, dbpath, dbname);
			CustomJsonMergeLegacyExtraction obj = new CustomJsonMergeLegacyExtraction();

			List<PayloadObj> output = obj.mergeJsonFiles("SUBSCRIBER.csv", "SUBSCRIBER.csv,PRODUCT.csv,BUCKET_BALANCE.csv,BUCKET_COUNTER.csv", dbpath + "/db_" + dbname, "test_chulkId-001");
			//System.out.println("total number of records == " + output.size())
			for (PayloadObj data : output) {
				System.out.println("luw_id == " + data.getPropertiesMap().get("luw_id") + "\nPayload == " + data.getJsonPayload() + "\n");
			}
			// obj.mergeJsonFiles("SUBSCRIBER.csv", "SUBSCRIBER.csv", dbpath +
			// "\\db_database", home + "\\jsonInput.gz")

		} catch (IOException e) {
			//e.printStackTrace()
			LOG.error(e.getLocalizedMessage());
		}

	}

}
