package com.swift.load.api.parser;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import static com.swift.load.api.utils.Constants.*;
import com.google.common.collect.Lists;

public class InputFile2LevelDBLegacyExtractor {
	protected Map<String, Map<String, List<String>>> entityColumnsMap;
	protected String dumpFilePath;
	protected String schemaFilePath;
	protected String dbPath;
	protected String filesToBeInserted;
	protected String configDir;
	protected String quoteSupress;
	
	private static final Logger LOG = LoggerFactory.getLogger(InputFile2LevelDBLegacyExtractor.class);
	private static final String COLUMNS = "columns";
	private static final String COLUMNS_TO_IGNORE = "columns_to_ignore";
	private static final String COLUMNS_INDEXES_TO_IGNORE = "columns_indexes_to_ignore";
	private static final String INDEXES = "indexes";
	private static final String SIZE = "split_size";
	private static final Map<String, List<String>> entityMap = new ConcurrentHashMap<>();
	protected Date migrationDate;
	protected String dupEntity;
	protected String dupCheck;
	protected static String chunkId;



/*
 * Constructor to do load leveldb configuration
 * @param dumpFilePath input dump file path
 * @param schemaFilePath path of the schema file, to know how to write schema fle refer documentation
 * @param filesToBeInserted list of files to be inserted
 * @param dbPath path where DB has to be created.
 */
	public InputFile2LevelDBLegacyExtractor(String dumpFilePath, String schemaFilePath, String filesToBeInserted, String dbPath, String dupEntity, String dupCheck, String chunkId, String quoteSupress) {
		this.dumpFilePath = dumpFilePath;
		this.schemaFilePath = schemaFilePath;
		this.filesToBeInserted = filesToBeInserted;
		this.dbPath = dbPath;
		this.dupEntity=dupEntity;
		this.dupCheck=dupCheck;
		this.chunkId=chunkId;
		this.quoteSupress=quoteSupress;
		
		createFileCacheMap(this.schemaFilePath);
	}

	private void createFileCacheMap(String schemaFilePathArg) {
		entityColumnsMap = parseSchemaCsvFiles(schemaFilePathArg);

	}

	protected void loadIntoLevelDB(String entity, File fullFile, String dbpath) throws IOException {

		DB levelDBStore;
		Options options = new Options();
		options.createIfMissing();
		options.blockSize(4194304);
		options.blockRestartInterval(64);
		options.cacheSize(41943040);
		options.maxOpenFiles(1000);
		options.writeBufferSize(134217728);
		options.compressionType(CompressionType.SNAPPY);
		Map<String, String> memory = new HashMap<>(100000, 0.75f);

		levelDBStore = factory.open(new File(dbpath + "/" + entity), options);

		int counter = 0;
		List<String> columnList = entityColumnsMap.get(entity).get(COLUMNS);
		List<String> indexList = entityColumnsMap.get(entity).get(INDEXES);
		int size = Integer.parseInt(entityColumnsMap.get(entity).get(SIZE).get(0));
		List<String> columns2Ignore = entityColumnsMap.get(entity).get(COLUMNS_TO_IGNORE);
		String sep = entityColumnsMap.get(entity).get("seperator").get(0);
		
		sep = "\\" + sep;
		int indxCounter = 1;	
		int datacounter = 0;
		
		Map<String, Integer> dupIndexs = new HashMap<>(); 
		Set<String> unqIndexes = new HashSet<>(100000, 0.75f);
		
		
		
		List<String> entityList = new ArrayList<>();
		entityList.add(entity);
		try (BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(fullFile), StandardCharsets.ISO_8859_1))) {
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				
				datacounter++;
				String[] arr = line.split(sep, -30);
				
				//-----------------------------------------------------------------------------------
				//LOG.info("arr == " + Arrays.toString(arr));
				
				if(quoteSupress.equalsIgnoreCase("yes")) {			
					for(int i=0; i<arr.length; i++) {
						String s = arr[i].substring(1, arr[i].length() - 1);
						arr[i] = s;
					}
				}
				
				//LOG.info("updated arr == " + Arrays.toString(arr));
				//-----------------------------------------------------------------------------------
				
				String dataIndex = "";

				if (counter != 0 && counter >= size) {
					WriteBatch wb = levelDBStore.createWriteBatch();
					for (java.util.Map.Entry<String, String> mem : memory.entrySet()) {
						wb.put(mem.getKey().getBytes(), mem.getValue().getBytes());
					}
					levelDBStore.write(wb);
					wb.close();
					memory.clear();
					counter = 0;
					levelDBStore.close();
					levelDBStore = factory.open(new File(dbpath + "/" + entity + "_" + indxCounter), options);
					entityList.add(entity + "_" + indxCounter);
					indxCounter++;

				}
				for (String index : indexList) {
					int indxNumber = columnList.indexOf(index);
					if (indxNumber > -1) {
						dataIndex = new StringBuilder().append(arr[indxNumber]).append(dataIndex).toString();
						if (dupCheck.equalsIgnoreCase("yes")) {
							if(dupEntity.equalsIgnoreCase(entity)) {
								
								if(unqIndexes.contains(dataIndex)) {
									if (dupIndexs.containsKey(dataIndex))
										dupIndexs.put(dataIndex, dupIndexs.get(dataIndex) + 1);
									else
										dupIndexs.put(dataIndex, 2);
								}
								unqIndexes.add(dataIndex);
							}
						}
					}
				}
				

				if (memory.containsKey(dataIndex)) {
					String temp = memory.get(dataIndex);
					temp = temp.replace("]}", ",");
					//String xml = temp.concat(formJson(columnList, arr, columns2Ignore)).concat("]}");
					String xml = temp.concat(formJson(columnList, arr, columns2Ignore, dataIndex)).concat("]}");
					memory.put(dataIndex, xml);
					

				} else {
					//String xml = "{\"".concat(entity).concat("\":[")
							//.concat(formJson(columnList, arr, columns2Ignore)).concat("]}");
					
					String xml = "{\"".concat(entity).concat("\":[")
						.concat(formJson(columnList, arr, columns2Ignore, dataIndex)).concat("]}");
				
					memory.put(dataIndex,xml );
				}

				counter++;
			}
			entityMap.put(entity, entityList);
		}
		
		

		WriteBatch wb = levelDBStore.createWriteBatch();
		for (java.util.Map.Entry<String, String> mem : memory.entrySet()) {
				
			wb.put(mem.getKey().getBytes(), mem.getValue().getBytes());
			
		}
		levelDBStore.write(wb);
		wb.close();
		
		LOG.info("INC1003 : Number of records for " + entity + " is : " + datacounter);
		//log KPI total data for each ennity
		this.logTotalNoOfRecordsKPIs(entity, datacounter,"Total Data loaded into leveldb Statistics.");
		
		if (unqIndexes.size() > 0 || dupIndexs.size() > 0) {
			
			LOG.info("INC1004 : Number of unique records for " + entity + " is : " + unqIndexes.size());
			LOG.info("INC1005 : Number of correct records for " + entity + " is : " + (unqIndexes.size() - dupIndexs.size()));
			LOG.info("INC1006 : Number of duplicate records for " + entity + " is : " + dupIndexs.size());
			
			for(java.util.Map.Entry<String, Integer> dupEntry : dupIndexs.entrySet()) {	
				//duplicate records count per key
				this.logKPIs(entity, datacounter,unqIndexes.size(),dupEntry.getKey(),dupEntry.getValue()-1, "Dublicate records with key Keys");
				LOG.error("INC1007 : Duplicate record for " + entity + " is with index : " + dupEntry.getKey() + " and number of entries for it is : " + dupEntry.getValue());
			}
		}
		
		
		
		memory.clear();
		levelDBStore.close();
		
	}

	private static String formJson(List<String> columnList, String[] data, List<String> columns2Ignore, String dataIndex) {
		StringBuilder sb = new StringBuilder("{");
		int index = 0;
		for (String column : columnList) {
			if (columns2Ignore == null || (columns2Ignore != null && !columns2Ignore.contains(column))) {
				if (index < columnList.size() && index < data.length) {
					sb.append('"').append(column).append('"').append(":").append('"').append(data[index]).append('"');

				}
				if (index == columnList.size() - 1) {
					// dont append here
				} else {
					sb.append(",");
				}
			}

			index++;

		}
		sb.append("}");
		String res = "";
		if (sb.toString().endsWith(",}")) {
			res = sb.toString().replace(",}", "}");
		} else {
			res = sb.toString();
		}
				
		String s2 = res.substring(res.indexOf("{") + 1);
		String s1 = "{\"chunk_id\":\"" + chunkId + "\",\"luw_id\":\""+ dataIndex + "\",";
		res = s1.concat(s2);
		
		return res;
	}
	
	private static String formJson(List<String> columnList, String[] data, List<String> columns2Ignore) {
		StringBuilder sb = new StringBuilder("{");
		int index = 0;
		for (String column : columnList) {
			if (columns2Ignore == null || (columns2Ignore != null && !columns2Ignore.contains(column))) {
				if (index < columnList.size() && index < data.length) {
					sb.append('"').append(column).append('"').append(":").append('"').append(data[index]).append('"');

				}
				if (index == columnList.size() - 1) {
					// dont append here
				} else {
					sb.append(",");
				}
			}

			index++;

		}
		sb.append("}");
		String res = "";
		if (sb.toString().endsWith(",}")) {
			res = sb.toString().replace(",}", "}");
		} else {
			res = sb.toString();
		}
		return res;
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
			int lineCounter = 0;
			Map<String, List<String>> columnsAndIndexes = new HashMap<>();
			while ((line = bufferedReader.readLine()) != null) {
				if (line.endsWith("i")) {
					parseSchemaCsvLine(line, columnsAndIndexes, lineCounter);
				} else {
					parseSchemaCsvLine(line, columnsAndIndexes, -1);
				}
				lineCounter += 1;
			}
			String tableName = fileArg.getName();
			mapOfTablesAndColumnsArg.put(tableName.replace(".csv", "").replace("tabledef_", ""), columnsAndIndexes);
		} catch (IOException e) {
			LOG.error(e.getClass().getName(), e);
		}
	}

	private static void parseSchemaCsvLine(String line, Map<String, List<String>> columnsAndIndexes, int columnIndex) {
		if (line.startsWith("seperator;")) {
			String[] temp = line.split(";", -1);
			List<String> sep = new ArrayList<>();
			if (temp[1] != null && temp[1].length() > 0) {
				sep.add(temp[1]);
			} else {
				sep.add(";");
			}
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
			if (temp[3].equalsIgnoreCase("i")) {

				if (columnsAndIndexes.containsKey(COLUMNS_INDEXES_TO_IGNORE)) {
					List<String> list1 = columnsAndIndexes.get(COLUMNS_INDEXES_TO_IGNORE);
					list1.add(columnIndex + "");
					columnsAndIndexes.put(COLUMNS_INDEXES_TO_IGNORE, list1);
				} else {
					List<String> list1 = new ArrayList<>();
					list1.add(columnIndex + "");
					columnsAndIndexes.put(COLUMNS_INDEXES_TO_IGNORE, list1);
				}
				if (columnsAndIndexes.containsKey(COLUMNS_TO_IGNORE)) {
					List<String> list1 = columnsAndIndexes.get(COLUMNS_TO_IGNORE);
					list1.add(temp[1] + "");
					columnsAndIndexes.put(COLUMNS_TO_IGNORE, list1);
				} else {
					List<String> list1 = new ArrayList<>();
					list1.add(temp[1] + "");
					columnsAndIndexes.put(COLUMNS_TO_IGNORE, list1);
				}
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

		} else if (line.startsWith("split_size;")) {
			String[] temp = line.split(";");
			if (columnsAndIndexes.containsKey(SIZE)) {
				List<String> list1 = columnsAndIndexes.get(SIZE);
				list1.add(temp[1]);
				columnsAndIndexes.put(SIZE, list1);
			} else {
				List<String> list1 = new ArrayList<>();
				list1.add(temp[1]);
				columnsAndIndexes.put(SIZE, list1);
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
	public boolean execute() {

		File folder = new File(dumpFilePath);
		ArrayList<String> listOfFilesToBeInserted = new ArrayList<>(Arrays.asList(filesToBeInserted.split(",")));

		int cores = Runtime.getRuntime().availableProcessors();
		if(listOfFilesToBeInserted.size()==1){
			cores = 2;
		}
		else if (listOfFilesToBeInserted.size() < cores) {
			cores = listOfFilesToBeInserted.size();
		}
		else{
			cores = Runtime.getRuntime().availableProcessors();
		}

		ExecutorService executor = Executors.newFixedThreadPool(cores - 1);
		FilenameFilter filter = new MyFilenameFilter(null, new String[] { ".csv", ".txt", ".unl" });
		List<File> listOfFiles = new ArrayList<>(Arrays.asList(folder.listFiles(filter)));
		List<List<String>> smallerLists = Lists.partition(listOfFilesToBeInserted,
				listOfFilesToBeInserted.size() / (cores - 1));
		for (int i = 0; i < smallerLists.size(); i++) {
			
			Runnable worker = new WorkerThreadForLevelDBLegacyExtractor(listOfFiles, smallerLists.get(i), dumpFilePath, schemaFilePath, dbPath, filesToBeInserted, dupEntity, dupCheck, chunkId, quoteSupress);
			executor.execute(worker);

		}
		executor.shutdown();
		while (!executor.isTerminated())
			;

		listOfFilesToBeInserted.clear();
		listOfFiles.clear();
		smallerLists.clear();
		entityColumnsMap.clear();
		entityColumnsMap = null;
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(dbPath + "/" + "database_info.txt"))) {
			for (java.util.Map.Entry<?, ?> entry : entityMap.entrySet()) {
				bw.write(entry.getKey().toString().concat("|")
						.concat(entry.getValue().toString().replace("[", "").replace("]", "").replaceAll(" ", ""))
						.concat("\n"));
			}

		} catch (IOException e) {
			LOG.error("IOException",e);
		}
		return true;

	}
	
	
	/**
	 * This method is used to generate logbased KPIS for number of records loaded into DB
	 * 
	 * @param entity
	 * @param totalRecords
	 * @param message
	 */
	private void logTotalNoOfRecordsKPIs(String entity,int totalRecords,String message) {
		//log messages for KPI
		MDC.clear();
		MDC.put(CHUNKID,chunkId);
		MDC.put(ENTITY,entity);
		MDC.put(LOGCODE,INPUT_TO_DB_TOTAL_DATA_STAT);
		MDC.put(TOTAL_RECORDS,String.valueOf(totalRecords));
		LOG.info(message);
		MDC.clear();
	}
	
	/**
	 * This method is used to generate number of duplicate keys and counts while loadeding data into DB
	 * 
	 * @param entity
	 * @param totalRecords
	 * @param uniqueRecords
	 * @param keys
	 * @param value
	 * @param message
	 */
	private void logKPIs(String entity,int totalRecords,int uniqueRecords,String keys,int value,String message) {
		//log messages for KPI
		MDC.clear();
		MDC.put(CHUNKID,chunkId);
		MDC.put(ENTITY,entity);
		MDC.put(LOGCODE,INPUT_TO_DB_DUPLICATE_DATA_STAT);
		MDC.put(TOTAL_RECORDS,String.valueOf(totalRecords));
		MDC.put(UNIQUE_RECORDS,String.valueOf(uniqueRecords));
		MDC.put(DUPLICATE_KEYS,keys);
		MDC.put(DUPLICATE_COUNT,String.valueOf(value));
		LOG.info(message);
		MDC.clear();
	}
	
	


	public static void main(String[] args) {
		String home = "C:\\Projects\\git\\dm_bss_generic_somiglite\\dev\\src\\";
		String dumpFilePath = home + "Input";
		String schemFilePath = home + "config\\Scheme\\";
		String files2BeLoaded = "Subscriber_Information.csv,Subscription_Information.csv,CUG.csv,SIM_Information.csv";
		String dbpath = home + "database\\";
		
		
		//InputFile2LevelDBLegacyExtractor obj = new InputFile2LevelDBLegacyExtractor(dumpFilePath, schemFilePath, files2BeLoaded, dbpath, "SUBSCRIBER", "yes");
		InputFile2LevelDBLegacyExtractor obj = new InputFile2LevelDBLegacyExtractor(dumpFilePath, schemFilePath, files2BeLoaded, dbpath, "SUBSCRIBER", "yes", "test_chunk-0100", "yes");
		obj.execute();
		

	}

}
