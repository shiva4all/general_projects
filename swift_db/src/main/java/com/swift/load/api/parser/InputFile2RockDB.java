package com.swift.load.api.parser;

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

import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class InputFile2RockDB {
	protected Map<String, Map<String, List<String>>> entityColumnsMap;
	protected String dumpFilePath;
	protected String schemaFilePath;
	protected String dbPath;
	protected String filesToBeInserted;
	protected String configDir;

	private static final Logger LOG = LoggerFactory.getLogger(InputFile2RockDB.class);
	private static final String COLUMNS = "columns";
	private static final String COLUMNS_TO_IGNORE = "columns_to_ignore";
	private static final String COLUMNS_INDEXES_TO_IGNORE = "columns_indexes_to_ignore";
	private static final String INDEXES = "indexes";
	private static final String SIZE = "split_size";
	private static final Map<String, List<String>> entityMap = new ConcurrentHashMap<>();
	protected Date migrationDate;

	/*
	 * Constructor to do load leveldb configuration
	 * 
	 * @param dumpFilePath input dump file path
	 * 
	 * @param schemaFilePath path of the schema file, to know how to write schema
	 * fle refer documentation
	 * 
	 * @param filesToBeInserted list of files to be inserted
	 * 
	 * @param dbPath path where DB has to be created.
	 */
	public InputFile2RockDB(String dumpFilePath, String schemaFilePath, String filesToBeInserted, String dbPath) {
		this.dumpFilePath = dumpFilePath;
		this.schemaFilePath = schemaFilePath;
		this.filesToBeInserted = filesToBeInserted;
		this.dbPath = dbPath;

		createFileCacheMap(this.schemaFilePath);
	}

	private void createFileCacheMap(String schemaFilePathArg) {
		entityColumnsMap = parseSchemaCsvFiles(schemaFilePathArg);

	}

	protected void loadIntoRocksDB(String entity, File fullFile, String dbpath)
			throws IOException, RocksDBException, InterruptedException {

		RocksDB rocksDBStore;
		Options options = new Options();
		options.setCreateIfMissing(true);
		// options.createIfMissing(true);
		// options.setAllowMmapWrites(true);

		// options.setWalRecoveryMode(WALRecoveryMode.AbsoluteConsistency );

		// options.setWriteBufferSize(4194304);
		// options.setMaxWriteBufferNumber(8);
		// options.setMinWriteBufferNumberToMerge(2);
		options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
		// options.setIncreaseParallelism(42);
		options.setBestEffortsRecovery(true);
		options.setMaxOpenFiles(-1);

		Map<String, String> memory = new HashMap<>(100000, 0.75f);

		rocksDBStore = RocksDB.open(options, dbpath + "/" + entity);

		rocksDBStore.flushWal(true);

		int counter = 0;

		List<String> columnList = entityColumnsMap.get(entity).get(COLUMNS);
		List<String> indexList = entityColumnsMap.get(entity).get(INDEXES);
		int size = Integer.parseInt(entityColumnsMap.get(entity).get(SIZE).get(0));
		List<String> columns2Ignore = entityColumnsMap.get(entity).get(COLUMNS_TO_IGNORE);
		String sep = entityColumnsMap.get(entity).get("seperator").get(0);

		sep = "\\" + sep;
		int indxCounter = 1;
		int datacounter = 0;

		List<String> entityList = new ArrayList<>();
		entityList.add(entity);
		int counter1 = 0;

		WriteOptions writeopts = new WriteOptions();
		writeopts.setDisableWAL(false);
		writeopts.setSync(true);

		try (BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(fullFile), StandardCharsets.ISO_8859_1))) {
			String line;
			while ((line = bufferedReader.readLine()) != null) {

				datacounter++;
				String[] arr = line.split(sep, -30);
				String dataIndex = "";

				if (counter != 0 && counter >= size) {
					WriteBatch wb = new WriteBatch();
					for (java.util.Map.Entry<String, String> mem : memory.entrySet()) {
						wb.put(mem.getKey().getBytes(), mem.getValue().getBytes());
					}
					wb.setSavePoint();

					Thread.sleep(100);
					rocksDBStore.write(writeopts, wb);
					Thread.sleep(100);

					rocksDBStore.syncWal();
					FlushOptions flushOptions = new FlushOptions();
					rocksDBStore.flush(flushOptions);
					Thread.sleep(100);
					wb.close();

					memory.clear();
					counter = 0;
					Set<String> set = getCompleteKeys(rocksDBStore);

					LOG.debug("INC99999 : Number of records for " + rocksDBStore.getName() + " is : " + set.size());

					rocksDBStore.closeE();
					Thread.sleep(10000);

					rocksDBStore = RocksDB.open(options, (dbpath + "/" + entity + "_" + indxCounter));
					rocksDBStore.flushWal(true);

					entityList.add(entity + "_" + indxCounter);
					indxCounter++;

				}
				for (String index : indexList) {
					int indxNumber = columnList.indexOf(index);
					if (indxNumber > -1) {
						dataIndex = new StringBuilder().append(arr[indxNumber]).append(dataIndex).toString();
					}
				}

				if (memory.containsKey(dataIndex)) {
					String temp = memory.get(dataIndex);
					temp = temp.replace("]}", ",");
					String xml = temp.concat(formJson(columnList, arr, columns2Ignore)).concat("]}");
					memory.put(dataIndex, xml);

				} else {
					String xml = "{\"".concat(entity).concat("\":[").concat(formJson(columnList, arr, columns2Ignore))
							.concat("]}");

					memory.put(dataIndex, xml);
				}

				counter++;
				if (counter1 % 100000 == 0) {
					LOG.debug("Printing Loading::::" + new Date() + "---->" + entity + "----->" + counter1);
				}
				counter1++;
			}
			entityMap.put(entity, entityList);
		}

		WriteBatch wb = new WriteBatch();
		for (java.util.Map.Entry<String, String> mem : memory.entrySet()) {
			wb.put(mem.getKey().getBytes(), mem.getValue().getBytes());
		}
		wb.setSavePoint();

		// writeopts.setSync(false);
		Thread.sleep(100);
		rocksDBStore.write(writeopts, wb);
		Thread.sleep(100);
		rocksDBStore.syncWal();
		rocksDBStore.flushWal(true);
		FlushOptions flushOptions = new FlushOptions();
		rocksDBStore.flush(flushOptions);
		Thread.sleep(100);
		wb.close();
		LOG.debug("INC1003 : Number of records for " + entity + " is : " + datacounter);

		memory.clear();

		Set<String> set = getCompleteKeys(rocksDBStore);
		LOG.debug("INC99999 : Number of records for " + rocksDBStore.getName() + " is : " + set.size());
		rocksDBStore.closeE();
		Thread.sleep(10000);

	}

	private String formJson(List<String> columnList, String[] data, List<String> columns2Ignore) {
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

	private Map<String, Map<String, List<String>>> parseSchemaCsvFiles(String schemaFilePathArg) {
		Map<String, Map<String, List<String>>> mapOfTablesAndColumns = new LinkedHashMap<>();
		File dir = new File(schemaFilePathArg);
		FilenameFilter filter = new MyFilenameFilter(new String[] { "tabledef" }, new String[] { ".csv" });
		List<File> list = Arrays.asList(dir.listFiles(filter));
		for (File file : list) {
			parseSchemaCsvFile(file, mapOfTablesAndColumns);
		}
		return mapOfTablesAndColumns;
	}

	private void parseSchemaCsvFile(File fileArg, Map<String, Map<String, List<String>>> mapOfTablesAndColumnsArg) {
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

	private void parseSchemaCsvLine(String line, Map<String, List<String>> columnsAndIndexes, int columnIndex) {
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
			if (temp.length > 3 && temp[3].equalsIgnoreCase("i")) {

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
		if (listOfFilesToBeInserted.size() == 1) {
			cores = 1;
		} else if (listOfFilesToBeInserted.size() < cores) {
			cores = listOfFilesToBeInserted.size();
		} else {
			cores = Runtime.getRuntime().availableProcessors() - 1;
		}

		ExecutorService executor = Executors.newFixedThreadPool(cores);
		FilenameFilter filter = new MyFilenameFilter(null, new String[] { ".csv", ".txt", ".unl" });
		List<File> listOfFiles = new ArrayList<>(Arrays.asList(folder.listFiles(filter)));
		List<List<String>> smallerLists = Lists.partition(listOfFilesToBeInserted,
				listOfFilesToBeInserted.size() / (cores));
		LOG.debug("SmallerLists::::::" + smallerLists);
		LOG.debug("Number Of Cores:::::" + (cores));
		for (int i = 0; i < smallerLists.size(); i++) {
			Runnable worker = new WorkerThreadForRocksDB(listOfFiles, smallerLists.get(i), dumpFilePath, schemaFilePath,
					dbPath, filesToBeInserted);
			executor.execute(worker);

		}
		executor.shutdown();
		while (!executor.isTerminated())
			;

		try (BufferedWriter bw = new BufferedWriter(new FileWriter(dbPath + "/" + "database_info.txt", true))) {
			for (java.util.Map.Entry<?, ?> entry : entityMap.entrySet()) {
				bw.write(entry.getKey().toString().concat("|")
						.concat(entry.getValue().toString().replace("[", "").replace("]", "").replaceAll(" ", ""))
						.concat("\n"));
			}

		} catch (IOException e) {
			LOG.error("IOException", e);
		}
		listOfFilesToBeInserted.clear();
		listOfFiles.clear();
		smallerLists.clear();
		entityColumnsMap.clear();
		entityColumnsMap = null;
		return true;

	}

	public static void main(String[] args) {
		String home = "C:\\Users\\egjklol\\Desktop\\Test\\";
		String dumpFilePath = home + "\\input\\";
		String schemFilePath = home + "\\Scheme\\";
		// String files2BeLoadedsdp =
		// "Account.csv,Subscriber.csv,Accumulator.csv,DedicatedAccount.csv,UsageCounter.csv,Offer.csv,OfferAttribute.csv";

		String files2BeLoadedamdocs = "INFILE_OPTIVA_RPP_Product.csv,INFILE_OPTIVA_RPPQoS_Product.csv";

		String dbpath = home + "database\\";

		InputFile2RockDB obj = new InputFile2RockDB(dumpFilePath, schemFilePath, files2BeLoadedamdocs, dbpath);
		obj.execute();
	}

	public Set<String> getCompleteKeys(org.rocksdb.RocksDB rocksDBStore) {
		RocksIterator rocksIterator = rocksDBStore.newIterator();
		Set<String> set = new HashSet<>();
		int cnt = 0;
		for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
			cnt++;
			set.add(new String(rocksIterator.key()));
		}
		
		try {
			rocksIterator.close();
		} catch (Exception e) {
			LOG.error("Exception 4", e);
		}
		return set;
	}
}
