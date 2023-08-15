package com.swift.load.api.db;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigureDB {
	protected final Map<String, List<Database>> dbConfig = new ConcurrentHashMap<>(10, 0.75f, 50);
	private static final Logger LOG = LoggerFactory.getLogger(ConfigureDB.class);

	String configDir;
	String dbType;

	public ConfigureDB(String configDir) {
		this.configDir = configDir;
	}

	public ConfigureDB(String configDir, String dbType) {
		this.configDir = configDir;
		this.dbType = dbType;
	}

	public void initialize() {
		Map<String, List<String>> dbConfigMap = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(configDir + "/database_info.txt"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String tmp[] = line.split("\\|", -4);
				List<String> list = Arrays.asList(tmp[1].split(",", -10));
				dbConfigMap.put(tmp[0], list);
			}
		} catch (IOException e) {
			LOG.error("IOException", e);
		}
		//dbConfigMap.entrySet().stream().collect(Collectors.toMap(elem -> elem.getKey(), elem -> elem.getValue())); // can further process it);
		for (Entry<String, List<String>> entry : dbConfigMap.entrySet()) {
			
			//entry.getValue().parallelStream().forEach(entity->createConnectionToDB(entry,entity));
			for (String entity : entry.getValue()) {
				System.out.println("Entity: "+ entity);
				LOG.info("Entity Connection Established: "+ entity);
				createConnectionToDB(entry, entity);
			}
		}
	}

	public Consumer<? super String> createConnectionToDB(Entry<String, List<String>> entry, String entity) {
		if (dbType != null && dbType.equals("ROCKSDB")) {
			createRockdbconnection(entry, entity);

		} else {
			createLevelDBConnection(entry, entity);
		}
		return null;
		
	}

	public void createLevelDBConnection(Entry<String, List<String>> entry, String entity) {
		Database db = new LevelDB(configDir.concat("/").concat(entity));
		LevelDB leveldb = (LevelDB) db.createConnection();
		if (dbConfig.containsKey(entry.getKey())) {
			List<Database> dbList = dbConfig.get(entry.getKey());
			dbList.add(leveldb);
			dbConfig.put(entry.getKey(), dbList);
		} else {
			List<Database> dbList = new ArrayList<>();
			dbList.add(leveldb);
			dbConfig.put(entry.getKey(), dbList);
		}
	}

	public void createRockdbconnection(Entry<String, List<String>> entry, String entity) {
		Database db = new RockDB(configDir.concat("/").concat(entity));
		RockDB rockdb = (RockDB) db.createConnection();
		if (dbConfig.containsKey(entry.getKey())) {
			List<Database> dbList = dbConfig.get(entry.getKey());
			dbList.add(rockdb);
			dbConfig.put(entry.getKey(), dbList);
		} else {
			List<Database> dbList = new ArrayList<>();
			dbList.add(rockdb);
			dbConfig.put(entry.getKey(), dbList);
		}
	}

	public Map<String, List<Database>> getDbConfigMap() {
		return dbConfig;
	}

	public void destroy() {
		for (Entry<String, List<Database>> entry : dbConfig.entrySet()) {
			for (Database db : entry.getValue()) {
				db.close();
			}
		}
	}

}
