package com.swift.load.api.db;


import com.swift.load.api.utils.lz4Compression;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.nio.charset.StandardCharsets;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/***
 * 
 * This is a class with ROCKSDB implementation to store and get the data
 * 
 * @author egjklol
 *
 */
public class RockDB implements Database {
	private static final Logger LOG = LoggerFactory.getLogger(RockDB.class);
	private static final Map<String, RocksDB> MAP_OF_DB_NAMES = new ConcurrentHashMap<>(1000, 0.75f, 200);
	private static final Map<String, Snapshot> MAP_OF_SNAPSHOTS = new ConcurrentHashMap<>(1000, 0.75f, 200);

	private String dbName;
	private RocksDB rocksDBStore;

	public RockDB(String dbName) {
		this.dbName = dbName;
	}

	public RockDB(String dbName, RocksDB levelDBStore) {
		this.dbName = dbName;
		this.rocksDBStore = levelDBStore;
	}

	@Override
	public RockDB createConnection() {

		// a factory method that returns a RocksDB instance
		/*final Options options = new Options();
		
		//options.createIfMissing();
		//options.setCreateIfMissing(true);
		//options.allowMmapReads();
		//options.allowMmapWrites();
		//options.setWriteBufferSize(4194304);
		//options.setMaxWriteBufferNumber(80)
		options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
		//options.setAllowMmapReads(true);
		//options.setIncreaseParallelism(200);
		options.setMaxOpenFiles(-1);
		options.setBestEffortsRecovery(true);
		//options.setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery );
		*/
	
		
		try {
			if (MAP_OF_DB_NAMES.containsKey(this.dbName)) {
				rocksDBStore = (RocksDB) MAP_OF_DB_NAMES.get(this.dbName).getRocksDBStore();
			} else {
				rocksDBStore = RocksDB.open(dbName);
				//rocksDBStore = RocksDB.openReadOnly(options, dbName);
				MAP_OF_DB_NAMES.put(this.dbName, rocksDBStore);
			}
			
			return this;
		} catch (Exception e) {
			LOG.error("Exception 1", e);

		}
		return null;
	}

	

	@Override
	public void close() {

		try {
			this.rocksDBStore.close();
		} catch (Exception e) {
			LOG.error("Exception", e);
		}
		MAP_OF_DB_NAMES.clear();
		MAP_OF_SNAPSHOTS.clear();
	}

	@Override
	public RocksDB getConnection() {
		return MAP_OF_DB_NAMES.get(this.dbName);
	}

	@Override
	public void initializeSnapshot(String uniqueNumber) {
		for (Entry<String, RocksDB> entry : MAP_OF_DB_NAMES.entrySet()) {
			RocksDB rockdb = entry.getValue();
			Snapshot snapshot = rocksDBStore.getSnapshot();
			//MAP_OF_SNAPSHOTS.put(rockdb.dbName + "_" + uniqueNumber, snapshot);
		}
	}

	@Override
	public String getString(String key) {
		try {
			byte[] result = MAP_OF_DB_NAMES.get(this.dbName).get(key.getBytes());
			if (result != null) {
				return new String(result);
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Exception", e);
		}
		return null;
	}

	@Override
	public String getStringUsingSnapshot(String key, String uniqueNumber) {
		try {
			ReadOptions readoptions = new ReadOptions();
			
			readoptions.setSnapshot(MAP_OF_SNAPSHOTS.get(this.dbName.concat("_").concat(uniqueNumber)));
			byte[] result = MAP_OF_DB_NAMES.get(this.dbName).get(readoptions,key.getBytes(StandardCharsets.UTF_8));
			if (result != null) {
				return new String(result, StandardCharsets.UTF_8);
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Exception 2", e);
		}
		return null;
	}

	@Override
	public String getStringUsingIterator(String key) {
		String result = null;
		
		return result;
	}

	@Override
	public String getDbName() {
		String names[] = dbName.split("/");
		return names[names.length - 1];
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public RocksDB getRocksDBStore() {
		return rocksDBStore;
	}

	public void setRocksDBStore(RocksDB rocksDBStore) {
		this.rocksDBStore = rocksDBStore;
	}

	@Override
	public Set<String> getKeySet() {
	
		RocksIterator rocksIterator = MAP_OF_DB_NAMES.get(this.dbName).newIterator();
		Set<String> set = new HashSet<>();
		int cnt=0;
		for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
			cnt++;
            set.add(new String(rocksIterator.key()));
        }
		System.out.println("INTO Database keys ..");
		System.out.println("DatabaseName:"+this.dbName+ "---------------------Counts:::"+ cnt);
		System.out.println("INTO Database keys ..");
		try {
			rocksIterator.close();
		} catch (Exception e) {
			LOG.error("Exception 4", e);
		}
		return set;
	}

	@Override
	public String getDecompressedString(String key) {
		try {
			byte[] result = MAP_OF_DB_NAMES.get(this.dbName).get(key.getBytes(StandardCharsets.UTF_8));
			if (result != null) {
				lz4Compression lz4 = new lz4Compression();
				return new String(lz4.decompress(result));
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Exception", e);
		}
		return null;
	}

}
