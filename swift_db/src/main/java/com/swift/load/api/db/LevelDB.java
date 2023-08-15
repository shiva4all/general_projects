package com.swift.load.api.db;

import com.swift.load.api.utils.lz4Compression;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/***
 * 
 * This is a class with LEVELDB implementation to store and get the data
 * 
 * @author egjklol
 *
 */
public class LevelDB implements Database {
	private static final Logger LOG = LoggerFactory.getLogger(LevelDB.class);
	private static final Map<String, LevelDB> MAP_OF_DB_NAMES = new ConcurrentHashMap<>(1000, 0.75f, 200);
	private static final Map<String, Snapshot> MAP_OF_SNAPSHOTS = new ConcurrentHashMap<>(1000, 0.75f, 200);

	private String dbName;
	private DB levelDBStore;

	public LevelDB(String dbName) {
		this.dbName = dbName;
	}

	public LevelDB(String dbName, DB levelDBStore) {
		this.dbName = dbName;
		this.levelDBStore = levelDBStore;
	}

	@Override
	public LevelDB createConnection() {

		// a factory method that returns a RocksDB instance
		final Options options = new Options();
		options.createIfMissing();
		options.blockSize(4096);
		options.blockRestartInterval(32);
		options.cacheSize(41943040);
		options.maxOpenFiles(1000);
		options.compressionType(CompressionType.SNAPPY);
		try {
			if(MAP_OF_DB_NAMES.containsKey(this.dbName)){
				levelDBStore = (DB)MAP_OF_DB_NAMES.get(this.dbName).getLevelDBStore();
			}
			else{
				levelDBStore = factory.open(new File(dbName), options);
			}
			
			// a factory method that returns a RocksDB instance
			final ReadOptions readoptions = new ReadOptions();
			readoptions.fillCache(true);
			Snapshot snapshot = this.getLevelDBStore().getSnapshot();
			readoptions.snapshot(snapshot);
			MAP_OF_DB_NAMES.put(this.dbName, this);
			return this;
		} catch (Exception e) {
			LOG.error("Exception 1",e);

		}
		return null;
	}

	public DBIterator getInstance() {

		// a factory method that returns a RocksDB instance
		final ReadOptions readoptions = new ReadOptions();
		readoptions.fillCache(true);

		try {
			readoptions.snapshot(this.getLevelDBStore().getSnapshot());
			return this.levelDBStore.iterator(readoptions);
		} catch (Exception e) {
			LOG.error("Exception",e);

		}
		return null;
	}

	public void close(DBIterator dbitr) {
		try {
			dbitr.close();
		} catch (IOException e) {
			LOG.error("IOException",e);
		}
	}

	@Override
	public void close() {

		try {
			this.levelDBStore.close();
		} catch (IOException e) {
			LOG.error("IOException",e);
		}
		MAP_OF_DB_NAMES.clear();
		MAP_OF_SNAPSHOTS.clear();
	}

	@Override
	public LevelDB getConnection() {
		return MAP_OF_DB_NAMES.get(this.dbName);
	}

	@Override
	public void initializeSnapshot(String uniqueNumber) {
		for (Entry<String, LevelDB> entry : MAP_OF_DB_NAMES.entrySet()) {
			LevelDB leveldb = entry.getValue();
			Snapshot snapshot = leveldb.levelDBStore.getSnapshot();
			MAP_OF_SNAPSHOTS.put(leveldb.dbName + "_" + uniqueNumber, snapshot);
		}
	}

	@Override
	public String getString(String key) {
		try {
			byte[] result = MAP_OF_DB_NAMES.get(this.dbName).levelDBStore.get(key.getBytes(StandardCharsets.UTF_8));
			if (result != null) {
				return new String(result, StandardCharsets.UTF_8);
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Exception",e);
		}
		return null;
	}

	@Override
	public String getStringUsingSnapshot(String key, String uniqueNumber) {
		try {
			ReadOptions readoptions = new ReadOptions();
			readoptions.snapshot(MAP_OF_SNAPSHOTS.get(this.dbName.concat("_").concat(uniqueNumber)));
			byte[] result = MAP_OF_DB_NAMES.get(this.dbName).levelDBStore.get(key.getBytes(StandardCharsets.UTF_8),
					readoptions);
			if (result != null) {
				return new String(result, StandardCharsets.UTF_8);
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Exception 2",e);
		}
		return null;
	}

	@Override
	public String getStringUsingIterator(String key) {
		String result = null;
		try {
			DBIterator dbIterator = MAP_OF_DB_NAMES.get(this.dbName).getInstance();
			dbIterator.seek(key.getBytes());
			if (dbIterator.hasNext()) {
				result = new String(dbIterator.next().getValue(), StandardCharsets.UTF_8);
			}
			dbIterator.close();
		} catch (Exception e) {
			LOG.error("Exception 3",e);
		}
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

	public DB getLevelDBStore() {
		return levelDBStore;
	}

	public void setLevelDBStore(DB levelDBStore) {
		this.levelDBStore = levelDBStore;
	}

	@Override
	public Set<String> getKeySet() {
		DBIterator dbIterator = MAP_OF_DB_NAMES.get(this.dbName).levelDBStore.iterator();
		Set<String> set = new HashSet<>();
		while (dbIterator.hasNext()) {
			set.add(new String(dbIterator.next().getKey()));
		}
		try {
			dbIterator.close();
		} catch (IOException e) {
			LOG.error("Exception 4",e);
		}
		return set;
	}
	
	@Override
	public String getDecompressedString(String key) {
		try {
			byte[] result = MAP_OF_DB_NAMES.get(this.dbName).levelDBStore.get(key.getBytes(StandardCharsets.UTF_8));
			if (result != null) {
				lz4Compression lz4Object = new lz4Compression();
				result=lz4Object.decompress(result);
				return new String(result, StandardCharsets.UTF_8);
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Exception", e);
		}
		return null;
	}

}
