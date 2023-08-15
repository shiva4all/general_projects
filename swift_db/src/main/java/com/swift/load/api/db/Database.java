package com.swift.load.api.db;

import java.util.Set;

public interface Database {

	public Object createConnection();

	public Object getConnection();

	public String getString(String key);
	
	public String getDecompressedString(String key);

	public String getStringUsingIterator(String key);

	public String getStringUsingSnapshot(String key, String uniqueNumber);

	public void initializeSnapshot(String uniqueNumber);

	public void close();

	public String getDbName();

	public Set<String> getKeySet();

}
