package com.swift.load.api.db;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagingService {
	private static final Logger LOG = LoggerFactory.getLogger(MessagingService.class);
	protected String uniqueNumber;

	public MessagingService(String uniqueNumber) {
		this.uniqueNumber = uniqueNumber;
	}

	public List<Object> getJsonObject(List<Database> databaseList, String key) {
		List<Object> resultList = new ArrayList<>();
		if (databaseList != null) {
			for (Database database : databaseList) {
				String result = database.getString(key);
				if (result != null && result.length() > 0) {
					resultList.add(result);
				}
			}
		}
		return resultList;
	}

	public String combineIntoOneJson(List<Database> databaseList, String key) {
		List<Object> resultList = getJsonObject(databaseList, key);
		StringBuilder sb = new StringBuilder();
		int indxCounter = 1;
		for (Object res : resultList) {
			if(indxCounter>1){
			res=res.toString().substring(res.toString().indexOf('[')+1);
			}
			if (resultList.size() == indxCounter) {
				sb.append(res);
			} else {
				res=res.toString().substring(0, res.toString().length()-2);
				sb.append(res).append(",");
			}
			indxCounter++;
		}
		return sb.toString();
	}

	public Set<String> getAllKeys(List<Database> databaseList) {
		Set<String> keyList = new HashSet<>();
		if (databaseList != null) {
			for (Database database : databaseList) {
				keyList.addAll(database.getKeySet());
			}
		}
		return keyList;
	}
}
