package com.swift.load.api.custom.parser;

import java.util.HashMap;
import java.util.Map;

public class PayloadObj {
	
	private String jsonPayload;
	private HashMap<String, String> propertiesMap;
	
	public PayloadObj() {
		
	}
	
	public PayloadObj(String jsonPayload, HashMap<String, String> propertiesMap) {
		//super();
		this.jsonPayload = jsonPayload;
		this.propertiesMap = propertiesMap;
	}
	
	public PayloadObj(String jsonPayload, String key, String value) {
		//super();
		this.jsonPayload = jsonPayload;
		this.propertiesMap = new HashMap<String, String>();
		this.propertiesMap.put(key, value);
	}

	public String getJsonPayload() {
		return jsonPayload;
	}
	public void setJsonPayload(String jsonPayload) {
		this.jsonPayload = jsonPayload;
	}
	public Map<String, String> getPropertiesMap() {
		return propertiesMap;
	}
	public void setPropertiesMap(Map<String, String> propertiesMap) {
		this.propertiesMap = (HashMap<String, String>) propertiesMap;
	}

	@Override
	public String toString() {
		return "PayloadObj [jsonPayload=" + jsonPayload + ", propertiesMap=" + propertiesMap + "]";
	}
	
	

}
