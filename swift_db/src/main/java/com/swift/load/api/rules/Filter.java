package com.swift.load.api.rules;

public class Filter {

	private String table;
	private String column;
	private String condition;
	private String value;
	private String format;
	private String multipleFilterCondition;
	
	public Filter(){
		
	}
	public Filter(String table, String column, String condition, String value, String format,String multipleFilterCondition) {
		super();
		this.table = table;
		this.column = column;
		this.condition = condition;
		this.value = value;
		this.format = format;
		this.multipleFilterCondition=multipleFilterCondition;
	}
	
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public String getCondition() {
		return condition;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getFormat() {
		return format;
	}
	public void setFormat(String format) {
		this.format = format;
	}
	
	public String getMultipleFilterCondition() {
		return multipleFilterCondition;
	}
	public void setMultipleFilterCondition(String multipleFilterCondition) {
		this.multipleFilterCondition = multipleFilterCondition;
	}
	@Override
	public String toString() {
		return "Filter [table=" + table + ", column=" + column + ", condition=" + condition + ", value=" + value
				+ ", format=" + format + ", multipleFilterCondition=" + multipleFilterCondition + "]";
	}
	
	
}
