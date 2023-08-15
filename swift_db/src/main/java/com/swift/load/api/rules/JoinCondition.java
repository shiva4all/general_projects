package com.swift.load.api.rules;

public class JoinCondition {
	private String maintableToBeSearched;
	private String columnToBesearched;
	private String subTableToBeSearched;
	
	public JoinCondition(String maintableToBeSearched, String columnToBesearched, String subTableToBeSearched) {
		super();
		this.maintableToBeSearched = maintableToBeSearched;
		this.columnToBesearched = columnToBesearched;
		this.subTableToBeSearched = subTableToBeSearched;
	}
	
	
	public String getMaintableToBeSearched() {
		return maintableToBeSearched;
	}
	public void setMaintableToBeSearched(String maintableToBeSearched) {
		this.maintableToBeSearched = maintableToBeSearched;
	}
	public String getColumnToBesearched() {
		return columnToBesearched;
	}
	public void setColumnToBesearched(String columnToBesearched) {
		this.columnToBesearched = columnToBesearched;
	}
	public String getSubTableToBeSearched() {
		return subTableToBeSearched;
	}
	public void setSubTableToBeSearched(String subTableToBeSearched) {
		this.subTableToBeSearched = subTableToBeSearched;
	}


	@Override
	public String toString() {
		return "JoinCondition [maintableToBeSearched=" + maintableToBeSearched + ", columnToBesearched="
				+ columnToBesearched + ", subTableToBeSearched=" + subTableToBeSearched + "]";
	}

	

}
