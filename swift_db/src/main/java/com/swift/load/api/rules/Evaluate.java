package com.swift.load.api.rules;

import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.swift.load.api.utils.Constants;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/*
 * This is a evalution class used to evaluate filter condition 
 * 
 */
public class Evaluate {
	
	private List<Filter> filterList = null;
	private String jsonString = null;
	private String tableName;
	private static final Logger LOG = LoggerFactory.getLogger(Evaluate.class);

	/*
	 * Constructor which takes inputs to calcuate the filtered data
	 * @param filterList list of filter condition to be provided to get filtered data
	 * @param jsonString represent Json string as json object to be provided to filter data
	 * @param tableName table name to be specified where you want to filter
	 */
	public Evaluate(List<Filter> filterList, String jsonString,String tableName) {
		this.filterList = filterList;
		this.jsonString = jsonString;
		this.tableName=tableName;
	}

	/*
	 * Provides filtered Json array based on the filter condition provided in the constructor
	 */
	public JSONArray evaluateFilterCondition() {
		JSONArray newArray = new JSONArray();
		try {
			JSONObject jsonObject1 = new JSONObject(jsonString);
			JSONArray jsonArray = jsonObject1.getJSONArray(tableName);
			for (int indx = 0; indx < jsonArray.length(); indx++) {
				JSONObject jsonObject = (JSONObject) jsonArray.get(indx);
				boolean validate = true;
				validate = evaluateFullFilterCondition(jsonObject);
				if (validate) {
					newArray.put(jsonObject);
				}else {
					String msg = MessageFormat.format("INC1000: SKIPPED RECORD AS CONDITION EVLUATION FAILED Json ={0} for Filter = {1} ", jsonString,filterList);
					LOG.warn(msg);
					//log KPIs
					this.logKPIs(jsonObject.optString(Constants.CHUNK_ID),jsonObject.optString(Constants.LUW_ID),tableName,filterList!=null?filterList.toString():"",msg);
				}
			}
		} catch (JSONException | ParseException err) {
			LOG.error("JSONException or ParseException ",err,jsonString);
			err.printStackTrace();
		}
		return newArray;
	}

	private boolean evaluateFullFilterCondition(JSONObject jsonObject) throws ParseException {
		boolean validate = true;
		String condition;
		if (filterList!=null && !filterList.isEmpty()) {
			for (Filter filter : filterList) {
				if(filter.getMultipleFilterCondition()!=null && filter.getMultipleFilterCondition().length()>0){
					condition=filter.getMultipleFilterCondition().toLowerCase();
				}
				else{
					condition = "and";
				}
				validate = evaluateConditionBasedonType(jsonObject, validate, condition, filter);
			}
		}
		return validate;
	}

	private boolean evaluateConditionBasedonType(JSONObject jsonObject, boolean validate, String condition,
			Filter filter) throws ParseException {
		if (filter.getCondition() != null) {
			String columnValue = (String) jsonObject.get(filter.getColumn());
			if (filter.getFormat().equalsIgnoreCase("STRING")) {
				validate = evaluateStringCondition(validate, condition, filter, columnValue);	
			} else if (filter.getFormat().equalsIgnoreCase("NUMBER")) {
				validate = evaluteNumberCondition(validate, condition, filter, columnValue);
			} else if (filter.getFormat().contains("DATE")) {
				
				validate = evaluateDateConditon(validate, condition, filter, columnValue);
			}
		}
		return validate;
	}

	private boolean evaluateDateConditon(boolean validate, String condition, Filter filter, String columnValue)
			throws ParseException {
		if(condition.equals("and")){
			validate = validate && compareFormatDate(columnValue, filter);
		}
		else{
			validate = validate || compareFormatDate(columnValue, filter);
		}
		return validate;
	}

	private boolean evaluateStringCondition(boolean validate, String condition, Filter filter, String columnValue) {
		if(condition.equals("and")){
			validate = validate && compareFormatString(columnValue, filter);
		}
		else{
			validate = validate || compareFormatString(columnValue, filter);
		}
		return validate;
	}

	private boolean evaluteNumberCondition(boolean validate, String condition, Filter filter, String columnValue) {
		if(condition.equals("and")){
			validate = validate && compareFormatNumber(columnValue, filter);
		}
		else{
			validate = validate || compareFormatNumber(columnValue, filter);
		}
		return validate;
	}

	private boolean compareFormatDate(String columnValue, Filter filter) throws ParseException {
		String tmp[] = filter.getFormat().split(";", -1);
		String pattern = tmp[1];
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Date columnDate = sdf.parse(columnValue);
		Date actualDate = sdf.parse(filter.getValue());
		return ((filter.getCondition().equals("==") && columnDate.compareTo(actualDate) == 0)
				|| (filter.getCondition().equals("!=") && columnDate.compareTo(actualDate) != 0)
				|| (filter.getCondition().equals("<") && columnDate.compareTo(actualDate) < 0)
				|| (filter.getCondition().equals("<=") && columnDate.compareTo(actualDate) <= 0)
				|| (filter.getCondition().equals(">=") && columnDate.compareTo(actualDate) >= 0)
				|| (filter.getCondition().equals(">") && columnDate.compareTo(actualDate) > 0));
	}

	private boolean compareFormatNumber(String columnValue, Filter filter) {
		Double coulmnValN = Double.parseDouble(columnValue);
		Double actualValue = Double.parseDouble(filter.getValue());
		return ((filter.getCondition().equals("==") && columnValue.equalsIgnoreCase(filter.getValue())
				|| (filter.getCondition().equals("!=") && !columnValue.equalsIgnoreCase(filter.getValue())))
				|| (filter.getCondition().equals("<") && (coulmnValN < actualValue))
				|| (filter.getCondition().equals("<=") && (coulmnValN <= actualValue))
				|| (filter.getCondition().equals(">=") && (coulmnValN >= actualValue))
				|| (filter.getCondition().equals(">") && (coulmnValN > actualValue)));
	}

	private boolean compareFormatString(String columnValue, Filter filter) {
		return (filter.getCondition().equals("==") && columnValue.equalsIgnoreCase(filter.getValue())
				|| (filter.getCondition().equals("!=") && !columnValue.equalsIgnoreCase(filter.getValue())));
	}
	
	/**
	 * This method is used to generate logbased KPIS for validation failure
	 * 
	 * @param chunkId
	 * @param luwId
	 * @param entity
	 * @param filter
	 * @param message
	 */
	private void logKPIs(String chunkId,String luwId,String entity,String filter,String message) {
		//log messages for KPI
		MDC.clear();
		MDC.put(Constants.CHUNKID,chunkId);
		MDC.put(Constants.LUWID,luwId);
		MDC.put(Constants.ENTITY,entity);
		MDC.put(Constants.FILTER,filter);
		MDC.put(Constants.LOGCODE, Constants.EE_FILTER_SKIP);
		LOG.warn(message);
		MDC.clear();
	}

}
