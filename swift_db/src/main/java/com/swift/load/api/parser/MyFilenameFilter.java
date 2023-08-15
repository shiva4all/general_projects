package com.swift.load.api.parser;

import java.io.File;
import java.io.FilenameFilter;

public final class MyFilenameFilter implements FilenameFilter {
	private String[] prefixes = null;
	private String[] suffixes = null;

	public MyFilenameFilter(String[] expectedPrefixes, String[] expectedSuffixes) {
		prefixes = expectedPrefixes;
		suffixes = expectedSuffixes;
	}

	private static final boolean checkCondition(boolean end, String[] affixes, String name) {
		boolean conditionFulfilled = false;
		if (affixes != null && affixes.length > 0) {
			for (int i = 0; i < affixes.length; i++) {
				if ( (!end && name.startsWith(affixes[i])) || (end && name.endsWith(affixes[i])) ) {
					conditionFulfilled = true;
					break;
				}
			}
		} else {
			conditionFulfilled = true;
		}
		return conditionFulfilled;
	}

	@Override
	public boolean accept(File dir, String name) {
		boolean condition1Fulfilled = checkCondition(false, prefixes, name);
		boolean condition2Fulfilled = checkCondition(true, suffixes, name);
		return condition1Fulfilled && condition2Fulfilled;
	}
}
