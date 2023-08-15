package com.swift.load.api.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;


import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.HTreeMap;
import org.mapdb.HTreeMap.KeySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuickGzipReader {

	
	private static final String DEFAULT_CHARSET = "UTF-8";
	private static final String EXC_OCC = "Exception occured: ";
	public static final String GZIP_EXTENSION = ".gz";
	private static final Logger LOG = LoggerFactory.getLogger(QuickGzipReader.class);

	public static long index = 0;
	
	private long readChars = 0;
	private long readLines = 0;
	private int minLenOfValue = Integer.MAX_VALUE;
	private int maxLenOfValue = Integer.MIN_VALUE;
	private String identity;
	private KeySet<Long> memcacheLong;
	private KeySet<String> memcacheString;
	private HTreeMap<Long, String> memcacheMapLong;
	private HTreeMap<String, String> memcacheMapString;
	private boolean extractAliasParent = false;

	public QuickGzipReader(String path, int parallelism, KeySet<Long> memcacheLongArg,
			KeySet<String> memcacheStringArg) {
		index = 0;
		int column = 0;
		int slash = path.replace('\\', '/').lastIndexOf('/');
		String filename = path.substring(slash + 1);
		int dot = filename.lastIndexOf('.');
		if (dot < 0)
			dot = filename.length();
		this.identity = filename.substring(0, dot);
		if (this.identity.startsWith("CUDB")) {
			column = 1;
		}
		prepareMemCaches(parallelism, memcacheLongArg, memcacheStringArg);
		run(filename, path, parallelism, column);
	}

	public QuickGzipReader(String path, int parallelism, HTreeMap<Long, String> memcacheLongArg,
			HTreeMap<String, String> memcacheStringArg, boolean argExtractAlias) {
		index = 0;
		int column = 0;
		this.extractAliasParent = argExtractAlias;
		int slash = path.replace('\\', '/').lastIndexOf('/');
		String filename = path.substring(slash + 1);
		int dot = filename.lastIndexOf('.');
		if (dot < 0)
			dot = filename.length();
		this.identity = filename.substring(0, dot);
		if (this.identity.startsWith("CUDB") || argExtractAlias) {
			column = 1;
		}
		prepareMemCacheMaps(parallelism, memcacheLongArg, memcacheStringArg);
		runMap(filename, path, parallelism, column, argExtractAlias);
	}

	private void prepareMemCaches(int parallelism, KeySet<Long> memcacheLongArg, KeySet<String> memcacheStringArg) {
		boolean noCacheLong = memcacheLongArg == null; // || memcacheLongArg.isEmpty()
		boolean noCacheString = memcacheStringArg == null; // || memcacheStringArg.isEmpty()
		boolean noCaches = noCacheLong && noCacheString;
		if (noCaches) {
			DB db = DBMaker.memoryDirectDB().closeOnJvmShutdown().concurrencyScale(parallelism).make();
			this.memcacheLong = db.hashSet(this.identity + "_LONG", Serializer.LONG)
					.counterEnable().createOrOpen();
			this.memcacheString = db.hashSet(this.identity + "_STRING", Serializer.STRING)
					.counterEnable().createOrOpen();
		} else {
			this.memcacheLong = memcacheLongArg;
			this.memcacheString = memcacheStringArg;
		}
	}

	private void prepareMemCacheMaps(int parallelism, HTreeMap<Long, String> memcacheMapLongArg,
			HTreeMap<String, String> memcacheMapStringArg) {
		boolean noCacheLong = memcacheMapLongArg == null; // || memcacheMapLongArg.isEmpty()
		boolean noCacheString = memcacheMapStringArg == null; // || memcacheMapStringArg.isEmpty()
		boolean noCaches = noCacheLong && noCacheString;
		if (noCaches) {
			DB db = DBMaker.memoryDirectDB().closeOnJvmShutdown().concurrencyScale(parallelism).make();
			this.memcacheMapLong = db.hashMap(this.identity + "_LONG", Serializer.LONG, Serializer.STRING)
					.counterEnable().createOrOpen();
			this.memcacheMapString = db.hashMap(this.identity + "_STRING", Serializer.STRING, Serializer.STRING)
					.counterEnable().createOrOpen();
		} else {
			this.memcacheMapLong = memcacheMapLongArg;
			this.memcacheMapString = memcacheMapStringArg;
		}
	}

	private void run(String filename, String path, int parallelism, int column) {
		String strLine = "";
		long readCharsInternal = 0;
		long readLinesInternal = 0;
		int numOfCols = 0;
		QuickGzipReader qzr = null;
		InputStream in = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try (BufferedReader brInternal = null) {
			if (filename.toLowerCase().endsWith(GZIP_EXTENSION)) {
				in = new GZIPInputStream(new FileInputStream(path));
			} else {
				in = new FileInputStream(path);
			}
			br = brInternal;
			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			// read header
			if ((strLine = br.readLine()) != null) {
				readCharsInternal += strLine.length();
				readLinesInternal++;
				String[] columns = strLine.split("\t");
				numOfCols = columns.length;
			}
			// read body
			qzr = new QuickGzipReader(br, parallelism, numOfCols, column, this.memcacheLong, this.memcacheString, this.identity);
			this.readChars = qzr.getReadChars() + readCharsInternal;
			this.readLines = qzr.getReadLines() + readLinesInternal;
			this.minLenOfValue = qzr.getMinLenOfValue();
			this.maxLenOfValue = qzr.getMaxLenOfValue();
			this.identity = qzr.getIdentity();
		} catch (UnsupportedEncodingException ex) {
			LOG.error("Wrong encoding: " + ex.toString());
		} catch (FileNotFoundException ex) {
			LOG.error("Cannot find file:" + ex);
		} catch (IOException ex) {
			LOG.error("Reading error: " + ex);
		} finally {
			closeReaders(isr, br, in);
		}
	}

	private void runMap(String filename, String path, int parallelism, int column, boolean extract) {
		String strLine = "";
		long readCharsInternal = 0;
		long readLinesInternal = 0;
		int numOfCols = 0;
		QuickGzipReader qzr = null;
		InputStream in = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try (BufferedReader brInternal = null) {
			if (filename.toLowerCase().endsWith(GZIP_EXTENSION)) {
				in = new GZIPInputStream(new FileInputStream(path));
			} else {
				in = new FileInputStream(path);
			}
			br = brInternal;
			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			// read header
			if ((strLine = br.readLine()) != null) {
				readCharsInternal += strLine.length();
				readLinesInternal++;
				String[] columns = strLine.split("\t");
				numOfCols = columns.length;
			}
			// read body
			qzr = new QuickGzipReader().setExtractAlias(extract).constr(br, parallelism, numOfCols, column, this.memcacheMapLong, this.memcacheMapString, this.identity);
			this.readChars = qzr.getReadChars() + readCharsInternal;
			this.readLines = qzr.getReadLines() + readLinesInternal;
			this.minLenOfValue = qzr.getMinLenOfValue();
			this.maxLenOfValue = qzr.getMaxLenOfValue();
			this.identity = qzr.getIdentity();
		} catch (UnsupportedEncodingException ex) {
			LOG.error("Wrong encoding: " + ex.toString());
		} catch (FileNotFoundException ex) {
			LOG.error("Cannot find file:" + ex);
		} catch (IOException ex) {
			LOG.error("Reading error: " + ex);
		} finally {
			closeReaders(isr, br, in);
		}
	}

	private void closeReaders(InputStreamReader isr, BufferedReader br, InputStream in) {
		if (isr != null) {
			try {
				isr.close();
			} catch (IOException ex) {
				LOG.warn("Cannot close Input Stream Reader of a file: " + ex);
			}
		}
		if (br != null) {
			try {
				br.close();
			} catch (IOException ex) {
				LOG.warn("Cannot close Buffered Reader of a file: " + ex);
			}
		}
		if (in != null) {
			try {
				in.close();
			} catch (IOException ex) {
				LOG.warn("Cannot close Input Stream of a file: " + ex);
			}
		}
	}

	public QuickGzipReader(BufferedReader br, int parallelism, int numberOfColumns, int expectedColumnNumber,
			KeySet<Long> memcacheLong, KeySet<String> memcacheString, String identityArg) {
		constr(br, parallelism, numberOfColumns, expectedColumnNumber, memcacheMapLong, memcacheMapString, identityArg);
	}

	public QuickGzipReader() {
		
	}
	
	public QuickGzipReader constr(BufferedReader br, int parallelism, int numberOfColumns, int expectedColumnNumber,
			HTreeMap<Long, String> memcacheMapLong, HTreeMap<String, String> memcacheMapString, String identityArg) {
		if (memcacheMapLong == null && memcacheMapString == null) {
			throw new IllegalArgumentException(
					"Cannot construct QuickGzipReader object as some arguments are null or empty");
		}
		index = 0;
		this.identity = identityArg;
		try {
			Thread[] pool = new Thread[parallelism];
			for (int threadNum = 0; threadNum < parallelism; threadNum++) {
				ReaderThread thread = new ReaderThread(br, numberOfColumns, expectedColumnNumber, threadNum,
						memcacheMapLong, memcacheMapString, identityArg).setEncoding(DEFAULT_CHARSET).setExtractAlias(this.extractAliasParent);
				thread.start();
				pool[threadNum] = thread;
			}
			for (int threadNum = 0; threadNum < parallelism; threadNum++) {
				ReaderThread thread = (ReaderThread) pool[threadNum];
				thread.join();
				this.readChars += thread.getReadCharsInt();
				this.readLines += thread.getReadLinesInt();
			}
		} catch (InterruptedException ex) {
			LOG.error("Interrupted" + EXC_OCC + ex.getMessage() + "\n" + Arrays.toString(ex.getStackTrace()));
			Thread.currentThread().interrupt();
		}
		return this;
	}

	public QuickGzipReader(BufferedReader br, int parallelism, int numberOfColumns, int expectedColumnNumber,
			HTreeMap<Long, String> memcacheMapLong, HTreeMap<String, String> memcacheMapString, String identityArg) {
		constr(br, parallelism, numberOfColumns, expectedColumnNumber, memcacheMapLong, memcacheMapString, identityArg);
	}

	/**
	 * @return the extractAlias
	 */
	public boolean isExtractAlias() {
		return extractAliasParent;
	}

	/**
	 * @param extractAlias the extractAlias to set
	 */
	public QuickGzipReader setExtractAlias(boolean extractAlias) {
		this.extractAliasParent = extractAlias;
		return this;
	}

	/**
	 * @return the readChars
	 */
	public long getReadChars() {
		return readChars;
	}

	/**
	 * @param readChars the readChars to set
	 */
	public void setReadChars(long readChars) {
		this.readChars = readChars;
	}

	/**
	 * @return the readLines
	 */
	public long getReadLines() {
		return readLines;
	}

	/**
	 * @param readLines the readLines to set
	 */
	public void setReadLines(long readLines) {
		this.readLines = readLines;
	}

	/**
	 * @return the minLenOfValue
	 */
	public int getMinLenOfValue() {
		return minLenOfValue;
	}

	/**
	 * @param minLenOfValue the minLenOfValue to set
	 */
	public void setMinLenOfValue(int minLenOfValue) {
		this.minLenOfValue = minLenOfValue;
	}

	/**
	 * @return the maxLenOfValue
	 */
	public int getMaxLenOfValue() {
		return maxLenOfValue;
	}

	/**
	 * @param maxLenOfValue the maxLenOfValue to set
	 */
	public void setMaxLenOfValue(int maxLenOfValue) {
		this.maxLenOfValue = maxLenOfValue;
	}

	/**
	 * @return the identity
	 */
	public String getIdentity() {
		return identity;
	}

	/**
	 * @param identity the identity to set
	 */
	public void setIdentity(String identity) {
		this.identity = identity;
	}

	/**
	 * @return the memcacheLong
	 */
	public KeySet<Long> getMemcacheLong() {
		return memcacheLong;
	}

	/**
	 * @param memcacheLong the memcacheLong to set
	 */
	public void setMemcacheLong(KeySet<Long> memcacheLong) {
		this.memcacheLong = memcacheLong;
	}

	/**
	 * @return the memcacheMapLong
	 */
	public HTreeMap<Long, String> getMemcacheMapLong() {
		return memcacheMapLong;
	}

	/**
	 * @param memcacheMapLong the memcacheMapLong to map
	 */
	public void setMemcacheMapLong(HTreeMap<Long, String> memcacheMapLong) {
		this.memcacheMapLong = memcacheMapLong;
	}

	/**
	 * @return the memcacheString
	 */
	public KeySet<String> getMemcacheString() {
		return memcacheString;
	}

	/**
	 * @param memcacheString the memcacheString to set
	 */
	public void setMemcacheString(KeySet<String> memcacheString) {
		this.memcacheString = memcacheString;
	}

	/**
	 * @return the memcacheString
	 */
	public HTreeMap<String, String> getMemcacheMapString() {
		return memcacheMapString;
	}

	/**
	 * @param memcacheString the memcacheString to set
	 */
	public void setMemcacheMapString(HTreeMap<String, String> memcacheMapString) {
		this.memcacheMapString = memcacheMapString;
	}

	class ReaderThread extends Thread {

		private int currentThread;
		private int numOfCols;
		private int column;
		private boolean extractAlias;
		private int numOfFragmentsPerThread;
		private String identity = "SOME_IDENTITY";
		private String encoding = DEFAULT_CHARSET;
		private KeySet<Long> memcacheL;
		private KeySet<String> memcacheS;
		private HTreeMap<Long, String> memcacheMapL;
		private HTreeMap<String, String> memcacheMapS;
		private BufferedReader br;
		private long readCharsInt = 0;
		private long readLinesInt = 0;

		/**
		 * @return the extractAlias
		 */
		public boolean isExtractAlias() {
			return extractAlias;
		}

		/**
		 * @param extractAlias the extractAlias to set
		 */
		public ReaderThread setExtractAlias(boolean extractAlias) {
			this.extractAlias = extractAlias;
			return this;
		}

		public ReaderThread(BufferedReader bufreader, int numberOfColumns, int expectedColumnNumber, int threadNum,
				KeySet<Long> memcacheLong, KeySet<String> memcacheString, String identityArg) {
			this.br = bufreader;
			this.currentThread = threadNum;
			this.memcacheL = memcacheLong;
			this.memcacheS = memcacheString;
			this.numOfCols = numberOfColumns;
			this.column = expectedColumnNumber;
			this.identity = identityArg;
		}

		public ReaderThread(BufferedReader bufreader, int numberOfColumns, int expectedColumnNumber, int threadNum,
				HTreeMap<Long, String> memcacheMapLong, HTreeMap<String, String> memcacheMapString, String identityArg) {
			this.br = bufreader;
			this.currentThread = threadNum;
			this.memcacheMapL = memcacheMapLong;
			this.memcacheMapS = memcacheMapString;
			this.numOfCols = numberOfColumns;
			this.column = expectedColumnNumber;
			this.identity = identityArg;
		}

		public int processRow(String line, KeySet<Long> memcacheL, KeySet<String> memcacheS) {
			if (memcacheL == null || memcacheS == null) {
				LOG.error("Fatal error! Memory cache objects are null! LONG cache: " + memcacheL + ", STRING cache: "
						+ memcacheS);
				return 0;
			}
			int processed = 0;
			if (line != null && !line.isEmpty() && line.length() >= 3) {
				String interestingValueAsString = chooseColumn(line);
				if (interestingValueAsString == null) {
					return 0;
				}
				boolean filled = chooseAndFillCache(interestingValueAsString);
				if (filled) {
					processed++;
				}
			}
			return processed;
		}

		public int processRow(String line, HTreeMap<Long, String> memcacheMapL, HTreeMap<String, String> memcacheMapS) {
			if (memcacheMapL == null || memcacheMapS == null) {
				LOG.error("Fatal error! Memory cache objects are null! LONG cache: " + memcacheMapL + ", STRING cache: "
						+ memcacheMapS);
				return 0;
			}
			int processed = 0;
			if (line != null && !line.isEmpty() && line.length() >= 3) {
				String interestingValueAsString = chooseColumn(line);
				if (interestingValueAsString == null) {
					return 0;
				}
				boolean filled = chooseAndFillMapCache(interestingValueAsString, line);
				if (filled) {
					processed++;
				}
			}
			return processed;
		}

		private String chooseColumn(String line) {
			return QuickReader.chooseColumn(line, this.identity, numOfCols, this.column, this.extractAlias);
		}

		private boolean chooseAndFillCache(String interestingValueAsString) {
			int lenOfValue = interestingValueAsString.length();
			if (lenOfValue < minLenOfValue) {
				minLenOfValue = lenOfValue;
			}
			if (lenOfValue > maxLenOfValue) {
				maxLenOfValue = lenOfValue;
			}
			return QuickReader.chooseAndFillCache(interestingValueAsString, this.identity, memcacheL, memcacheS);
		}

		private boolean chooseAndFillMapCache(String interestingValueAsString, String fullLine) {
			int lenOfValue = interestingValueAsString.length();
			if (lenOfValue < minLenOfValue) {
				minLenOfValue = lenOfValue;
			}
			if (lenOfValue > maxLenOfValue) {
				maxLenOfValue = lenOfValue;
			}
			if (extractAliasParent && (this.identity.toUpperCase().contains("IMSI") || this.identity.toUpperCase().contains("MSISDN"))) {
				return QuickReader.chooseAndFillMapCache(interestingValueAsString, fullLine, "", memcacheMapL, memcacheMapS);
			} else {
				return QuickReader.chooseAndFillMapCache(interestingValueAsString, fullLine, this.identity, memcacheMapL, memcacheMapS);
			}
		}

		@Override
		public void run() {
			// read body
			String strLine = "";
			try {
				if (memcacheL != null || memcacheS != null) {
					while ((strLine = br.readLine()) != null) {
						int readCharsHere = strLine.length();
						int readLinesHere = processRow(strLine, memcacheL, memcacheS);
						readCharsInt += readCharsHere;
						readLinesInt += readLinesHere;
						index += readLinesHere;
						if (index % 50000 == 0) {
							System.out.println("Processing row " + index);
						}
					}
				} else if (memcacheMapL != null || memcacheMapS != null) {
					while ((strLine = br.readLine()) != null) {
						int readCharsHere = strLine.length();
						int readLinesHere = processRow(strLine, memcacheMapL, memcacheMapS);
						readCharsInt += readCharsHere;
						readLinesInt += readLinesHere;
						index += readLinesHere;
						if (index % 50000 == 0) {
							System.out.println("Processing row " + index);
						}
					}
				} else {
					LOG.error("All of mamcaches KeySet or HTreeMap STRING/LONG are null");
				}
			} catch (UnsupportedEncodingException ex) {
				LOG.error("Wrong encoding: " + ex.toString());
			} catch (IOException ex) {
				LOG.error("Reading error: " + ex);
			}
		}

		/**
		 * @return the readCharsInt
		 */
		public long getReadCharsInt() {
			return readCharsInt;
		}

		/**
		 * @param readCharsInt the readCharsInt to set
		 */
		public void setReadCharsInt(long readCharsInt) {
			this.readCharsInt = readCharsInt;
		}

		/**
		 * @return the readLinesInt
		 */
		public long getReadLinesInt() {
			return readLinesInt;
		}

		/**
		 * @param readLinesInt the readLinesInt to set
		 */
		public void setReadLinesInt(long readLinesInt) {
			this.readLinesInt = readLinesInt;
		}

		/**
		 * @return the currentThread
		 */
		public int getCurrentThread() {
			return currentThread;
		}

		/**
		 * @param currentThread the currentThread to set
		 */
		public void setCurrentThread(int currentThread) {
			this.currentThread = currentThread;
		}

		/**
		 * @return the memcacheL
		 */
		public KeySet<Long> getMemcacheL() {
			return memcacheL;
		}

		/**
		 * @param memcacheL the memcacheL to set
		 */
		public ReaderThread setMemcacheL(KeySet<Long> memcacheL) {
			this.memcacheL = memcacheL;
			return this;
		}

		/**
		 * @return the memcacheS
		 */
		public KeySet<String> getMemcacheS() {
			return memcacheS;
		}

		/**
		 * @param memcacheS the memcacheS to set
		 */
		public ReaderThread setMemcacheS(KeySet<String> memcacheS) {
			this.memcacheS = memcacheS;
			return this;
		}

		/**
		 * @return the numOfFragmentsPerThread
		 */
		public int getNumOfFragmentsPerThread() {
			return numOfFragmentsPerThread;
		}

		/**
		 * @param numOfFragmentsPerThread the numOfFragmentsPerThread to set
		 */
		public ReaderThread setNumOfFragmentsPerThread(int numOfFragmentsPerThread) {
			this.numOfFragmentsPerThread = numOfFragmentsPerThread;
			return this;
		}

		/**
		 * @return the column
		 */
		public int getColumn() {
			return column;
		}

		/**
		 * @param column the column to set
		 */
		public ReaderThread setColumn(int column) {
			this.column = column;
			return this;
		}

		/**
		 * @return the identity
		 */
		public String getIdentity() {
			return identity;
		}

		/**
		 * @param identity the identity to set
		 */
		public ReaderThread setIdentity(String identity) {
			this.identity = identity;
			return this;
		}

		/**
		 * @return the encoding
		 */
		public String getEncoding() {
			return encoding;
		}

		/**
		 * @param encoding the encoding to set
		 */
		public ReaderThread setEncoding(String encoding) {
			this.encoding = encoding;
			return this;
		}

	}

	public static void main(String[] args) {
		String inputDirCommon = "c:/Git_Repository/Tools/_gitlab.rosetta.ericssondevops.com,git.rosetta.ericssondevops.com/dm_app_pdi_etl_udc2udc/dev/src/input/target_udc";
		String inputDir = inputDirCommon + "/";
		String filename = inputDir + "IDEN_MSISDN.log" + GZIP_EXTENSION;
		int parallelThreads = 128;
		long timestamp0 = System.nanoTime();
		KeySet<Long> memcacheLong = null;
		KeySet<String> memcacheString = null;
		QuickGzipReader qzr = new QuickGzipReader(filename, parallelThreads, memcacheLong, memcacheString);
		long timestamp1 = System.nanoTime();
		double timeInSec = (timestamp1 - timestamp0) / 1000000000.0;
		LOG.info("Time elapsed: " + timeInSec + " seconds. Read " + qzr.readChars + " characters forming "
				+ qzr.getReadLines() + " lines from file: " + filename);
		int sizeLong = qzr.getMemcacheLong() != null ? qzr.getMemcacheLong().size() : 0;
		int sizeString = qzr.getMemcacheString() != null ? qzr.getMemcacheString().size() : 0;
		int size = sizeLong + sizeString;
		LOG.info("Number of elements stored in memory KeySet cache of identity " + qzr.getIdentity() + " is: " + size
				+ " (Longs: " + sizeLong + ", Strings: " + sizeString + ", Length: " + qzr.getMinLenOfValue() + ".."
				+ qzr.getMaxLenOfValue() + ")");
		if (sizeLong > 0) {
			LOG.info("First sample data in KeySet LONG: " + qzr.getMemcacheLong().iterator().next());
		} else if (sizeString > 0) {
			LOG.info("First sample data in KeySet STRING: " + qzr.getMemcacheString().iterator().next());
		}
		HTreeMap<Long, String> memcacheMapLong = null;
		HTreeMap<String, String> memcacheMapString = null;
		qzr = new QuickGzipReader(filename, parallelThreads, memcacheMapLong, memcacheMapString, false);
		timestamp1 = System.nanoTime();
		timeInSec = (timestamp1 - timestamp0) / 1000000000.0;
		LOG.info("Time elapsed: " + timeInSec + " seconds. Read " + qzr.readChars + " characters forming "
				+ qzr.getReadLines() + " lines from file: " + filename);
		sizeLong = qzr.getMemcacheMapLong() != null ? qzr.getMemcacheMapLong().size() : 0;
		sizeString = qzr.getMemcacheMapString() != null ? qzr.getMemcacheMapString().size() : 0;
		size = sizeLong + sizeString;
		LOG.info("Number of elements stored in memory HTreeMap cache of identity " + qzr.getIdentity() + " is: " + size
				+ " (Longs: " + sizeLong + ", Strings: " + sizeString + ", Length: " + qzr.getMinLenOfValue() + ".."
				+ qzr.getMaxLenOfValue() + ")");
		if (sizeLong > 0) {
			LOG.info("First sample data in HTreeMap LONG: " + qzr.getMemcacheMapLong().entrySet().iterator().next());
		} else if (sizeString > 0) {
			LOG.info("First sample data in HTreeMap STRING: " + qzr.getMemcacheMapString().entrySet().iterator().next());
		}
	}

}
