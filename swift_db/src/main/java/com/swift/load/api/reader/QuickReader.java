package com.swift.load.api.reader;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.filefilter.RegexFileFilter;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.HTreeMap.KeySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mapdb.Serializer;

public class QuickReader {

	private static final Logger LOG = LoggerFactory.getLogger(QuickReader.class);
	private static final int DEFAULT_NUMBER_OF_FILE_FRAGMENTS_PER_THREAD = 1;
	private static final boolean DEFAULT_VALUE_FOR_EXTRACT_FIELD_FROM_ALIAS = false;
	private static final int KILO = 1024;
	private static final String DEFAULT_CHARSET = "UTF-8";
	private static final String EXC_OCC = "Exception occured: ";
	private static final String MSG_WARN_FILE = "Warning! File ";
	private static final String IS = " is: ";

	public static long index = 0;

	private int numOfThreads = 16;
	private int bufferSize = 32768;
	private int column = 0;
	private boolean extract = false;
	private String filePathAsString = "";
	private String identity = "ANY_IDENTITY";
	private KeySet<Long> cacheL = null;
	private KeySet<String> cacheS = null;
	private HTreeMap<Long, String> cacheMapL = null;
	private HTreeMap<String, String> cacheMapS = null;
	protected int minLenOfValue = Integer.MAX_VALUE;
	protected int maxLenOfValue = Integer.MIN_VALUE;
	protected int numOfCols = 0;
	protected int numOfFrags = 0;

	public class MoreFiles {

		private String[] regexVarValues;
		private String[] cacheVarValues;
		private int numerOfIdentityFiles;
		private int[] interestingColumns;
		private int[] parallelisms;
		private KeySet<?>[] caches;
		private HTreeMap<?, ?>[] cacheMaps;
		private String[] cacheNames;
		private String[] identityNames;
		private String[] fileNames;
		private String[] filePaths;
		private Thread[] pool;
		private File[][] selectedFilesPerId;
		private int bufsize = 64 * 1024;
		private int numOfThreads = 16;
		private int howMuchLoadedInTotal = 0;
		private int howMuchThreadsInTotal = 1;

		/**
		 * @return the howMuchLoadedInTotal
		 */
		public int getHowMuchLoadedInTotal() {
			return howMuchLoadedInTotal;
		}

		/**
		 * @return the howMuchThreadsInTotal
		 */
		public int getHowMuchThreadsInTotal() {
			return howMuchThreadsInTotal;
		}

		public MoreFiles(int parallelismFactor, int bufSizeArg, String[] regexVarArg, String[] cacheVarArg,
				KeySet<?>[] cachesArg) {
			this.numOfThreads = parallelismFactor;
			this.bufsize = bufSizeArg;
			this.regexVarValues = regexVarArg;
			this.cacheVarValues = cacheVarArg;
			this.numerOfIdentityFiles = regexVarValues.length;
			this.interestingColumns = new int[numerOfIdentityFiles];
			this.parallelisms = new int[numerOfIdentityFiles];
			this.caches = cachesArg;
			this.cacheNames = cacheVarArg;
			this.identityNames = new String[numerOfIdentityFiles];
			this.fileNames = new String[numerOfIdentityFiles];
			this.filePaths = new String[numerOfIdentityFiles];
			this.pool = new Thread[this.numerOfIdentityFiles];
			this.selectedFilesPerId = new File[this.numerOfIdentityFiles][];
		}

		public MoreFiles(int parallelismFactor, int bufSizeArg, String[] regexVarArg, String[] cacheVarArg,
				HTreeMap<?, ?>[] cachesArg) {
			this.numOfThreads = parallelismFactor;
			this.bufsize = bufSizeArg;
			this.regexVarValues = regexVarArg;
			this.cacheVarValues = cacheVarArg;
			this.numerOfIdentityFiles = regexVarValues.length;
			this.interestingColumns = new int[numerOfIdentityFiles];
			this.parallelisms = new int[numerOfIdentityFiles];
			this.cacheMaps = cachesArg;
			this.cacheNames = cacheVarArg;
			this.identityNames = new String[numerOfIdentityFiles];
			this.fileNames = new String[numerOfIdentityFiles];
			this.filePaths = new String[numerOfIdentityFiles];
			this.pool = new Thread[this.numerOfIdentityFiles];
			this.selectedFilesPerId = new File[this.numerOfIdentityFiles][];
		}

		public void init(String directory) {
			if (directory == null || regexVarValues == null || cacheVarValues == null) {
				throw new IllegalArgumentException(
						"Cannot initialize MoreFiles object as some argumenta are null or empty");
			}
			File dir = precheckDir(directory);
			if (dir != null) {
				processFilesFromDir(dir);
			}
		}

		private File precheckDir(String directory) {
			File dir = new File(directory);
			if (!dir.exists()) {
				LOG.error("Initialization failure: Cannot find a directory: " + directory);
				return null;
			} else if (!dir.isDirectory()) {
				LOG.error("Initialization failure: Path is not a directory: " + directory);
				return null;
			}
			return dir;
		}

		private void processFilesFromDir(File dir) {
			long totalSize = chooseFiles(dir);
			long averageSize = totalSize / this.numerOfIdentityFiles;
			int averagePercentage = 100 / this.numerOfIdentityFiles;
			for (int i = 0; i < this.numerOfIdentityFiles; i++) {
				File[] selectedFiles = this.selectedFilesPerId[i];
				if (selectedFiles == null) {
					continue;
				}
				for (int j = 0; j < selectedFiles.length; j++) {
					File file = selectedFiles[j];
					boolean processed = processFile(file, i, selectedFiles.length, totalSize, averageSize, averagePercentage);
					if (!processed) {
						LOG.warn("File " + file.getAbsolutePath() + " has not been processed due to issue happened");
					}
				}
			}
		}

		private long chooseFiles(File dir) {
			long totalSize = 0;
			for (int i = 0; i < this.numerOfIdentityFiles; i++) {
				String regexPattern = regexVarValues[i];
				String cacheName = cacheVarValues[i];
				if (regexPattern == null || cacheName == null) {
					continue;
				}
				String cacheNameUp = cacheName.toUpperCase();
				boolean identityIsMscId = cacheNameUp.contains("MSCID");
				boolean identityIsAssocId = cacheNameUp.contains("ASSOC");
				if (identityIsMscId || identityIsAssocId) {
					interestingColumns[i] = 1;
				} else {
					interestingColumns[i] = 0;
				}
				FileFilter filter = new RegexFileFilter(regexPattern);
				File[] selectedFiles = dir.listFiles(filter);
				this.selectedFilesPerId[i] = selectedFiles;
				for (int j = 0; j < selectedFiles.length; j++) {
					File file = selectedFiles[j];
					long fileSize = file.length();
					if (file.getAbsolutePath().endsWith(".gz")
							|| file.getAbsolutePath().endsWith(".gzip")
							|| file.getAbsolutePath().endsWith(".tgz")
					) {
						fileSize = fileSize << 2;
					}
					totalSize += fileSize;
				}
				this.cacheNames[i] = cacheName;
			}
			return totalSize;
		}

		private boolean processFile(File file, int i, int numOfFilesFound, long totalSize, long averageSize, int averagePercentage) {
			if (file == null) {
				return false;
			}
			String path = file.getAbsolutePath();
			long size = file.length();
			if (path.endsWith(".gz") || path.endsWith(".gzip") || path.endsWith(".tgz")) {
				size = size << 2;
			}
			boolean fileCheckedOk = precheckFile(file, path, size);
			if (!fileCheckedOk) {
				return false;
			}
			int slash = path.lastIndexOf('/');
			int backslash = path.lastIndexOf('\\');
			if (slash < 0 || backslash > slash) {
				slash = backslash;
			}
			if (slash < 0) {
				slash = path.length() - 1;
			}
			String filename = path.substring(slash + 1);
			if (file.exists()) {
				this.fileNames[i] = filename;
				this.filePaths[i] = path;
				identityNames[i] = takeIdentity(filename);
				LOG.info("File to process: " + filename);
				processFileInternal(i, size, numOfFilesFound, totalSize, averageSize, averagePercentage);
			} else {
				LOG.error("Initialization failure: Cannot find file " + filename);
			}
			return true;
		}

		private void processFileInternal(int i, long size, int numOfFilesFound, long totalSize, long averageSize, int averagePercentage) {
			float percentage = totalSize != 0 ? (100 * size) / (float) totalSize : 100.0f;
			boolean idFileIsHuge = size > averageSize || percentage > averagePercentage || size >= 1 * 1024 * 1024;
			LOG.info("File size is percentage of total size to process: " + percentage + " %");
			LOG.info("File is detected as being big or even huge: " + idFileIsHuge);
			if (!idFileIsHuge) {
				int megabytes = (int) (size >> 20);
				int meg8 = megabytes >> 3;
				parallelisms[i] = meg8;
			} else {
				parallelisms[i] = totalSize > 0 ? (int) (Integer.MAX_VALUE & ((this.numOfThreads * size) / totalSize)) : this.numOfThreads / numOfFilesFound;
			}
			if (parallelisms[i] < 1) {
				parallelisms[i] = 1;
			} else if (parallelisms[i] > 1024) {
				parallelisms[i] = 1024;
			}
			int howMuchThreadsPerThisFile = parallelisms[i] + 1;
			LOG.info("How much threads will be used to process this file: " + howMuchThreadsPerThisFile);
			this.howMuchThreadsInTotal += howMuchThreadsPerThisFile;
		}

		private boolean precheckFile(File file, String path, long size) {
			if (file == null) {
				return false;
			}
			if (!file.exists()) {
				LOG.error(MSG_WARN_FILE + path + " does not exist! Skipping it!");
				return false;
			} else if (!file.isFile()) {
				LOG.error(MSG_WARN_FILE + path + " looks to be not a file! Skipping it!");
				return false;
			} else if (!file.canRead()) {
				LOG.error(MSG_WARN_FILE + path + " is not readable! Skipping it!");
				return false;
			} else if (size <= 0) {
				LOG.error(
						MSG_WARN_FILE + path + " is wrong or empty as its size is " + size + ". Skipping it!");
				return false;
			}
			return true;
		}

		private String takeIdentity(String filename) {
			int dot = filename.lastIndexOf('.');
			if (dot < 0) {
				dot = filename.length();
			}
			return filename.substring(0, dot);
		}

		public void run() {
			for (int i = 0; i < this.numerOfIdentityFiles; i++) {
				pool[i] = new ParentThread(i);
				pool[i].start();
			}
			for (int i = 0; i < this.numerOfIdentityFiles; i++) {
				try {
					pool[i].join();
				} catch (InterruptedException ex) {
					LOG.error("InterruptedException occured: " + ex.getMessage() + "\n"
							+ Arrays.toString(ex.getStackTrace()));
					Thread.currentThread().interrupt();
				}
			}
		}

		public class ParentThread extends Thread {

			private int currentThread = 0;

			public ParentThread(int threadNum) {
				this.currentThread = threadNum;
			}

			@Override
			public void run() {
				int i = this.currentThread;
				KeySet<Long> memcacheLong = null;
				KeySet<String> memcacheString = null;
				HTreeMap<Long, String> memcacheMapLong = null;
				HTreeMap<String, String> memcacheMapString = null;
				if (caches == null && cacheMaps == null) {
					LOG.error("Unfortunately caches object is null");
					return;
				} else if (caches != null && caches[i] == null && cacheMaps != null && cacheMaps[i] == null) {
					LOG.error("Unfortunately caches[" + i + "] object is null");
					return;
				} else if (caches != null && caches[i] instanceof KeySet<?>) {
					KeySet<?> memcacheObject = caches[i];
					Iterator<?> it = memcacheObject.iterator();
					if (it.hasNext()) {
						while (it.hasNext()) {
							Object o = it.next();
							if (o instanceof String) {
								memcacheString = (KeySet<String>) caches[i];
							} else if (o instanceof Long) {
								memcacheLong = (KeySet<Long>) caches[i];
							} else {
								LOG.error("Unsupported type of key in memory cache: " + o.getClass().getName());
							}
							if (memcacheString != null && memcacheLong != null) {
								break;
							}
						}
					} else {
						memcacheString = (KeySet<String>) caches[i];
						memcacheLong = (KeySet<Long>) caches[i];
					}
					int howMuchLoaded = invokeRawOrGzipReader(i, memcacheLong, memcacheString);
					howMuchLoadedInTotal += howMuchLoaded;
				} else if (cacheMaps != null && cacheMaps[i] instanceof HTreeMap<?, ?>) {
					HTreeMap<?, ?> memcacheObject = cacheMaps[i];
					Iterator<?> it = memcacheObject.keySet().iterator();
					if (it.hasNext()) {
						while (it.hasNext()) {
							Object o = it.next();
							if (o instanceof String) {
								memcacheMapString = (HTreeMap<String, String>) cacheMaps[i];
							} else if (o instanceof Long) {
								memcacheMapLong = (HTreeMap<Long, String>) cacheMaps[i];
							} else {
								LOG.error("Unsupported type of key in memory cache: " + o.getClass().getName());
							}
							if (memcacheMapString != null && memcacheMapLong != null) {
								break;
							}
						}
					} else {
						memcacheMapString = (HTreeMap<String, String>) cacheMaps[i];
						memcacheMapLong = (HTreeMap<Long, String>) cacheMaps[i];
					}
					int howMuchLoaded = invokeRawOrGzipReader(i, memcacheMapLong, memcacheMapString);
					howMuchLoadedInTotal += howMuchLoaded;
				} else {
					LOG.error("Unsupported type of cache object: " + cacheMaps[i].getClass().getName());
				}
			}

			private int invokeRawOrGzipReader(int i, KeySet<Long> memcacheLong, KeySet<String> memcacheString) {
				int howMuchLoaded = 0;
				String path = filePaths[i];
				if (path.endsWith(QuickGzipReader.GZIP_EXTENSION)) {
					QuickGzipReader qzr = new QuickGzipReader(path, parallelisms[i], memcacheLong, memcacheString);
					LOG.info("Read " + qzr.getReadChars() + " characters forming " + qzr.getReadLines()
							+ " lines from file: " + path);
					int sizeLong = qzr.getMemcacheLong() != null ? qzr.getMemcacheLong().size() : 0;
					int sizeString = qzr.getMemcacheString() != null ? qzr.getMemcacheString().size() : 0;
					if (sizeLong >= sizeString) {
						caches[i] = qzr.getMemcacheLong();
					} else if (sizeString > sizeLong) {
						caches[i] = qzr.getMemcacheString();
					}
					int size = sizeLong + sizeString;
					LOG.info("Number of elements stored in memory cache of identity " + qzr.getIdentity() + IS
							+ size + " (Longs: " + sizeLong + ", Strings: " + sizeString + ", Length: "
							+ qzr.getMinLenOfValue() + ".." + qzr.getMaxLenOfValue() + ")");
					howMuchLoaded = size;
				} else {
					new QuickReader(parallelisms[i], bufsize, memcacheLong, memcacheString, interestingColumns[i],
							cacheNames[i], path);
					LOG.info("Read lines from file: " + path);
					int size = caches[i].size();
					LOG.info("Number of elements stored in memory cache " + cacheNames[i] + IS + size);
					howMuchLoaded = size;
				}
				return howMuchLoaded;
			}

			private int invokeRawOrGzipReader(int i, HTreeMap<Long, String> memcacheMapLong, HTreeMap<String, String> memcacheMapString) {
				int howMuchLoaded = 0;
				String path = filePaths[i];
				if (path.endsWith(QuickGzipReader.GZIP_EXTENSION)) {
					QuickGzipReader qzr = new QuickGzipReader(path, parallelisms[i], memcacheMapLong, memcacheMapString, extract);
					LOG.info("Read " + qzr.getReadChars() + " characters forming " + qzr.getReadLines()
							+ " lines from file: " + path);
					int sizeLong = qzr.getMemcacheMapLong() != null ? qzr.getMemcacheMapLong().size() : 0;
					int sizeString = qzr.getMemcacheMapString() != null ? qzr.getMemcacheMapString().size() : 0;
					if (sizeLong >= sizeString) {
						cacheMaps[i] = qzr.getMemcacheMapLong();
					} else if (sizeString > sizeLong) {
						cacheMaps[i] = qzr.getMemcacheMapString();
					}
					int size = sizeLong + sizeString;
					LOG.info("Number of elements stored in memory cache of identity " + qzr.getIdentity() + IS
							+ size + " (Longs: " + sizeLong + ", Strings: " + sizeString + ", Length: "
							+ qzr.getMinLenOfValue() + ".." + qzr.getMaxLenOfValue() + ")");
					howMuchLoaded = size;
				} else {
					QuickReader qr = new QuickReader();
					qr.invokeConstrForMap(parallelisms[i], bufsize, memcacheMapLong, memcacheMapString, interestingColumns[i], cacheNames[i], path);
					qr.setExtract(extract);
					qr.createMapThreadPool();
					LOG.info("Read lines from file: " + path);
					int size = cacheMaps[i].size();
					LOG.info("Number of elements stored in memory cache " + cacheNames[i] + IS + size);
					howMuchLoaded = size;
				}
				return howMuchLoaded;
			}

		}

	}

	private MoreFiles mr;
	public static final String ERR_MSG_P1 = "Unfortunately cannot ";
	public static final String ERR_MSG_P2 = " of MoreFiles object as this object is null :(";

	public void run() {
		if (mr == null) {
			LOG.error(ERR_MSG_P1 + "execure run()" + ERR_MSG_P2);
			return;
		}
		mr.run();
	}

	public int identitiesLoaded() {
		if (mr == null) {
			LOG.error(ERR_MSG_P1 + "getHowMuchLoadedInTotal()" + ERR_MSG_P2);
			return 0;
		}
		int loaded = mr.getHowMuchLoadedInTotal();
		if (loaded <= 0 && mr.caches != null) {
			loaded = 0;
			for (int i = 0; i < mr.caches.length; i++) {
				loaded += mr.caches[i].getSize();
			}
			mr.howMuchLoadedInTotal = loaded;
		}
		if (loaded <= 0 && mr.cacheMaps != null) {
			loaded = 0;
			for (int i = 0; i < mr.cacheMaps.length; i++) {
				loaded += mr.cacheMaps[i].getSize();
			}
			mr.howMuchLoadedInTotal = loaded;
		}
		return loaded;
	}

	public int threadsUsed() {
		if (mr == null) {
			LOG.error(ERR_MSG_P1 + "getHowMuchThreadsInTotal()" + ERR_MSG_P2);
			return 0;
		}
		return mr.getHowMuchThreadsInTotal();
	}

	public QuickReader(int parallelismFactor, int bufSizeArg, String directory, String[] regexVarArg,
			String[] cacheVarArg, KeySet<?>[] cachesArg) {
		this.mr = new MoreFiles(parallelismFactor, bufSizeArg, regexVarArg, cacheVarArg, cachesArg);
		this.mr.init(directory);
	}

	public QuickReader(int parallelismFactor, int bufSizeArg, String directory, String[] regexVarArg,
			String[] cacheVarArg, HTreeMap<?, ?>[] cachesArg) {
		this.mr = new MoreFiles(parallelismFactor, bufSizeArg, regexVarArg, cacheVarArg, cachesArg);
		this.mr.init(directory);
	}

	public QuickReader(int parallelism, KeySet<Long> memcacheLong, KeySet<String> memcacheString, int interestingColumn,
			String identityName, String filePath) {
		this(parallelism, 64 * KILO, memcacheLong, memcacheString, interestingColumn, identityName, filePath);
	}

	public QuickReader setExtract(boolean v) {
		this.extract = v;
		return this;
	}

	public QuickReader() {
		// nothing
	}

	public void invokeConstrForSet(int parallelism, int bufsize, KeySet<Long> memcacheLong, KeySet<String> memcacheString,
			int interestingColumn, String identityName, String filePath) {
		if (filePath == null || filePath.isEmpty() || (memcacheLong == null && memcacheString == null)) {
			throw new IllegalArgumentException(
					"Cannot construct QuickReader object as some arguments are null or empty (filePath: " + filePath + ", memcacheLong: " + memcacheLong + ", memcacheString: " + memcacheString + ")");
		}
		this.filePathAsString = filePath;
		this.bufferSize = bufsize;
		this.numOfThreads = parallelism;
		this.cacheL = memcacheLong;
		this.cacheS = memcacheString;
		this.column = interestingColumn;
		this.identity = identityName;
		this.numOfFrags = DEFAULT_NUMBER_OF_FILE_FRAGMENTS_PER_THREAD;
	}
	
	public void invokeConstrForMap(int parallelism, int bufsize, HTreeMap<Long, String> memcacheLong, HTreeMap<String, String> memcacheString,
			int interestingColumn, String identityName, String filePath) {
		if (filePath == null || filePath.isEmpty() || (memcacheLong == null && memcacheString == null)) {
			throw new IllegalArgumentException(
					"Cannot construct QuickReader object as some arguments are null or empty (filePath: " + filePath + ", memcacheLong: " + memcacheLong + ", memcacheString: " + memcacheString + ")");
		}
		this.filePathAsString = filePath;
		this.bufferSize = bufsize;
		this.numOfThreads = parallelism;
		this.cacheMapL = memcacheLong;
		this.cacheMapS = memcacheString;
		this.column = interestingColumn;
		this.identity = identityName;
		this.extract = DEFAULT_VALUE_FOR_EXTRACT_FIELD_FROM_ALIAS;
		this.numOfFrags = DEFAULT_NUMBER_OF_FILE_FRAGMENTS_PER_THREAD;
	}

	public QuickReader(int parallelism, int bufsize, KeySet<Long> memcacheLong, KeySet<String> memcacheString,
			int interestingColumn, String identityName, String filePath) {
		invokeConstrForSet(parallelism, bufsize, memcacheLong, memcacheString, interestingColumn, identityName, filePath);
		createThreadPool();
	}

	public QuickReader(int parallelism, int bufsize, HTreeMap<Long, String> memcacheLong, HTreeMap<String, String> memcacheString,
			int interestingColumn, String identityName, String filePath) {
		invokeConstrForMap(parallelism, bufsize, memcacheLong, memcacheString, interestingColumn, identityName, filePath);
		createMapThreadPool();
	}

	private int perThreadNumOfFragments = 0;
	private int perThreadBufSize = 0;
	private int perThreadColumn = 0;
	private String perThreadEncoding = null;
	private long perThreadFileFrom = 0;
	private long perThreadFileTo = 0;
	private long perThreadIdOfThread = 0;
	private String perThreadIdentity = null;
	private long perThreadMapSize = 0;
	private int perThreadCacheSetLongSize = 0;
	private int perThreadCacheSetStringSize = 0;
	private int perThreadCacheMapLongSize = 0;
	private int perThreadCacheMapStringSize = 0;
	private String perThreadNameOfThread = null;
	private int perThreadPrioOfThread = 0;
	private String perThreadStateOfThread = "";
	private boolean perThreadIfExtractedAlias = false;
	private boolean perThreadIsAlive = false;
	private boolean perThreadIsInterrupted = false;
	private boolean perThreadIsDaemon = false;

	/**
	 * @return the numOfThreads
	 */
	public int getNumOfThreads() {
		return numOfThreads;
	}

	/**
	 * @param numOfThreads the numOfThreads to set
	 */
	public void setNumOfThreads(int numOfThreads) {
		this.numOfThreads = numOfThreads;
	}

	/**
	 * @return the numOfCols
	 */
	public int getNumOfCols() {
		return numOfCols;
	}

	/**
	 * @param numOfCols the numOfCols to set
	 */
	public void setNumOfCols(int numOfCols) {
		this.numOfCols = numOfCols;
	}

	/**
	 * @return the numOfFrags
	 */
	public int getNumOfFrags() {
		return numOfFrags;
	}

	/**
	 * @param numOfFrags the numOfFrags to set
	 */
	public void setNumOfFrags(int numOfFrags) {
		this.numOfFrags = numOfFrags;
	}

	/**
	 * @return the perThreadNumOfFragments
	 */
	public int getPerThreadNumOfFragments() {
		return perThreadNumOfFragments;
	}

	/**
	 * @param perThreadNumOfFragments the perThreadNumOfFragments to set
	 */
	public void setPerThreadNumOfFragments(int perThreadNumOfFragments) {
		this.perThreadNumOfFragments = perThreadNumOfFragments;
	}

	/**
	 * @return the perThreadBufSize
	 */
	public int getPerThreadBufSize() {
		return perThreadBufSize;
	}

	/**
	 * @param perThreadBufSize the perThreadBufSize to set
	 */
	public void setPerThreadBufSize(int perThreadBufSize) {
		this.perThreadBufSize = perThreadBufSize;
	}

	/**
	 * @return the perThreadColumn
	 */
	public int getPerThreadColumn() {
		return perThreadColumn;
	}

	/**
	 * @param perThreadColumn the perThreadColumn to set
	 */
	public void setPerThreadColumn(int perThreadColumn) {
		this.perThreadColumn = perThreadColumn;
	}

	/**
	 * @return the perThreadEncoding
	 */
	public String getPerThreadEncoding() {
		return perThreadEncoding;
	}

	/**
	 * @param perThreadEncoding the perThreadEncoding to set
	 */
	public void setPerThreadEncoding(String perThreadEncoding) {
		this.perThreadEncoding = perThreadEncoding;
	}

	/**
	 * @return the perThreadFileFrom
	 */
	public long getPerThreadFileFrom() {
		return perThreadFileFrom;
	}

	/**
	 * @param perThreadFileFrom the perThreadFileFrom to set
	 */
	public void setPerThreadFileFrom(long perThreadFileFrom) {
		this.perThreadFileFrom = perThreadFileFrom;
	}

	/**
	 * @return the perThreadFileTo
	 */
	public long getPerThreadFileTo() {
		return perThreadFileTo;
	}

	/**
	 * @param perThreadFileTo the perThreadFileTo to set
	 */
	public void setPerThreadFileTo(long perThreadFileTo) {
		this.perThreadFileTo = perThreadFileTo;
	}

	/**
	 * @return the perThreadIdOfThread
	 */
	public long getPerThreadIdOfThread() {
		return perThreadIdOfThread;
	}

	/**
	 * @param perThreadIdOfThread the perThreadIdOfThread to set
	 */
	public void setPerThreadIdOfThread(long perThreadIdOfThread) {
		this.perThreadIdOfThread = perThreadIdOfThread;
	}

	/**
	 * @return the perThreadIdentity
	 */
	public String getPerThreadIdentity() {
		return perThreadIdentity;
	}

	/**
	 * @param perThreadIdentity the perThreadIdentity to set
	 */
	public void setPerThreadIdentity(String perThreadIdentity) {
		this.perThreadIdentity = perThreadIdentity;
	}

	/**
	 * @return the perThreadMapSize
	 */
	public long getPerThreadMapSize() {
		return perThreadMapSize;
	}

	/**
	 * @param perThreadMapSize the perThreadMapSize to set
	 */
	public void setPerThreadMapSize(long perThreadMapSize) {
		this.perThreadMapSize = perThreadMapSize;
	}

	/**
	 * @return the perThreadCacheSetLongSize
	 */
	public int getPerThreadCacheSetLongSize() {
		return perThreadCacheSetLongSize;
	}

	/**
	 * @param perThreadCacheSetLongSize the perThreadCacheSetLongSize to set
	 */
	public void setPerThreadCacheSetLongSize(int perThreadCacheSetLongSize) {
		this.perThreadCacheSetLongSize = perThreadCacheSetLongSize;
	}

	/**
	 * @return the perThreadCacheSetStringSize
	 */
	public int getPerThreadCacheSetStringSize() {
		return perThreadCacheSetStringSize;
	}

	/**
	 * @param perThreadCacheSetStringSize the perThreadCacheSetStringSize to set
	 */
	public void setPerThreadCacheSetStringSize(int perThreadCacheSetStringSize) {
		this.perThreadCacheSetStringSize = perThreadCacheSetStringSize;
	}

	/**
	 * @return the perThreadCacheMapLongSize
	 */
	public int getPerThreadCacheMapLongSize() {
		return perThreadCacheMapLongSize;
	}

	/**
	 * @param perThreadCacheMapLongSize the perThreadCacheMapLongSize to set
	 */
	public void setPerThreadCacheMapLongSize(int perThreadCacheMapLongSize) {
		this.perThreadCacheMapLongSize = perThreadCacheMapLongSize;
	}

	/**
	 * @return the perThreadCacheMapStringSize
	 */
	public int getPerThreadCacheMapStringSize() {
		return perThreadCacheMapStringSize;
	}

	/**
	 * @param perThreadCacheMapStringSize the perThreadCacheMapStringSize to set
	 */
	public void setPerThreadCacheMapStringSize(int perThreadCacheMapStringSize) {
		this.perThreadCacheMapStringSize = perThreadCacheMapStringSize;
	}

	/**
	 * @return the perThreadNameOfThread
	 */
	public String getPerThreadNameOfThread() {
		return perThreadNameOfThread;
	}

	/**
	 * @param perThreadNameOfThread the perThreadNameOfThread to set
	 */
	public void setPerThreadNameOfThread(String perThreadNameOfThread) {
		this.perThreadNameOfThread = perThreadNameOfThread;
	}

	/**
	 * @return the perThreadPrioOfThread
	 */
	public int getPerThreadPrioOfThread() {
		return perThreadPrioOfThread;
	}

	/**
	 * @param perThreadPrioOfThread the perThreadPrioOfThread to set
	 */
	public void setPerThreadPrioOfThread(int perThreadPrioOfThread) {
		this.perThreadPrioOfThread = perThreadPrioOfThread;
	}

	/**
	 * @return the perThreadStateOfThread
	 */
	public String getPerThreadStateOfThread() {
		return perThreadStateOfThread;
	}

	/**
	 * @param perThreadStateOfThread the perThreadStateOfThread to set
	 */
	public void setPerThreadStateOfThread(String perThreadStateOfThread) {
		this.perThreadStateOfThread = perThreadStateOfThread;
	}

	/**
	 * @return the perThreadIfExtractedAlias
	 */
	public boolean isPerThreadIfExtractedAlias() {
		return perThreadIfExtractedAlias;
	}

	/**
	 * @param perThreadIfExtractedAlias the perThreadIfExtractedAlias to set
	 */
	public void setPerThreadIfExtractedAlias(boolean perThreadIfExtractedAlias) {
		this.perThreadIfExtractedAlias = perThreadIfExtractedAlias;
	}

	/**
	 * @return the perThreadIsAlive
	 */
	public boolean isPerThreadIsAlive() {
		return perThreadIsAlive;
	}

	/**
	 * @param perThreadIsAlive the perThreadIsAlive to set
	 */
	public void setPerThreadIsAlive(boolean perThreadIsAlive) {
		this.perThreadIsAlive = perThreadIsAlive;
	}

	/**
	 * @return the perThreadIsInterrupted
	 */
	public boolean isPerThreadIsInterrupted() {
		return perThreadIsInterrupted;
	}

	/**
	 * @param perThreadIsInterrupted the perThreadIsInterrupted to set
	 */
	public void setPerThreadIsInterrupted(boolean perThreadIsInterrupted) {
		this.perThreadIsInterrupted = perThreadIsInterrupted;
	}

	/**
	 * @return the perThreadIsDaemon
	 */
	public boolean isPerThreadIsDaemon() {
		return perThreadIsDaemon;
	}

	/**
	 * @param perThreadIsDaemon the perThreadIsDaemon to set
	 */
	public void setPerThreadIsDaemon(boolean perThreadIsDaemon) {
		this.perThreadIsDaemon = perThreadIsDaemon;
	}

	private void createThreadPoolMapOrSet(boolean useMap) {
		Path filePathAsPath = FileSystems.getDefault().getPath(filePathAsString);
		EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.READ);
		try (FileChannel channel = FileChannel.open(filePathAsPath, options)) {
			LOG.info("Creating thread pool consisting of " + numOfThreads + " threads...");
			Thread[] pool = new Thread[numOfThreads];
			LOG.info("Initializing and starting " + numOfThreads + " threads...");
			for (int threadNum = 0; threadNum < numOfThreads; threadNum++) {
				ReaderThread rt = null;
				if (useMap) {
					rt = new ReaderThread(channel, bufferSize, numOfThreads,
						threadNum, cacheMapL, cacheMapS);
				} else {
					rt = new ReaderThread(channel, bufferSize, numOfThreads,
							threadNum, cacheL, cacheS);
				}
				pool[threadNum] = rt.setColumn(column).setExtractAlias(extract)
								.setIdentity(identity).setEncoding(DEFAULT_CHARSET)
								.setNumOfFragmentsPerThread(numOfFrags);
				pool[threadNum].start();
			}
			LOG.info("Joining and executing " + numOfThreads + " threads...");
			for (int threadNum = 0; threadNum < numOfThreads; threadNum++) {
				pool[threadNum].join();
			}
			LOG.info("Collecting results of " + numOfThreads + " threads execution...");
			ReaderThread firstThread = (ReaderThread) pool[(numOfThreads >> 1)];
			this.perThreadBufSize = firstThread.getBufSize();
			this.perThreadColumn = firstThread.getColumn();
			this.perThreadEncoding = firstThread.getEncoding();
			this.perThreadFileFrom = firstThread.getFileFrom();
			this.perThreadFileTo = firstThread.getFileTo();
			this.perThreadIdOfThread = firstThread.getId();
			this.perThreadIdentity = firstThread.getIdentity();
			this.perThreadMapSize = firstThread.getMapSize();
			this.perThreadCacheSetLongSize = firstThread.getMemcacheL() == null ? 0 : firstThread.getMemcacheL().size();
			this.perThreadCacheSetStringSize = firstThread.getMemcacheS() == null ? 0 : firstThread.getMemcacheS().size();
			this.perThreadCacheMapLongSize = firstThread.getMemcacheMapL() == null ? 0 : firstThread.getMemcacheMapL().size();
			this.perThreadCacheMapStringSize = firstThread.getMemcacheMapS() == null ? 0 : firstThread.getMemcacheMapS().size();
			this.perThreadNameOfThread = firstThread.getName();
			this.perThreadNumOfFragments = firstThread.getNumOfFragmentsPerThread();
			this.perThreadPrioOfThread = firstThread.getPriority();
			this.perThreadStateOfThread = firstThread.getState().toString();
			this.perThreadIfExtractedAlias = firstThread.isExtractAlias();
			this.perThreadIsAlive = firstThread.isAlive();
			this.perThreadIsInterrupted = firstThread.isInterrupted();
			this.perThreadIsDaemon = firstThread.isDaemon();
			LOG.info("perThreadBufSize = " + perThreadBufSize);
			LOG.info("perThreadColumn = " + perThreadColumn);
			LOG.info("perThreadEncoding = " + perThreadEncoding);
			LOG.info("perThreadFileFrom = " + perThreadFileFrom);
			LOG.info("perThreadFileTo = " + perThreadFileTo);
			LOG.info("perThreadIdOfThread = " + perThreadIdOfThread);
			LOG.info("perThreadIdentity = " + perThreadIdentity);
			LOG.info("perThreadMapSize = " + perThreadMapSize);
			LOG.info("perThreadCacheSetLongSize = " + perThreadCacheSetLongSize);
			LOG.info("perThreadCacheSetStringSize = " + perThreadCacheSetStringSize);
			LOG.info("perThreadCacheMapLongSize = " + perThreadCacheMapLongSize);
			LOG.info("perThreadCacheMapStringSize = " + perThreadCacheMapStringSize);
			LOG.info("perThreadNameOfThread = " + perThreadNameOfThread);
			LOG.info("perThreadNumOfFragments = " + perThreadNumOfFragments);
			LOG.info("perThreadPrioOfThread = " + perThreadPrioOfThread);
			LOG.info("perThreadStateOfThread = " + perThreadStateOfThread);
			LOG.info("perThreadIfExtractedAlias = " + perThreadIfExtractedAlias);
			LOG.info("perThreadIsAlive = " + perThreadIsAlive);
			LOG.info("perThreadIsInterrupted = " + perThreadIsInterrupted);
			LOG.info("perThreadIsDaemon = " + perThreadIsDaemon);
		} catch (InterruptedException ex) {
			LOG.error("Interrupted" + EXC_OCC + ex.getMessage() + "\n" + Arrays.toString(ex.getStackTrace()));
			Thread.currentThread().interrupt();
		} catch (IOException ex) {
			LOG.error("IO" + EXC_OCC + ex.getMessage() + "\n" + Arrays.toString(ex.getStackTrace()));
		}
	}

	public void createThreadPool() {
		createThreadPoolMapOrSet(false);
	}

	public void createMapThreadPool() {
		createThreadPoolMapOrSet(true);
	}

	class ReaderThread extends Thread {

		private FileChannel fileChannel;
		private long fileSize;
		private long fileFrom;
		private long fileTo;
		private long mapSize;
		private int bufSize;
		private int currentThread;
		private int column;
		private boolean extractAlias;
		private int numOfFragmentsPerThread;
		private String identity = "SOME_IDENTITY";
		private String encoding = DEFAULT_CHARSET;
		private KeySet<Long> memcacheL;
		private KeySet<String> memcacheS;
		private HTreeMap<Long, String> memcacheMapL;
		private HTreeMap<String, String> memcacheMapS;

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
		 * @return the fileChannel
		 */
		public FileChannel getFileChannel() {
			return fileChannel;
		}

		/**
		 * @param fileChannel the fileChannel to set
		 */
		public ReaderThread setFileChannel(FileChannel fileChannel) {
			this.fileChannel = fileChannel;
			return this;
		}

		/**
		 * @return the fileSize
		 */
		public long getFileSize() {
			return fileSize;
		}

		/**
		 * @param fileSize the fileSize to set
		 */
		public ReaderThread setFileSize(long fileSize) {
			this.fileSize = fileSize;
			return this;
		}

		/**
		 * @return the fileFrom
		 */
		public long getFileFrom() {
			return fileFrom;
		}

		/**
		 * @param fileFrom the fileFrom to set
		 */
		public ReaderThread setFileFrom(long fileFrom) {
			this.fileFrom = fileFrom;
			return this;
		}

		/**
		 * @return the fileTo
		 */
		public long getFileTo() {
			return fileTo;
		}

		/**
		 * @param fileTo the fileTo to set
		 */
		public ReaderThread setFileTo(long fileTo) {
			this.fileTo = fileTo;
			return this;
		}

		/**
		 * @return the mapSize
		 */
		public long getMapSize() {
			return mapSize;
		}

		/**
		 * @param mapSize the mapSize to set
		 */
		public ReaderThread setMapSize(long mapSize) {
			this.mapSize = mapSize;
			return this;
		}

		/**
		 * @return the bufSize
		 */
		public int getBufSize() {
			return bufSize;
		}

		/**
		 * @param bufSize the bufSize to set
		 */
		public ReaderThread setBufSize(int bufSize) {
			this.bufSize = bufSize;
			return this;
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
		public ReaderThread setCurrentThread(int currentThread) {
			this.currentThread = currentThread;
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
		 * @return the memcacheL
		 */
		public HTreeMap<Long, String> getMemcacheMapL() {
			return memcacheMapL;
		}

		/**
		 * @param memcacheMapL the memcacheMapL to map
		 */
		public ReaderThread setMemcacheL(HTreeMap<Long, String> memcacheMapL) {
			this.memcacheMapL = memcacheMapL;
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
		 * @return the memcacheS
		 */
		public HTreeMap<String, String> getMemcacheMapS() {
			return memcacheMapS;
		}

		/**
		 * @param memcacheS the memcacheS to set
		 */
		public ReaderThread setMemcacheS(HTreeMap<String, String> memcacheMapS) {
			this.memcacheMapS = memcacheMapS;
			return this;
		}

		public ReaderThread(FileChannel channel, int bufferSize, int numOfThreads,
				int threadNum/* , KeySet<Long> internalStore */, KeySet<Long> memcacheLong,
				KeySet<String> memcacheString) throws IOException {
			index = 0;
			this.fileSize = channel.size();
			long maxFragmentSize = this.fileSize / numOfThreads;
			long fragmentSize = maxFragmentSize;
			if (threadNum + 1 == numOfThreads) {
				int modulo = (int) (fileSize % numOfThreads);
				if (modulo > 0) {
					fragmentSize += modulo;
				}
			}
			this.fileFrom = threadNum * maxFragmentSize;
			this.fileTo = this.fileFrom + fragmentSize - 1;
			this.mapSize = fragmentSize;
			this.bufSize = bufferSize;
			this.fileChannel = channel;
			this.currentThread = threadNum;
			this.memcacheL = memcacheLong;
			this.memcacheS = memcacheString;
		}

		public ReaderThread(FileChannel channel, int bufferSize, int numOfThreads,
				int threadNum/* , KeySet<Long> internalStore */, HTreeMap<Long, String> memcacheLong,
				HTreeMap<String, String> memcacheString) throws IOException {
			index = 0;
			this.fileSize = channel.size();
			long maxFragmentSize = this.fileSize / numOfThreads;
			long fragmentSize = maxFragmentSize;
			if (threadNum + 1 == numOfThreads) {
				int modulo = (int) (fileSize % numOfThreads);
				if (modulo > 0) {
					fragmentSize += modulo;
				}
			}
			this.fileFrom = threadNum * maxFragmentSize;
			this.fileTo = this.fileFrom + fragmentSize - 1;
			this.mapSize = fragmentSize;
			this.bufSize = bufferSize;
			this.fileChannel = channel;
			this.currentThread = threadNum;
			this.memcacheMapL = memcacheLong;
			this.memcacheMapS = memcacheString;
		}

		@Override
		public void run() {
			processSelectedFileFragmentsByThread();
		}

		private void processSelectedFileFragmentsByThread() {
			long origFileFrom = this.fileFrom;
			long origMapSize = this.mapSize;
			long divMapSize = origMapSize / numOfFragmentsPerThread;
			long modMapSize = origMapSize % numOfFragmentsPerThread;
			int additionalIterationNeeded = modMapSize > 0 ? 1 : 0;
			for (int fragmNumPerThread = 0; fragmNumPerThread
					+ additionalIterationNeeded <= this.numOfFragmentsPerThread; fragmNumPerThread++) {
				this.mapSize = fragmNumPerThread + 1 <= this.numOfFragmentsPerThread ? divMapSize : modMapSize;
				this.fileFrom = origFileFrom + fragmNumPerThread * divMapSize;
				this.fileTo = this.fileFrom + this.mapSize;
				boolean continueProcessing = processSelectedFileFragmentByThread(fragmNumPerThread);
				if (!continueProcessing) {
					break;
				}
			}
		}

		private boolean processSelectedFileFragmentByThread(int fragmNumPerThread) {
			boolean continueProcessing = true;
			MappedByteBuffer byteBufferAsBuffer;
			try {
				justInCaseFix(3);
				long initialPosA = this.fileFrom;
				long initialSize = this.mapSize;
				long overallSize = this.fileSize;
				byteBufferAsBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, initialPosA, initialSize);
				if (this.fileFrom == 0) {
					numOfCols = 1;
				}
				mapBeginningFix(byteBufferAsBuffer);
				boolean thisIsTheEnd = mapEndingFix(byteBufferAsBuffer, overallSize);
				if (thisIsTheEnd) {
					continueProcessing = false;
				}
				byteBufferAsBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, this.fileFrom, this.mapSize);
				if (this.mapSize < 1 || (this.mapSize == 1 && byteBufferAsBuffer.get() == 0x0A))
					continueProcessing = false;
				/*processedRows += */processFileFragments(byteBufferAsBuffer, fragmNumPerThread);
			} catch (IOException ex) {
				LOG.error(EXC_OCC + ex.getMessage() + "\n" + Arrays.toString(ex.getStackTrace()));
			}
			return continueProcessing;
		}

		private void mapBeginningFix(MappedByteBuffer byteBufferAsBuffer) {
			if (this.fileFrom >= 0) {
				int delta = 0;
				while (delta < byteBufferAsBuffer.limit() && byteBufferAsBuffer.get(delta) != 0x0A) {
					if (this.fileFrom == 0 && byteBufferAsBuffer.get(delta) == 0x09) {
						numOfCols++;
					}
					delta++;
				}
				this.fileFrom += delta;
				this.mapSize -= delta;
			}
		}

		private boolean mapEndingFix(MappedByteBuffer byteBufferAsBuffer, long overallSize) throws IOException {
			int sizeAsInt = (int) (overallSize & Integer.MAX_VALUE);
			int jump = 64;
			long lim = Math.min(this.fileFrom + byteBufferAsBuffer.limit(), this.fileTo);
			boolean thisIsTheEnd = false;
			for (long i = lim; i < overallSize && !thisIsTheEnd; i += jump) {
				jump = Math.min(jump, sizeAsInt);
				int endingBlockSizeAsInt = jump;
				if (i + jump >= overallSize) {
					thisIsTheEnd = true;
					endingBlockSizeAsInt = (int) ((overallSize - i - 1) & Integer.MAX_VALUE);
					if (endingBlockSizeAsInt >= 0) {
						jump = endingBlockSizeAsInt;
					}
					this.mapSize = this.fileSize - this.fileFrom;
					this.fileTo = this.fileFrom + this.mapSize - 1;
				}
				MappedByteBuffer byteBufferAsBufferTmp = fileChannel.map(FileChannel.MapMode.READ_ONLY, i, endingBlockSizeAsInt);
				boolean anotherEnd = mapEndingFixInternal(i, endingBlockSizeAsInt, byteBufferAsBufferTmp);
				if (thisIsTheEnd || anotherEnd) {
					break;
				}
			}
			return thisIsTheEnd;
		}

		private boolean mapEndingFixInternal(long i, int jump, MappedByteBuffer byteBufferAsBufferTmp) {
			long prevFileTo = this.fileTo;
			int delta = 0;
			for (; delta < jump && byteBufferAsBufferTmp.get(delta) != 0x0A; delta++)
				;
			if (delta < jump) {
				this.fileTo = i + delta - 1;
				this.mapSize = this.fileTo - this.fileFrom + 1;
				if (this.fileTo > this.fileSize) {
					this.fileTo = this.fileSize - 1;
				} else if (this.fileFrom >= this.fileTo) {
					this.fileTo = prevFileTo;
				}
				return false;
			}
			return true;
		}

		private void justInCaseFix(int delta) {
			for (int i = 0; i < delta && this.fileFrom > 0; i++, this.fileFrom--, this.mapSize++)
				;
			for (int i = 0; i < delta && this.fileTo + 1 < this.fileSize; i++, this.fileTo++, this.mapSize++)
				;
		}

		private int processFileFragments(MappedByteBuffer byteBufferAsBuffer, int fragmNumPerThread) throws UnsupportedEncodingException {
			int processedRows = 0;
			int byteBufferLen = byteBufferAsBuffer.limit();
			int byteBufferSize = Math.min(byteBufferLen, bufSize);
			int offsetDelta = 0;
			for (int offset = 0; offset < byteBufferLen; offset += byteBufferSize, offset += offsetDelta) {
				int length = processFileAtOffset(offset, byteBufferAsBuffer, byteBufferLen, byteBufferSize, fragmNumPerThread);
				if (length < 0) {
					break;
				}
				byte[] byteBufferAsArr = new byte[length];
				byteBufferAsBuffer.get(byteBufferAsArr, 0, length);
				offsetDelta = length - byteBufferSize + 1;
				int[] positions = findNewLineLinux(byteBufferAsArr);
				processedRows += processRows(positions, byteBufferAsArr);
				if (offset + byteBufferSize >= byteBufferLen) {
					byteBufferSize = byteBufferLen - (offset + offsetDelta);
				}
				index += processedRows;
				if (index % 50000 == 0) {
					System.out.println("Processing row " + index);
				}
			}
			return processedRows;
		}

		private int processFileAtOffset(int offset, MappedByteBuffer byteBufferAsBuffer, int byteBufferLen, int byteBufferSize, int fragmNumPerThread) {
			int length = byteBufferSize;
			if (offset + byteBufferSize > byteBufferLen) {
				length = byteBufferLen - offset;
			}
			int min = offset;
			int max = offset + length - 1;
			if (offset >= 0 && min < max) {
				min = findFirstNewLine(byteBufferAsBuffer, min, max);
			}
			if (offset < byteBufferLen) {
				max = findLastNewLine(byteBufferAsBuffer, byteBufferLen, max);
			}
			if (max < min) {
				if (max + 1 < byteBufferLen)
					max++;
				else if (min > 0)
					min--;
			}
			if (max < min) {
				LOG.debug("Thread #" + currentThread + " (fragment #" + fragmNumPerThread
						+ ") met problematic situation as we cannot read data from pointer " + min
						+ " to pointer " + max + ", so skipping this fragment");
				return -1;
			}
			length = max - min + 1;
			Buffer buffer = byteBufferAsBuffer;
			buffer.position(min);
			return length;
		}

		private int findFirstNewLine(MappedByteBuffer byteBufferAsBuffer, int minArg, int maxArg) {
			int min = minArg;
			while (min < maxArg && byteBufferAsBuffer.get(min) != 0x0A)
				min++;
			if (byteBufferAsBuffer.get(min) == 0x0A)
				min++;
			return min;
		}

		private int findLastNewLine(MappedByteBuffer byteBufferAsBuffer, int byteBufferLen, int maxArg) {
			int max = maxArg;
			while (max < byteBufferLen && byteBufferAsBuffer.get(max) != 0x0A)
				max++;
			if (max >= byteBufferLen || byteBufferAsBuffer.get(max) == 0x0A)
				max--;
			return max;
		}

		private int processRows(int[] positions, byte[] byteBufferAsArr) throws UnsupportedEncodingException {
			byte[] row = null;
			int processedRowsNow = 0;
			if (positions != null && positions.length > 0) {
				int l = positions.length;
				row = Arrays.copyOfRange(byteBufferAsArr, 0, positions[0]);
				processedRowsNow += processRow(row);
				for (int i = 0; i + 1 < l; i++) {
					row = Arrays.copyOfRange(byteBufferAsArr, positions[i] + 1, positions[i + 1]);
					processedRowsNow += processRow(row);
				}
				row = Arrays.copyOfRange(byteBufferAsArr, positions[l - 1] + 1, byteBufferAsArr.length);
				processedRowsNow += processRow(row);
			} else {
				row = byteBufferAsArr;
				processedRowsNow += processRow(row);
			}
			return processedRowsNow;
		}

		public int processRow(byte[] row) throws UnsupportedEncodingException {
			if ((memcacheL == null && memcacheMapL == null) || (memcacheS == null && memcacheMapS == null)) {
				LOG.error("Fatal error! Memory cache objects are null!"
						+ "LONG cache KeySet: " + memcacheL + ","
						+ "LONG cache HTreeMap: " + memcacheMapL + ","
						+ "STRING cache KeySet: " + memcacheS + ","
						+ "STRING cache HTreeMap: " + memcacheMapS);
				return 0;
			}
			int processed = 0;
			String line = new String(row, encoding);
			if (!line.isEmpty() && line.length() >= 3) {
				String interestingValueAsString = chooseColumn(line);
				if (interestingValueAsString == null) {
					return 0;
				}
				boolean filled = false;
				if ((memcacheMapL == null || memcacheMapS == null) && (memcacheL != null || memcacheS != null)) {
					filled = chooseAndFillCache(interestingValueAsString);
				} else if ((memcacheL == null || memcacheS == null) && (memcacheMapL != null || memcacheMapS != null)) {
					filled = chooseAndFillMapCache(interestingValueAsString, line);
				} else {
					LOG.error("Unexpected situation happened, as some caches are null, some not, and no logic is found in this");
				}
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
			if (extractAlias && (this.identity.toUpperCase().contains("IMSI") || this.identity.toUpperCase().contains("MSISDN"))) {
				return QuickReader.chooseAndFillMapCache(interestingValueAsString, fullLine, "", memcacheMapL, memcacheMapS);
			} else {
				return QuickReader.chooseAndFillMapCache(interestingValueAsString, fullLine, this.identity, memcacheMapL, memcacheMapS);
			}
		}

	}

	public static String chooseColumn(String line, String identity, int numOfCols, int column, boolean extractAlias) {
		int tab1 = line.indexOf('\t');
		if (numOfCols >= 2 && tab1 < 0) {
			return null;
		}
		String interestingValueAsString = line;
		if (tab1 > -1) {
			String col1 = line.substring(0, tab1);
			String col2 = line.substring(tab1 + 1);
			if (column == 0) {
				interestingValueAsString = col1;
			} else if (column == 1) {
				int tab2 = col2.indexOf('\t');
				if (tab2 > -1) {
					col2 = col2.substring(0, tab2);
				}
				interestingValueAsString = extractField(col2, extractAlias);
			} else {
				String[] cols = line.split("\t", -1);
				int len = cols.length;
				if (column < len) {
					interestingValueAsString = cols[column];
				} else {
					LOG.error("Configuration issue: Column " + column + " was not found in " + identity
							+ " memory cache");
				}
			}
		}
		return interestingValueAsString;
	}

	public static String extractField(String aliasedObjectName, boolean extractAlias) {
		String interestingValueAsString = aliasedObjectName;
		if (extractAlias) {
			int eq = interestingValueAsString.indexOf('=');
			if (eq >= 0) {
				String val = interestingValueAsString.substring(eq + 1);
				int comma = val.indexOf(',');
				if (comma >= 0) {
					interestingValueAsString = val.substring(0, comma);
				} else {
					interestingValueAsString = val;
				}
			}
		}
		return interestingValueAsString;
	}

	public static boolean chooseAndFillCache(String interestingValueAsString, String identity, KeySet<Long> memcacheL, KeySet<String> memcacheS) {
		boolean filled = false;
		if ((identity.toUpperCase().contains("IMSI") || identity.toUpperCase().contains("MSISDN"))
				&& interestingValueAsString.matches("^[0-9]{1,19}$")) {
			try {
				long interestingValueAsLong = Long.parseLong(interestingValueAsString);
				memcacheL.add(interestingValueAsLong);
				filled = true;
			} catch (NumberFormatException ex) {
				LOG.error(EXC_OCC + ex.getMessage() + "\n" + Arrays.toString(ex.getStackTrace()));
			}
		} else {
			try {
				memcacheS.add(interestingValueAsString);
			} catch (ClassCastException ex) {
				String msg1 = ex.getMessage();
				String typeArg = msg1.substring(msg1.lastIndexOf(' ') + 1);
				LOG.error("Cannot put identity " + identity + " = \"" + interestingValueAsString + "\" as String to memcache of type " + memcacheS.getClass().getName() + "<" + typeArg + ">");
			}
			filled = true;
		}
		return filled;
	}

	public static boolean chooseAndFillMapCache(String interestingValueAsString, String fullLineArg, String identity, HTreeMap<Long, String> memcacheMapL, HTreeMap<String, String> memcacheMapS) {
		String fullLine = fullLineArg.trim().replaceAll("^[\r\n]+|[\r\n]+$|", "");
		if (identity.isEmpty()) {
			fullLine = fullLine.split("\t", 2)[0];
		}
		interestingValueAsString = interestingValueAsString.trim().replaceAll("[\r\n\t]+", "");
		boolean filled = false;
		if ((identity.toUpperCase().contains("IMSI") || identity.toUpperCase().contains("MSISDN"))
				&& interestingValueAsString.matches("^[0-9]{1,19}$")) {
			try {
				long interestingValueAsLong = Long.parseLong(interestingValueAsString);
				memcacheMapL.put(interestingValueAsLong, fullLine);
				filled = true;
			} catch (NumberFormatException ex) {
				LOG.error(EXC_OCC + ex.getMessage() + "\n" + Arrays.toString(ex.getStackTrace()));
			}
		} else {
			try {
				memcacheMapS.put(interestingValueAsString, fullLine);
			} catch (ClassCastException ex) {
				String msg1 = ex.getMessage();
				String typeArg = msg1.substring(msg1.lastIndexOf(' ') + 1);
				LOG.error("Cannot put identity " + identity + " = \"" + interestingValueAsString + "\" as String to memcache of type " + memcacheMapS.getClass().getName() + "<" + typeArg + ">");
			}
			filled = true;
		}
		return filled;
	}

	public static void main(String[] args) {
//		String directory = "c:/Git_Repository/Tools/_gitlab.rosetta.ericssondevops.com,git.rosetta.ericssondevops.com/dm_app_pdi_etl_udc2udc/dev/src/input/target_udc/";
		String directory = "c:\\Git_Repository\\rosetta\\20_006_wind3_it_mass_changes_eps-cust\\dev\\src\\input\\";
		int numOfThreads = 384;
		int bufsize = 64 * KILO;
		String[] regexVarValues = new String[] {
//				"CUDBAssociation.log|CUDBAssociation.log.gz",
//				"CUDBMultiServiceConsumer.log|CUDBMultiServiceConsumer.log.gz",
				"IDEN_IMSI.log|IDEN_IMSI.log.gz",
//				"IDEN_MSISDN.log|IDEN_MSISDN.log.gz",
//				"IDEN_IMPI.log|IDEN_IMPI.log.gz",
//				"IDEN_IMPU.log|IDEN_IMPU.log.gz",
//				"IDEN_IPADDRESS.log|IDEN_IPADDRESS.log.gz",
//				"IDEN_SUBSID.log|IDEN_SUBSID.log.gz",
//				"IDEN_TRAFFICID.log|IDEN_TRAFFICID.log.gz",
//				"IDEN_WIMPU.log|IDEN_WIMPU.log.gz",
		};
		String[] cacheVarValues = new String[] {
//				"assocId_STRING",
				/*"mscId_STRING",*/ "dataMscidToImsi",
//				"IMSI_LONG",
//				"MSISDN_LONG",
//				"IMPI_STRING",
//				"IMPU_STRING",
//				"IPADDRESS_STRING",
//				"SUBSID_STRING",
//				"TRAFFICID_STRING",
//				"WIMPU_STRING",
		};
		int numerOfIdentityFiles = regexVarValues.length;
		DB db = DBMaker.memoryDirectDB().closeOnJvmShutdown().concurrencyScale(numOfThreads).make();
		KeySet<?>[] caches = new KeySet[] {
//				db.hashSet(cacheVarValues[0], Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashSet(cacheVarValues[1], Serializer.STRING).counterEnable().createOrOpen(),
				db.hashSet(cacheVarValues[0], Serializer.LONG).counterEnable().createOrOpen(),
//				db.hashSet(cacheVarValues[3], Serializer.LONG).counterEnable().createOrOpen(),
//				db.hashSet(cacheVarValues[4], Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashSet(cacheVarValues[5], Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashSet(cacheVarValues[6], Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashSet(cacheVarValues[7], Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashSet(cacheVarValues[8], Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashSet(cacheVarValues[9], Serializer.STRING).counterEnable().createOrOpen(),
		};
		LOG.info("Initialization of the Identity Loader with HashSets and " + numerOfIdentityFiles + " Identity Files, Buffer size "
				+ bufsize + " and " + numOfThreads + " Threads for reading bigger files ...");
		QuickReader qr = new QuickReader(numOfThreads, bufsize, directory, regexVarValues, cacheVarValues, caches);

		LOG.info("Execution of Identity Loader with HashSets ...");
//		qr.run();
		int loadedIdentities = 0; //qr.identitiesLoaded();
		int threadsUsed = 0; //qr.threadsUsed();
//		LOG.info("Loaded in total " + loadedIdentities + " identities using " + threadsUsed + " threads in total and HashSets");
		LOG.info("Part 1/2 Done.");
		db = DBMaker.memoryDirectDB().closeOnJvmShutdown().concurrencyScale(numOfThreads).make();
		HTreeMap<?, ?>[] cacheMaps = new HTreeMap[] {
				db.hashMap(cacheVarValues[0], Serializer.STRING, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[1], Serializer.STRING, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[2], Serializer.LONG, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[3], Serializer.LONG, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[4], Serializer.STRING, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[5], Serializer.STRING, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[6], Serializer.STRING, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[7], Serializer.STRING, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[8], Serializer.STRING, Serializer.STRING).counterEnable().createOrOpen(),
//				db.hashMap(cacheVarValues[9], Serializer.STRING, Serializer.STRING).counterEnable().createOrOpen(),
		};
		LOG.info("Initialization of the Identity Loader with TreeMaps and " + numerOfIdentityFiles + " Identity Files, Buffer size "
				+ bufsize + " and " + numOfThreads + " Threads for reading bigger files ...");
		qr = new QuickReader(numOfThreads, bufsize, directory, regexVarValues, cacheVarValues, cacheMaps).setExtract(true);
		LOG.info("Execution of Identity Loader with TreeMaps ...");
		qr.run();
		loadedIdentities = qr.identitiesLoaded();
		threadsUsed = qr.threadsUsed();
		LOG.info("Loaded in total " + loadedIdentities + " identities using " + threadsUsed + " threads in total and TreeMaps");
		LOG.info("Part 2/2 Done.");
		LOG.info("First 3 items: " + cacheMaps[0].entrySet().stream().limit(3).collect(Collectors.toList()));
		LOG.info("All Done.");
	}

	public static int[] findNewLineLinux(byte[] arrayToDeepIn) {
		return findLF(arrayToDeepIn);
	}

	public static int[] findNewLineWindows(byte[] arrayToDeepIn) {
		return findCRLF(arrayToDeepIn);
	}

	public static int[] findTab(byte[] arrayToDeepIn) {
		return findDelimiter(arrayToDeepIn, new byte[] { 0x09 });
	}

	public static int[] findLF(byte[] arrayToDeepIn) {
		return findDelimiter(arrayToDeepIn, new byte[] { 0x0A });
	}

	public static int[] findCRLF(byte[] arrayToDeepIn) {
		return findDelimiter(arrayToDeepIn, new byte[] { 0x0D, 0x0A });
	}

	public static int[] findString(byte[] arrayToDeepIn, String stringToFind, String encoding)
			throws UnsupportedEncodingException {
		return findDelimiter(arrayToDeepIn, stringToFind.getBytes(encoding));
	}

	public static int[] findNewSubscriber(byte[] arrayToDeepIn) {
		return findDelimiter(arrayToDeepIn, "# New Subscriber: ".getBytes());
	}

	public static int[][] findSubArray(byte[] arrayToDeepIn, byte[] arrayToFind) {
		Set<int[]> result = new LinkedHashSet<>();
		for (int posInArrayToFind = 0; posInArrayToFind < arrayToFind.length; posInArrayToFind++) {
			for (int posInArrayToDeepIn = 0; posInArrayToDeepIn < arrayToDeepIn.length; posInArrayToDeepIn++) {
				boolean bytesEqual = false;
				int finalLen = 0;
				int internalPosInArrayToFind = posInArrayToFind;
				int internalPosInArrayToDeepIn = posInArrayToDeepIn;
				do {
					byte byteInArrayToFind = arrayToFind[internalPosInArrayToFind];
					byte byteInArrayToDeepIn = arrayToDeepIn[internalPosInArrayToDeepIn];
					bytesEqual = byteInArrayToDeepIn == byteInArrayToFind;
					internalPosInArrayToDeepIn++;
					internalPosInArrayToFind++;
					finalLen++;
				} while (bytesEqual && internalPosInArrayToDeepIn < arrayToDeepIn.length
						&& internalPosInArrayToFind < arrayToFind.length);
				if (bytesEqual) {
					int[] singleFoundResult = { posInArrayToDeepIn, finalLen };
					result.add(singleFoundResult);
				}
			}
		}
		int[][] resAsArrOfArr = new int[0][0];
		return result.toArray(resAsArrOfArr);
	}

	public static int[] findDelimiter(byte[] arrayToDeepIn, byte[] arrayToFind) {
		int[][] results = findSubArray(arrayToDeepIn, arrayToFind);
		int[] result = null;
		if (results != null && results.length > 0) {
			result = new int[results.length];
			for (int i = 0; i < results.length; i++) {
				int[] r = results[i];
				int position = r.length != 2 ? -1 : r[0];
				result[i] = position;
			}
		}
		return result;
	}

}
