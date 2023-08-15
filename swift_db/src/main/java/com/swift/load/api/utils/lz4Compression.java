package com.swift.load.api.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;


import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class lz4Compression {

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\egjklol\\Projects\\VIL\\dm_cs_2021_vil_ap_india\\dev\\src\\Working\\Subscriber_223674.csv"));
		String line = br.readLine();
		br.close();
		System.out.println(line);
		lz4Compression object = new lz4Compression();
		
		//System.out.println(compressed);
		//System.out.println();
		 java.util.Date stdt = new java.util.Date();
		//for(int i =0 ; i<100000;i++) {
			byte[] compressed =  object.compress( line.getBytes());
			System.out.println("Actual:::"+line.getBytes().length);
			System.out.println("compressed:::"+compressed.length);
			new String(object.decompress(compressed));
		//}
		 java.util.Date end = new java.util.Date();
		 System.out.println((end.getTime()-stdt.getTime()));
	}

	public  byte[]  compress(final byte[] data) {
	     LZ4Factory lz4Factory = LZ4Factory.safeInstance();
	     LZ4Compressor fastCompressor = lz4Factory.fastCompressor();
	     int maxCompressedLength = fastCompressor.maxCompressedLength(data.length);
	     byte[] comp = new byte[maxCompressedLength];
	     int compressedLength = fastCompressor.compress(data, 0, data.length, comp, 0, maxCompressedLength);
	     return Arrays.copyOf(comp, compressedLength);
	}

	public  byte[] decompress(final byte[] compressed) {
	     LZ4Factory lz4Factory = LZ4Factory.safeInstance();
	     LZ4SafeDecompressor decompressor = lz4Factory.safeDecompressor();
	     byte[] decomp = new byte[compressed.length * 4];//you might need to allocate more
	     decomp = decompressor.decompress(Arrays.copyOf(compressed, compressed.length), decomp.length);
	     return decomp;
	  }
}
