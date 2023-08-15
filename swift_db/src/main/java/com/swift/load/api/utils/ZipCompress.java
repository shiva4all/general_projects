package com.swift.load.api.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ZipCompress {
	public static void main(String args[]) throws IOException {
		
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\egjklol\\Projects\\VIL\\dm_cs_2021_vil_ap_india\\dev\\src\\Working\\Subscriber_223674.csv"));
		String line = br.readLine();
		br.close();
	try {
		  // Encode a String into bytes
		
		  String inputString = line;
		  java.util.Date stdt = new java.util.Date();
		  
		//  for(int i=0;i<100000;i++) {
		  byte[] input = inputString.getBytes("UTF-8");

		  // Compress the bytes
		  byte[] output = inputString.getBytes("UTF-8");;
		  Deflater compresser = new Deflater();
		  compresser.setInput(input);
		  compresser.finish();
		  int compressedDataLength = compresser.deflate(output);

		  // Decompress the bytes
		  Inflater decompresser = new Inflater();
		  decompresser.setInput(output, 0, compressedDataLength);
		  byte[] result = inputString.getBytes("UTF-8");
		  int resultLength = decompresser.inflate(result);
		  
		  decompresser.end();
		  System.out.println("Compressed :::"+result+":::"+compressedDataLength);

		  // Decode the bytes into a String
		  String outputString = new String(result, 0, resultLength, "UTF-8");
		  System.out.println("outputString:::"+outputString+":::"+resultLength);
		  //}
		  java.util.Date enddt = new java.util.Date();
		  System.out.println("Stdt:::"+stdt);
		  System.out.println("enddt:::"+enddt);
		  
		} catch(java.io.UnsupportedEncodingException ex) {
		   // handle
			ex.printStackTrace();
		} catch (java.util.zip.DataFormatException ex2) {
		   // handle
		}
	
	}
}
