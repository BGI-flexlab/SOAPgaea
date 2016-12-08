package org.bgi.flexlab.gaea.data.structure.vcf.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;

import htsjdk.variant.variantcontext.VariantContext;

public class WindowBasedIndex {
	
	private Map<String, IndexDatum> collect;
	
	private IndexDatum datum;
	
	private int winSize;
	
	private final static String WINDOW_TAG = "#window_size";
	
	public WindowBasedIndex(int winSize) {
		this.winSize = winSize;
	}
	
	public WindowBasedIndex(InputStream is) throws IOException {
		datum = new IndexDatum();
		read(is);
	}
	
	public ArrayList<VariantContext> query(String chr, int start, int end) {
		if(collect == null)
			throw new RuntimeException("Index has not been rightly initialized");
		if(collect.get(chr) == null)
			throw new RuntimeException("Chromosome " + chr + " is not contained in index");
		
		int winID = start / winSize;
		IndexDatum datum = collect.get(chr);
		long startPos = datum.getStart(winID);
		ArrayList<VariantContext> result = new ArrayList<>();
		result.a
		return null;
	}
	
	public void combine(IndexDatum datum) {
		if(collect.get(datum.getChr()) == null){
			collect.put(datum.getChr(), datum);
		}
	}
	
	public void write(OutputStream os) {
		try {
			os.write(formatFirstLine().getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(IndexDatum datum : collect)
			datum.write(os);
	}
	
	public void read(InputStream is) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String line = reader.readLine();
		if(line.startsWith(WINDOW_TAG)) {
			winSize = Integer.parseInt(line.split("\t")[1]);
		} else {
			throw new RuntimeException("Malformed window-based index file");
		}
		IndexDatum datum = new IndexDatum();
		while((line = reader.readLine()) != null) {
			datum.read(line);	
			combine(datum);
		}
		reader.close();
	}

	public int getWinSize() {
		return winSize;
	}

	public void setWinSize(int winSize) {
		this.winSize = winSize;
	}
	
	private String formatFirstLine() {
		String firstLine = WINDOW_TAG + "=" + winSize + "\n";
		return firstLine;
	}
}
