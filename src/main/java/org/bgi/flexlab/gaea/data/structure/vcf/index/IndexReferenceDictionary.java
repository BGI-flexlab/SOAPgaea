package org.bgi.flexlab.gaea.data.structure.vcf.index;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;



public class IndexReferenceDictionary {
	private final Index.TYPE type=Index.TYPE.D;
	private final String CHR_TAG="chr";
	private int index=0;
	private HashMap<String,Integer> dictionary=new HashMap<String, Integer>();
	
	public void add(String chr) {
		chr=modify(chr);
		if(dictionary.containsKey(chr)) {
			throw new IllegalArgumentException(chr);
		}
		dictionary.put(chr, index++);
	}
	
	public void add(String chr,int index) {
		chr=modify(chr);
		if(dictionary.containsKey(chr)) {
			throw new IllegalArgumentException(chr);
		}
		dictionary.put(chr, index);
	}
	
	public int get(String chr) {
		chr=modify(chr);
		return dictionary.get(chr);
	}
	
	public boolean containsKey(String chr) {
		chr=modify(chr);
		return dictionary.containsKey(chr);
	}
	
	public void write(OutputStream out) {
		StringBuilder sb=null;
		for(String key:dictionary.keySet() )
		{
			sb=new StringBuilder();
			sb.append(type.toString());sb.append("\t");
			sb.append(key);sb.append("\t");
			sb.append(dictionary.get(key));sb.append("\n");
			try {
				out.write(sb.toString().getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void read(String line) {
		if(line.charAt(0)!='D')
		{
			System.err.println(line+"is not dictionary!");
			System.exit(-1);
		}
		String[] dic=line.split("\t");
		if(dictionary.containsKey(dic[1]))
		{
			System.err.println(dic[1]+"has exist,whats wrong!");
			System.exit(-1);
		}
		dictionary.put(dic[1], Integer.parseInt(dic[2]));
	}
	
	private String modify(String chr) {
		chr=chr.toLowerCase();
		if(chr.startsWith(CHR_TAG)) {
			return chr;
		} else {
			return CHR_TAG+chr;
		}
	}
	
	
}
