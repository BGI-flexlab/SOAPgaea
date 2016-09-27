package org.bgi.flexlab.gaea.util;

public class ArrayUtils {
	public static String[] subArray(String[] args,int start,int length){
		String[] sub = new String[length];
		for(int i = 0; i < length ; i++){
			sub[i] = args[start+i];
		}
		return sub;
	}
	
	public static String[] subArray(String[] args,int start){
		return subArray(args,start,args.length-start);
	}
}
