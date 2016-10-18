package org.bgi.flexlab.gaea.data.structure.vcf.index;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface Index<H,D,B> {

	public H header();
	public D dictionary();
	public HashMap<Integer,ArrayList<B>> blocks();
	public List<B> getBlock(String chr,int start,int end);
	
	public boolean containsChromosome(final String chr);
	
	public void read(InputStream index);
	
	public void write(OutputStream index);
	
	public void create(InputStream file);
	
	public enum INDEX
	{
		SEEK,SIZE,TREE;
	}
	public enum TYPE
	{
		H,B,D;
	}
	
}
