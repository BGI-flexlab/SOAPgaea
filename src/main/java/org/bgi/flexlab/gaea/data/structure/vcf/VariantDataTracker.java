package org.bgi.flexlab.gaea.data.structure.vcf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.bgi.flexlab.gaea.data.structure.location.GenomeLocation;
import org.bgi.flexlab.gaea.data.structure.vcf.index.VCFIndexCreator2;
import org.bgi.flexlab.gaea.data.structure.vcf.index.VCFLoader;
import org.bgi.flexlab.gaea.util.Window;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;



public class VariantDataTracker {
	
	public enum Style {
		ALL,
		POS;
	}
	
	private String file;
	private String type;
	private Window window;
	private VCFLoader loader = null;;
	private List<VariantContext> data = null;
	private HashSet<Integer> site = null;
	private boolean bound;
	private Style style;
	
	public VariantDataTracker(){
		this.bound=false;
	};
	
	public VariantDataTracker(String source,String type ,Style s) {
		this.file=source;
		this.type=type;
		this.bound=true;
		this.style=s;
		initializeLoader();
	}
	
	public VariantDataTracker(String source,String type, Window window, Style s) {
		this(source, type, s);
		this.window=window;
		initializeData();
	}
	
	public VariantDataTracker(VCFLoader loader,String type,Style s) {
		this.loader=loader;
		this.type=type;
		this.style=s;
	}
	
	public void setSource(String source,String type) throws IOException {
		this.file=source;
		this.type=type;
		initializeLoader();
		initializeData();
	}
	
	/*public void setWindow(Window win) throws IOException
	{
		this.window=win;
		initializeData();
	}*/
	
	private void initializeLoader() {
		if(loader==null) {
			VCFIndexCreator2 creator = new VCFIndexCreator2();
			try {
				htsjdk.tribble.index.Index idx = creator.createIndex(file);
				loader = new VCFLoader(file, idx);
				loader.loadHeader();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
	}

	public void initializeData() {
		if(window == null)
			return;
		if(data == null)
			data = new ArrayList<VariantContext>();
		
		loadVariantContext();
		loadVariantContextPos();
	}

	/*public boolean getValue(String name, GenomeLoc onlyAtThisLoc)
	{
		return addValue( onlyAtThisLoc, true, false);
	}*/
	
	public boolean getValue(GenomeLocation onlyAtThisLoc) {
		return addValue(onlyAtThisLoc, true, false);
	}
	
	/*public List<VariantContext> getValues(String name, GenomeLoc onlyAtThisLoc) throws IOException {
		
		return addValues(new ArrayList<VariantContext>(1), onlyAtThisLoc, true, false);
	}*/
	

	private boolean addValue(GenomeLocation curLocation,  boolean requireStartHere,
			final boolean takeFirstOnly)  {
		HashSet<Integer> val=null;
		if(site!=null) {
			val = site;
		} else {
			try {
				List<VariantContext> temp = loader.load(curLocation.getContig(), curLocation.getStart(), curLocation.getStop());
				if(temp!=null) {
					val=new HashSet<Integer>();
					for(VariantContext c:temp) {
						val.add(c.getStart());
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(val==null || val.size() == 0)
			return false;
		if(!requireStartHere || val.contains(curLocation.getStart()))
			return true;
		return false;
	}
	
	public List<VariantContext> getValues(GenomeLocation onlyAtThisLoc) {
		return addValues(new ArrayList<VariantContext>(1), onlyAtThisLoc, true, false);
	}

	private List<VariantContext> addValues( List<VariantContext> values, GenomeLocation curLocation,  boolean requireStartHere,
			final boolean takeFirstOnly)  {
		List<VariantContext> val=null;
		val = loadVariantContext(val, curLocation);
		if(val==null)
			return Collections.<VariantContext> emptyList();
		
		for (VariantContext rec : val) {
			if (!requireStartHere || rec.getStart() == curLocation.getStart()) { 
				if (takeFirstOnly) {
					if (values == null)
						values = Arrays.asList(rec);
					else
						values.add(rec);
					break;
				} else {
					if (values == null)
						values = new ArrayList<VariantContext>();
					values.add(rec);
				}
			}
		}

		return values == null ? Collections.<VariantContext> emptyList() : values;
	}

	public VCFHeader getHeader() {
		if(loader!=null)
			return loader.getHeader();
		return null;
	}
	
	public String getName() {
		return file;
	}

	public String getType() {
		return type;
	}

	public boolean isBound() {
		return bound;
	}
	
	
	private void loadVariantContext() {
		try {
			ArrayList<VariantContext> temp = loader.load(window.getContigName(), window.getStart(), window.getStop());
			if(temp!=null)
				data = temp;
			else
				System.out.println("temp is null");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private List<VariantContext> loadVariantContext(List<VariantContext> val, GenomeLocation curLocation) {
		List<VariantContext> result = val;
		if(data!=null) {
			result = data;
		} else {
			try {
				result = loader.load(curLocation.getContig(), curLocation.getStart(), curLocation.getStop());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	private void loadVariantContextPos() {
		if(style == Style.POS) {
			site = new HashSet<Integer>();
			if(data==null)
				return;
			
			for(VariantContext c:data) {
				site.add(c.getStart());
			}
			data=null;
		}
	}
}
