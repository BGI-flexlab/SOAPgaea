package org.bgi.flexlab.gaea.tools.annotator;

import htsjdk.variant.variantcontext.VariantContext;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class variantAnnotator implements PairFunction<VariantContext,String,VariantContext>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, VariantContext> call(VariantContext v1)
			throws Exception {
		String chr = v1.getContig();
		int start = v1.getStart();
		int end = v1.getEnd();
		
		String key = chr + "_" + start;
		
//		VariantContext variantContext = new VariantContext();
		
		
//		VariantContext.addInfo("");
		
		String[] DBList = new String[]{"a","b","c"};
		for (String dbName : DBList) {
			Class<?> db=null;
	        try{
	        	db = Class.forName("org.bgi.flexlab.gaea.gaeavcfanno.db."+dbName+"query");
	        }catch (Exception e) {
	            e.printStackTrace();
	        }
//	        DBAnno = db.newInstance();
//	        db.getName();
//	        db.query(key, {"a","b"});
	        v1.getCommonInfo();
	        
		}
		
		Tuple2<String, VariantContext> tuple = new Tuple2<String, VariantContext>(key, v1);
		
		return tuple;
	}


}
