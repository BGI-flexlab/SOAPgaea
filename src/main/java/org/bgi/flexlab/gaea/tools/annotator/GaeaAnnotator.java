package org.bgi.flexlab.gaea.tools.annotator;

import org.apache.hadoop.util.ToolRunner;

/**
 * Annotator's main command line program!
 *
 */
public class GaeaAnnotator 
{
	
	public int runMR(String[] args) throws Exception {
		return ToolRunner.run(new VariantAnnotation(), args);
	}
	
	public static void main(String[] args) throws Exception {
		GaeaAnnotator annotator = new GaeaAnnotator();
		annotator.runMR(args);
	}

}
