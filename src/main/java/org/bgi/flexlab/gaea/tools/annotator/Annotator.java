package org.bgi.flexlab.gaea.tools.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bgi.flexlab.gaea.tools.annotator.inputformat.VCFReader;

import scala.Tuple2;

import com.beust.jcommander.JCommander;

/**
 * Annotator's main command line program!
 *
 */
public class Annotator 
{
	// Version info
	public static final String SOFTWARE_NAME = "GaeaVcfAnno";
	public static final String REVISION = "";
	public static final String BUILD = "2016-08-22";
	public static final String VERSION_MAJOR = "0.1";
	public static final String AUTHOR = "Gaea";
	public static final String VERSION_SHORT = VERSION_MAJOR + REVISION;
	public static final String VERSION_NO_NAME = VERSION_SHORT + " (build " + BUILD + "), by " + AUTHOR;
	public static final String VERSION = SOFTWARE_NAME + " " + VERSION_NO_NAME;
		
    public static void main( String[] args )
    {
        CommandLineParser options = new CommandLineParser();
    	JCommander jc = new JCommander(options);
    	jc.setProgramName(VERSION);
//    	if(options.help){
//    		jc.usage();
//    		System.exit(1);
//    	}
    	
    	String filename = "/Users/huangzhibo/workitems/06.demo/demo/demo.vcf";
    	File file = new File(filename);
    	
    	SparkConf conf = new SparkConf().setAppName("GaeaVcfAnno");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	VCFReader vcf = new VCFReader(file, false);
    	VCFHeader vcfHeader = vcf.getFileHeader();
    	
    	sc.broadcast(vcfHeader);
    	
    	
    	JavaRDD<VariantContext> variantListRDD = sc.parallelize(vcf.getVariantList());
    	
    	System.out.println("variantListRDD:----------:" + variantListRDD.count());
    	vcf.close();
    	
    	JavaPairRDD<String, VariantContext> annoVariantListRDD = variantListRDD.mapToPair(new variantAnnotator());
    	JavaPairRDD<String, VariantContext> sortAnnoRDD = annoVariantListRDD.sortByKey();
//    	System.out.println("sortAnnoRDD:----------:" + sortAnnoRDD.count());
    	List<Tuple2<String,VariantContext>> output = sortAnnoRDD.collect();
    	
    	for (Tuple2<?,?> tuple : output) {
    	      System.out.println(tuple._1() + ": " + tuple._2());
    	 }
    	
    	sc.stop();
    	sc.close();
    	
    	
    }
}
