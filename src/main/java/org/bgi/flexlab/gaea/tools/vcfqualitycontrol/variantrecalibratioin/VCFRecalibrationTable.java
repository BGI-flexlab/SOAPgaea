package org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bgi.flexlab.gaea.data.exception.UserException;
import org.bgi.flexlab.gaea.data.structure.header.VCFConstants;
import org.bgi.flexlab.gaea.tools.mapreduce.vcfqualitycontrol.VCFQualityControl;
import org.bgi.flexlab.gaea.tools.mapreduce.vcfqualitycontrol.VCFQualityControlOptions;
import org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.model.GaussianMixtureModel;
import org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.model.VariantDataManager;
import org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.traindata.VariantDatum;
import org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.traindata.VariantDatumMessenger;
import org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.tranche.Tranche;
import org.bgi.flexlab.gaea.tools.vcfqualitycontrol.variantrecalibratioin.tranche.TrancheManager;
import org.bgi.flexlab.gaea.util.ExpandingArrayList;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;

public class VCFRecalibrationTable {
	/**
	 * parameter
	 */
    private VCFQualityControlOptions options; 
    
    /**
     * engine that generate recal table
     */
    private VariantRecalibrationEngine engine;
    
    /**
     * data Manager
     */
    private VariantDataManager dataManager;
    
    /**
     * tranches of recal data
     */
    private List<Tranche> tranches;
    
    
    /**
     * create dummy alleles to be used
     */
    private final List<Allele> alleles = new ArrayList<Allele>(2);   
    
    /**
     * to be used for the important INFO tags
     */
    private final HashMap<String, Object> attributes = new HashMap<String, Object>(3);
    
    /**
     * data of data manager
     */
    ExpandingArrayList<VariantDatum> data;
	Map<String, Map<Integer, ArrayList<Integer>>> dataIndex;

	/**
	 * construct function 
	 * @param options
	 * @throws IOException
	 */
	public VCFRecalibrationTable(VCFQualityControlOptions options) throws IOException {
		this.options = options;
    	engine = new VariantRecalibrationEngine(options);
		dataManager = new VariantDataManager( options.getUseAnnotations(), options );
		data = dataManager.getData();
		alleles.add(Allele.create("N", true));
	    alleles.add(Allele.create("<VQSR>", false)); 
	}
	
	/**
	 * add data
	 * @param data
	 */
	public void addData(VariantDatumMessenger data) {
		dataManager.addData(data);
	}
	
	/**
	 * generate recal table
	 * @return
	 */
	public final void getRecalibrationTable() {    
		dataManager.normalizeData(); // Each data point is now (x - mean) / standard deviation
	
	    // Generate the positive model using the training data and evaluate each variant
	    final GaussianMixtureModel goodModel = engine.generateModel( dataManager.getTrainingData() );
	    engine.evaluateData( dataManager.getData(), goodModel, false );
	
	    // Generate the negative model using the worst performing data and evaluate each variant contrastively
	  	final ExpandingArrayList<VariantDatum> negativeTrainingData = dataManager.selectWorstVariants( options.getPercentBadVariants(), options.getMinNumBadVariants() );
	 	GaussianMixtureModel badModel = engine.generateModel( negativeTrainingData );
	  	engine.evaluateData( dataManager.getData(), badModel, true );
	
	  	// Detect if the negative model failed to converge because of too few points and/or too many Gaussians and try again
	  	while( badModel.failedToConverge && options.getMaxGaussians() > 4 ) {
	  		System.out.println("Negative model failed to converge. Retrying...");
	       	options.setMaxGaussians(options.getMaxGaussians() - 1);
	       	badModel = engine.generateModel( negativeTrainingData );
	      	engine.evaluateData( dataManager.getData(), goodModel, false );
	       	engine.evaluateData( dataManager.getData(), badModel, true );
	   	}
	
	   	if( badModel.failedToConverge || goodModel.failedToConverge ) {
	       	throw new UserException("NaN LOD value assigned. Clustering with this few variants and these annotations is unsafe. Please consider raising the number of variants used to train the negative model (via --percentBadVariants 0.05, for example) or lowering the maximum number of Gaussians to use in the model (via --maxGaussians 4, for example)");
	   	}
	
	   	engine.calculateWorstPerformingAnnotation( dataManager.getData(), goodModel, badModel );
	
	   	// Find the VQSLOD cutoff values which correspond to the various tranches of calls requested by the user
	   	final int nCallsAtTruth = TrancheManager.countCallsAtTruth( dataManager.getData(), Double.NEGATIVE_INFINITY );
	   	final TrancheManager.SelectionMetric metric = new TrancheManager.TruthSensitivityMetric( nCallsAtTruth );
	   	tranches = TrancheManager.findTranches( dataManager.getData(), options.getTsTranchesDouble(), metric, options.getMode() );
	   	Collections.sort( tranches, new Tranche.TrancheTruthSensitivityComparator() );
	   	Tranche prev = null;
        for ( Tranche t : tranches ) {
            t.name = String.format("VQSRTranche%s%.2fto%.2f", t.model.toString(),(prev == null ? 0.0 : prev.ts), t.ts, t.model.toString());
            prev = t;
        }
        System.out.println(Tranche.tranchesString( tranches ));
	}
	
//	/**
//	 * get recal data
//	 * @param chrName
//	 * @param start
//	 * @param end
//	 * @return recal table in chr:start-end
//	 */
//    public List<VariantContext> getData(String chrName, int start, int end) {
//    	List<VariantContext> vcs = new ArrayList<VariantContext>();
//    	ArrayList<Integer> index = dataIndex.get(chrName).get(start);
//    	if(index == null)
//    		return null;
//    	for(int i : index) {
//    		VariantDatum datum = data.get(i);
//    		if(datum.loc.getStart() == start && datum.loc.getStop() <= end) {
//    			attributes.put(VCFConstants.END_KEY, datum.loc.getStop());
//                attributes.put(VariantRecalibration.VQS_LOD_KEY, String.format("%.4f", datum.lod));
//                attributes.put(VariantRecalibration.CULPRIT_KEY, (datum.worstAnnotation != -1 ? options.getUseAnnotations().get(datum.worstAnnotation) : "NULL"));
//
//                VariantContextBuilder builder = new VariantContextBuilder("VQSR", datum.loc.getContig(), datum.loc.getStart(), datum.loc.getStop(), alleles).attributes(attributes);
//                vcs.add(builder.make());
//    		}
//    		else if(datum.loc.getStart() > start)
//    			break;
//    	}
//    	return vcs;
//    }
    
	public VariantContext getData(String chrName, int start, int end) {
    	ArrayList<Integer> index = dataIndex.get(chrName).get(start);
    	if(index == null)
    		return null;
    	for(int i : index) {
    		VariantDatum datum = data.get(i);
    		if(datum.loc.getStart() == start && datum.loc.getStop() == end) {
    			attributes.put(VCFConstants.END_KEY, datum.loc.getStop());
                attributes.put(VCFQualityControl.VQS_LOD_KEY, String.format("%.4f", datum.lod));
                attributes.put(VCFQualityControl.CULPRIT_KEY, (datum.worstAnnotation != -1 ? options.getUseAnnotations().get(datum.worstAnnotation) : "NULL"));

                VariantContextBuilder builder = new VariantContextBuilder("VQSR", datum.loc.getContig(), datum.loc.getStart(), datum.loc.getStop(), alleles).attributes(attributes);
                return builder.make();
    		}
    		else if(datum.loc.getStart() > start)
    			break;
    	}
    	return null;
    }
		
    public void indexData() {
    	dataIndex = new HashMap<String, Map<Integer,ArrayList<Integer>>>();
    	int i = 0;
    	while(i < data.size()) {
    		VariantDatum datum = data.get(i);
    		if(!dataIndex.containsKey(datum.loc.getContig())) {
    			Map<Integer, ArrayList<Integer>> posIndex = new HashMap<Integer, ArrayList<Integer>>();
    			dataIndex.put(datum.loc.getContig(), posIndex);
    		}
    		Map<Integer, ArrayList<Integer>> posIndex = dataIndex.get(datum.loc.getContig());
    		if(!posIndex.containsKey(datum.loc.getStart())) {
    			ArrayList<Integer> index = new ArrayList<Integer>();
    			posIndex.put(datum.loc.getStart(), index);
    		}
    		posIndex.get(datum.loc.getStart()).add(i);
    		i ++;
    	}
    }
    
    public List<Tranche> getTranches() {
    	return tranches;
    }
}
