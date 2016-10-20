package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.combinar;

import htsjdk.samtools.SAMFileHeader;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationDatum;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationTables;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationUtils;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.Covariate;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.ReadGroupCovariate;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.BaseRecalibrationOptions;
import org.bgi.flexlab.gaea.util.HdfsFileManager;
import org.bgi.flexlab.gaea.util.NestedIntegerArray;
import org.bgi.flexlab.gaea.util.Pair;


public class CombinarTable {
	private Covariate[] requestedCovariates;
	private RecalibrationTables recalibrationTables;
	private SAMFileHeader mHeader;
	private Configuration conf=new Configuration();
	private FileSystem fs;
	private String path;
	private BaseRecalibrationOptions RAC=null;
	
	public CombinarTable(SAMFileHeader mHeader, BaseRecalibrationOptions RAC){
		this.mHeader = mHeader;
		this.path =RAC.getTempOutput();
		this.RAC = RAC;
	}
	
	 public void initialize() {
		 	fs= HdfsFileManager.getFileSystem(new Path(path), conf);
	        init();
	 }
	 
	 public void init() {
		 	// initialize the required and optional covariates
	        Pair<ArrayList<Covariate>, ArrayList<Covariate>> covariates = RecalibrationUtils.initializeCovariates(RAC);       
	        ArrayList<Covariate> requiredCovariates = covariates.getFirst();
	        ArrayList<Covariate> optionalCovariates = covariates.getSecond();

	        requestedCovariates = new Covariate[requiredCovariates.size() + optionalCovariates.size()];
	      
	        int covariateIndex = 0;
	        for (final Covariate covariate : requiredCovariates)
	            requestedCovariates[covariateIndex++] = covariate;
	        for (final Covariate covariate : optionalCovariates)
	            requestedCovariates[covariateIndex++] = covariate;
	        for (Covariate cov : requestedCovariates) {// list all the covariates being used
	            cov.initialize(RAC); // initialize any covariate member variables using the shared argument collection
	            if(cov instanceof ReadGroupCovariate)
	            	((ReadGroupCovariate) cov).initializeReadGroup(mHeader);
	        }
	        
	        int numReadGroups = mHeader.getReadGroups().size();
	        recalibrationTables = new RecalibrationTables(requestedCovariates, numReadGroups);
	 }
		
	public void updateDataForTable(String path) throws IOException {		
		Path p=new Path(path);
		if(p.getName().startsWith("_"))
			return;
		if(!fs.exists(p)){
			System.out.println("the table path is no exist!");
			System.exit(-1);
		}
		
		if(fs.isFile(p)){
			updateDataFromFile(p);
		} else {
			FileStatus stats[]=fs.listStatus(p);
			for(FileStatus state:stats) {
				Path statePath=state.getPath();
				if(statePath.getName().startsWith("_"))
					continue;
				if(fs.isFile(statePath))
					updateDataFromFile(statePath);
				else
					updateDataForTable(statePath.toString());
			}
		}
	}
	
	private void updateDataFromFile(Path p){
		try {
			FSDataInputStream table=fs.open(p);
			LineReader lineReader = new LineReader(table, conf);
			Text line = new Text();
			String tempString=null;
			while(lineReader.readLine(line) > 0 && line.getLength() != 0) {
				tempString=line.toString();
				updateData(tempString);
			}
			lineReader.close();
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void updateData(String line) {
		String[] splite=line.split("\t");
		int tableIndex=Integer.parseInt(splite[0]);
		if(tableIndex==0) {//RG table
			updateDateForRGTable(tableIndex,splite);
		} else if(tableIndex==1) {//qual table
			updateDateForQualTable(tableIndex,splite);
		} else if(tableIndex==2) {//Covariate table
			upadateDateForCovariateTable(tableIndex,splite);
		}
	}
	
	private void updateDateForRGTable(int tableIndex,String[] splite){
		int[] keys=new int[2];
		keys[0]=Integer.parseInt(splite[1]);
		keys[1]=Integer.parseInt(splite[2]);
		RecalibrationDatum rgThisDatum=createDatumData(splite[3],splite[4],splite[5]);
		final NestedIntegerArray<RecalibrationDatum> rgRecalTable = recalibrationTables.getTable(tableIndex);
		final RecalibrationDatum rgPreviousDatum = rgRecalTable.get(keys[0], keys[1]);
		if (rgPreviousDatum == null)// key doesn't exist yet in the map so make a new bucket and add it
            rgRecalTable.put(rgThisDatum, keys[0], keys[1]);
        else
            rgPreviousDatum.combine(rgThisDatum);
	}
	
	private void updateDateForQualTable(int tableIndex,String[] splite){
		int[] keys=new int[3];
		keys[0]=Integer.parseInt(splite[1]);
		keys[1]=Integer.parseInt(splite[2]);
		keys[2]=Integer.parseInt(splite[3]);
		RecalibrationDatum qualThisDatum=createDatumData(splite[4],splite[5],splite[6]);
		final NestedIntegerArray<RecalibrationDatum> qualRecalTable = recalibrationTables.getTable(tableIndex);
        final RecalibrationDatum qualPreviousDatum = qualRecalTable.get(keys[0], keys[1], keys[2]);
        if (qualPreviousDatum == null)
            qualRecalTable.put(qualThisDatum, keys[0], keys[1], keys[2]);
        else
            qualPreviousDatum.increment(qualThisDatum);
	}
	
	private void upadateDateForCovariateTable(int tableIndex,String[] splite){
		int[] keys=new int[4];
		
		keys[0]=Integer.parseInt(splite[1]);;
		keys[1]=Integer.parseInt(splite[2]);
		keys[2]=Integer.parseInt(splite[3]);
		keys[3]=Integer.parseInt(splite[4]);;
		RecalibrationDatum covariateThisDatum=createDatumData(splite[5],splite[6],splite[7]);
		
		if (keys[tableIndex] < 0)
			return;
		final NestedIntegerArray<RecalibrationDatum> covRecalTable = recalibrationTables.getTable(tableIndex);
		final RecalibrationDatum covPreviousDatum = covRecalTable.get(keys[0], keys[1],
				keys[tableIndex], keys[3]);
		if (covPreviousDatum == null)
			covRecalTable.put(covariateThisDatum, keys[0], keys[1],
					keys[tableIndex], keys[3]);
		else
			covPreviousDatum.increment(covariateThisDatum);
	}
	
	private RecalibrationDatum createDatumData(String a,String b,String c) {
		return new RecalibrationDatum(Long.parseLong(a),Long.parseLong(b),Double.parseDouble(c));
	}

	public void onCombianarDone(){
		TableReport report=new TableReport();
		report.initazie(RAC, recalibrationTables, requestedCovariates);
		report.reprot();
	}
	
	public void print(Context context){
		DecimalFormat df = new DecimalFormat("#.####");
		for (int tableIndex = 0; tableIndex < recalibrationTables.numTables(); tableIndex++) {
			final NestedIntegerArray<RecalibrationDatum> table = recalibrationTables.getTable(tableIndex);
			for (final NestedIntegerArray.Leaf row : table.getAllLeaves()) {
                final RecalibrationDatum datum = (RecalibrationDatum)row.value;
                final int[] keys = row.keys;
                
                StringBuilder sb = new StringBuilder();
                int i;
                sb.append(tableIndex+"\t");
                for(i=0;i<keys.length;i++)sb.append(keys[i]+"\t");
                sb.append(datum.getNumObservations()+"\t");
                sb.append(datum.getNumMismatches()+"\t");
                sb.append(df.format(datum.getEstimatedQReported()));
                try {
					context.write(NullWritable.get(), new Text(sb.toString()));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
		}
	}
	
}
