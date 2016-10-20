package org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.combinar;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bgi.flexlab.gaea.exception.UserException;
import org.bgi.flexlab.gaea.tools.baserecalibration.QuantizationInformation;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationTables;
import org.bgi.flexlab.gaea.tools.baserecalibration.RecalibrationUtils;
import org.bgi.flexlab.gaea.tools.baserecalibration.covariates.Covariate;
import org.bgi.flexlab.gaea.tools.mapreduce.recalibrator.BaseRecalibrationOptions;
import org.bgi.flexlab.gaea.util.HdfsFileManager;

public class TableReport {

	private BaseRecalibrationOptions RAC=null;
	private QuantizationInformation quantizationInfo; 
    private RecalibrationTables recalibrationTables;
    private Covariate[] requestedCovariates;    
    private FileSystem fs;
    private Configuration conf=new Configuration();
    
    public void initazie(BaseRecalibrationOptions RAC,RecalibrationTables recalibrationTables,Covariate[] requestedCovariates)
    {
    	fs = HdfsFileManager.getFileSystem(new Path(RAC.getOutputPath()),conf);
    	this.RAC=RAC;
    	this.recalibrationTables=recalibrationTables;
    	this.requestedCovariates=requestedCovariates;
    }
    
	public void reprot() {
        quantizeQualityScores();
        generateReport();
	}

	private void quantizeQualityScores() {
	    quantizationInfo = new QuantizationInformation(recalibrationTables, RAC.QUANTIZING_LEVELS);
	}

	 private void generateReport() {
         FSDataOutputStream output = null;
         try {
			output=fs.create(new Path(RAC.getOutputPath()+"/result.grp"));
         } catch (FileNotFoundException e) {
            throw new UserException.CouldNotCreateOutputFile(RAC.getOutputPath(), "could not be created");
         } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		 }

         RecalibrationUtils.outputRecalibrationReport(RAC, quantizationInfo, recalibrationTables, requestedCovariates, output);
         try {
		   	output.close();
	   	 } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     }
}
