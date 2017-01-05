package org.bgi.flexlab.gaea.tools.recalibrator.report;

import java.util.ArrayList;
import java.util.List;

import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RecalibratorOptions;
import org.bgi.flexlab.gaea.tools.recalibrator.covariate.Covariate;
import org.bgi.flexlab.gaea.tools.recalibrator.table.RecalibratorTable;
import org.bgi.flexlab.gaea.tools.recalibrator.table.RecalibratorTableCombiner;

import htsjdk.samtools.SAMFileHeader;

public class RecalibratorReportTableEngine {
	private RecalibratorOptions option = null;
	private SAMFileHeader header = null;
	private List<RecalibratorReportTable> reportTables = null;
	private RecalibratorReportWriter writer = null;
	
	public RecalibratorReportTableEngine(RecalibratorOptions option,SAMFileHeader header,RecalibratorReportWriter writer){
		this.header = header;
		this.option = option;
		this.writer = writer;
	}
	
	public void writeReportTable(String input){
		getReportTables(input);
		print();
		clear();
	}
	
	private void getReportTables(String input){
		RecalibratorTableCombiner combiner = new RecalibratorTableCombiner(option,header);
		combiner.combineTable(input);
		RecalibratorTable table = combiner.getRecalibratorTable();
		
		reportTables = new ArrayList<RecalibratorReportTable>();
		
		reportTables.add(RecalibratorReportTable.reportTableBuilder(option, covariateNames(combiner.getCovariates())));
		reportTables.add(RecalibratorReportTable.reportTableBuilder(table, option.QUANTIZING_LEVELS));
		reportTables.addAll(RecalibratorReportTable.reportTableBuilder(table, combiner.getCovariates()));
	}
	
	private void print(){
		for(RecalibratorReportTable table : reportTables){
			writer.write(table);
		}
	}
	
	private void clear(){
		reportTables.clear();
		writer.close();
	}
	
	private String covariateNames(Covariate[] covariates) {
		boolean first = true;
		StringBuilder sb  = new StringBuilder();
		for (final Covariate cov : covariates){
			if(first){
				sb.append(cov.getClass().getSimpleName());
				first = false;
			}else{
				sb.append(","+cov.getClass().getSimpleName());
			}
		}
		
		return sb.toString();
	}
}
