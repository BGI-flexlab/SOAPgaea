package org.bgi.flexlab.gaea.tools.recalibrator.report;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.bgi.flexlab.gaea.data.mapreduce.util.HdfsFilesReader;
import org.bgi.flexlab.gaea.tools.mapreduce.realigner.RecalibratorOptions;
import org.bgi.flexlab.gaea.tools.recalibrator.RecalibratorDatum;
import org.bgi.flexlab.gaea.tools.recalibrator.RecalibratorUtil;
import org.bgi.flexlab.gaea.tools.recalibrator.covariate.Covariate;
import org.bgi.flexlab.gaea.tools.recalibrator.covariate.CovariateUtil;
import org.bgi.flexlab.gaea.tools.recalibrator.table.RecalibratorTable;
import org.bgi.flexlab.gaea.util.EventType;
import org.bgi.flexlab.gaea.util.NestedObjectArray;

import htsjdk.samtools.SAMFileHeader;

public class RecalibratorReport {
	private HashMap<String, RecalibratorReportTable> tables = null;
	private RecalibratorOptions option = new RecalibratorOptions();
	private HashMap<String, Integer> optionalIndex = new HashMap<String, Integer>();
	private RecalibratorTable recalTable = null;
	private Covariate[] covariates = null;

	public RecalibratorReport(String input, SAMFileHeader header) {
		tables = new HashMap<String, RecalibratorReportTable>();
		addTables(input);
		option.parse(tables.get(RecalibratorUtil.ARGUMENT_TABLE_NAME));

		covariates = CovariateUtil.initializeCovariates(option, header);
		for (int i = 2; i < covariates.length; i++) {
			String covName = covariates[i].getClass().getSimpleName().split("Covariate")[0];
			optionalIndex.put(covName, i - 2);
		}

		recalTable = new RecalibratorTable(covariates,readGroupSize(tables.get(RecalibratorUtil.RECALIBRATOR_TABLE_NAME[0])));
		readGroupParser(tables.get(RecalibratorUtil.RECALIBRATOR_TABLE_NAME[0]),recalTable.getTable(RecalibratorTable.Type.READ_GROUP_TABLE));
		
		/**
		 * continue
		 */
	}

	public int readGroupSize(RecalibratorReportTable table) {
		Set<String> readGroup = new HashSet<String>();

		for (int i = 0; i < table.getRowNumber(); i++)
			readGroup.add(table.get(i, RecalibratorUtil.READGROUP_COLUMN_NAME).toString());
		return readGroup.size();
	}

	public void addTables(String input) {
		HdfsFilesReader reader = new HdfsFilesReader();
		reader.traversal(input);

		String header = null;
		if (reader.hasNext()) {
			header = reader.next();
		}
		if (header == null)
			throw new RuntimeException("heade is null");

		int ntables = Integer.parseInt(header.split(":")[2]);

		for (int i = 0; i < ntables; i++) {
			RecalibratorReportTable table = new RecalibratorReportTable(reader);
			tables.put(table.getTableName(), table);
		}
	}
	
	private void readGroupParser(RecalibratorReportTable table , NestedObjectArray<RecalibratorDatum> nestedArray){
		/**
		 * array initialize in old version
		 */
		//final int[] rgArray = new int[2];
		
		for ( int i = 0; i < table.getRowNumber(); i++ ) {
			int[] rgArray = new int[2];
	        final Object rg = table.get(i, RecalibratorUtil.READGROUP_COLUMN_NAME);
	        rgArray[0] = covariates[0].keyFromValue(rg);
	        final EventType event = EventType.eventFrom((String)table.get(i, RecalibratorUtil.EVENT_TYPE_COLUMN_NAME));
	        rgArray[1] = event.index;

	        nestedArray.put(RecalibratorDatum.build(table, i, true), rgArray);
	    }
	}

	public void clear() {
		if (tables != null)
			tables.clear();
		optionalIndex.clear();
	}
}
