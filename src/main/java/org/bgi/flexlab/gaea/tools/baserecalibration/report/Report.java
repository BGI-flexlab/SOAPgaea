
package org.bgi.flexlab.gaea.tools.baserecalibration.report;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.bgi.flexlab.gaea.exception.UserException;


public class Report {
    public static final String REPORT_HEADER_PREFIX = "#:Report.";
    public static final ReportVersion LATEST_REPORT_VERSION = ReportVersion.V1_1;
    private static final String SEPARATOR = ":";
    private ReportVersion version = LATEST_REPORT_VERSION;

    private final TreeMap<String, ReportTable> tables = new TreeMap<String, ReportTable>();

    public Report() {
    }

    public Report(String file) {
        loadReport(file);
    }

    public Report(ReportTable... tables) {
        for( ReportTable table: tables)
            addTable(table);
    }

    private void loadReport(String file) {
    	Configuration conf=new Configuration();
    	FileSystem fs=getFileSystem(file,conf);
    	FSDataInputStream fsDataInputStream=null;
    	LineReader lineReader=null;
    	Text line = new Text();
    	String reportHeader = null;
    	try {
			 fsDataInputStream=fs.open(new Path(file));
			 lineReader = new LineReader(fsDataInputStream, conf);
			 lineReader.readLine(line);
			 if(line.getLength()>0)
			     reportHeader=line.toString();
			 else
				 throw new UserException.CouldNotReadInputFile("the input file is empty!");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

        // Read the first line for the version and number of tables.
        version = ReportVersion.fromHeader(reportHeader);
        if (version.equals(ReportVersion.V0_1) ||
                version.equals(ReportVersion.V0_2))
            throw new UserException(" Please use v1.0 or newer reports version.");

        int nTables = Integer.parseInt(reportHeader.split(":")[2]);

        // Read each table according ot the number of tables
        for (int i = 0; i < nTables; i++) {
            addTable(new ReportTable(lineReader, version));
        }
    }

    private static FileSystem getFileSystem(String path,Configuration conf)
 	{
 		FileSystem fs=null;
 		Path p=new Path(path);
 		try {
			fs=p.getFileSystem(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
 	}
    
    /**
     * Add a new, empty table to the report
     *
     * @param tableName        the name of the table
     * @param tableDescription the description of the table
     * @param numColumns       the number of columns in this table
     */
    public void addTable(final String tableName, final String tableDescription, final int numColumns) {
        addTable(tableName, tableDescription, numColumns, false);
    }

    /**
     * Add a new, empty table to the report
     *
     * @param tableName        the name of the table
     * @param tableDescription the description of the table
     * @param numColumns       the number of columns in this table
     * @param sortByRowID      whether to sort the rows by the row ID
     */
    public void addTable(final String tableName, final String tableDescription, final int numColumns, final boolean sortByRowID) {
        ReportTable table = new ReportTable(tableName, tableDescription, numColumns, sortByRowID);
        tables.put(tableName, table);
    }

    /**
     * Adds a table, empty or populated, to the report
     *
     * @param table the table to add
     */
    public void addTable(ReportTable table) {
        tables.put(table.getTableName(), table);
    }

    public void addTables(List<ReportTable> gatkReportTableV2s) {
        for ( ReportTable table : gatkReportTableV2s )
            addTable(table);
    }

    /**
     * Return true if table with a given name exists
     *
     * @param tableName the name of the table
     * @return true if the table exists, false otherwise
     */
    public boolean hasTable(String tableName) {
        return tables.containsKey(tableName);
    }

    /**
     * Return a table with a given name
     *
     * @param tableName the name of the table
     * @return the table object
     */
    public ReportTable getTable(String tableName) {
        ReportTable table = tables.get(tableName);
        if (table == null)
            throw new RuntimeException("Table is not in Report: " + tableName);
        return table;
    }

    /**
     * Print all tables contained within this container to a PrintStream
     *
     * @param out the PrintStream to which the tables should be written
     * @throws IOException 
     */
    public void print(OutputStream out) throws IOException {
        out.write(REPORT_HEADER_PREFIX.getBytes());
        out.write( getVersion().toString().getBytes());
        out.write( SEPARATOR.getBytes());
        out.write(String.valueOf(getTables().size()).getBytes());
        out.write("\n".getBytes());
        for (ReportTable table : tables.values())
            table.write(out);
    }

    public Collection<ReportTable> getTables() {
        return tables.values();
    }

    /**
     * This is the main function is charge of gathering the reports. It checks that the reports are compatible and then
     * calls the table gathering functions.
     *
     * @param input another report of the same format
     */
    public void concat(Report input) {

        if ( !isSameFormat(input) ) {
            throw new RuntimeException("Failed to combine GATKReport, format doesn't match!");
        }

        for ( Map.Entry<String, ReportTable> table : tables.entrySet() ) {
            table.getValue().concat(input.getTable(table.getKey()));
        }
    }

    public ReportVersion getVersion() {
        return version;
    }

    /**
     * Returns whether or not the two reports have the same format, from columns, to tables, to reports, and everything
     * in between. This does not check if the data inside is the same. This is the check to see if the two reports are
     * gatherable or reduceable.
     *
     * @param report another report
     * @return true if the the reports are gatherable
     */
    public boolean isSameFormat(Report report) {
        if (!version.equals(report.version)) {
            return false;
        }
        if (!tables.keySet().equals(report.tables.keySet())) {
            return false;
        }
        for (String tableName : tables.keySet()) {
            if (!getTable(tableName).isSameFormat(report.getTable(tableName)))
                return false;
        }
        return true;
    }

    /**
     * Checks that the reports are exactly the same.
     *
     * @param report another report
     * @return true if all field in the reports, tables, and columns are equal.
     */
    public boolean equals(Report report) {
        if (!version.equals(report.version)) {
            return false;
        }
        if (!tables.keySet().equals(report.tables.keySet())) {
            return false;
        }
        for (String tableName : tables.keySet()) {
            if (!getTable(tableName).equals(report.getTable(tableName)))
                return false;
        }
        return true;
    }

    /**
     * The constructor for a simplified Report. Simplified report are designed for reports that do not need
     * the advanced functionality of a full report.
     * <p/>
     * A simple report consists of:
     * <p/>
     * - A single table
     * - No primary key ( it is hidden )
     * <p/>
     * Optional:
     * - Only untyped columns. As long as the data is an Object, it will be accepted.
     * - Default column values being empty strings.
     * <p/>
     * Limitations:
     * <p/>
     * - A simple report cannot contain multiple tables.
     * - It cannot contain typed columns, which prevents arithmetic gathering.
     *
     * @param tableName The name of your simple report table
     * @param columns   The names of the columns in your table
     * @return a simplified report
     */
    public static Report newSimpleReport(final String tableName, final String... columns) {
        ReportTable table = new ReportTable(tableName, "A simplified table report", columns.length);

        for (String column : columns) {
            table.addColumn(column, "");
        }

        Report output = new Report();
        output.addTable(table);

        return output;
    }

    /**
     * The constructor for a simplified report. Simplified report are designed for reports that do not need
     * the advanced functionality of a full report.
     * <p/>
     * A simple report consists of:
     * <p/>
     * - A single table
     * - No primary key ( it is hidden )
     * <p/>
     * Optional:
     * - Only untyped columns. As long as the data is an Object, it will be accepted.
     * - Default column values being empty strings.
     * <p/>
     * Limitations:
     * <p/>
     * - A simple report cannot contain multiple tables.
     * - It cannot contain typed columns, which prevents arithmetic gathering.
     *
     * @param tableName The name of your simple report table
     * @param columns   The names of the columns in your table
     * @return a simplified report
     */
    public static Report newSimpleReport(final String tableName, final List<String> columns) {
        ReportTable table = new ReportTable(tableName, "A simplified GATK table report", columns.size());

        for (String column : columns) {
            table.addColumn(column, "");
        }

        Report output = new Report();
        output.addTable(table);

        return output;
    }

    /**
     * This method provides an efficient way to populate a simplified report. This method will only work on reports
     * that qualify as simplified reports. See the newSimpleReport() constructor for more information.
     *
     * @param values     the row of data to be added to the table.
     *               Note: the number of arguments must match the columns in the table.
     */
    public void addRow(final Object... values) {
        // Must be a simple report
        if ( tables.size() != 1 )
            throw new RuntimeException("Cannot write a row to a complex GATK Report");

        ReportTable table = tables.firstEntry().getValue();
        if ( table.getNumColumns() != values.length )
            throw new RuntimeException("The number of arguments in writeRow() must match the number of columns in the table");

        final int rowIndex = table.getNumRows();
        for ( int i = 0; i < values.length; i++ )
            table.set(rowIndex, i, values[i]);
    }

    /**
     * This method provides an efficient way to populate a simplified report. This method will only work on reports
     * that qualify as simplified reports. See the newSimpleReport() constructor for more information.
     *
     * @param values     the row of data to be added to the table.
     *               Note: the number of arguments must match the columns in the table.
     */
    public void addRowList(final List<Object> values) {
        if ( tables.size() != 1 )
            throw new RuntimeException("Cannot write a row to a complex GATK Report");

        ReportTable table = tables.firstEntry().getValue();
        if ( table.getNumColumns() != values.size() )
            throw new RuntimeException("The number of arguments in writeRow() must match the number of columns in the table");

        final int rowIndex = table.getNumRows();
        int idx = 0;
        for ( Object value : values ) {
            table.set(rowIndex,idx,value);
            idx++;
        }
    }
}
