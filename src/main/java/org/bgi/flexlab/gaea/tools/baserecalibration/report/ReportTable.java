
package org.bgi.flexlab.gaea.tools.baserecalibration.report;


import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class ReportTable {
    /**
     * REGEX that matches any table with an invalid name
     */
    public static final String INVALID_TABLE_NAME_REGEX = "[^a-zA-Z0-9_\\-\\.]";
    private static final String TABLE_HEADER_PREFIX = "#:Table";
    private static final String SEPARATOR = ":";
    private static final String ENDLINE = ":;";
    private static final String ENTER="\n";
    private static final String TAB="	";
    private final String tableName;
    private final String tableDescription;

    private final boolean sortByRowID;

    private List<Object[]> underlyingData;
    private final List<ReportColumn> columnInfo;
    private final Map<Object, Integer> columnNameToIndex;
    private final HashMap<Object, Integer> rowIdToIndex;

    private static final String OLD_TABLE_VERSION = "We no longer support older versions of the Tables";
    private static final int INITITAL_ARRAY_SIZE = 10000;

    protected enum TableDataHeaderFields {
        COLS(2),
        ROWS(3),
        FORMAT_START(4);

        private final int index;
        TableDataHeaderFields(int index) { this.index = index; }
        public int index() { return index; }
    }

    protected enum TableNameHeaderFields {
        NAME(2),
        DESCRIPTION(3);

        private final int index;
        TableNameHeaderFields(int index) { this.index = index; }
        public int index() { return index; }
    }

    /**
     * Construct a new report table from the reader
     * Note that the row ID mappings are just the index -> index
     *
     * @param reader        the reader
     * @param version       the report version
     */
    public ReportTable(LineReader reader, ReportVersion version) {
        switch ( version ) {
            case V1_1:
                // read in the header lines
                final String[] tableData, tableNameData;
                tableData = readLine(reader).split(SEPARATOR);
                tableNameData = readLine(reader).split(SEPARATOR);

                // parse the header fields
                tableName = tableNameData[TableNameHeaderFields.NAME.index()];
                tableDescription = (tableNameData.length <= TableNameHeaderFields.DESCRIPTION.index()) ? "" : tableNameData[TableNameHeaderFields.DESCRIPTION.index()];                                           // table may have no description! (and that's okay)

                // when reading from a file, we do not re-sort the rows
                sortByRowID = false;

                // initialize the data
                final int nColumns = Integer.parseInt(tableData[TableDataHeaderFields.COLS.index()]);
                final int nRows = Integer.parseInt(tableData[TableDataHeaderFields.ROWS.index()]);
                underlyingData = new ArrayList<Object[]>(nRows);
                columnInfo = new ArrayList<ReportColumn>(nColumns);
                columnNameToIndex = new HashMap<Object, Integer>(nColumns);

                // when reading from a file, the row ID mapping is just the index
                rowIdToIndex = new HashMap<Object, Integer>();
                for ( int i = 0; i < nRows; i++ )
                    rowIdToIndex.put(i, i);

                // read the column names
                final String columnLine;
                columnLine = readLine(reader);

                final List<Integer> columnStarts = TextFormattingUtils.getWordStarts(columnLine);
                final String[] columnNames = TextFormattingUtils.splitFixedWidth(columnLine, columnStarts);

                // Put in columns using the format string from the header
                for ( int i = 0; i < nColumns; i++ ) {
                    final String format = tableData[TableDataHeaderFields.FORMAT_START.index() + i];
                    addColumn(columnNames[i], format);
                }

			for ( int i = 0; i < nRows; i++ ) {
			    // read a data line
			    final String dataLine = readLine(reader);
			    final List<String> lineSplits = Arrays.asList(TextFormattingUtils.splitFixedWidth(dataLine, columnStarts));

			    underlyingData.add(new Object[nColumns]);
			    for ( int columnIndex = 0; columnIndex < nColumns; columnIndex++ ) {

			        final ReportDataType type = columnInfo.get(columnIndex).getDataType();
			        final String columnName = columnNames[columnIndex];
			        set(i, columnName, type.Parse(lineSplits.get(columnIndex)));

			    }
			}

			readLine(reader);
            break;

            default:
                throw new RuntimeException(OLD_TABLE_VERSION);
        }
    }
    
    private String readLine(LineReader reader){
    	Text line=new Text();
    	String tempString=null;
    	try {
			reader.readLine(line);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	tempString=line.toString();
    	return tempString;
    }
    
    /**
     * Construct a new  report table with the specified name and description
     *
     * @param tableName        the name of the table
     * @param tableDescription the description of the table
     * @param numColumns       the number of columns in this table
     */
    public ReportTable(final String tableName, final String tableDescription, final int numColumns) {
        this(tableName, tableDescription, numColumns, true);
    }

    /**
     * Construct a new  report table with the specified name and description and whether to sort rows by the row ID.
     *
     * @param tableName          the name of the table
     * @param tableDescription   the description of the table
     * @param numColumns         the number of columns in this table
     * @param sortByRowID        whether to sort rows by the row ID (instead of the order in which they were added)
     */
    public ReportTable(final String tableName, final String tableDescription, final int numColumns, final boolean sortByRowID) {
        if ( !isValidName(tableName) ) {
            throw new RuntimeException("Attempted to set a ReportTable name of '" + tableName + "'.  ReportTable names must be purely alphanumeric - no spaces or special characters are allowed.");
        }

        if ( !isValidDescription(tableDescription) ) {
            throw new RuntimeException("Attempted to set a ReportTable description of '" + tableDescription + "'.  ReportTable descriptions must not contain newlines.");
        }

        this.tableName = tableName;
        this.tableDescription = tableDescription;
        this.sortByRowID = sortByRowID;

        underlyingData = new ArrayList<Object[]>(INITITAL_ARRAY_SIZE);
        columnInfo = new ArrayList<ReportColumn>(numColumns);
        columnNameToIndex = new HashMap<Object, Integer>(numColumns);
        rowIdToIndex = new HashMap<Object, Integer>();
    }

    /**
     * Create a new ReportTable with the same structure
     * @param tableToCopy
     */
    public ReportTable(final ReportTable tableToCopy, final boolean copyData) {
        this(tableToCopy.getTableName(), tableToCopy.getTableDescription(), tableToCopy.getNumColumns(), tableToCopy.sortByRowID);
        for ( final ReportColumn column : tableToCopy.getColumnInfo() )
            addColumn(column.getColumnName(), column.getFormat());
        if ( copyData )
            throw new IllegalArgumentException("sorry, copying data in ReportTable isn't supported");
    }

        /**
        * Verifies that a table or column name has only alphanumeric characters - no spaces or special characters allowed
        *
        * @param name the name of the table or column
        * @return true if the name is valid, false if otherwise
        */
    private boolean isValidName(String name) {
        Pattern p = Pattern.compile(INVALID_TABLE_NAME_REGEX);
        Matcher m = p.matcher(name);

        return !m.find();
    }

    /**
     * Verifies that a table or column name has only alphanumeric characters - no spaces or special characters allowed
     *
     * @param description the name of the table or column
     * @return true if the name is valid, false if otherwise
     */
    private boolean isValidDescription(String description) {
        Pattern p = Pattern.compile("\\r|\\n");
        Matcher m = p.matcher(description);

        return !m.find();
    }

    /**
     * Add a mapping from ID to the index of a new row added to the table.
     *
     * @param ID                    the unique ID
     */
    public void addRowID(final String ID) {
        addRowID(ID, false);
    }

    /**
     * Add a mapping from ID to the index of a new row added to the table.
     *
     * @param ID                    the unique ID
     * @param populateFirstColumn   should we automatically populate the first column with the row's ID?
     */
    public void addRowID(final String ID, final boolean populateFirstColumn) {
        addRowIDMapping(ID, underlyingData.size(), populateFirstColumn);
    }

    /**
     * Add a mapping from ID to row index.
     *
     * @param ID                    the unique ID
     * @param index                 the index associated with the ID
     */
    public void addRowIDMapping(final String ID, final int index) {
        addRowIDMapping(ID, index, false);
    }

    /**
     * Add a mapping from ID to row index.
     *
     * @param ID                    the unique ID
     * @param index                 the index associated with the ID
     * @param populateFirstColumn   should we automatically populate the first column with the row's ID?
     */
    public void addRowIDMapping(final Object ID, final int index, final boolean populateFirstColumn) {
        expandTo(index, false);
        rowIdToIndex.put(ID, index);

        if ( populateFirstColumn )
            set(index, 0, ID);
    }

    /**
     * Remove a mapping from ID to row index.
     *
     * @param ID   the row ID
     */
    public void removeRowIDMapping(final Object ID) {
        rowIdToIndex.remove(ID);
    }

    /**
     * Add a column to the report
     *
     * @param columnName   the name of the column
     */
    public void addColumn(String columnName) {
        addColumn(columnName, "");
    }

    /**
     * Add a column to the report and the format string used to display the data.
     *
     * @param columnName   the name of the column
     * @param format       the format string used to display data
     */
    public void addColumn(String columnName, String format) {
        columnNameToIndex.put(columnName, columnInfo.size());
        columnInfo.add(new ReportColumn(columnName, format));
    }

    /**
     * Check if the requested cell is valid and expand the table if necessary
     *
     * @param rowIndex    the row index
     * @param colIndex    the column index
     */
    private void verifyEntry(final int rowIndex, final int colIndex) {
        if ( rowIndex < 0 || colIndex < 0 || colIndex >= getNumColumns() )
            throw new RuntimeException("attempted to access a cell that does not exist in table '" + tableName + "'");
    }

    /**
     * expand the underlying table if needed to include the given row index
     *
     * @param rowIndex        the row index
     * @param updateRowIdMap  should we update the row ID map?
     */
    private void expandTo(final int rowIndex, final boolean updateRowIdMap) {
        int currentSize = underlyingData.size();
        if ( rowIndex >= currentSize ) {
            final int numNewRows = rowIndex - currentSize + 1;
            for ( int i = 0; i < numNewRows; i++ ) {
                if ( updateRowIdMap )
                    rowIdToIndex.put(currentSize, currentSize);
                underlyingData.add(new Object[getNumColumns()]);
                currentSize++;
            }
        }
    }

    /**
     * Set the value for a given position in the table.
     * If the row ID doesn't exist, it will create a new row in the table with the given ID.
     *
     * @param rowID        the row ID
     * @param columnName   the name of the column
     * @param value        the value to set
     */
    public void set(final Object rowID, final String columnName, final Object value) {
        if ( !rowIdToIndex.containsKey(rowID) ) {
            rowIdToIndex.put(rowID, underlyingData.size());
            expandTo(underlyingData.size(), false);
        }
        set(rowIdToIndex.get(rowID), columnNameToIndex.get(columnName), value);
    }

    /**
     * Set the value for a given position in the table.
     * If the row index doesn't exist, it will create new rows in the table accordingly.
     *
     * @param rowIndex     the row index
     * @param colIndex     the column index
     * @param value        the value to set
     */
    public void set(final int rowIndex, final int colIndex, Object value) {
        expandTo(rowIndex, true);
        verifyEntry(rowIndex, colIndex);
       ReportColumn column = columnInfo.get(colIndex);

        // We do not accept internal null values
        if (value == null)
            value = "null";
        else
            value = fixType(value, column);

        if ( column.getDataType().equals(ReportDataType.fromObject(value)) || column.getDataType().equals(ReportDataType.Unknown) ) {
            underlyingData.get(rowIndex)[colIndex] = value;
            column.updateFormatting(value);
        } else {
            throw new RuntimeException(String.format("Tried to add an object of type: %s to a column of type: %s", ReportDataType.fromObject(value).name(), column.getDataType().name()));
        }
    }

    /**
     * Returns true if the table contains a row mapping with the given ID
     *
     * @param rowID        the row ID
     */
    public boolean containsRowID(final Object rowID) {
        return rowIdToIndex.containsKey(rowID);
    }

    /**
     * Returns the row mapping IDs
     *
     */
    public Collection<Object> getRowIDs() {
        return rowIdToIndex.keySet();
    }

    /**
    * Increment the value for a given position in the table.
    * Throws an exception if the value in the cell is not an integer.
    *
    * @param rowID        the row ID
    * @param columnName   the name of the column
    */
    public void increment(final Object rowID, final String columnName) {
        int prevValue;
        if ( !rowIdToIndex.containsKey(rowID) ) {
            rowIdToIndex.put(rowID, underlyingData.size());
            underlyingData.add(new Object[getNumColumns()]);
            prevValue = 0;
        } else {
            Object obj = get(rowID, columnName);
            if ( !(obj instanceof Integer) )
                throw new RuntimeException("Attempting to increment a value in a cell that is not an integer");
            prevValue = (Integer)obj;
        }

        set(rowIdToIndex.get(rowID), columnNameToIndex.get(columnName), prevValue + 1);
    }

    /**
     * Returns the index of the first row matching the column values.
     * Ex: "CountVariants", "dbsnp", "eval", "called", "all", "novel", "all"
     *
     * @param columnValues column values.
     * @return The index of the first row matching the column values or -1 if no such row exists.
     */
    public int findRowByData(final Object... columnValues) {
        if ( columnValues == null || columnValues.length == 0 || columnValues.length > getNumColumns() )
            return -1;

        for ( int rowIndex = 0; rowIndex < underlyingData.size(); rowIndex++ ) {

            final Object[] row = underlyingData.get(rowIndex);

            boolean matches = true;
            for ( int colIndex = 0; colIndex < columnValues.length; colIndex++ ) {
                if ( !columnValues[colIndex].equals(row[colIndex]) ) {
                    matches = false;
                    break;
                }
            }

            if ( matches )
                return rowIndex;
        }

        return -1;
    }

    private Object fixType(final Object value, final ReportColumn column) {
        // Below is some code to convert a string into its appropriate type.

        // todo -- Types have to be more flexible. For example, %d should accept Integers, Shorts and Bytes.

        Object newValue = null;
        if ( value instanceof String && !column.getDataType().equals(ReportDataType.String) ) {
            // Integer case
            if ( column.getDataType().equals(ReportDataType.Integer) ) {
                try {
                    newValue = Long.parseLong((String) value);
                } catch (Exception e) {
                    /** do nothing */
                }
            }
            if ( column.getDataType().equals(ReportDataType.Decimal) ) {
                try {
                    newValue = Double.parseDouble((String) value);
                } catch (Exception e) {
                    /** do nothing */
                }
            }
            if ( column.getDataType().equals(ReportDataType.Character) && ((String) value).length() == 1 ) {
                newValue = ((String) value).charAt(0);
            }
        }

        return  (newValue != null) ? newValue : value;
    }

    /**
     * Get a value from the given position in the table
     *
     * @param rowID       the row ID
     * @param columnName  the name of the column
     * @return the value stored at the specified position in the table
     */
    public Object get(final Object rowID, final String columnName) {
        return get(rowIdToIndex.get(rowID), columnNameToIndex.get(columnName));
    }

    /**
     * Get a value from the given position in the table
     *
     * @param rowIndex       the row ID
     * @param columnName  the name of the column
     * @return the value stored at the specified position in the table
     */
    public Object get(final int rowIndex, final String columnName) {
        return get(rowIndex, columnNameToIndex.get(columnName));
    }

    /**
     * Get a value from the given position in the table
     *
     * @param rowIndex    the index of the row
     * @param columnIndex the index of the column
     * @return the value stored at the specified position in the table
     */
    public Object get(int rowIndex, int columnIndex) {
        verifyEntry(rowIndex, columnIndex);
        return underlyingData.get(rowIndex)[columnIndex];
    }

    /**
     * Write the table to the PrintStream, formatted nicely to be human-readable, AWK-able, and R-friendly.
     *
     * @param out the PrintStream to which the table should be written
     * @throws IOException 
     */
     void write( OutputStream out) throws IOException {

         /*
          * Table header:
          * #:Table:nColumns:nRows:(DataType for each column):;
          * #:Table:TableName:Description :;
          * key   colA  colB
          * row1  xxxx  xxxxx
         */

         // write the table definition
         //out.printf(TABLE_HEADER_PREFIX + ":%d:%d", getNumColumns(), getNumRows());
    	 out.write(TABLE_HEADER_PREFIX.getBytes());
    	 out.write(SEPARATOR.getBytes());
    	 out.write(String.valueOf(getNumColumns()).getBytes());
    	 out.write(SEPARATOR.getBytes());
    	 out.write(String.valueOf(getNumRows()).getBytes());
         // write the formats for all the columns
         for ( final ReportColumn column : columnInfo )
         {
        	 //out.print(SEPARATOR + column.getFormat());
        	 out.write(SEPARATOR.getBytes());
        	 out.write(column.getFormat().getBytes());
         }
        // out.println(ENDLINE);
         out.write(ENDLINE.getBytes());
         out.write(ENTER.getBytes());
         // write the table name & description
        // out.printf(TABLE_HEADER_PREFIX + ":%s:%s\n", tableName, tableDescription);
         out.write(TABLE_HEADER_PREFIX.getBytes());
         out.write(SEPARATOR.getBytes());
         out.write(tableName.getBytes());
         out.write(SEPARATOR.getBytes());
         out.write(tableDescription.getBytes());
         out.write(ENTER.getBytes());
         // write the column names
         boolean needsPadding = false;
         for ( final ReportColumn column : columnInfo ) {
             if ( needsPadding )
                // out.printf("  ");
            	 out.write(TAB.getBytes());
             needsPadding = true;

             //out.printf(column.getColumnFormat().getNameFormat(), column.getColumnName());
             out.write(String.format(column.getColumnFormat().getNameFormat(), column.getColumnName()).getBytes());
        }
        //out.println();
         out.write(ENTER.getBytes());

        // write the table body
        if ( sortByRowID ) {
            // make sure that there are exactly the correct number of ID mappings
            if ( rowIdToIndex.size() != underlyingData.size() )
                throw new RuntimeException("There isn't a 1-to-1 mapping from row ID to index; this can happen when rows are not created consistently");

            final TreeMap<Object, Integer> sortedMap;
            try {
                sortedMap = new TreeMap<Object, Integer>(rowIdToIndex);
            } catch (ClassCastException e) {
                throw new RuntimeException("Unable to sort the rows based on the row IDs because the ID Objects are of different types");
            }
            for ( final Map.Entry<Object, Integer> rowKey : sortedMap.entrySet() )
                writeRow(out, underlyingData.get(rowKey.getValue()));
        } else {
            for ( final Object[] row : underlyingData )
                writeRow(out, row);
        }

       // out.println();
        out.write(ENTER.getBytes());
    }

    private void writeRow(final OutputStream out, final Object[] row) throws IOException {
        boolean needsPadding = false;
        for ( int i = 0; i < row.length; i++ ) {
            if ( needsPadding )
                //out.printf("  ");
            	out.write(TAB.getBytes());
            needsPadding = true;

            final Object obj = row[i];
            final String value;

            final ReportColumn info = columnInfo.get(i);

            if ( obj == null )
                value = "null";
            else if ( info.getDataType().equals(ReportDataType.Unknown) && (obj instanceof Double || obj instanceof Float) )
                value = String.format("%.8f", obj);
            else
                value = String.format(info.getFormat(), obj);

            //out.printf(info.getColumnFormat().getValueFormat(), value);
            out.write(String.format(info.getColumnFormat().getValueFormat(), value).getBytes());
        }

        //out.println();
        out.write(ENTER.getBytes());
    }

    public int getNumRows() {
        return underlyingData.size();
    }

    public int getNumColumns() {
        return columnInfo.size();
    }

    public List<ReportColumn> getColumnInfo() {
        return columnInfo;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableDescription() {
        return tableDescription;
    }

    /**
     * Concatenates the rows from the table to this one
     *
     * @param table another table
     */
    public void concat(final ReportTable table) {
        if ( !isSameFormat(table) )
            throw new RuntimeException("Error trying to concatenate tables with different formats");

        // add the data
        underlyingData.addAll(table.underlyingData);

        // update the row index map
        final int currentNumRows = getNumRows();
        for ( Map.Entry<Object, Integer> entry : table.rowIdToIndex.entrySet() )
            rowIdToIndex.put(entry.getKey(), entry.getValue() + currentNumRows);
    }

    /**
     * Returns whether or not the two tables have the same format including columns and everything in between. This does
     * not check if the data inside is the same. This is the check to see if the two tables are gatherable or
     * reduceable
     *
     * @param table another  table
     * @return true if the the tables are gatherable
     */
    public boolean isSameFormat(final ReportTable table) {
        if ( !tableName.equals(table.tableName) ||
                !tableDescription.equals(table.tableDescription) ||
                columnInfo.size() != table.columnInfo.size() )
            return false;

        for ( int i = 0; i < columnInfo.size(); i++ ) {
            if ( !columnInfo.get(i).getFormat().equals(table.columnInfo.get(i).getFormat()) ||
                    !columnInfo.get(i).getColumnName().equals(table.columnInfo.get(i).getColumnName()) )
                return false;
        }

        return true;
    }

    /**
     * Checks that the tables are exactly the same.
     *
     * @param table another report
     * @return true if all field in the reports, tables, and columns are equal.
     */
    public boolean equals(final ReportTable table) {
        if ( !isSameFormat(table) ||
                underlyingData.size() != table.underlyingData.size() )
            return false;

        final List<Object[]> myOrderedRows = getOrderedRows();
        final List<Object[]> otherOrderedRows = table.getOrderedRows();

        for ( int i = 0; i < underlyingData.size(); i++ ) {
            final Object[] myData = myOrderedRows.get(i);
            final Object[] otherData = otherOrderedRows.get(i);
            for ( int j = 0; j < myData.length; j++ ) {
                if ( !myData[j].toString().equals(otherData[j].toString()) )       // need to deal with different typing (e.g. Long vs. Integer)
                    return false;
            }
        }

        return true;
    }

    private List<Object[]> getOrderedRows() {
        if ( !sortByRowID )
            return underlyingData;

        final TreeMap<Object, Integer> sortedMap;
        try {
            sortedMap = new TreeMap<Object, Integer>(rowIdToIndex);
        } catch (ClassCastException e) {
            return underlyingData;
        }

        final List<Object[]> orderedData = new ArrayList<Object[]>(underlyingData.size());
        for ( final int rowKey : sortedMap.values() )
            orderedData.add(underlyingData.get(rowKey));

        return orderedData;
    }
}
