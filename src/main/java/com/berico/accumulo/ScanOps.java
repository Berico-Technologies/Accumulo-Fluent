package com.berico.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * Fluent Interface for performing Scan operations in Accumulo.
 * 
 * @author Richard Clayton (Berico Technologies)
 */
public class ScanOps extends FluentExtension {
	
	Scanner scanner;
	
	public ScanOps(Cirrus cirrus, Scanner scanner) {
		super(cirrus);
		
		this.scanner = scanner;
	}
	
	public Iterator<Entry<Key, Value>> getRows(String start, String end){
	
		Range range = new Range(start,end);
		
		this.scanner.setRange(range);
		
		return this.scanner.iterator();
	}
	
	/* ####### Cell Retrieval ############################################################## */
	
	/**
	 * Retrieve the value of a cell.
	 * @param rowId Row
	 * @param columnFamily Column Family
	 * @param columnQualifier Column Qualifier
	 * @return Fluent interface for accessing the cell's value.
	 */
	public CellValueRetrievalOps cell(
			String rowId, String columnFamily, String columnQualifier){
		
		return cell(rowId, columnFamily, columnQualifier, null, 0);
	}
	
	/**
	 * Retrieve the value of a cell.
	 * @param rowId Row
	 * @param columnFamily Column Family
	 * @param columnQualifier Column Qualifier
	 * @param columnVisibility Visibility of the Column
	 * @return Fluent interface for accessing the cell's value.
	 */
	public CellValueRetrievalOps cell(
			String rowId, String columnFamily, String columnQualifier, 
			String columnVisibility){
		
		return cell(rowId, columnFamily, columnQualifier, columnVisibility, 0);
	}
	
	/**
	 * Retrieve the value of a cell.
	 * @param rowId Row
	 * @param columnExpression Expression representing the family, qualifier and visibility
	 * @return Fluent interface for accessing the cell's value.
	 */
	public CellValueRetrievalOps cell(
			String rowId, String columnExpression){
		
		return cell(rowId, columnExpression, 0);
	}
	
	/**
	 * Retrieve the value of a cell.
	 * @param rowId Row
	 * @param columnExpression Expression representing the family, qualifier and visibility
	 * @param timestamp Specific time of the cell's mutation.
	 * @return Fluent interface for accessing the cell's value.
	 */
	public CellValueRetrievalOps cell(
			String rowId, String columnExpression, long timestamp){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		return cell(rowId, columnIdentifiers.getColumnFamily(), 
			columnIdentifiers.getColumnQualifier(), columnIdentifiers.getVisibilityExpression(), timestamp);
	}
	
	/**
	 * Retrieve the value of a cell.
	 * @param rowId Row
	 * @param columnFamily Column Family
	 * @param columnQualifier Column Qualifier
	 * @param columnVisibility Visibility of the Column
	 * @param timestamp Specific time of the cell's mutation.
	 * @return Fluent interface for accessing the cell's value.
	 */
	public CellValueRetrievalOps cell(
			String rowId, String columnFamily, String columnQualifier, 
			String columnVisibility, long timestamp){
		
		Range range = getExactRange(rowId, columnFamily, columnQualifier, columnVisibility, timestamp);
		
		this.scanner.setRange(range);
		
		
		Entry<Key, Value> cell = null;
		
		Iterator<Entry<Key, Value>> iterator = this.scanner.iterator();
		
		if (iterator.hasNext()){
			
			cell = iterator.next();
		}
		
		return new CellValueRetrievalOps(this, cell);
	}
	
	/* ####### Column Family Retrieval ########################################################## */
	
	public RowValueRetrievalOps family(String rowId, String columnFamily){
		
		Key k = new KeyBuilder().row(rowId).family(columnFamily).build();
		
		Range r = RangeBuilder.exact(k);
		
		return retrieveRow(r);
	}
	
	/* ####### Row Retrieval #################################################################### */
	
	public RowValueRetrievalOps row(String rowId){
		
		Key k = new KeyBuilder().row(rowId).build();
		
		Range r = RangeBuilder.exact(k);
		
		return retrieveRow(r);
	}
	
	RowValueRetrievalOps retrieveRow(Range range){
		
		this.scanner.setRange(range);
		
		return new RowValueRetrievalOps(this, this.scanner.iterator()); 
	}
	
	/**
	 * Construct a Range object for retrieving only one cell.
	 * @param rowId Row
	 * @param columnFamily Column Family
	 * @param columnQualifier Column Qualifier
	 * @param columnVisibility Visibility of the Column
	 * @param timestamp Specific time of the cell's mutation.
	 * @return What the scanner needs to return one specific cell.
	 */
	Range getExactRange(String rowId, String columnFamily, String columnQualifier, String columnVisibility, long timestamp){
		
		Text r = new Text(rowId);
		Text cf = new Text(columnFamily);
		Text cq = new Text(columnQualifier);
		
		if (columnVisibility != null && timestamp != 0){
			
			Text cv = new Text(columnVisibility);
			
			return Range.exact(r, cf, cq, cv, timestamp);
		}
		else if (columnVisibility != null){
			
			Text cv = new Text(columnVisibility);
			
			return Range.exact(r, cf, cq, cv);
		}
		
		return Range.exact(r, cf, cq);
	}
}
