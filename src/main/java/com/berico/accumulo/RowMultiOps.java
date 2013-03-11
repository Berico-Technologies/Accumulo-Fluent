package com.berico.accumulo;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import static com.berico.accumulo.ConversionUtils.*;

/**
 * A fluent interface for executing multiple mutations on the same row.
 * 
 * @author Richard Clayton (Berico Technologies)
 */
public class RowMultiOps {

	/**
	 * Row key for the row in scope.
	 */
	String rowKey = null;
	
	/**
	 * The parent MutationOps object this instance was spawned from.
	 */
	MutationOps parent = null;
	
	/**
	 * The mutation we are building on.
	 */
	Mutation mutation = null;
	
	/**
	 * Initialize the fluent interface with the key of the row in scope and
	 * the parent MutationOps object.
	 * @param rowKey Key of the row in scope.
	 * @param parent The parent fluent interface.
	 */
	public RowMultiOps(String rowKey, MutationOps parent){
		
		this.rowKey = rowKey;
		this.parent = parent;
		
		this.mutation = new Mutation(this.rowKey);
	}
	
	
	/* ############## Puts ########################################################################## */
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the Column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, String value){
		
		this.mutation.put(family, qualifier, value);
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the Column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, int value){
		
		this.mutation.put(family, qualifier, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the Column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, boolean value){
		
		this.mutation.put(family, qualifier, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the Column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, long value){
		
		this.mutation.put(family, qualifier, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the Column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, double value){
		
		this.mutation.put(family, qualifier, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the Column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, byte[] value){
		
		this.mutation.put(family, qualifier, new Value(value));
		
		return this;
	}
	
	/**
	 * Add a cell value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, String value, long timestamp){
		
		this.mutation.put(family, qualifier, timestamp, value);
		
		return this;
	}
	
	/**
	 * Add a cell value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, long value, long timestamp){
		
		this.mutation.put(family, qualifier, timestamp, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a cell value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, int value, long timestamp){
		
		this.mutation.put(family, qualifier, timestamp,  new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a cell value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, double value, long timestamp){
		
		this.mutation.put(family, qualifier, timestamp,  new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a cell value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, boolean value, long timestamp){
		
		this.mutation.put(family, qualifier, timestamp,  new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a cell value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, byte[] value, long timestamp){
		
		this.mutation.put(family, qualifier, timestamp, new Value(value));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, String value, String visibilityExpression){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, value);
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, long value, String visibilityExpression){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, int value, String visibilityExpression){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, double value, String visibilityExpression){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, boolean value, String visibilityExpression){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, byte[] value, String visibilityExpression){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, new Value(value));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, String value, String visibilityExpression, long timestamp){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, timestamp, value);
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, long value, String visibilityExpression, long timestamp){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, timestamp, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, int value, String visibilityExpression, long timestamp){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, timestamp, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, double value, String visibilityExpression, long timestamp){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, timestamp, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, boolean value, String visibilityExpression, long timestamp){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, timestamp, new Value(toByteArray(value)));
		
		return this;
	}
	
	/**
	 * Add a column value for the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param value Value of the cell.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String family, String qualifier, byte[] value, String visibilityExpression, long timestamp){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.put(family, qualifier, columnVisibility, timestamp, new Value(value));
		
		return this;
	}
	
	/**
	 * Add a mutation using a column identifier expression and a value.
	 * @param columnExpression Column Identifier Expression (see below).
	 * @param value Value of the column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String columnExpression, String value){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		if (columnIdentifiers.hasVisibilityExpression()){
			
			return this.put(
				columnIdentifiers.columnFamily, 
				columnIdentifiers.columnQualifier, 
				columnIdentifiers.visibilityExpression, 
				value);
		}
		
		return this.put(columnIdentifiers.columnFamily, columnIdentifiers.columnQualifier, value);
	}
	
	/**
	 * Add a mutation using a column identifier expression and a value.
	 * @param columnExpression Column Identifier Expression (see below).
	 * @param value Value of the column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String columnExpression, long value){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		if (columnIdentifiers.hasVisibilityExpression()){
			
			return this.put(
				columnIdentifiers.columnFamily, 
				columnIdentifiers.columnQualifier, 
				columnIdentifiers.visibilityExpression, 
				value);
		}
		
		return this.put(columnIdentifiers.columnFamily, columnIdentifiers.columnQualifier, value);
	}
	
	/**
	 * Add a mutation using a column identifier expression and a value.
	 * @param columnExpression Column Identifier Expression (see below).
	 * @param value Value of the column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String columnExpression, int value){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		if (columnIdentifiers.hasVisibilityExpression()){
			
			return this.put(
				columnIdentifiers.columnFamily, 
				columnIdentifiers.columnQualifier, 
				columnIdentifiers.visibilityExpression, 
				value);
		}
		
		return this.put(columnIdentifiers.columnFamily, columnIdentifiers.columnQualifier, value);
	}
	
	/**
	 * Add a mutation using a column identifier expression and a value.
	 * @param columnExpression Column Identifier Expression (see below).
	 * @param value Value of the column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String columnExpression, double value){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		if (columnIdentifiers.hasVisibilityExpression()){
			
			return this.put(
				columnIdentifiers.columnFamily, 
				columnIdentifiers.columnQualifier, 
				value,
				columnIdentifiers.visibilityExpression);
		}
		
		return this.put(columnIdentifiers.columnFamily, columnIdentifiers.columnQualifier, value);
	}
	
	/**
	 * Add a mutation using a column identifier expression and a value.
	 * @param columnExpression Column Identifier Expression (see below).
	 * @param value Value of the column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String columnExpression, boolean value){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		if (columnIdentifiers.hasVisibilityExpression()){
			
			return this.put(
				columnIdentifiers.columnFamily, 
				columnIdentifiers.columnQualifier, 
				value,
				columnIdentifiers.visibilityExpression);
		}
		
		return this.put(columnIdentifiers.columnFamily, columnIdentifiers.columnQualifier, value);
	}
	
	/**
	 * Add a mutation using a column identifier expression and a value.
	 * @param columnExpression Column Identifier Expression (see below).
	 * @param value Value of the column.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps put(String columnExpression, byte[] value){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		if (columnIdentifiers.hasVisibilityExpression()){
			
			return this.put(
				columnIdentifiers.columnFamily, 
				columnIdentifiers.columnQualifier, 
				value,
				columnIdentifiers.visibilityExpression);
		}
		
		return this.put(columnIdentifiers.columnFamily, columnIdentifiers.columnQualifier, value);
	}
	
	/* ############## Deletes ########################################################################## */
	
	/**
	 * Delete a cell from the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps delete(String family, String qualifier){
		
		this.mutation.putDelete(family, qualifier);
		
		return this;
	}
	
	/**
	 * Delete a cell from the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps delete(String family, String qualifier, String visibilityExpression){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.putDelete(family, qualifier, columnVisibility);
		
		return this;
	}
	
	/**
	 * Delete a cell from the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param visibilityExpression Accumulo Security/Visibility Expression for the cell.
	 * @param timestamp Timestamp
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps delete(String family, String qualifier, String visibilityExpression, long timestamp){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.putDelete(family, qualifier, columnVisibility, timestamp);
		
		return this;
	}
	
	/**
	 * Delete a cell from the row.
	 * @param columnExpression Column Identifier Expression (see below).
	 * @return Returns the fluent interface.
	 */
	public RowMultiOps delete(String columnExpression){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		if (columnIdentifiers.hasVisibilityExpression()){
			
			return this.delete(
				columnIdentifiers.columnFamily, 
				columnIdentifiers.columnQualifier, 
				columnIdentifiers.visibilityExpression);
		}
		
		return this.delete(columnIdentifiers.columnFamily, columnIdentifiers.columnQualifier);
	}
	
	/* ############## Transitions ###################################################################### */
	
	/**
	 * Finish the mutations on the current row and start mutations on the next row.
	 * @param rowKey Key of the next row.
	 * @return A new RowMultiOps fluent interface.
	 * @throws MutationsRejectedException
	 */
	public RowMultiOps nextRow(String rowKey) throws MutationsRejectedException {
		
		return this.done().withRow(rowKey);
	}
	
	/**
	 * Finish the mutations on this row and return to the MutationOps fluent interface.
	 * @return The MutationOps fluent interface.
	 * @throws MutationsRejectedException
	 */
	public MutationOps done() throws MutationsRejectedException {
		
		this.parent.queueMutations(mutation);
		
		return parent;
	}
	
	/**
	 * Represents a parsed Column Identifier expression.
	 * 
	 * A column identifier is my own construct for representing
	 * the column family, column qualifier, and column visibility.
	 * It looks like this:
	 * 
	 * column.family:column.qualifier:[(public&private)|admin]
	 * 
	 * Note that the "brackets" are optional:
	 * 
	 * column.family:column.qualifier:(public&private)|admin
	 * 
	 * @author Richard Clayton (Berico Technologies)
	 */
	public class ColumnIdentifiers {
		
		public static final String COLUMN_IDENT_SEPARATOR = ":";
		
		public String columnFamily = null;
		public String columnQualifier = null;
		public String visibilityExpression = null;
		
		/**
		 * Parse and set column identifiers.
		 * @param identifierExpression Column Identifier expression.
		 */
		public ColumnIdentifiers(String identifierExpression){
			
			String[] parts = identifierExpression.split(COLUMN_IDENT_SEPARATOR);
			
			this.columnFamily = parts[0];
			this.columnQualifier = parts[1];
			
			if (parts.length > 2){
				
				this.visibilityExpression = stripBrackets(parts[2]);
			}
		}
		
		/**
		 * Does the Column Identifier have a visibility expression.
		 * @return true if it does.
		 */
		public boolean hasVisibilityExpression(){
			
			return this.visibilityExpression != null;
		}
		
		/**
		 * Strip leading and trailing brackets "[]" from the string.
		 * @param expression Expression potentially containing brackets.
		 * @return Leading and trailing brackets removed.
		 */
		String stripBrackets(String expression){
			
			String cleaned = expression;
			
			if (cleaned.startsWith("[")){
				
				cleaned = cleaned.substring(1);
			}
			
			if (cleaned.endsWith("]")){
				
				cleaned = cleaned.substring(0, cleaned.length() - 2);
			}
			
			return cleaned.trim();
		}
	}
}
