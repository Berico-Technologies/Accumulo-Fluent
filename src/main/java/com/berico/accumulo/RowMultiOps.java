package com.berico.accumulo;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

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
	 * Set the column information, returning the ValueOps fluent interface (to set the value).
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @return ValueOps interface.
	 */
	public ValueOps column(String family, String qualifier){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(family, qualifier);
		
		return new ValueOps(columnIdentifiers, this);
	}
	
	/**
	 * Set the column information, returning the ValueOps fluent interface (to set the value).
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param visibility Column visibility expression.
	 * @return ValueOps interface.
	 */
	public ValueOps column(String family, String qualifier, String visibility){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(family, qualifier, visibility);
		
		return new ValueOps(columnIdentifiers, this);
	}
	
	/**
	 * Set the column information, returning the ValueOps fluent interface (to set the value).
	 * @param columnExpression Condensed expression for representing column family, qualifier and visibility.
	 * @return ValueOps interface.
	 */
	public ValueOps column(String columnExpression){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		return new ValueOps(columnIdentifiers, this);
		
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
				columnIdentifiers.getColumnFamily(), 
				columnIdentifiers.getColumnQualifier(), 
				columnIdentifiers.getVisibilityExpression());
		}
		
		return this.delete(columnIdentifiers.getColumnFamily(), columnIdentifiers.getColumnQualifier());
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
}
