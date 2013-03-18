package com.berico.accumulo;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.berico.accumulo.ValueMutationOps.CompletionHandler;

/**
 * A fluent interface for executing multiple mutations on the same row.
 * 
 * @author Richard Clayton (Berico Technologies)
 */
public class RowMutationOps {

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
	public RowMutationOps(String rowKey, MutationOps parent){
		
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
	public ValueMutationOps<RowMutationOps> column(String family, String qualifier) throws MutationsRejectedException {
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(family, qualifier);
		
		return new ValueMutationOps<RowMutationOps>(columnIdentifiers, this.mutation, this, new NoOpCompletionHandler());
	}
	
	/**
	 * Set the column information, returning the ValueOps fluent interface (to set the value).
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @param visibility Column visibility expression.
	 * @return ValueOps interface.
	 */
	public ValueMutationOps<RowMutationOps> column(String family, String qualifier, String visibility)  throws MutationsRejectedException {
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(family, qualifier, visibility);
		
		return new ValueMutationOps<RowMutationOps>(columnIdentifiers, this.mutation, this, new NoOpCompletionHandler());
	}
	
	/**
	 * Set the column information, returning the ValueOps fluent interface (to set the value).
	 * @param columnExpression Condensed expression for representing column family, qualifier and visibility.
	 * @return ValueOps interface.
	 */
	public ValueMutationOps<RowMutationOps> column(String columnExpression) throws MutationsRejectedException {
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		return new ValueMutationOps<RowMutationOps>(columnIdentifiers, this.mutation, this, new NoOpCompletionHandler());
		
	}
	
	/* ############## CompletionHandler ################################################################ */
	
	/**
	 * Does nothing since the mutation is processed when the fluent interface is done().
	 * 
	 * @author Richard Clayton (Berico Technologies)
	 */
	public class NoOpCompletionHandler implements CompletionHandler {
		
		@Override
		public void complete(Mutation mutation) throws MutationsRejectedException {}
	}
	
	/* ############## Deletes ########################################################################## */
	
	/**
	 * Delete a cell from the row.
	 * @param family Column Family.
	 * @param qualifier Column Qualifier.
	 * @return Returns the fluent interface.
	 */
	public RowMutationOps delete(String family, String qualifier){
		
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
	public RowMutationOps delete(String family, String qualifier, String visibilityExpression){
		
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
	public RowMutationOps delete(String family, String qualifier, String visibilityExpression, long timestamp){
		
		ColumnVisibility columnVisibility = new ColumnVisibility(visibilityExpression);
		
		this.mutation.putDelete(family, qualifier, columnVisibility, timestamp);
		
		return this;
	}
	
	/**
	 * Delete a cell from the row.
	 * @param columnExpression Column Identifier Expression (see below).
	 * @return Returns the fluent interface.
	 */
	public RowMutationOps delete(String columnExpression){
		
		ColumnIdentifiers columnIdentifiers = new ColumnIdentifiers(columnExpression);
		
		if (columnIdentifiers.hasVisibilityExpression()){
			
			return this.delete(
				columnIdentifiers.getColumnFamily(), 
				columnIdentifiers.getColumnQualifier(), 
				columnIdentifiers.getVisibilityExpression());
		}
		
		return this.delete(columnIdentifiers.getColumnFamily(), columnIdentifiers.getColumnQualifier());
	}
	
	/**
	 * Delete all column qualifiers in the specified column family for the row in scope.
	 * @param columnFamily Column Family.
	 * @param columnQualifiers Each Qualifier to Delete.
	 * @return Return the fluent interface.
	 */
	public RowMutationOps deleteMany(String columnFamily, String... columnQualifiers){
		
		return this.deleteMany(columnFamily, null, 0, columnQualifiers);
	}
	
	/**
	 * Delete all column qualifiers in the specified column family for the row in scope.
	 * @param columnFamily Column Family.
	 * @param visibility Column Visibility.
	 * @param columnQualifiers Each Qualifier to Delete.
	 * @return Return the fluent interface.
	 */
	public RowMutationOps deleteMany(String columnFamily, String visibility, String... columnQualifiers){
			
		return this.deleteMany(columnFamily, visibility, 0, columnQualifiers);
	}
	
	/**
	 * Delete all column qualifiers in the specified column family for the row in scope.
	 * @param columnFamily Column Family.
	 * @param timestamp Specified time of the Deletion.
	 * @param columnQualifiers Each Qualifier to Delete.
	 * @return Return the fluent interface.
	 */
	public RowMutationOps deleteMany(String columnFamily, long timestamp, String... columnQualifiers){
		
		return this.deleteMany(columnFamily, null, timestamp, columnQualifiers);
	}
	
	/**
	 * Delete all column qualifiers in the specified column family for the row in scope.
	 * @param columnFamily Column Family.
	 * @param visibility Column Visibility.
	 * @param timestamp Specified time of the Deletion.
	 * @param columnQualifiers Each Qualifier to Delete.
	 * @return Return the fluent interface.
	 */
	public RowMutationOps deleteMany(String columnFamily, String visibility, long timestamp, String... columnQualifiers){
		
		Text cf = new Text(columnFamily);
		
		// This is gross, but more efficient than trying to do something fancy with reflection or
		// creating some handler interface to consolidate the loops.
		if (visibility != null && timestamp != 0){
			
			ColumnVisibility cv = new ColumnVisibility(visibility);
			
			for (String columnQualifier : columnQualifiers){
				
				this.mutation.putDelete(cf, new Text(columnQualifier), cv, timestamp);
			}
		}
		else if (visibility != null){
			
			ColumnVisibility cv = new ColumnVisibility(visibility);
			
			for (String columnQualifier : columnQualifiers){
				
				this.mutation.putDelete(cf, new Text(columnQualifier), cv);
			}
		}
		else if (timestamp != 0){
			
			for (String columnQualifier : columnQualifiers){
				
				this.mutation.putDelete(cf, new Text(columnQualifier), timestamp);
			}
		}
		else {
			
			for (String columnQualifier : columnQualifiers){
				
				this.mutation.putDelete(cf, new Text(columnQualifier));
			}
		}
		
		return this;
	}
	
	/* ############## Transitions ###################################################################### */
	
	/**
	 * Finish the mutations on the current row and start mutations on the next row.
	 * @param rowKey Key of the next row.
	 * @return A new RowMultiOps fluent interface.
	 * @throws MutationsRejectedException
	 */
	public RowMutationOps nextRow(String rowKey) throws MutationsRejectedException {
		
		return this.endRow().withRow(rowKey);
	}
	
	/**
	 * Finish the mutations on this row and return to the MutationOps fluent interface.
	 * @return The MutationOps fluent interface.
	 * @throws MutationsRejectedException
	 */
	public MutationOps endRow() throws MutationsRejectedException {
		
		this.parent.queueMutations(mutation);
		
		return parent;
	}
}
