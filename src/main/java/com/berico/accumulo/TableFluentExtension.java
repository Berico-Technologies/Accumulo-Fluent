package com.berico.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;

/**
 * Encapsulates the common initialization of FluentExtensions scoped
 * to a single table.
 * 
 * @author Richard Clayton (Berico Technologies)
 */
public abstract class TableFluentExtension extends FluentExtension {

	String tableName = null;
	
	TableOperations table = null;
	
	/**
	 * Initialize the extension with the table name and the outer fluent interface.
	 * 
	 * This extension will create the table if it does not exist!
	 * 
	 * @param tableName Name of the table that is in scope of the operations.
	 * @param cirrus Outer fluent interface.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 */
	public TableFluentExtension(String tableName, Cirrus cirrus) throws AccumuloException, AccumuloSecurityException, TableExistsException {
		
		super(cirrus);
		
		this.tableName = tableName;
		
		this.table = cirrus.connector.tableOperations();
		
		if (!this.table.exists(tableName)){
			
			this.table.create(this.tableName);
		}
	}

}
