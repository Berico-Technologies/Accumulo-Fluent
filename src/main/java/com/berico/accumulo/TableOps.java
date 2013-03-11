package com.berico.accumulo;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.io.Text;

/**
 * A collection of fluent Table Operations.
 * @author Richard Clayton (Berico Technologies)
 */
public class TableOps extends TableFluentExtension {

	private static final String TABLE_MAX_VERSIONS = "table.iterator.majc.vers.opt.maxVersions";
	
	/**
	 * Instantiate the class passing the name of the target table and the base
	 * fluent interface.
	 * @param tableName Name of the table.
	 * @param cirrus The outer fluent interface.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 */
	public TableOps(String tableName, Cirrus cirrus) 
		throws AccumuloException, AccumuloSecurityException, TableExistsException{
		
		super(tableName, cirrus);
	}
	
	/**
	 * Delete the table in scope.
	 * @return Returns the fluent interface.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public TableOps delete() 
		throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		
		this.table.delete(this.tableName);
		
		return this;
	}
	
	/**
	 * Remove the rows starting and ending with the provide keys.
	 * @param startingKey Starting Key
	 * @param endingKey Ending Key
	 * @return Returns the fluent interface.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public TableOps removeRows(String startingKey, String endingKey) 
		throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		
		Text start = new Text(startingKey);
		Text end = new Text(endingKey);
		
		this.table.deleteRows(this.tableName, start, end);
		
		return this;
	}
	
	/**
	 * Rename the table.
	 * @param newName New name of the table.
	 * @return Returns the fluent interface.
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 * @throws AccumuloException
	 * @throws TableExistsException
	 */
	public TableOps rename(String newName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException, TableExistsException{
		
		this.table.rename(this.tableName, newName);
		
		this.tableName = newName;
		
		return this;
	}
	
	/**
	 * Does the table in scope exist?
	 * @return True if it does exist.
	 */
	public boolean exists(){
		
		return this.table.exists(this.tableName);
	}
	
	/**
	 * Expand into the Mutation interface for performing CRUD operations on the table in scope.
	 * @return the Mutation Fluent Interface.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 * @throws TableNotFoundException 
	 */
	public MutationOps mutate() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException{
		
		return new MutationOps(this.tableName, this.cirrus);
	}
	
	/**
	 * Set the maximum number of versions to keep for each column.
	 * @param maxNumberOfVersions Maximum number of versions.
	 * @return Returns the fluent interface.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public TableOps maxVersions(int maxNumberOfVersions) throws AccumuloException, AccumuloSecurityException{
		
		this.table.setProperty(this.tableName, TABLE_MAX_VERSIONS, Integer.toString(maxNumberOfVersions));
		
		return this;
	}
	
	/**
	 * Set a table property.
	 * @param key A string representing the key of a configuration setting.
	 * @param value A string representing the value of the configuration setting.
	 * @return Returns the fluent interface.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public TableOps setProperty(String key, String value) throws AccumuloException, AccumuloSecurityException{
		
		this.table.setProperty(this.tableName, key, value);
		
		return this;
	}
	
	/**
	 * Get the properties on the table in scope.
	 * @return An iterable of String key, String value (table properties).
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 */
	public Iterable<Entry<String, String>>  properties() throws AccumuloException, TableNotFoundException{
		
		return this.table.getProperties(this.tableName);
	}
	
	/**
	 * Take the Table offline.
	 * @return Returns the fluent interface.
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 */
	public TableOps takeOffline() throws AccumuloSecurityException, AccumuloException, TableNotFoundException{
	
		this.table.offline(this.tableName);
		
		return this;
	}
	
	/**
	 * Bring the Table back online.
	 * @return Returns the fluent interface.
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 */
	public TableOps bringOnline() throws AccumuloSecurityException, AccumuloException, TableNotFoundException{
		
		this.table.online(this.tableName);
		
		return this;
	}
}
