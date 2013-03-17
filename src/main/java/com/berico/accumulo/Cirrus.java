package com.berico.accumulo;

import java.util.Arrays;
import java.util.Collection;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;

/**
 * Cirrus - A Fluent API for Apache Accumulo.
 * 
 * Why Cirrus?  Cirrus is the highest type of cloud that forms in the sky,
 * a fitting name for a high-level abstraction to the "Accumulo API".
 * 
 * @author Richard Clayton (Berico Technologies)
 */
public class Cirrus {
	
	/**
	 * Default set of ZooKeeper Instances to use.
	 */
	public static Collection<String> DEFAULT_ZOOKEEPERS = Arrays.asList(new String[]{ "localhost" });
	
	ZooKeeperInstance zookeeper = null;
	
	Connector connector = null;
	
	/**
	 * Instantiate the Accumulo Fluent API.  Uses the default ZooKeeper instances
	 * set on the DEFAULT_ZOOKEEPERS field of this class.  
	 * Please use this only in development unless you override the default value.
	 * 
	 * @param instanceName Name of the Accumulo instance.
	 * @param username User that will interact with Accumulo.
	 * @param password Password of the user that will interact with Accumulo.
	 * @throws AccumuloException Thrown if Accumulo cannot connect.
	 * @throws AccumuloSecurityException Thrown if the user does not have permission to connect.
	 */
	public Cirrus(String instanceName, String username, String password)
		throws AccumuloException, AccumuloSecurityException {
		
		this(instanceName, username, password, DEFAULT_ZOOKEEPERS.toArray(new String[]{}));
	}
	
	/**
	 * Instantiate the Accumulo Fluent API.
	 * @param instanceName Name of the Accumulo instance.
	 * @param username User that will interact with Accumulo.
	 * @param password Password of the user that will interact with Accumulo.
	 * @param zooKeepers A set of ZooKeeper servers.
	 * @throws AccumuloException Thrown if Accumulo cannot connect.
	 * @throws AccumuloSecurityException Thrown if the user does not have permission to connect.
	 */
	public Cirrus(String instanceName, String username, String password, String... zooKeepers) 
		throws AccumuloException, AccumuloSecurityException {
		
		this.connect(instanceName, username, password, zooKeepers);
	}
	
	/**
	 * Create a connection to the Accumulo instance.
	 * @param instanceName Name of the Accumulo instance.
	 * @param username User that will interact with Accumulo.
	 * @param password Password of the user that will interact with Accumulo.
	 * @param zooKeepers A set of ZooKeeper servers.
	 * @throws AccumuloException Thrown if Accumulo cannot connect.
	 * @throws AccumuloSecurityException Thrown if the user does not have permission to connect.
	 */
	void connect(String instanceName, String username, String password, String[] zooKeepers)  
		throws AccumuloException, AccumuloSecurityException {
		
		this.zookeeper = new ZooKeeperInstance(instanceName, flatten(zooKeepers, ","));
		
		this.connector = this.zookeeper.getConnector(username, password);
	}
	
	
	/* ############## Table Actions ##################################################################### */
	
	/**
	 * Create a table or grab a handle on an existing table (exposing a number of Table operations).
	 * @param tableName Name of the table
	 * @return A set of operations for the table.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 */
	public TableOps table(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException{
		
		return new TableOps(tableName, this);
	}
	
	/**
	 * Create a table or grab a handle on an existing table (exposing a number of Table operations).
	 * Identical to "table()", but probably more understandable when you know you are creating a new table.
	 * @param tableName Name of the table
	 * @return A set of operations for the table.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 */
	public TableOps createTable(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException{
		
		return table(tableName);
	}
	
	/**
	 * Get all tables visible to the user.
	 * @return Collection of table names.
	 */
	public Collection<String> tables() {
		
		return this.connector.tableOperations().list();
	}
	
	/* ############## Mutations ########################################################################## */
	
	public MutationOps mutate(String table) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException{
		
		return new MutationOps(table, this);
	}
	
	/* ############## Helpers ############################################################################ */
	
	/**
	 * Set the static list of Default ZooKeeper servers.  You can also set via field, but this is here
	 * to ease Spring (and other IOC) configuration.
	 * @param zookeepers A collection of default ZooKeeper instances.
	 */
	public void setDefaultZookeepers(Collection<String> zookeepers){
		DEFAULT_ZOOKEEPERS = zookeepers;
	}
	
	/* ############## Utils ############################################################################## */
	
	/**
	 * Turn an array of strings into a comma-separated String
	 * @param strings Strings to join
	 * @param separator the separator to use.
	 * @return comma-separated String representing the joined String array items.
	 */
	private static String flatten(String[] strings, String separator){
		
		StringBuilder sb = new StringBuilder();
		
		for (String string : strings){
			
			sb.append(string).append(separator);
		}
		
		// Remove the trailing separator.
		sb.delete(sb.length() - separator.length(), sb.length() - 1);
		
		return sb.toString();
	}
}
