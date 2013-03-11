package com.berico.accumulo;

import java.util.Arrays;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

/**
 * A collection of fluent Mutation Operations for a table.
 * @author Richard Clayton (Berico Technologies)
 */
public class MutationOps extends TableFluentExtension {

	/**
	 * Bytes to store before sending a batch.
	 */
	public static long WRITER_MEMORY_BUFFER = 1000000L;
	
	/**
	 * Milliseconds to wait before sending.
	 */
    public static long WRITER_TIMEOUT = 0L;
    
    /**
     * Number of threads to use.
     */
    public static int WRITER_NUMBER_OF_THREADS = 10;
    
    /**
     * The underlying writer instance the fluent interface uses
     * to mutate the table in scope.
     */
    BatchWriter writer = null;
	
	/**
	 * Initialize the extension with table name and outer fluent interface.
	 * @param tableName Name of the table in scope.
	 * @param cirrus The outer fluent interface.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 * @throws TableNotFoundException 
	 */
	public MutationOps(String tableName, Cirrus cirrus)
		throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		
		super(tableName, cirrus);
		
		this.writer = this.cirrus.connector.createBatchWriter(
				this.tableName, WRITER_MEMORY_BUFFER, WRITER_TIMEOUT, WRITER_NUMBER_OF_THREADS);
		
		registerShutdownHook();
	}

	public RowMultiOps withRow(String rowKey){
		
		return new RowMultiOps(rowKey, this);
	}
	
	public MutationOps put(String rowKey, String columnFamily, String columnQualifier, String value) throws MutationsRejectedException{
		
		Mutation mutation = new Mutation(rowKey);
		
		mutation.put(columnFamily, columnQualifier, value);
		
		writer.addMutation(mutation);
		
		return this;
	}
	
	
	
	
	/**
	 * Set the bytes to store before sending a batch.
	 * 
	 * !!!! This sets the global value for the FluentExtension. !!!!
	 * 
	 * @param bufferSizeBytes Buffer size in bytes
	 */
	public void setWriterMemoryBuffer(long bufferSizeBytes){
		
		WRITER_MEMORY_BUFFER = bufferSizeBytes;
	}
	
	/**
	 * Set the milliseconds to wait before sending a batch.
	 *  
	 * !!!! This sets the global value for the FluentExtension. !!!!
	 * 
	 * @param timeout Timeout in milliseconds
	 */
	public void setWriterTimeout(long timeout){
	
		WRITER_TIMEOUT = timeout;
	}
	
	/**
	 * Set the number of threads to use for the writer.
	 * 
	 * !!!! This sets the global value for the FluentExtension. !!!!
	 * 
	 * @param numberOfThreads Number of threads to use
	 */
	public void setWriterNumberOfThreads(int numberOfThreads){
		
		WRITER_NUMBER_OF_THREADS = numberOfThreads;
	}
	
	/**
	 * A convenience method for child fluents to queue mutations with the encapsulated
	 * BatchWriter.
	 * @param mutations Mutations to be queued.
	 * @throws MutationsRejectedException
	 */
	void queueMutations(Mutation... mutations) throws MutationsRejectedException{
		
		writer.addMutations(Arrays.asList(mutations));
	}
	
	/**
	 * This will be called when the Application terminates.  In this case, any remaining
	 * mutations will be flushed when the writer closes.
	 */
	void registerShutdownHook(){
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			
			@Override
			public void run(){
				
				System.out.println("Closing the writer.");
				
				try {
					
					writer.close();
					
				} catch (MutationsRejectedException e) {
					
					e.printStackTrace();
				}
			}
		});
	}
}
