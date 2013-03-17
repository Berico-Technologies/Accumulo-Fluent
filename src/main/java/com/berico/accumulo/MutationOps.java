package com.berico.accumulo;

import java.util.Arrays;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

import com.berico.accumulo.ValueOps.CompletionHandler;

/**
 * A collection of fluent Mutation Operations for a table.
 * 
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

	/**
	 * Perform multiple operations on the same row, queuing the mutations until the done() function
	 * is called.
	 * @param rowKey ID of the Row being operated on.
	 * @return A set of Row related operations.
	 */
	public RowMultiOps withRow(String rowKey){
		
		return new RowMultiOps(rowKey, this);
	}
	
	/**
	 * Perform an atomic creation/insertion mutation.
	 * @param rowKey Row Key.
	 * @param columnFamily Column Family
	 * @param columnQualifer Column Qualifier
	 * @return Fluent interface to set the value and timestamp.
	 */
	public ValueOps<MutationOps> put(String rowKey, String columnFamily, String columnQualifer){
		
		return put(rowKey, new ColumnIdentifiers(columnFamily, columnQualifer));
	}
	
	/**
	 * Perform an atomic creation/insertion mutation.
	 * @param rowKey Row Key.
	 * @param columnFamily Column Family
	 * @param columnQualifer Column Qualifier
	 * @param visibilityExpression Column Visibility Expression
	 * @return Fluent interface to set the value and timestamp.
	 */
	public ValueOps<MutationOps> put(String rowKey, String columnFamily, String columnQualifer, String visibilityExpression){
		
		return put(rowKey, new ColumnIdentifiers(columnFamily, columnQualifer, visibilityExpression));
	}
	
	/**
	 * Perform an atomic creation/insertion mutation.
	 * @param rowKey Row Key.
	 * @param columnExpression Column Expression.
	 * @return Fluent interface to set the value and timestamp.
	 */
	public ValueOps<MutationOps> put(String rowKey, String columnExpression){
		
		return put(rowKey, new ColumnIdentifiers(columnExpression));
	}
	
	/**
	 * Forms the mutation and completion handler and passes control to the ValueOps fluent interface.
	 * @param rowKey Row Key
	 * @param columnIdentifiers Column Identifiers
	 * @return Fluent interface for setting the value and timestamp.
	 */
	ValueOps<MutationOps> put(String rowKey, ColumnIdentifiers columnIdentifiers) {
		
		Mutation mutation = new Mutation(rowKey);
		
		return new ValueOps<MutationOps>(columnIdentifiers, mutation, this, new CompletionHandler(){
			@Override
			public void complete(Mutation mutation)  throws MutationsRejectedException {
				
				writer.addMutation(mutation);
			}
		});
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
