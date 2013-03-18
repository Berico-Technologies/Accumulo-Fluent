package com.berico.accumulo;

import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.berico.accumulo.CellValueRetrievalOps.ValueHandler;

public class App 
{	
    public static void main( String[] args ) 
    	throws AccumuloException, AccumuloSecurityException, 
    		   TableNotFoundException, TableExistsException
    {
    	
        Cirrus cirrus = new Cirrus("titan-test", "root", "password");
        
        cirrus.table("us_cities").delete();
        
        long timestamp = 12345678l;
        
        cirrus
        	.table("us_cities")
        	.mutate()
        		// Add a single row by specifying Row ID, Column Family,
        		// Column Qualifier, Value and Timestamps
	        	.put("usa.va.reston", "meta", "commonname", "SECRET").value("Reston", timestamp)
	        	// But you can also use Column Expressions to avoid writing separate strings
	        	// (and obviously the timestamp is optional).
	        	.put("usa.va.fairfax", "meta:commonname:SECRET").value("Fairfax")
	        	// Or you can operate on a single row, adding a number of cells
	        	.withRow("usa.va.manassas")
	        		// Strings!
	        		.column("meta", "commonname").value("Manassas")
	        		// Ints
	        		.column("meta", "zipcode", "SECRET").value(20110)
	        		// Doubles!
	        		.column("meta", "latitude", "SECRET").value(38.7514)
	        		.column("meta", "longitude", "SECRET").value(77.4764)
	        		// Dates!
	        		.column("meta", "last update").value(new Date())
	        		// UUIDs!
	        		.column("meta", "city.id").value(UUID.randomUUID())
	        		// BigInts!
	        		.column("meta", "population.ants").value(new BigInteger("12345678901234567890"))
	        		// And you can add a timestamp with values.
	        		.column("meta", "areacode", "SECRET").value(703, timestamp)
	        		// And take advantage of column expressions.
	        		.column("meta:state").value("Virginia")
	        		// And you can delete cells.
	        		.delete("meta", "some-cell")
	        		// As well as use Column Expressions on Deletes
	        		.delete("meta:some-other-cell")
	        		// All mutations are queued, executed only when done() is called.
	        		.endRow()
	        .done();
        
        int zipcode = cirrus.table("us_cities").scan("SECRET").cell("usa.va.manassas", "meta", "zipcode").asInt();
        
        p("Found zipcode: %s", zipcode);
        
        final StringBuffer combinedList = new StringBuffer();
        
        cirrus.table("us_cities").scan("SECRET")
        	.cell("usa.va.manassas", "meta:commonname").asString(
        	  new ValueHandler<String>(){
				@Override
				public void handle(String value) {
					
					combinedList.append(value).append(",");
				}
        	})
        	.cell("usa.va.reston", "meta:commonname").asString(
        	  new ValueHandler<String>(){
				@Override
				public void handle(String value) {
					
					combinedList.append(value).append(",");
				}
        	})
        .done();
        
        p("Combined List: %s", combinedList.toString());
       
        
        Iterator<CellValueRetrievalOps> i = cirrus.table("us_cities").scan("SECRET").row("usa.va.manassas").iterator();
        
        while(i.hasNext()){
        	
        	CellValueRetrievalOps cell = i.next();
        	
        	p("%s => %s", cell.key().toString(), cell.asString());
        }
        
        p("Done!");
    }
    
    public static void p(String message){
    	System.out.println(message);
    }
    
    public static void p(String format, Object... objs){
    	System.out.println(String.format(format, objs));
    }
}