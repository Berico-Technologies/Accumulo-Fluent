package com.berico.accumulo;

import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

public class App 
{	
    public static void main( String[] args ) 
    		throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException
    {
    	
    	
        p("Hello Accumulo!");
        
        Cirrus cirrus = new Cirrus("titan-test", "root", "password");
        
        cirrus.table("cirrus").delete();
        
        cirrus
        	.table("cirrus")
        	.mutate()
        	.withRow("usa.va.manassas")
        		.column("meta", "commonname").value("Manassas")
        		.column("meta", "last update").value(new Date())
        		.column("meta", "city.id").value(UUID.randomUUID())
        		.column("meta", "population.ants").value(new BigInteger("12345678901234567890"))
        		.column("meta", "zipcode", "SECRET").value(20110)
        		.column("meta:areacode:SECRET").value(703, 12345678)
        		.column("meta:latitude:SECRET").value(38.7514)
        		.column("meta:longitude:SECRET").value(77.4764)
        		.column("meta:state").value("Virginia")
        	.done();
        
        p("Done!");
    }
    
    public static void p(String message){
    	System.out.println(message);
    }
    
    public static void p(String format, Object... objs){
    	System.out.println(String.format(format, objs));
    }
}