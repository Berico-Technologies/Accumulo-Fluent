package com.berico.accumulo;

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
        	.put("meta:zipcode:public", 20110)
        	.put("meta:areacode:public", 703)
        	.put("meta:latitude:public", 38.7514)
        	.put("meta:longitude:public", 77.4764)
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