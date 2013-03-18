package com.berico.accumulo;

/**
 * Encapsulates the common initialization of a Fluent Extension,
 * as well as, providing some helpful functions to those extensions.
 * 
 * @author Richard Clayton (Berico Technologies)
 */
public abstract class FluentExtension {

	Cirrus cirrus = null;
	
	/**
	 * Initialize with the outer fluent interface.
	 * @param cirrus The outer fluent interface.
	 */
	public FluentExtension(Cirrus cirrus){
		
		this.cirrus = cirrus;
	}
	
	/**
	 * Return the outer fluent interface.
	 * @return Outer fluent interface.
	 */
	public Cirrus and(){
		
		return this.cirrus;
	}
	
	/**
	 * Return the outer fluent interface.
	 * No difference between "and" outside of indicating
	 * the intent of developer.
	 * @return Outer fluent interface.
	 */
	public Cirrus done(){
		
		return this.cirrus;
	}
}
