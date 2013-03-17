package com.berico.accumulo;

/**
 * Represents the pieces of information required in specifying
 * a cell's location in Accumulo (minus the Row ID).
 * 
 * There's also support for an expression, which is my own construct 
 * for representing the column family, column qualifier, and 
 * column visibility as a String.
 * 
 * It looks like this:
 * 
 * column.family:column.qualifier:[(public&private)|admin]
 * 
 * Note that the "brackets" are optional:
 * 
 * column.family:column.qualifier:(public&private)|admin
 * 
 * @author Richard Clayton (Berico Technologies)
 */
public class ColumnIdentifiers {
	
	public static final String COLUMN_IDENT_SEPARATOR = ":";
	
	public String columnFamily = null;
	public String columnQualifier = null;
	public String visibilityExpression = null;
	
	/**
	 * Parse and set column identifiers.
	 * @param identifierExpression Column Identifier expression.
	 */
	public ColumnIdentifiers(String identifierExpression){
		
		String[] parts = identifierExpression.split(COLUMN_IDENT_SEPARATOR);
		
		this.columnFamily = parts[0];
		this.columnQualifier = parts[1];
		
		if (parts.length > 2){
			
			this.visibilityExpression = stripBrackets(parts[2]);
		}
	}
	
	/**
	 * Initialize with the Column Information
	 * @param columnFamily Column Family
	 * @param columnQualifier Column Qualifier
	 */
	public ColumnIdentifiers(String columnFamily, String columnQualifier){
		
		this.columnFamily = columnFamily;
		this.columnQualifier = columnQualifier;
	}
	
	/**
	 * Initialize with the Column Information
	 * @param columnFamily Column Family
	 * @param columnQualifier Column Qualifier
	 * @param visibilityExpression Visibility Expression
	 */
	public ColumnIdentifiers(String columnFamily, String columnQualifier, String visibilityExpression){
		
		this.columnFamily = columnFamily;
		this.columnQualifier = columnQualifier;
		this.visibilityExpression = visibilityExpression;
	}
	
	/**
	 * Does the Column Identifier have a visibility expression.
	 * @return true if it does.
	 */
	public boolean hasVisibilityExpression(){
		
		return this.visibilityExpression != null;
	}

	/**
	 * Get the name of the Column Family
	 * @return Column Family name
	 */
	public String getColumnFamily() {
		return columnFamily;
	}

	/**
	 * Get the name of the Column
	 * @return Column name
	 */
	public String getColumnQualifier() {
		return columnQualifier;
	}

	/**
	 * Get the visibility field expression
	 * @return Visibility expression
	 */
	public String getVisibilityExpression() {
		return visibilityExpression;
	}

	/**
	 * Strip leading and trailing brackets "[]" from the string.
	 * @param expression Expression potentially containing brackets.
	 * @return Leading and trailing brackets removed.
	 */
	static String stripBrackets(String expression){
		
		String cleaned = expression.trim();
		
		if (cleaned.startsWith("[") && cleaned.endsWith("]")){
			
			cleaned = cleaned.substring(1);
			cleaned = cleaned.substring(0, cleaned.length() - 1);
		}
		
		return cleaned.trim();
	}
}