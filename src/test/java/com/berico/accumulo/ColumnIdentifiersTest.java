package com.berico.accumulo;

import static org.junit.Assert.*;

import org.junit.Test;

public class ColumnIdentifiersTest {
	
	@Test
	public void correcly_parses_column_family_from_expression(){
		
		ColumnIdentifiers columnIdentifiersSimple = new ColumnIdentifiers("meta:zipcode:public");
		
		assertEquals("meta", columnIdentifiersSimple.getColumnFamily());
		
		ColumnIdentifiers columnWithoutVisibility = new ColumnIdentifiers("meta:zipcode");
		
		assertEquals("meta", columnWithoutVisibility.getColumnFamily());
		
		ColumnIdentifiers columnWithComplexVisibility = new ColumnIdentifiers("meta:zipcode:(public|protected)&admin");
		
		assertEquals("meta", columnWithComplexVisibility.getColumnFamily());
		
		ColumnIdentifiers columnWithCamelCasedFamilyName = new ColumnIdentifiers("CamelCasedFamilyName:zipcode:public");
		
		assertEquals("CamelCasedFamilyName", columnWithCamelCasedFamilyName.getColumnFamily());
		
		ColumnIdentifiers columnWithDashedFamilyName = new ColumnIdentifiers("Dashed-Family-Name:zipcode:public");
		
		assertEquals("Dashed-Family-Name", columnWithDashedFamilyName.getColumnFamily());
		
		ColumnIdentifiers columnWithDottedFamilyName = new ColumnIdentifiers("Dotted.Family.Name:zipcode:public");
		
		assertEquals("Dotted.Family.Name", columnWithDottedFamilyName.getColumnFamily());
		
		ColumnIdentifiers columnWithSpacedFamilyName = new ColumnIdentifiers("Spaced Family Name:zipcode:public");
		
		assertEquals("Spaced Family Name", columnWithSpacedFamilyName.getColumnFamily());
	}
	
	@Test
	public void correcly_parses_column_qualifier_from_expression(){
		
		ColumnIdentifiers columnIdentifiersSimple = new ColumnIdentifiers("meta:zipcode:public");
		
		assertEquals("zipcode", columnIdentifiersSimple.getColumnQualifier());
		
		ColumnIdentifiers columnWithoutVisibility = new ColumnIdentifiers("meta:zipcode");
		
		assertEquals("zipcode", columnWithoutVisibility.getColumnQualifier());
		
		ColumnIdentifiers columnWithComplexVisibility = new ColumnIdentifiers("meta:zipcode:(public|protected)&admin");
		
		assertEquals("zipcode", columnWithComplexVisibility.getColumnQualifier());
		
		ColumnIdentifiers columnWithCamelCasedQualifierName = new ColumnIdentifiers("meta:CamelCasedQualifierName:public");
		
		assertEquals("CamelCasedQualifierName", columnWithCamelCasedQualifierName.getColumnQualifier());
		
		ColumnIdentifiers columnWithDashedQualifierName = new ColumnIdentifiers("meta:Dashed-Qualifier-Name:public");
		
		assertEquals("Dashed-Qualifier-Name", columnWithDashedQualifierName.getColumnQualifier());
		
		ColumnIdentifiers columnWithDottedQualifierName = new ColumnIdentifiers("meta:Dotted.Qualifier.Name:public");
		
		assertEquals("Dotted.Qualifier.Name", columnWithDottedQualifierName.getColumnQualifier());
		
		ColumnIdentifiers columnWithSpacedQualifierName = new ColumnIdentifiers("meta:Spaced Qualifier Name:public");
		
		assertEquals("Spaced Qualifier Name", columnWithSpacedQualifierName.getColumnQualifier());
	}

	@Test
	public void correcly_parses_column_visibility_from_expression(){
		
		ColumnIdentifiers columnIdentifiersWithoutVisibilityBrackets = new ColumnIdentifiers("meta:zipcode:public");
		
		assertEquals("public", columnIdentifiersWithoutVisibilityBrackets.getVisibilityExpression());
		
		ColumnIdentifiers columnIdentifiersWithVisibilityBrackets = new ColumnIdentifiers("meta:zipcode:[public]");
		
		assertEquals("public", columnIdentifiersWithVisibilityBrackets.getVisibilityExpression());
		
		ColumnIdentifiers columnIdentifiersWithComplexExpressionWithoutBrackets = new ColumnIdentifiers("meta:zipcode:(public|protected)&admin");
		
		assertEquals("(public|protected)&admin", columnIdentifiersWithComplexExpressionWithoutBrackets.getVisibilityExpression());
		
		ColumnIdentifiers columnIdentifiersWithComplexExpressionWithBrackets = new ColumnIdentifiers("meta:zipcode:[(public|protected)&admin]");
		
		assertEquals("(public|protected)&admin", columnIdentifiersWithComplexExpressionWithBrackets.getVisibilityExpression());
		
		ColumnIdentifiers columnIdentifiersWithoutVisibilityField = new ColumnIdentifiers("meta:zipcode");
		
		assertNull(columnIdentifiersWithoutVisibilityField.getVisibilityExpression());
	}
	
	@Test
	public void correctly_expresses_existence_of_visibility_expression_when_present() {
		
		ColumnIdentifiers columnIdentifiersWithoutVisibilityBrackets = new ColumnIdentifiers("meta:zipcode:public");
		
		assertTrue(columnIdentifiersWithoutVisibilityBrackets.hasVisibilityExpression());
		
		ColumnIdentifiers columnIdentifiersWithVisibilityBrackets = new ColumnIdentifiers("meta:zipcode:[public]");
		
		assertTrue(columnIdentifiersWithVisibilityBrackets.hasVisibilityExpression());
		
		ColumnIdentifiers columnIdentifiersWithComplexExpressionWithoutBrackets = new ColumnIdentifiers("meta:zipcode:(public|protected)&admin");
		
		assertTrue(columnIdentifiersWithComplexExpressionWithoutBrackets.hasVisibilityExpression());
		
		ColumnIdentifiers columnIdentifiersWithComplexExpressionWithBrackets = new ColumnIdentifiers("meta:zipcode:[(public|protected)&admin]");
		
		assertTrue(columnIdentifiersWithComplexExpressionWithBrackets.hasVisibilityExpression());
		
		ColumnIdentifiers columnIdentifiersWithoutVisibilityField = new ColumnIdentifiers("meta:zipcode");
		
		assertFalse(columnIdentifiersWithoutVisibilityField.hasVisibilityExpression());
	}

	@Test
	public void stripBrackets_removes_leading_and_trailing_brackets_from_string(){
		
		String actualSimpleCase = ColumnIdentifiers.stripBrackets("[public]");
		
		assertEquals("public", actualSimpleCase);
		
		String actualComplexCase = ColumnIdentifiers.stripBrackets("[(public|protected)&admin]");
		
		assertEquals("(public|protected)&admin", actualComplexCase);
	}
	
	@Test
	public void stripBrackets_does_not_remove_brackets_found_in_middle_of_string(){
		
		String actualBracketsInMiddle = ColumnIdentifiers.stripBrackets("[public&[private|protected]]");
		
		assertEquals("public&[private|protected]", actualBracketsInMiddle);
	}
	
	@Test
	public void stripBrackets_does_not_remove_brackets_if_expression_leads_but_does_not_trail_with_brackets(){
		
		String actualLeadsButNotTrailsWithBrackets = ColumnIdentifiers.stripBrackets("[private|protected]&public");
		
		assertEquals("[private|protected]&public", actualLeadsButNotTrailsWithBrackets);
	}
	
	@Test
	public void stripBrackets_does_not_remove_brackets_if_expression_trails_but_does_not_lead_with_brackets(){
		
		String actualTrailsButNotLeadsWithBrackets = ColumnIdentifiers.stripBrackets("public&[private|protected]");
		
		assertEquals("public&[private|protected]", actualTrailsButNotLeadsWithBrackets);
	}
	
}
