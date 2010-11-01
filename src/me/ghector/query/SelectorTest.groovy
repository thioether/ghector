package me.ghector.query
;

import java.util.regex.Matcher;

import groovy.util.GroovyTestCase;

class SelectorTest extends GroovyTestCase{

	void testIdentifySetRequest() {
		assert Selector.parse("cf/key/col", SelectorType.SET) == "set_slice_col";
	}
		
	void testIdentifyGetRequest() {
		assert Selector.parse("cf/key/col", SelectorType.GET) == "get_col";
		
		assert Selector.parse("cf/key/[col1,col2]", SelectorType.GET) == "get_slice_col"; // key slice		
		assert Selector.parse("cf/key/[col1-col2]", SelectorType.GET) == "get_slice_col";	// col range 
		assert Selector.parse("cf/key/[col1-]", SelectorType.GET) == "get_slice_col"; // start to end 
		assert Selector.parse("cf/key/[*]", SelectorType.GET) ==  "get_slice_col"; // all cols
		
		//assert Selector.parse("cf/[key1-key2]",SelectorType.GET) == "get_range_key"; // key range
		
		assert Selector.parse("cf/[key1-key2]/[col1-col2]",SelectorType.GET) == "get_range_slice"; // range
	}
	
/*	
	void testIdentifySuperColumnRequest() {
		assert Selector.parse("cf/key/sc/col", SelectorType.GET) == "get_scol_col";
		assert Selector.parse("cf/key/sc/*", SelectorType.GET) ==  "get_scol_col_all";
		assert Selector.parse("cf/key1,key2/sc/",SelectorType.GET) == "get_key_range";
	}
*/
	
}
