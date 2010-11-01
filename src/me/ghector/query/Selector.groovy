package me.ghector.query

import groovy.lang.Closure;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Selector {
	
	def static final SET_REGEX = [
			set_slice_col: ~/(\w+)\/(\w+)\/(\w+)/
		];
	
	def static final GET_REGEX = [
		get_col: ~/(\w+)\/(\w+)\/(\w+)/, //  column get
		get_slice_col: ~/(\w+)\/(\w+)\/((\[(\w+[,-]?)+\])|(\[\*\]))/, // give me all columns
		get_range_slice: ~ /(\w+)\/((\[(\w+[,-]?)+\])|(\[\*\]))\/((\[(\w+[,-]?)+\])|(\[\*\]))/,   // get range slice
		get_range_key: ~/(\w+)\/\[(\w+[,-]?)+\]/  // give me a key range
	] as LinkedHashMap;

	def static final REGEX_FIELDS = [
		set_slice_col:[1:"cf", 2:"key", 3:"col"],
		get_col: [1:"cf", 2:"key", 3:"col"],
		get_slice_col: [1:"cf", 2:"key", 
			3:{ matcher, ctx, idx->
				ctx['request_type'] = 'col_slice';
				ctx['key_start'] = matcher.group(2)
				ctx['key_end'] = matcher.group(2)
				// [*] or [1,x?] or [1-x]
				// enough of generic stuff .. some place i will have to put an if ....
				if (matcher.group(6)) {
					ctx['col_slice'] = '*';
				} else {
				   def grp3 = matcher.group(3);
				   grp3 = grp3.substring(1, grp3.size()-1) 					   // TODO .. not to good .. use regexp to capture all this ..
					if (grp3 =~  ~/,/) {
						ctx["cols"] = grp3.split(",") as List
					} else {
						if (grp3 =~  ~/-/) {
							def startAndEnd = grp3.split("-")
							ctx["col_start"] = startAndEnd[0]
							ctx["col_end"] = startAndEnd[1]
						} else {
							ctx["col_start"] =grp3
							ctx["col_end"] =grp3
						}
					}
				}
			}
		],
		get_range_slice: [
				1: "cf",
				2: { matcher, ctx, idx->
					ctx['request_type'] = 'col_slice';
					ctx["col_start"] = ""
					ctx["col_end"] = ""
					println "key parsing..."
					// [*] or [1,x?] or [1-x]
					// enough of generic stuff .. some place i will have to put an if ....
					if (matcher.group(5)) { // TODO .. not required 
						ctx['col_slice'] = '*';
					} else {
					   def grp2 = matcher.group(2);
					   grp2 = grp2.substring(1, grp2.size()-1) 					   // TODO .. not to good .. use regexp to capture all this ..
						if (grp2 =~  ~/,/) {
							ctx["rows"] = grp2.split(",") as List
						} else {
							if (grp2 =~  ~/-/) {
								def startAndEnd = grp2.split("-")
								ctx["key_start"] = startAndEnd[0]
								ctx["key_end"] = startAndEnd[1]
							} else {
								ctx["key_start"] =grp2
								ctx["key_end"] =grp2
							}
						}
					}
				
				},
			6:{ matcher, ctx, idx->
				ctx['request_type'] = 'col_slice';
				// [*] or [1,x?] or [1-x]
				// enough of generic stuff .. some place i will have to put an if ....
				if (matcher.group(9)) {
					ctx['col_slice'] = '*';
				} else {
				   def grp3 = matcher.group(6);
				   grp3 = grp3.substring(1, grp3.size()-1) 					   // TODO .. not to good .. use regexp to capture all this ..
					if (grp3 =~  ~/,/) {
						ctx["cols"] = grp3.split(",") as List
					} else {
						if (grp3 =~  ~/-/) {
							def startAndEnd = grp3.split("-")
							ctx["col_start"] = startAndEnd[0]
							ctx["col_end"] = startAndEnd[1]
						} else {
							ctx["col_start"] =grp3
							ctx["col_end"] =grp3
						}
					}
				}
			}
			]
	]

	def static selectorMap = [
		(SelectorType.GET) :  GET_REGEX,
		(SelectorType.SET) :   SET_REGEX
	]
		
	def static parse(String str, SelectorType getOrSet) {
		def entry  = selectorMap[getOrSet].find { 
			it.value.matcher(str).matches()
		}
		if (entry == null) throw new UnsupportedOperationException("Does not support the scheme:" + str);
		entry.key
	}
	
	def static readContext(String path) {
		context path, SelectorType.GET
	}
	
	def static writeContext(String path) {
		context path, SelectorType.SET
	}
	
	def static context(path, getOrSet) {
		println path
		def matchedExpKey = Selector.parse(path, getOrSet)
		def ctx = [:]
		def pattern = selectorMap[getOrSet][matchedExpKey];
		def Matcher matcher = pattern.matcher(path);
		def colKeys = REGEX_FIELDS[matchedExpKey];
		println matchedExpKey + ":" + REGEX_FIELDS;
		if(colKeys == null) throw new Exception("Bug. Did not find any maping for pattern" + matchedExpKey);
		if (matcher.find()) {
				println matcher.groupCount();
				matcher.groupCount().times {
					println (it +":" + matcher.group(it+1));
				}
			matcher.groupCount().times {
				println matcher.group(it+1);
				def colKey = colKeys[it+1];
				if (colKey instanceof Closure) {
					colKey(matcher,ctx,it+1)
				} else {
				if (colKey == null) {
					println "Ignored group # " + (it+1)
				} else {
					println "should not be here .."
					ctx[colKey] = matcher.group(it+1);	 // 0 is full match
				}
				}
			}
		}
		ctx
	}
}
