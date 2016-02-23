
# stmtmppr

super simple Java Collections Framework Object and RDBMS table data mapping library. 

simpler version of mybatis, but LinkedHashMap and other similar classes are used instead of user-defined-value-object-classes.   

## 1 Minute Tutorial 

* configure a statement in xml file.  
```xml 
	<statement id="stmt01"><![CDATA[ 
SELECT col01, col02 FROM table01 WHERE col01 < ${param01}
]]></statement>

```

* prepare a parameter object.
```java
LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
params.put("param01", 3);
... 
```

* invoke StmtMapper class method 
```java
StmtMapper mapper = new StmtMapper("some/package/my-stmtmppr-conf.xml");
String stmtId     = "stmt01";
Connection conn ;
try{
	...
	result = mapper.doSelects(conn, stmtId, params);
	...
}
```

* result object looks like this
```
List<LinkedHashMap<String , Object>> contains 
  1. LinkedHashMap<String , Object> -> {col01:  1, col02: "A" }
  2. LinkedHashMap<String , Object> -> {col01:  2, col02: "B" }
  3. ...
```

## Others

* java1.6+

* Always. PreparedStatements are used internally.

* ResultSetMetaData are highly used and internally cached.

* no dependency for 3rd party library

* java logging framework not supported. 
  You can customize Logger class if you want.

* It's name is the acronym for 'statement mapper'.

* Execuse me if my English is not good enough. My first language is not English.

