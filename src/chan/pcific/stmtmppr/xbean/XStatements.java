package chan.pcific.stmtmppr.xbean;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

@XmlAccessorType(XmlAccessType.NONE)
@XmlRootElement(name = "statements")
public class XStatements {

	@XmlElements(@XmlElement(name="statement", type=XStatement.class))
    List<XStatement> statements = new ArrayList<XStatement>();

    public List<XStatement> getList() {
        return this.statements;
    }
    
    @XmlAccessorType(XmlAccessType.NONE)
    @XmlType(name = "statementType", propOrder = {
        "value"
    })
    public static class XStatement{
    	
        @XmlValue
        protected String value;
        @XmlAttribute(name = "id", required = true)
        protected String id;
        
		public String getValue() {
			return value;
		}
		public String getId() {
			return id;
		}
    	
    }
    
	public Map<String, XStatements.XStatement> map(){
		Map<String, XStatements.XStatement> map = new HashMap<String, XStatements.XStatement>(); 
		for(XStatements.XStatement stmt:this.getList()){
			String id = stmt.getId();
			map.put(id, stmt);
		}
		return map;
	} 
    
    public static XStatements unmarshall(InputStream in) throws JAXBException{
    	XStatements stmts = null;
    	try{
    		JAXBContext jaxbContext = JAXBContext.newInstance(XStatements.class);
    		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
    		stmts = (XStatements) jaxbUnmarshaller.unmarshal(in);
    	}catch(JAXBException je){
//    		je.printStackTrace();
    		throw je;
    	}
    	return stmts;
    }
    
    public static void marshall(XStatements stmts, OutputStream out) throws JAXBException{
		try{
			JAXBContext jaxbContext = JAXBContext.newInstance(XStatements.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			jaxbMarshaller.marshal(stmts, out);
		}catch(JAXBException je) {
//			je.printStackTrace();
			throw je;
		}
    }
    
    public static void printOut(XStatements stmts){
    	try{
    		marshall(stmts, System.out);
    	}catch(JAXBException je){
    		je.printStackTrace();
    	}
    }
    
    public static XStatements loadUp(final String resourcePath){
    	XStatements stmts = null;
		InputStream in = null;
		try{
			in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
			if(in==null){
				System.out.println("Err! not found resource " + resourcePath);
				return new XStatements();
			}
			stmts = unmarshall(in);
		}catch(JAXBException je) {
			je.printStackTrace();
		}finally{
			try{if(in!=null){in.close();in=null;}}catch(Exception ig){}
		}
		return stmts;
    }
    
    static void test_marshall(){
    	XStatements stmts = loadUp("sesman/conf/pdb-minibar-conf.xml");
    	String id = stmts.getList().get(0).getId();
    	String value = stmts.getList().get(0).getValue();
    	System.out.println(id);
    	System.out.println(value);
    	printOut(stmts);
    }
    
    public static void main(String[] args) {
		test_marshall();
	}
    
}
