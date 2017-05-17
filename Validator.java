import java.io.File;
import java.io.StringReader;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.*;


/*
 * This class is used to verify XML files according to a given schema before
 * trying to retrieve information off them.
 */

public class Validator {

    private static final String XSDPATH="medals.xsd";


    public static boolean isValid(String xml){


        Source xsdFile = new StreamSource(new File(XSDPATH));
        Source xmlFile = new StreamSource(new StringReader(xml));
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        javax.xml.validation.Validator val=null;
        try{
            Schema schema = schemaFactory.newSchema(xsdFile);
            val = schema.newValidator();
        }
        catch(Exception e){
            System.out.println("xsd file is ill-formed");
            return false;
        }

        try {
            val.validate(xmlFile);
            return true;
        } catch (Exception e) {
            return false;
        }

    }




}
