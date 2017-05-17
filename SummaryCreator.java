import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.StringReader;


/**
 * This class is responsible for retrieving the XML posted in the topic, and generate an HTML file from it and the
 * XSLT file.
 */

public class SummaryCreator {

    private static ConnectionFactory cf;
    private static Topic topic;
    private static JMSContext jcontext;

    private static String xml;

    //used to makenew htmls each time
    private static int IDHTML=0;

    //connects to topic.
    private static void connectToTopic(){

        if(jcontext!=null)
            jcontext.close();


        //try to connect to topic until successful. wait 5 seconds before retrying on fail.
        while(true) {
            try {
                cf = InitialContext.doLookup("jms/RemoteConnectionFactory");
                topic = InitialContext.doLookup("jms/topic/PlayTopic");
                jcontext = cf.createContext("bata", "cenas");
                System.out.println("connected to topic.");
                break;
            } catch (NamingException e) {
                System.out.println("error connecting to topic.retrying in 5 seconds.");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }

    }

    //obtain medals XML from topic
    private static void getXML(){
        TextMessage tm=null;
        JMSConsumer mc=null;

        try{
            //connect to topic
            connectToTopic();
            jcontext.setClientID("summarycreator");
            mc = jcontext.createDurableConsumer(topic,"Summary");
            tm = (TextMessage)mc.receive();

            if(tm==null)
                return;

            xml=tm.getText();
            System.out.println("medals XML obtained. now validating");

            //validate xml before creating summary
            if(Validator.isValid(xml)){
                System.out.println("xml is valid");
            }
            else {
                throw new Exception("invalid xml");
            }

        } catch (Exception e) {
            System.out.println("error obtaining xml file:"+e);

        }
        finally{
            if(mc!=null) mc.close();

            if(jcontext!=null){
                jcontext.close();
            }

            System.out.println("retrying in 5 seconds...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

        }
    }

    //creates HTML file from the xslt template, and the xml obtained from the topic
    private static void generateHTML(){
        try {
            TransformerFactory tFactory=TransformerFactory.newInstance();

            Source xslDoc=new StreamSource("is-xslt.xsl");
            Source xmlDoc=new StreamSource(new StringReader(xml));

            String outputFileName="is-html"+IDHTML+".html";
            IDHTML++;

            OutputStream htmlFile=new FileOutputStream(outputFileName);
            Transformer trasform=tFactory.newTransformer(xslDoc);
            trasform.transform(xmlDoc, new StreamResult(htmlFile));
            System.out.println("html summary created");
        }
        catch (Exception e)
        {
            System.out.println("error creating html file");

            if(jcontext!=null)
                jcontext.close();

        }

        finally{
            if(jcontext!=null)
                jcontext.close();
        }
    }



    //waits for threads to return and closes jmscontext
    private static void stop(){
        System.out.println("Closing jms context...");

        if(jcontext!=null)
            jcontext.close();
    }

    //terminates the jcontext on exit
    private static class HookThread extends Thread {
        public void run(){
            SummaryCreator.stop();
        }
    }


    public static void main(String[] args) {

        //Set saxon as transformer.
        //needed to use xslt 2.0
        System.setProperty("javax.xml.transform.TransformerFactory",
                "net.sf.saxon.TransformerFactoryImpl");

        //close jcontext on exit
        Runtime.getRuntime().addShutdownHook(new HookThread());

        while(true) {
            getXML();

            generateHTML();
        }
    }

}

