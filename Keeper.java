import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * This class manages the Medal Keeper. It is responsible for responding to client requests for medals, and
 * reading new xml files every time the crawler publishes them.
 */
public class Keeper {
    private static ConnectionFactory cf;
    private static Destination queue,topic;
    private static JMSContext jcontext;
    private static Countries countries;

    private static JMSConsumer mctopic,mcqueue;
    private static JMSProducer mpqueue;

    private static String xml;

    private static ReadWriteLock lock = new ReentrantReadWriteLock();


    //returns the medals for a given search query
    private static String getMedals(String query){

        String res="results from query "+query+":";

        query=query.toLowerCase();

        //each query item is split by ;
        String[] terms=query.split(";");

        //no data available or query invalid
        if(countries==null ){

            System.out.println("no xml available.returning nothing...");
            return "NOT AVAILABLE";
        }

        String countryquery="";
        String sportquery="";
        String categoryquery="";
        String athletenamequery="";
        String medaltypequery="";

        //fix queries according to the number of parameters
        if(terms.length>0)
            countryquery=terms[0];

        if(terms.length>1)
            sportquery=terms[1];

        if(terms.length>2)
            categoryquery=terms[2];

        if(terms.length>3)
            athletenamequery=terms[3];

        if(terms.length>4)
            medaltypequery=terms[4];

        //make sure we are not writing while we read from variable countries!
        lock.readLock().lock();
        System.out.println("locked for writing...");

        //traverse each country/medal and filter out the ones that don't meat criteria.
        for(Countries.Country country:countries.country){
            //filter results by country
            //the country query can refer to either the country code, or the name
            if(!country.getCountrycode().toLowerCase().contains(countryquery)
                    && !country.getCountryname().toLowerCase().contains(countryquery))
                continue;
            res+="\ncountry:"+country.getCountryname()+"("+country.getCountrycode()+")\n";
            for(Countries.Country.Medals.Medal medal:country.medals.medal){
                //filter results by the medal's sport
                if(!medal.getSport().toLowerCase().contains(sportquery))
                    continue;
                //filter results by the medal's sport category
                if(!medal.getCategory().toLowerCase().contains(categoryquery))
                    continue;
                //filter results by the medal's athlete name
                if(!medal.getAthletename().toLowerCase().contains(athletenamequery))
                    continue;
                //filter results by the medal's type
                if(!medal.getMedaltype().toLowerCase().contains(medaltypequery))
                    continue;

                //if the medal passed on all filters, we add it to the result
                res+="\n"+medal;
            }
        }

        //release the lock after getting the results we need.
        lock.readLock().unlock();
        System.out.println("can write(assuming no more threads are reading...)");

        return res;
    }

    //reads smartphones xml file from the topic
    private static boolean readXML(){
        boolean res=false;;
        JAXBContext context = null;
        Unmarshaller um = null;

        TextMessage tm=null;

        try{
            tm=null;

            //create unmarshaller
            context=JAXBContext.newInstance(Countries.class);
            um=context.createUnmarshaller();

            //read xml string from topic
            tm = (TextMessage)mctopic.receive();

            if(tm==null)
                return false;

            xml=tm.getText();

            //validate xml before creating summary
            if(Validator.isValid(xml)){
                System.out.println("xml is valid");
            }
            else {
                throw new Exception("invalid xml");
            }

            StringReader sr=new StringReader(xml);

            //convert xml string to Countries object.
            //we need to use the read write lock to make sure no reads are being made.
            lock.writeLock().lock();
            System.out.println("locked for reading...");
            countries=(Countries)um.unmarshal(sr);
            //release the lock after updating the variable
            lock.writeLock().unlock();
            System.out.println("unlocked for reading!");
            res=true;
            System.out.println("successfully read XML from topic.");

        } catch (Exception e) {
            return false;
        }
        finally{
            if(tm==null)
                res=false;
        }
        return res;
    }



    //waits for the next client's request and creates a thread to answer it.
    private static void waitForRequest() {
        TextMessage request;

        try{

            while(true) {
                request = (TextMessage) mcqueue.receive();
                new ProcessRequest(request);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //terminates the jcontext on exit
    private static class HookThread extends Thread {
        public void run(){
            if(Keeper.jcontext!=null)
                Keeper.jcontext.close();
        }
    }

    //listens topic for xml files
    private static class ListenXML extends Thread{

        ListenXML(){
            this.start();
        }

        public void run(){
            //each time a new XML is published on the topic, update the existing one.
            //note that the process of reading is blocking, so there is no active wait.
            while(true){
                //we only continue executing while the connection to the topic remains.
                if(!Keeper.readXML()) {
                    System.out.println("topic is offline.shutting down...");
                    if(jcontext!=null)
                        Keeper.jcontext.close();
                    System.exit(1);
                }
            }
        }

    }

    //processes a given client request for a medal
    private static class ProcessRequest extends Thread{

        private static TextMessage request;

        public ProcessRequest(TextMessage request){
            this.request=request;
            this.start();
        }

        public void run(){
            Destination clientqueue;
            String query;

            try{
                //get client's temp queue
                clientqueue=request.getJMSReplyTo();
                query=request.getText();

                System.out.println("answering query:"+query);
                //Thread.sleep(5000);

                //get the medals according to the given query, and send them to the client
                mpqueue.send(clientqueue,getMedals(request.getText()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }


    //constantly checks queue for messages and treats each client request.
    public static void start(){
        //close jcontext on exit
        Runtime.getRuntime().addShutdownHook(new HookThread());

        try {
            //connect to topic and queue
            cf = InitialContext.doLookup("jms/RemoteConnectionFactory");
            queue = InitialContext.doLookup("jms/queue/PlayQueue");
            topic = InitialContext.doLookup("jms/topic/PlayTopic");

            //create jcontext session
            jcontext = cf.createContext("bata", "cenas");
            jcontext.setClientID("keeper");

            //create a consumer for the queue, and a durable consumer for the topic.
            mcqueue = jcontext.createConsumer(queue);
            mctopic = jcontext.createDurableConsumer((Topic) topic, "Keeper");

            //create a producer
            mpqueue = jcontext.createProducer();

        }
        catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }

        //create thread for checking the topic for new medals
        new ListenXML();

        //waits for new requests
        waitForRequest();



    }



    public static void main(String[] args){
        start();
    }
}
