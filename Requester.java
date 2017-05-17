import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Scanner;

/**
 * This class represents the client side, and asks the user for the query to be used to search medals on the keeper,
 * proceeding to output the answer to it.
 */
public class Requester {

    //gets a list of medals from the keeper according to the query given.
    private static void getMedals(String text) throws Exception {
        ConnectionFactory cf;
        Destination mainqueue;

        String answer;
        Destination tempqueue;

        //connect to keeper queue
        cf = InitialContext.doLookup("jms/RemoteConnectionFactory");
        mainqueue = InitialContext.doLookup("jms/queue/PlayQueue");

        try (JMSContext jcontext = cf.createContext("bata", "cenas");) {

            //create temporary queue and producer
            tempqueue=jcontext.createTemporaryQueue();
            JMSProducer mp = jcontext.createProducer();

            //send text message with the query to the queue and set the reply to to the temporary queue.
            TextMessage tm=jcontext.createTextMessage(text);
            tm.setJMSReplyTo(tempqueue);
            mp.send(mainqueue,tm);

            //create consumer, receive response from the temp queue and output it.
            JMSConsumer mc=jcontext.createConsumer(tempqueue);
            answer = mc.receiveBody(String.class);
            System.out.println(answer);

        } catch (JMSRuntimeException re) {
            re.printStackTrace();
        }
    }

    public static void main(String[] args){

        //get search terms from user
        String query;
        System.out.println("search terms(country;sport;category;athlete name;medal type):");
        Scanner sc=new Scanner(System.in);
        query=sc.nextLine();

        //get medals from topic
        try{
            getMedals(query);}
        catch(Exception e){
            e.printStackTrace();
        }
        sc.close();



    }

}
