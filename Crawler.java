import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;



public class Crawler {

    private static final String url="https://www.rio2016.com/en/medal-count-country";

    private static String USER_AGENT="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36";

    private static Document htmlDocument=null;

    private static Countries countrylist=new Countries();

    private static String xml;

    //connects to the given url address and obtains its contents
    private static void obtainHTML(){
        int tries=0;

        //attempts to fetch the HTML page 3 times before giving up.
        while(tries<3){
            try
            {
                Connection connection = Jsoup.connect(url).userAgent(USER_AGENT);
                //connection.timeout(200);
                Document htmlDocument = connection.get();
                Crawler.htmlDocument = htmlDocument;

                System.out.println("Received web page at " + url);
                return;
            }
            catch(Exception ioe)
            {
                System.out.println("failed to fetch html document.retrying in 5 seconds...");
                tries++;

                //wait 3 seconds before trying again...
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("error retrieving webpage...");
        System.exit(1);
    }

    //obtains the list of medals on the given url
    private static void obtainMedals()
    {
        String auxstr;
        String[] tokens;
        Element country;

        Countries.Country.Medals.Medal medalobject;
        Countries.Country countryobject;
        countrylist.country=new ArrayList<>();
        Countries.Country.Medals medallist;

        int goldmedals,silvermedals,bronzemedals,totalmedals;

        Elements auxels,countrymedals;

        //obtain HTML DOM from webpage
        obtainHTML();

        //get list of countries and their medals
        Elements countries=htmlDocument.select("[data-odfcode]");
        Elements medals=htmlDocument.select(".table-expand");


        for(int i=0;i<countries.size();i++){

            auxels=countries.get(i).select(".country");

            /*
            goldmedals=Integer.parseInt(countries.get(i).select(".col-4").text());
            silvermedals=Integer.parseInt(countries.get(i).select(".col-5").text());
            bronzemedals=Integer.parseInt(countries.get(i).select(".col-6").text());
            totalmedals=goldmedals+silvermedals+bronzemedals;


            System.out.println("gm:"+goldmedals);
            System.out.println("sm:"+silvermedals);
            System.out.println("bm:"+bronzemedals);
            System.out.println("tm:"+totalmedals);*/

            String countrycode=auxels.get(0).text();
            String countryname=auxels.get(1).text();
            //System.out.println(countrycode+" "+countryname);

            countryobject=new Countries.Country();
            countryobject.setCountryname(countryname);
            countryobject.setCountrycode(countrycode);


            countrymedals=medals.get(i).select("tr");
            //we get the data for each medal won by this country
            String medaltype=null;


            medallist=new Countries.Country.Medals();
            medallist.medal=new ArrayList<>();

            for(Element countrymedal:countrymedals){

                if(medaltype!=null && countrymedals.size()==2)
                    continue;

                //new medal type
                auxstr=countrymedal.select("td.col-1").text();

                if(auxstr==null)
                    continue;

                if(auxstr.length()>6)
                    continue;

                if(auxstr.equals("Gold") || auxstr.equals("Silver") || auxstr.equals("Bronze")) {
                    medaltype = auxstr;
                }

                String sport=countrymedal.select(".col-2").text();
                String category=countrymedal.select(".col-3").text();
                String athletename=countrymedal.select(".col-4").text();

                //System.out.println(medaltype+","+sport+","+category+","+athletename);

                medalobject=new Countries.Country.Medals.Medal();
                medalobject.setAthletename(athletename);
                medalobject.setSport(sport);
                medalobject.setMedaltype(medaltype);
                medalobject.setCategory(category);

                medallist.getMedal().add(medalobject);


            }
            countryobject.setMedals(medallist);
            countrylist.getCountry().add(countryobject);

        }
    }

    //generates xml from medals list
    private static void generateXML(){
        StringWriter sw = new StringWriter();

        try{
            //create marshaller
            JAXBContext context = JAXBContext.newInstance(Countries.class);
            Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            //get xml string from marshaller and the list of medals
            m.marshal(countrylist, sw);
            xml=sw.toString();

            //write xml to a file for debugging
            PrintWriter writer = new PrintWriter("xmlobtained.xml", "UTF-8");
            writer.println(xml);
            writer.close();

        }
        catch(Exception e){
            System.out.println("error creating xml file"+e);
            System.exit(1);
        }
    }



    //publish XML on topic
    private static void publishXML(){
        ConnectionFactory cf;
        Topic topic;
        TextMessage tm;
        JMSContext jcontext;

        int waittime=10;

        while(true) {
            try {
                //connect to topic
                cf = InitialContext.doLookup("jms/RemoteConnectionFactory");
                topic = InitialContext.doLookup("jms/topic/PlayTopic");
                jcontext = cf.createContext("bata", "cenas");

                //publish xml
                JMSProducer prod = jcontext.createProducer();
                tm=jcontext.createTextMessage(xml);
                prod.send(topic,tm);

                break;
            } catch (NamingException e) {
                System.out.println("error connecting to topic.retrying in "+waittime+" seconds...");

                //waiting time is initially 10 seconds, and doubles each time we fail to access topic.
                try {
                    Thread.sleep(waittime*1000);
                    waittime*=2;
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }

        }

        System.out.println("xml published on topic");

    }

    public static void main(String[] args){
        obtainMedals();
        generateXML();
        publishXML();
    }





}
