package Worker;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class Worker {

	public static String[] tweets = { "https://www.twitter.com/BarackObama/status/710517154987122689"};
	private static String APIUrl = "https://kgsearch.googleapis.com/v1/entities:search?key=AIzaSyBA0QFiW2iZtNmhrwf-YRRQSVimKA8Y8G8&query=";
	static StanfordCoreNLP sentimentPipeline;
	static StanfordCoreNLP EntitiesPipeline;
	private static UUID uuid = UUID.randomUUID();
	private static AmazonSQSClient sqs;
	private static AWSCredentials credentials;
	private static Logger logger = Logger.getLogger("Worker Logger");
	private static String logFilePath = "C:\\Users\\Haymi\\Documents\\BGU\\DSP\\workerlog"+uuid.toString()+".txt";//"/tmp/workerLog.txt"\
	private static String credentialsFilePath = "C:\\Users\\Haymi\\Documents\\BGU\\DSP\\rootkey.properties"; 
	private static String jobsQueueURL;
	private static String managerWorkersQueueURL;
	private static String jobDoneAckQueueURL;	
	/*
	 * returns the tweet fetched from the given url
	 */
	public static String getTweet(String url) throws IOException {
		Document doc = Jsoup.connect(url).get();
		return doc.title().split("\"")[1];
	}

	/*
	 * Returns a list of entities and their description, for a given tweet
	 */
	public static Map<String, String> getEntities(String tweet) {

		Map<String, String> ret = new HashMap<String, String>();

		// create an empty Annotation just with the given text
		Annotation document = new Annotation(tweet);

		// run all Annotators on this text
		EntitiesPipeline.annotate(document);

		// these are all the sentences in this document
		// a CoreMap is essentially a Map that uses class objects as keys and
		// has values with custom types
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		System.out.println("Entities: ");
		int i = 1;
		for (CoreMap sentence : sentences) {
			// traversing the words in the current sentence
			// a CoreLabel is a CoreMap with additional token-specific methods
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				// this is the text of the token
				String word = token.get(TextAnnotation.class);
				// this is the NER label of the token
				String ne = token.get(NamedEntityTagAnnotation.class);
				if (ne.equals("PERSON") || ne.equals("ORGANIZATION") || ne.equals("LOCATION")) {
					System.out.println("\t" + i++ + " -" + word + ":" + ne);
					ret.put(word, ne);
				}

			}
		}
		if (i == 0)
			System.out.println("No Entities Here...!");

		return ret;
	}

	/*
	 * calculates the sentiment for a given tweet
	 */
	public static int findSentiment(String tweet) {

		int mainSentiment = 0;
		if (tweet != null && tweet.length() > 0) {
			int longest = 0;
			Annotation annotation = sentimentPipeline.process(tweet);
			for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sentence.toString();
				if (partText.length() > longest) {
					mainSentiment = sentiment;
					longest = partText.length();
				}

			}
		}
		return mainSentiment;
	}

	public static void logInfo(String msg){
		//logger.info("[WORKER] - " + msg);
		return;
	}
	
	public static void main(String[] args) throws IOException {
		FileHandler fh;
		try {
	        fh = new FileHandler(logFilePath);
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();
	        fh.setFormatter(formatter);
	        logInfo("===== Worker started =====");
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
			
		logInfo("Init Annotations Pipeline");
		Properties AnnotProps = new Properties();
		AnnotProps.put("annotators", "tokenize, ssplit, parse, sentiment");
		sentimentPipeline = new StanfordCoreNLP(AnnotProps);

		logInfo("Init Entities Pipeline");
		Properties EntProps = new Properties();
		EntProps.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		EntitiesPipeline = new StanfordCoreNLP(EntProps);

		logInfo("Init credentials and sqs");
		credentials = new PropertiesCredentials(new FileInputStream(credentialsFilePath));
		sqs = new AmazonSQSClient(credentials);
		jobsQueueURL =  sqs.getQueueUrl("JobQueueQueue").getQueueUrl();
		managerWorkersQueueURL = sqs.getQueueUrl("ManagerWorkersQueue").getQueueUrl();
		jobDoneAckQueueURL = sqs.getQueueUrl("jobDoneAckQueue").getQueueUrl();
		logInfo("start working:");
		work();
	}
	
	private static void work(){
		List<Message> messages;
		ReceiveMessageResult result;
		
		boolean shouldWork = true;
		while(shouldWork){
			if(hasTerminateMsg()){
				logInfo("Should Terminate");
				shouldWork = false;
				break;
			}
			result = getMsgFromQueue(jobsQueueURL, "All", 1);
	        messages = result.getMessages();
	        
	        if(messages.isEmpty()){
	        	logInfo("Didn't found messages");
	        	shouldWork = false;
	        	continue;
	        }
	        
	        Map<String, MessageAttributeValue> msgAttributes;
	        for(Message message : messages){
	        	String receipt = message.getReceiptHandle();
	        	
	        	logInfo("Working on msg:");
	        	msgAttributes = message.getMessageAttributes();
	        	String localAppUUID = msgAttributes.get("UUID").getStringValue();
	        	String tweetURL = msgAttributes.get("Url").getStringValue();
	        	logInfo("localUUID - " + localAppUUID + " tweetURL " + tweetURL);
	        	String responseQueueURL =  sqs.getQueueUrl("Responses-"+localAppUUID).getQueueUrl();
	        	String tweetResponse = "";
	        	try {
	        		tweetResponse = workOnTweet(tweetURL);
    			} catch(org.jsoup.HttpStatusException e){
    				logInfo("Got Google's API Exception " + e.getMessage() + "\nnot deleting msg");
    				continue;
    			}
	        	catch (Exception e) {//TODO: handle IO Exception (maybe return msg to jobQueue with higher priority)
    				logInfo("Got Exception " + e.getMessage());
					e.printStackTrace();
					shouldWork = false;
					break;//TODO change to terminate (new func that sends worker stat to manager)
				}
	        	
	    		sendResponse(tweetResponse, localAppUUID, responseQueueURL);
	    		sqs.deleteMessage(new DeleteMessageRequest(jobsQueueURL, receipt));
	    		
	    		sendResponse(localAppUUID, jobDoneAckQueueURL);
	        }          	
		}
		terminate();
	}
	private static void terminate() {
		//TODO: finish this func...
		logInfo("Terminating instance\n"
				+ "in future - send back the logs and stats...");
	}

	private static boolean hasTerminateMsg() {
		logInfo("Checking for termination");
		ReceiveMessageResult result = getMsgFromQueue(managerWorkersQueueURL, "Terminate", 1);
		if(result.getMessages().isEmpty())
			return false;
		logInfo("I Should terminate. deleting termination msg");
		String receipt = result.getMessages().get(0).getReceiptHandle();
		sqs.deleteMessage(new DeleteMessageRequest(managerWorkersQueueURL, receipt));
		return true;
	}

	private static ReceiveMessageResult getMsgFromQueue(String queueURL,
			String tagName, int maxMessages) {
		return sqs.receiveMessage(
    			new ReceiveMessageRequest()
    			.withQueueUrl(queueURL)
    			.withMessageAttributeNames(tagName)
    			.withMaxNumberOfMessages(maxMessages)
	    	  ); 
	}

	private static String workOnTweet(String tweetURL) throws Exception{
		logInfo("Working on tweet url " + tweetURL);
		JSONObject entities = handleTweet(tweetURL);
		String result = entities.toJSONString();
		logInfo("GOT - " + result);
		return result;
	}
	private static void sendResponse(String response, String uuid, String queueURL){
		Map<String, MessageAttributeValue> attributes = new HashMap<String, MessageAttributeValue>();
        attributes.put("UUID", new MessageAttributeValue().withDataType("String").withStringValue(uuid));
        attributes.put("result", new MessageAttributeValue().withDataType("String").withStringValue(response));
        sqs.sendMessage(new SendMessageRequest().withQueueUrl(queueURL).withMessageBody("New Result :)").withMessageAttributes(attributes));
	}
	private static void sendResponse(String uuid, String queueURL){
		Map<String, MessageAttributeValue> attributes = new HashMap<String, MessageAttributeValue>();
        attributes.put("UUID", new MessageAttributeValue().withDataType("String").withStringValue(uuid));
        sqs.sendMessage(new SendMessageRequest().withQueueUrl(queueURL).withMessageBody("New Result :)").withMessageAttributes(attributes));
	}
	
	private static JSONObject handleTweet(String tweetURL) throws org.json.simple.parser.ParseException, IOException {
		String tweet;
		JSONObject JsonTweet = new JSONObject();

		logInfo("Getting tweet content");
		tweet = getTweet(tweetURL);
		JsonTweet.put("tweet", tweet);

		logInfo("Getting tweet sentiment");
		int sentiment = findSentiment(tweet);
		JsonTweet.put("sentiment", new Integer(sentiment));

		logInfo("Getting tweet entities");
		Map<String, String> entities = getEntities(tweet);
		JSONArray JsonEntities = new JSONArray();
		if (entities.isEmpty()) {
			logInfo("Tweet doesn't have entities");
			JsonTweet.put("entities", JsonEntities);
			return JsonTweet;
		}
		logInfo("Tweet has " + entities.size() + " entities");
		for (String entity : entities.keySet()) {
			JSONObject JsonEntity = new JSONObject();

			String desc = null;
			String wikiUrl = null;

			String jsonResponse = getKnowladge(entity);
			if(jsonResponse == null)
				continue;
			
			JSONParser parser = new JSONParser();

			Object KnowladgeObj = parser.parse(new StringReader(jsonResponse));
			JSONObject KnowladgeJson = (JSONObject) KnowladgeObj;
			JSONArray results = (JSONArray) KnowladgeJson.get("itemListElement");
			
			Iterator<JSONObject> iterator = results.iterator();
			// get top result:
			if (iterator.hasNext()) {
				JSONObject result = iterator.next();
				
				// get full name and wikipedia url into a json object:
				if(!result.containsKey("result"))
					continue;
				result = (JSONObject) result.get("result");
				if(!result.containsKey("name") || !result.containsKey("detailedDescription"))
					continue;
				JsonEntity.put("google_name", (String) result.get("name"));
				result = (JSONObject) result.get("detailedDescription");
				if(!result.containsKey("url"))
					continue;
				JsonEntity.put("wiki_url", (String) result.get("url"));
			}
			JsonEntity.put("entity_name", entity);
			JsonEntities.add(JsonEntity);
		}
		JsonTweet.put("entities", JsonEntities);
		return JsonTweet;

	}

	private static String getKnowladge(String entity) throws IOException {
		Response response = Jsoup.connect(APIUrl + entity).ignoreContentType(true).execute();
		if(response.statusCode() != 200)
			return null;
		return response.body();
	}


}
