package Worker;

import java.io.File;
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
import org.json.simple.parser.ParseException;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
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
	private static String APIUrl = "https://kgsearch.googleapis.com/v1/entities:search?key=AIzaSyBA0QFiW2iZtNmhrwf-YRRQSVimKA8Y8G8&limit=1&query=";
	static StanfordCoreNLP sentimentPipeline;
	static StanfordCoreNLP EntitiesPipeline;
	private static UUID uuid = UUID.randomUUID();
	private static AmazonSQSClient sqs;
	private static AWSCredentials credentials;
	private static Logger logger = Logger.getLogger("Worker Logger");
	private static String logFilePath = "C:\\Users\\Haymi\\Documents\\BGU\\DSP\\workerlog"+uuid.toString()+".txt";//"/tmp/workerLog.txt"\
	private static String logFileName = "workerlog"+uuid.toString()+".txt";
	private static String credentialsFilePath = "C:\\Users\\Haymi\\Documents\\BGU\\DSP\\rootkey.properties"; 
	private static String jobsQueueURL;
	private static String managerWorkersQueueURL;
	private static String jobDoneAckQueueURL;
	private static String workersStatsQueueURL;
	private static String[] KnowladgeAPITypes = {"", "Person", "Organization", "Place"};
	private static int waitForNewMsgesTime = 500;
	private static int receiveCountLimit = 3;
	private static int numOfSuccessfulTweets = 0; 
	private static int numOfFailedTweets = 0;
	private static long startTime = 0;
	private static double avgRuntimePerSuccessfulMessage = 0;
	private static double avgRuntimePerFailedMessage = 0;
	private static String bucketName = "hayminirhodadi";
	private static FileHandler logFileHandler;
	/*
	 * returns the tweet fetched from the given url
	 */
	public static String getTweet(String url) throws BadURLException{
		Document doc;
		try {
			doc = Jsoup.connect(url).get();
		} catch (IOException e) {
			throw new BadURLException(e);
		}
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
		logger.info("[WORKER] - " + msg);
		return;
	}
	
	public static void main(String[] args) throws IOException {
		startTime = System.currentTimeMillis();
		
		try {
	        logFileHandler = new FileHandler(logFilePath);
	        logger.addHandler(logFileHandler);
	        SimpleFormatter formatter = new SimpleFormatter();
	        logFileHandler.setFormatter(formatter);
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
		jobsQueueURL =  sqs.getQueueUrl("JobsQueue").getQueueUrl();
		managerWorkersQueueURL = sqs.getQueueUrl("ManagerWorkersQueue").getQueueUrl();
		jobDoneAckQueueURL = sqs.getQueueUrl("jobDoneAckQueue").getQueueUrl();
		workersStatsQueueURL = sqs.getQueueUrl("WorkersStatsQueue").getQueueUrl();
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
	        	logInfo("Didn't find messages, going to sleep");
	        	try {
					Thread.sleep(waitForNewMsgesTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	        	continue;
	        }
	        
	        Map<String, MessageAttributeValue> msgAttributes;
	        for(Message message : messages){
	        	long messageStartTime = System.currentTimeMillis();	        
				deleteTweetIfLimitExceeded(message);
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
    			}
	        	catch(GoogleAPIException | ParseException e){
    				handleFailedTweet(messageStartTime, e);
    				sendResponse(localAppUUID, jobDoneAckQueueURL);
	        		deleteTweetIfLimitExceeded(message);
    				continue;
    			}
	        	catch(BadURLException e){
	        		handleFailedTweet(messageStartTime, e);
	        		sendResponse(localAppUUID, jobDoneAckQueueURL);
	        		deleteMsg(receipt, jobsQueueURL);
	        		continue;
	        	}
	        	catch (IOException e) { // Assume issue with worker, terminate it (return msg to queue).
	        		handleFailedTweet(messageStartTime, e);
					shouldWork = false;
					break;
				}
	        	
	    		sendResponse(tweetResponse, localAppUUID, responseQueueURL);
	    		deleteMsg(receipt, jobsQueueURL);
	    		sendResponse(localAppUUID, jobDoneAckQueueURL);
	    		avgRuntimePerSuccessfulMessage = getNewAverage(messageStartTime, true); 
	    		numOfSuccessfulTweets++;
	    		
	        }          	
		}
		terminate();
	}
	
	private static double getNewAverage(long messageStartTime, boolean isSuccessful) {
		int counter = (isSuccessful) ? numOfSuccessfulTweets : numOfFailedTweets;
		double currentAvg = (isSuccessful) ? avgRuntimePerSuccessfulMessage : avgRuntimePerFailedMessage;
		long currentRuntime = System.currentTimeMillis() - messageStartTime;
		if(counter == 0)
			return currentRuntime;
		return ((currentAvg * counter) + currentRuntime) / (counter + 1);				
	}

	private static void handleFailedTweet(long messageStartTime, Exception e) {
		logInfo(e.getMessage());
		numOfFailedTweets++;
		avgRuntimePerFailedMessage = getNewAverage(messageStartTime, false);
	}

	private static void deleteTweetIfLimitExceeded(Message message) {
		if(shouldDeleteMsg(message.getAttributes().get("ApproximateReceiveCount"))){
			logInfo("Receive count limit exceeded. deleting message " + message.getReceiptHandle());
			deleteMsg(message.getReceiptHandle(), jobsQueueURL);
		}
	}

	private static boolean shouldDeleteMsg(String receiveCount) {
		return Integer.parseInt(receiveCount) > receiveCountLimit;
	}

	private static void deleteMsg(String receipt, String queueUrl){
		logInfo("Deleting msg " + receipt);
		sqs.deleteMessage(new DeleteMessageRequest(queueUrl, receipt));
	}
	
	private static void terminate() {
		//TODO: finish this func...
		logInfo("successfull tweets - " + numOfSuccessfulTweets + ", Failed tweets - " + numOfFailedTweets);
		logInfo("Terminating instance\n"
				+ "in future - send back the logs and stats...");

		sendStatisticsMessage();
		uploadLog();   
	}

	private static void uploadLog() {
		logInfo("Uploading file to S3");
		AmazonS3 s3 = new AmazonS3Client(credentials);
        logInfo("Created Amazon S3 Client");
        if (!s3.doesBucketExist(bucketName)){
            s3.createBucket(bucketName);
        	logInfo("Created bucket.");
        }
        logFileHandler.close();
        File f = new File(logFilePath);
        PutObjectRequest por = new PutObjectRequest(bucketName, logFileName, f);
        por.withCannedAcl(CannedAccessControlList.PublicRead);
        s3.putObject(por);		
	}

	private static void sendStatisticsMessage() {
		Map<String, MessageAttributeValue> statsAttributes = new HashMap<String, MessageAttributeValue>();
		statsAttributes.put("Runtime", new MessageAttributeValue().withDataType("String").withStringValue(calcTotalRuntime() + ""));
		statsAttributes.put("SuccessfulRuntimePerMessage", new MessageAttributeValue().withDataType("String").withStringValue(avgRuntimePerSuccessfulMessage + ""));
		statsAttributes.put("FailedRuntimePerMessage", new MessageAttributeValue().withDataType("String").withStringValue(avgRuntimePerFailedMessage + ""));
		statsAttributes.put("SuccessfulTweets", new MessageAttributeValue().withDataType("String").withStringValue(numOfSuccessfulTweets + ""));
		statsAttributes.put("FailedTweets", new MessageAttributeValue().withDataType("String").withStringValue(numOfFailedTweets + ""));
        sqs.sendMessage(new SendMessageRequest().withQueueUrl(workersStatsQueueURL).withMessageBody("New worker stats.").withMessageAttributes(statsAttributes));
		
	}

	private static long calcTotalRuntime() {
		return System.currentTimeMillis() - startTime;
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
    			.withAttributeNames("ApproximateReceiveCount")
    			.withMaxNumberOfMessages(maxMessages)
	    	  ); 
	}

	private static String workOnTweet(String tweetURL) throws BadURLException, IOException, ParseException, GoogleAPIException{
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
	
	private static JSONObject handleTweet(String tweetURL) throws BadURLException, IOException, ParseException, GoogleAPIException  {
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
			String jsonResponse= getKnowladge(entity, entities.get(entity).equals("PERSON") ? 1 
													: (entities.get(entity).equals("ORGANIZATION") ? 2 
													: (entities.get(entity).equals("LOCATION") ? 3
													: 0 )));
			if(jsonResponse == null)
				continue;
			
			JSONParser parser = new JSONParser();

			Object KnowladgeObj = parser.parse(new StringReader(jsonResponse));
			JSONObject KnowladgeJson = (JSONObject) KnowladgeObj;
			JSONArray results = (JSONArray) KnowladgeJson.get("itemListElement");
			
			if (results.size() == 0){
				logInfo("No results for '" + entity + "' from the Knowladge API.");
				continue;
			}

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

	private static String getKnowladge(String entity, int type) throws GoogleAPIException{
		StringBuilder sb = new StringBuilder();
		sb.append(APIUrl);
		sb.append(entity);
		sb.append("&types=");
		sb.append(KnowladgeAPITypes[type]);
		String apiQeury = sb.toString();
		logInfo("Querying google's API - " + apiQeury);
		Response response;
		try {
			response = Jsoup.connect(apiQeury).ignoreContentType(true).execute();
		} catch (Exception e) {
			throw new GoogleAPIException(e);
		}
		if(response.statusCode() != 200)
			return null;
		return response.body();
	}


}

