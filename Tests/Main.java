package Tests;
import org.jsoup.Jsoup;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;



// From down here below - sentiment imports.
import java.util.List;
import java.util.Properties;
 

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

import java.io.IOException;



public class Main {
	
	static StanfordCoreNLP  sentimentPipeline;
	
	public static String getTweet(String url) throws IOException {
		Document doc = Jsoup.connect(url).get();
		return doc.title().split("\"")[1];
	}
	
	public static void printEntities(String tweet){
		
		// Init
		Properties props = new Properties();
		props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		StanfordCoreNLP NERPipeline =  new StanfordCoreNLP(props);
		// Done Init.
		
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(tweet);
 
        // run all Annotators on this text
        NERPipeline.annotate(document);
 
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
 
        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                System.out.println("\t-" + word + ":" + ne);
            }
        }
 
    }
	
	public static int findSentiment(String tweet) {
		 		
		
        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
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
	
	public static void main(String[] args) throws IOException {
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, parse, sentiment");
		sentimentPipeline =  new StanfordCoreNLP(props);

		
		String[] tweets = {"https://www.twitter.com/bengurionu/status/702075106038833153",
						   "https://twitter.com/BarackObama/status/710119666937499649"
						  };
		for(String tweet : tweets)
			handleTweet(tweet);
	}
	private static void handleTweet(String tweetURL) {
		String tweet;
		try {
			tweet = getTweet(tweetURL);
			System.out.println(tweet);
			System.out.println(findSentiment(tweet));
			printEntities(tweet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
