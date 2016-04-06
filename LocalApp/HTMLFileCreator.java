package ass1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.amazonaws.services.s3.model.S3Object;


public class HTMLFileCreator {
	private static String HTMLHeader = "<!DOCTYPE html><html lang=\"en\"><head> <title>Tweets Analysis</title><link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css\"><link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap-theme.min.css\"><script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js\"></script><script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js\"></script></head><body><div class=\"bg-1\"> <div class=\"container text-center\"> <h1>Tweets Entity Analysis</h1> </div></div><script>$(document).ready(function(){$('[data-toggle=\"tooltip\"]').tooltip()}),function(){function l(l){for(var t=0;t<l.length;t++){var a=l[t],i=\",n=a.entities;if(n.length>0)for(var o=0;o<n.length;o++)i+='<li class=\"list-group-item\"> <a href=\"'+n[o].wiki_url+'\" data-toggle=\"tooltip\" data-placement=\"right\" title=\"'+n[o].google_name+'\">'+n[o].entity_name+\"</a></li>\n\";else i+='<li class=\"list-group-item\"> <div><h4>No Entities!</h4></div></li>\n';document.write('<div class=\"panel-group\"> <div class=\"panel panel-default\"><div class=\"panel-heading\"><h4 class=\"panel-title\" style=\"color:'+e(a.sentiment)+';\"><a data-toggle=\"collapse\" href=\"#collapse'+t+'\">'+a.tweet+'</a></h4></div><div id=\"collapse'+t+'\" class=\"panel-collapse collapse\"><ul class=\"list-group\">'+i+\"</ul></div></div></div>\")}}function e(l){var e=[\"#8C0000\",\"red\",\"black\",\"#66FF65\",\"#238423\"];return e[l]}var t=";
	private static String HTMLFooter = ";l(t)}();</script></body></html>";
	
	public static void CreateHTMLFromResponse(S3Object inputFile, String filename) throws IOException {
		PrintWriter writer = new PrintWriter(filename, "UTF-8");

		InputStream inputFileData = inputFile.getObjectContent();
		System.out.println("Done Downloading input file");

		writer.write(HTMLHeader);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputFileData));
		String nextLine;
		System.out.println("Printing file.");
		try {
			while ((nextLine = reader.readLine()) != null) {
				writer.write(nextLine);
			}
			System.out.println("Done Printing file.");
			inputFileData.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error writing to file");
		} finally {
			reader.close();
		}
		
		writer.write(HTMLFooter);
		inputFile.close();
		writer.close();
		
	}
}
