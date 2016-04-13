package LocalApp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;

import com.amazonaws.services.s3.model.S3Object;


public class HTMLFileCreator {
	
	private static String HTMLTemplatePathHeader = "http://www.srulix.net/~haymi/htmlTemplateHeader.html";
	private static String HTMLTemplatePathFooter = "http://www.srulix.net/~haymi/htmlTemplateFooter.html";
	
	private static String getHTML(String URL) throws IOException{
		Response response = Jsoup.connect(URL).ignoreContentType(true).execute();
		if(response.statusCode() != 200)
			return null;
		return response.body();
	}
	
	public static String getHTMLHeader() throws IOException{
		return getHTML(HTMLTemplatePathHeader);
	}
	
	public static String getHTMLFooter() throws IOException{
		return getHTML(HTMLTemplatePathFooter);
	}
	
	public static void CreateHTMLFromResponse(S3Object inputFile, String filename) throws IOException {
		PrintWriter writer = new PrintWriter(filename, "UTF-8");

		InputStream inputFileData = inputFile.getObjectContent();
		System.out.println("Done Downloading input file");

		writer.write(getHTMLHeader());
		
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
		
		writer.write(getHTMLFooter());
		inputFile.close();
		writer.close();
		
	}
}

