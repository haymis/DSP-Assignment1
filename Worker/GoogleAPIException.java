package Worker;

public class GoogleAPIException extends Exception{
	
	public GoogleAPIException(Exception e){
		super(e);
	}
	
	public String getMessage(){
		return "GoogleAPIException: " + super.getMessage();
	}
}
