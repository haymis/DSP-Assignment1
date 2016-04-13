package Worker;

public class BadURLException extends Exception{
	
	public BadURLException(Exception e){
		super(e);
	}
	
	public String getMessage(){
		return "BadURLException: " + super.getMessage();
	}
}
