import org.apache.log4j.Logger;

public class Main {
public static Logger logger = Logger.getLogger(Main.class);
	
	public static void main(String args[])
	{
		logger.info("This is Info Log");
		logger.debug("This is Debug Log");
		logger.warn("This is Error Log");
		logger.error("THis is Error Log");
	}
}
