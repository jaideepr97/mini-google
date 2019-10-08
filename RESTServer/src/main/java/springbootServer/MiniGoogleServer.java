package springbootServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//@ImportResource("hadoop-context.xml")
public class MiniGoogleServer {

	public static void main(String args[]) {
		SpringApplication.run(MiniGoogleServer.class, args);
	}

}
