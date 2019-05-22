import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class Subscriber {

    public static void main(String[] args) throws IOException {


                Process process = Runtime.getRuntime().exec("mosquitto_sub -h comp3310.ddns.net -u students -P 33106331 -i 3310-u6211344 -t /slow/#\n");

                InputStreamReader ir = new InputStreamReader(process.getInputStream());
                LineNumberReader input = new LineNumberReader(ir);

                String line;
                while ((line = input.readLine()) != null) {
                    System.out.println(line);
                }
            }
    }