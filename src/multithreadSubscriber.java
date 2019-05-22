import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;


class Subscriber extends Thread {

    private static String topic;


    Subscriber(String topic) {
        this.topic = topic;
    }

    static void openSingleTerminal() throws IOException {

        Process process = Runtime.getRuntime().exec("mosquitto_sub -h comp3310.ddns.net -u students -P 33106331 -i 3310-u6211344 -t " + topic + "/#");

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), "utf-8"));

        String line;

        while ((line = topic + ", " + new Date().toString() + ": " + reader.readLine()) != null) {
            System.out.println(line);
        }


    }

    public void run() {
        try {
            openSingleTerminal();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

public class multithreadSubscriber {


    public static void main(String[] args) throws IOException {

        ArrayList<String> topicString = new ArrayList<>();
        ArrayList<Subscriber> subscriberManager = new ArrayList<>();


        topicString.add("counter/slow/q0");
        topicString.add("counter/slow/q1");
        topicString.add("counter/slow/q2");
        topicString.add("counter/fast/q0");
        topicString.add("counter/fast/q1");
        topicString.add("counter/fast/q2");

        for (int i = 0; i < topicString.size(); i ++){
            Subscriber subscriber = new Subscriber(topicString.get(i));
            System.out.println("Subscriber for: " + topicString.get(i) + "established!");
            subscriberManager.add(subscriber);
        }

        for (int i = 0; i < subscriberManager.size(); i ++){
            subscriberManager.get(i).start();
        }



//        Process process = Runtime.getRuntime().exec("mosquitto_sub -h comp3310.ddns.net -u students -P 33106331 -i 3310-u6211344 -t counter/slow/q1\n");
//
//        InputStreamReader ir = new InputStreamReader(process.getInputStream());
//        LineNumberReader input = new LineNumberReader(ir);
//
//        String line;
//        int count = 0;
//                while ((line = input.readLine()) != null) {
//                    count ++;
//                    System.out.println(count + ": " +line);
//                }
            }
    }