import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

public class SubscriberMain implements MqttCallback {

    private ArrayList<String> topics = new ArrayList<String>();

    private ArrayList<String> allData = new ArrayList<String>();
    private ArrayList<Integer> allDup = new ArrayList<Integer>();

    private ArrayList<String> qos0data = new ArrayList<String>();
    private ArrayList<String> qos1data = new ArrayList<String>();
    private ArrayList<String> qos2data = new ArrayList<String>();

    private ArrayList<Long> qos0time = new ArrayList<Long>();
    private ArrayList<Long> qos1time = new ArrayList<Long>();
    private ArrayList<Long> qos2time = new ArrayList<Long>();


    int qos0dup;
    int qos1dup;
    int qos2dup;
    int qos0ooo;
    int qos1ooo;
    int qos2ooo;
    double qos0rate;
    double qos1rate;
    double qos2rate;
    double qos0drate;
    double qos1drate;
    double qos2drate;
    double qos0orate;
    double qos1orate;
    double qos2orate;
    double qos0gap;
    double qos1gap;
    double qos2gap;

    File outputPlace;
    BufferedWriter out;

    InputStreamReader reader;
    BufferedReader in;



    DecimalFormat df = new DecimalFormat("0.00000");

    private MqttClient client;

    public SubscriberMain(ArrayList<String> topics) throws MqttException {

        this.topics = topics;

        outputPlace = new File("data.txt");
        try {
            outputPlace.createNewFile();
        } catch (IOException e) {
            System.out.println("Error in create data file");
            System.exit(1);
            e.printStackTrace();
        }

        try {
            out = new BufferedWriter(new FileWriter(outputPlace));
        } catch (IOException e) {
            System.out.println("Error in create writer");
            System.exit(1);
            e.printStackTrace();
        }


        String host = "tcp://comp3310.ddns.net:1883";
        String username = "students";
        String password = "33106331";
        String clientId = "3310-u6211344";


        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        connectOptions.setAutomaticReconnect(true);
        connectOptions.setUserName(username);
        connectOptions.setPassword(password.toCharArray());
        connectOptions.setConnectionTimeout(1000);
        connectOptions.setKeepAliveInterval(10);

        this.client = new MqttClient(host, clientId, new MemoryPersistence());
        this.client.setCallback(this);
        System.out.println("Connecting to broker: "+ host);
        this.client.connect(connectOptions);
        System.out.println("Connected");

        for (int i = 0; i < topics.size(); i ++){
            this.client.subscribe(this.topics.get(i), i);
            int dup = 0;
            allDup.add(dup);

        }
    }


    private void stop() throws MqttException {
        try {
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println("Fail to close the file");
            e.printStackTrace();
        }

        this.client.disconnect();
        this.client.close();
    }


    public void connectionLost(Throwable throwable) {
        System.out.println("Connection lost because: " + throwable);
        System.exit(1);
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

        if (mqttMessage.isDuplicate()){
            allDup.set(mqttMessage.getQos(), allDup.get(mqttMessage.getQos()) + 1);
        }

        out.write(mqttMessage.getQos() + ":" + new String(mqttMessage.getPayload()) + "@" + System.currentTimeMillis() + "\r\n");
        allData.add(mqttMessage.getQos() + ":" + new String(mqttMessage.getPayload()) + "@" + System.currentTimeMillis());

    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }

    private void analyze(){

        try {
            reader = new InputStreamReader(new FileInputStream("data.txt"));
            in = new BufferedReader(reader);
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
            e.printStackTrace();
        }

        try {
            String line=null;
            while ((line = in.readLine()) != null && line.length() > 20) {
                int index1 = line.indexOf(':');
                int index2 = line.indexOf('@');
                String data = line.substring(index1 + 1, index2);
                Long timeStamp = Long.parseLong(line.substring(index2 + 1));
                System.out.println(data);
                System.out.println(timeStamp);
                if (line.charAt(0) == '0'){
                    qos0data.add(data);
                    qos0time.add(timeStamp);
                } else if (line.charAt(0) == '1'){
                    qos1data.add(data);
                    qos1time.add(timeStamp);
                } else if (line.charAt(0) == '2'){
                    qos2data.add(data);
                    qos2time.add(timeStamp);
                }
            }
            in.close();


        } catch (IOException e) {
            System.out.println("Error in reading the file");
            e.printStackTrace();
        }

        qos0dup = allDup.get(0);
        qos1dup = allDup.get(1);
        qos2dup = allDup.get(2);
    }


    private boolean isNumber(String string){
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(string).matches();
    }




    /*
    * Calculate the result
    * Duplicate Rate - drate
    * Speed Rate - rate
    * Out of Order Rate -orate
    * Inter-data-gap - gap
    * Standard Deviation - gvar
    * */

    private void calculate(){

        /*
        * calculate the duplicate rate for each Qos
        * duplicate rate = number of duplicate / total
        * */
        qos0drate = (double)qos0dup / (double)qos0data.size();
        qos1drate = (double)qos1dup / (double)qos1data.size();
        qos2drate = (double)qos2dup / (double)qos2data.size();

        /*
        * calculate the speed rate for each Qos
        * speed rate = total number / total time
        * total time = the timestamp of last message - the timestamp of the first message
        * */
        qos0rate = (double)1000 * qos0data.size() / (double) (qos0time.get(qos0time.size() - 1) - qos0time.get(0));
        qos1rate = (double)1000 * qos1data.size() / (double) (qos1time.get(qos1time.size() - 1) - qos1time.get(0));
        qos2rate = (double)1000 * qos2data.size() / (double) (qos2time.get(qos2time.size() - 1) - qos2time.get(0));



        /*
        * calculate the out of order for each Qos
        * if the data is number, compare it to the last one
        * if it is smaller than the last one
        * then it is out of order
        * and update the "last" number
        * */
        int last = -1;
        for (String qos0datum : qos0data) {
            if (isNumber(qos0datum)) {
                if (Integer.parseInt(qos0datum) < last) {
                    qos0ooo++;
                }
                last = Integer.parseInt(qos0datum);
            }
        }

        last = -1;
        for (String qos1datum : qos1data) {
            if (isNumber(qos1datum)) {
                if (Long.parseLong(qos1datum) < last) {
                    qos1ooo++;
                }
                last = Integer.parseInt(qos1datum);
            }
        }

        last = -1;
        for (String qos2datum : qos2data) {
            if (isNumber(qos2datum)) {
                if (Long.parseLong(qos2datum) < last) {
                    qos2ooo++;
                }
                last = Integer.parseInt(qos2datum);
            }
        }


        /*
        * calculate the out of order rate for each Qos
        * out-of-order rate = number of ooo / total number
        * */
        qos0orate = (double) qos0ooo / (double) qos0data.size();
        qos1orate = (double) qos1ooo / (double) qos1data.size();
        qos2orate = (double) qos2ooo / (double) qos2data.size();


        /*
        * calculate the inter-message-gap for each Qos
        * inter-message-gap = the timestamp of last message - the timestamp of the first message  / total number
        * */
        qos0gap = (double) (qos0time.get(qos0time.size() - 1) - qos0time.get(0)) / (double)qos0data.size();
        qos1gap = (double) (qos1time.get(qos1time.size() - 1) - qos1time.get(0)) / (double)qos1data.size();
        qos2gap = (double) (qos2time.get(qos2time.size() - 1) - qos2time.get(0)) / (double)qos2data.size();

    }



    /*
    * print out the result
    * */
    private void print(){
        System.out.println("\nQos0 receive: " + qos0data.size() + ", with: " + df.format(qos0drate) + "% duplicate.");
        System.out.println("Qos1 receive: " + qos1data.size() + ", with: " + df.format(qos1drate) + "% duplicate.");
        System.out.println("Qos2 receive: " + qos2data.size() + ", with: " + df.format(qos2drate) + "% duplicate.");

        System.out.println("\nQos0 rate: " + df.format(qos0rate) + " per sec.");
        System.out.println("Qos1 rate: " + df.format(qos1rate) + " per sec.");
        System.out.println("Qos2 rate: " + df.format(qos2rate) + " per sec.");

        System.out.println("\nQos0 out-of-order rate: " + df.format(qos0orate) + "%");
        System.out.println("Qos1 out-of-order rate: " + df.format(qos1orate) + "%");
        System.out.println("Qos2 out-of-order rate: " + df.format(qos2orate) + "%");

        System.out.println("\nQos0 inter-message-gap: " + df.format(qos0gap) + "%");
        System.out.println("Qos1 inter-message-gap: " + df.format(qos1gap) + "%");
        System.out.println("Qos2 inter-message-gap: " + df.format(qos2gap) + "%");
    }




    public static void main(String[] args) throws MqttException {
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("counter/fast/q0");
        topics.add("counter/fast/q1");
        topics.add("counter/fast/q2");


        final SubscriberMain subscriberMain = new SubscriberMain(topics);

        Timer timer = new Timer();

        final TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    subscriberMain.analyze();
                    subscriberMain.stop();
                    subscriberMain.calculate();
                    subscriberMain.print();
                    cancel();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(timerTask, 100000);

    }
}
