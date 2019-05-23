import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

public class SubscriberMain implements MqttCallback {

    private ArrayList<String> topics = new ArrayList<String>();
    private ArrayList<String> qos0data = new ArrayList<String>();
    private ArrayList<String> qos1data = new ArrayList<String>();
    private ArrayList<String> qos2data = new ArrayList<String>();
    private ArrayList<Long> qos0time = new ArrayList<Long>();
    private ArrayList<Long> qos1time = new ArrayList<Long>();
    private ArrayList<Long> qos2time = new ArrayList<Long>();

    private int qos0dup;
    private int qos1dup;
    private int qos2dup;
    private int qos0ooo;
    private int qos1ooo;
    private int qos2ooo;
    private double qos0rate;
    private double qos1rate;
    private double qos2rate;
    private double qos0d;
    private double qos1d;
    private double qos2d;
    private double qos0o;
    private double qos1o;
    private double qos2o;
    private double qos0mean;
    private double qos1mean;
    private double qos2mean;
    private double qos0gvar;
    private double qos1gvar;
    private double qos2gvar;

    private MqttClient client;

    private DecimalFormat df = new DecimalFormat("0.00000");


    public SubscriberMain(ArrayList<String> topics) throws MqttException {

        this.topics = topics;


        String host = "tcp://comp3310.ddns.net:1883";
        String username = "students";
        String password = "33106331";
        String clientId = "3310-u6211344";


        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        connectOptions.setAutomaticReconnect(true);
        connectOptions.setUserName(username);
        connectOptions.setPassword(password.toCharArray());
        connectOptions.setConnectionTimeout(100);
        connectOptions.setKeepAliveInterval(10);

        this.client = new MqttClient(host, clientId, new MemoryPersistence());
        this.client.setCallback(this);
        System.out.println("Connecting to broker: "+ host);
        try {
            this.client.connect(connectOptions);
        } catch (MqttException e){
            System.out.println("Fail to connect!");
            System.exit(1);
        }
        System.out.println("Connected");

        for (int i = 0; i < topics.size(); i ++){
            System.out.println("Subscribe: " + topics.get(i));
            this.client.subscribe(topics.get(i), i);

        }
    }


    private void stop() throws MqttException {

        try {
            this.client.disconnect();
            this.client.close();
        } catch (MqttException e){
            System.out.println("Fail to stop!");
        }

    }


    public void connectionLost(Throwable throwable) {
        System.out.println("Connection lost because: " + throwable);
        System.exit(1);
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        if (mqttMessage.getQos() == 0){
            if (mqttMessage.isDuplicate()){
                qos0dup ++;
            }
            qos0data.add(new String(mqttMessage.getPayload()));
            qos0time.add(System.currentTimeMillis());
        } else if (mqttMessage.getQos() == 1){
            if (mqttMessage.isDuplicate()){
                qos1dup ++;
            }
            qos1data.add(new String(mqttMessage.getPayload()));
            qos1time.add(System.currentTimeMillis());
        } else if (mqttMessage.getQos() == 2){
            if (mqttMessage.isDuplicate()){
                qos2dup ++;
            }
            qos2data.add(new String(mqttMessage.getPayload()));
            qos2time.add(System.currentTimeMillis());
        }
    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }


    private boolean isNumber(String string){
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(string).matches();
    }

    private void calculateDup(){
        qos0d = (double)qos0dup / (double)qos0data.size();
        qos1d = (double)qos1dup / (double)qos1data.size();
        qos2d = (double)qos2dup / (double)qos2data.size();

        System.out.println("\nQos0 receive: " + qos0data.size() + ", with: " + df.format(qos0d) + "% duplicate.");
        System.out.println("Qos1 receive: " + qos1data.size() + ", with: " + df.format(qos1d) + "% duplicate.");
        System.out.println("Qos2 receive: " + qos2data.size() + ", with: " + df.format(qos2d) + "% duplicate.");
    }


    private void calculateRate(){
        qos0rate = (double)1000 * qos0data.size() / (double) (qos0time.get(qos0time.size() - 1) - qos0time.get(0));
        qos1rate = (double)1000 * qos1data.size() / (double) (qos1time.get(qos1time.size() - 1) - qos1time.get(0));
        qos2rate = (double)1000 * qos2data.size() / (double) (qos2time.get(qos2time.size() - 1) - qos2time.get(0));

        System.out.println(qos0time.get(qos0time.size() - 1) - qos0time.get(0));
        System.out.println(qos1time.get(qos1time.size() - 1) - qos1time.get(0));
        System.out.println(qos2time.get(qos2time.size() - 1) - qos2time.get(0));

        System.out.println("\nQos0 rate: " + df.format(qos0rate) + " message per second.");
        System.out.println("Qos1 rate: " + df.format(qos1rate) + " message per second.");
        System.out.println("Qos2 rate: " + df.format(qos2rate) + " message per second.");
    }

    private void calculateOoo(){
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


        qos0o = (double) qos0ooo / (double) qos0data.size();
        qos1o = (double) qos1ooo / (double) qos1data.size();
        qos2o = (double) qos2ooo / (double) qos2data.size();

        System.out.println("\nQos0 out-of-order rate: " + df.format(qos0o) + "%");
        System.out.println("Qos1 out-of-order rate: " + df.format(qos1o) + "%");
        System.out.println("Qos2 out-of-order rate: " + df.format(qos2o) + "%");
    }


    private void calculateTime(){

        qos0gvar = 0;
        qos1gvar = 0;
        qos2gvar = 0;

        qos0mean = (double) (qos0time.get(qos0time.size() - 1) - qos0time.get(0)) / (double)qos0data.size();
        qos1mean = (double) (qos1time.get(qos1time.size() - 1) - qos1time.get(0)) / (double)qos1data.size();
        qos2mean = (double) (qos2time.get(qos2time.size() - 1) - qos2time.get(0)) / (double)qos2data.size();

        System.out.println("\nQos0 mean gap between messages: " + df.format(qos0mean) + " ms");
        System.out.println("Qos1 mean gap between messages: " + df.format(qos1mean) + " ms");
        System.out.println("Qos2 mean gap between messages: " + df.format(qos2mean) + " ms");

        for (int i = 1; i < qos0time.size(); i ++) {
            qos0gvar =+ Math.pow(qos0time.get(i) - qos0time.get(i-1) - qos0mean, 2) / qos0time.size();
        }

        for (int i = 1; i < qos1time.size(); i ++) {
            qos1gvar =+ Math.pow(qos1time.get(i) - qos1time.get(i-1) - qos1mean, 2) / qos1time.size();
        }

        for (int i = 1; i < qos2time.size(); i ++) {
            qos2gvar =+ Math.pow(qos2time.get(i) - qos2time.get(i-1) - qos2mean, 2) / qos2time.size();
        }
        System.out.println("\nQos0 inter-message-gap variation: " + df.format(Math.sqrt(qos0gvar)) + " ms");
        System.out.println("Qos1 inter-message-gap variation: " + df.format(Math.sqrt(qos1gvar)) + " ms");
        System.out.println("Qos2 inter-message-gap variation: " + df.format(Math.sqrt(qos2gvar)) + " ms");

    }



    public void calculate(){

        calculateDup();
        calculateRate();
        calculateOoo();
        calculateTime();

    }

    public static void main(String[] args) throws MqttException {
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("counter/fast/q0");
        topics.add("counter/fast/q1");
        topics.add("counter/fast/q2");


        final SubscriberMain subscriberMain = new SubscriberMain(topics);

        Timer timer = new Timer();

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    subscriberMain.stop();
                    subscriberMain.calculate();
                    System.exit(0);
                } catch (MqttException e) {

                    System.out.println("Something wrong with MQTT!");
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(timerTask, 30000);

    }
}
