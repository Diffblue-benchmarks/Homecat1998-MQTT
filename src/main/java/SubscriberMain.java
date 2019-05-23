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

    DecimalFormat df = new DecimalFormat("0.00000");

    private MqttClient client;

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
        connectOptions.setConnectionTimeout(1000);
        connectOptions.setKeepAliveInterval(10);

        this.client = new MqttClient(host, clientId, new MemoryPersistence());
        this.client.setCallback(this);
        System.out.println("Connecting to broker: "+ host);
        this.client.connect(connectOptions);
        System.out.println("Connected");

        for (int i = 0; i < topics.size(); i ++){
            this.client.subscribe(this.topics.get(i), i);

        }
    }


    private void stop() throws MqttException {
        this.client.disconnect();
        this.client.close();
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



    private void calculate(){

        qos0drate = (double)qos0dup / (double)qos0data.size();
        qos1drate = (double)qos1dup / (double)qos1data.size();
        qos2drate = (double)qos2dup / (double)qos2data.size();


        qos0rate = (double)1000 * qos0data.size() / (double) (qos0time.get(qos0time.size() - 1) - qos0time.get(0));
        qos1rate = (double)1000 * qos1data.size() / (double) (qos1time.get(qos1time.size() - 1) - qos1time.get(0));
        qos2rate = (double)1000 * qos2data.size() / (double) (qos2time.get(qos2time.size() - 1) - qos2time.get(0));


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

        qos0orate = (double) qos0ooo / (double) qos0data.size();
        qos1orate = (double) qos1ooo / (double) qos1data.size();
        qos2orate = (double) qos2ooo / (double) qos2data.size();


    }


    private void print(){
        System.out.println("Qos0 receive: " + qos0data.size() + ", with: " + df.format(qos0drate) + "% duplicate.");
        System.out.println("Qos1 receive: " + qos1data.size() + ", with: " + df.format(qos1drate) + "% duplicate.");
        System.out.println("Qos2 receive: " + qos2data.size() + ", with: " + df.format(qos2drate) + "% duplicate.");

        System.out.println("Qos0 rate: " + df.format(qos0rate) + " per sec.");
        System.out.println("Qos1 rate: " + df.format(qos1rate) + " per sec.");
        System.out.println("Qos2 rate: " + df.format(qos2rate) + " per sec.");

        System.out.println("Qos0 out-of-order rate: " + df.format(qos0orate) + "%");
        System.out.println("Qos1 out-of-order rate: " + df.format(qos1orate) + "%");
        System.out.println("Qos2 out-of-order rate: " + df.format(qos2orate) + "%");
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
                    subscriberMain.print();
                    System.exit(0);
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(timerTask, 30000);

    }
}
