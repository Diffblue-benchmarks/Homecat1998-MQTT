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
    double qos0d;
    double qos1d;
    double qos2d;
    double qos0o;
    double qos1o;
    double qos2o;

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


    public void stop() throws MqttException {
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


    public boolean isNumber (String string){
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(string).matches();
    }



    public void calculate(){

        qos0d = (double)qos0dup / (double)qos0data.size();
        qos1d = (double)qos1dup / (double)qos1data.size();
        qos2d = (double)qos2dup / (double)qos2data.size();

        System.out.println("Qos0 receive: " + qos0data.size() + ", with: " + qos0d + "% duplicate.");
        System.out.println("Qos1 receive: " + qos1data.size() + ", with: " + qos1d + "% duplicate.");
        System.out.println("Qos2 receive: " + qos2data.size() + ", with: " + qos2d + "% duplicate.");

        qos0rate = (double)1000 * qos0data.size() / (double) (qos0time.get(qos0time.size() - 1) - qos0time.get(0));
        qos1rate = (double)1000 * qos1data.size() / (double) (qos1time.get(qos1time.size() - 1) - qos1time.get(0));
        qos2rate = (double)1000 * qos2data.size() / (double) (qos2time.get(qos2time.size() - 1) - qos2time.get(0));

        System.out.println("Qos0 rate: " + qos0rate);
        System.out.println("Qos1 rate: " + qos1rate);
        System.out.println("Qos2 rate: " + qos2rate);

        int last = -1;

        for (int i = 0; i < qos0data.size(); i ++){

            if (isNumber(qos0data.get(i))){
                if (Integer.parseInt(qos0data.get(i)) < last){
                    qos0ooo ++;
                }
                last = Integer.parseInt(qos0data.get(i));
            }
        }

        last = -1;

        for (int i = 0; i < qos1data.size(); i ++){

            if (isNumber(qos1data.get(i))){
                if (Long.parseLong(qos1data.get(i)) < last){
                    qos1ooo ++;
                }
                last = Integer.parseInt(qos1data.get(i));
            }
        }

        last = -1;

        for (int i = 0; i < qos2data.size(); i ++){

            if (isNumber(qos2data.get(i))){
                if (Long.parseLong(qos2data.get(i)) < last){
                    qos2ooo ++;
                }
                last = Integer.parseInt(qos2data.get(i));
            }
        }

        qos0o = (double) qos0ooo / (double) qos0data.size();
        qos1o = (double) qos1ooo / (double) qos1data.size();
        qos2o = (double) qos2ooo / (double) qos2data.size();


        System.out.println("Qos0 out-of-order rate: " + qos0o + "%");
        System.out.println("Qos1 out-of-order rate: " + qos1o + "%");
        System.out.println("Qos2 out-of-order rate: " + qos2o + "%");



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
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(timerTask, 30000);

    }
}
