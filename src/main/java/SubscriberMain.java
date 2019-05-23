import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.URL;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

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
    }

    public void output() throws MqttException {
        System.out.println("Qos0: " + qos0data.size() + ", with: " + qos0dup + " duplicate.");
        System.out.println("Qos1: " + qos1data.size() + ", with: " + qos1dup + " duplicate.");
        System.out.println("Qos2: " + qos2data.size() + ", with: " + qos2dup + " duplicate.");
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
        System.out.println(mqttMessage.getQos() + ": " + new String(mqttMessage.getPayload()));
    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }

    public static void main(String[] args) throws MqttException {
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("counter/slow/q0");
        topics.add("counter/slow/q1");
        topics.add("counter/slow/q2");


        final SubscriberMain subscriberMain = new SubscriberMain(topics);

        Timer timer = new Timer();

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    subscriberMain.stop();
                    subscriberMain.output();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(timerTask, 30000);

    }
}
