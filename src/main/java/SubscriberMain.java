import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.URL;
import java.util.ArrayList;

public class SubscriberMain implements MqttCallback {

    private ArrayList<String> topics = new ArrayList<String>();

    int qos0count;
    int qos1count;
    int qos2count;

    private MqttClient client;

    public SubscriberMain(ArrayList<String> topics) throws MqttException {

        this.topics = topics;


        String host = String.format("tcp://%s:%d", "comp3310.ddns.net", 1883);
        String username = "students";
        String password = "33106331";
        String clientId = "3310-u6xxxxxx";


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


    public void connectionLost(Throwable throwable) {
        System.out.println("Connection lost because: " + throwable);
        System.exit(1);
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        if (mqttMessage.getQos() == 0){
            qos0count ++;
        } else if (mqttMessage.getQos() == 1){
            qos1count ++;
        } else if (mqttMessage.getQos() == 2){
            qos2count ++;
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


        SubscriberMain subscriberMain = new SubscriberMain(topics);
    }
}
