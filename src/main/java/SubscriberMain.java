import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.URL;
import java.util.ArrayList;

public class SubscriberMain implements MqttCallback {


    private String topic0 = "counter/fast/q0";
    private String topic1 = "counter/fast/q1";
    private String topic2 = "counter/fast/q2";
    private MqttClient client;

    public SubscriberMain() throws MqttException {


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
        this.client.subscribe(this.topic0, 0);
        this.client.subscribe(this.topic1, 1);
        this.client.subscribe(this.topic2, 2);
    }


    public void connectionLost(Throwable throwable) {
        System.out.println("Connection lost because: " + throwable);
        System.exit(1);
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        System.out.println(mqttMessage.getQos() + ": " + new String(mqttMessage.getPayload()));
    }

    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }

    public static void main(String[] args) throws MqttException {
        SubscriberMain subscriberMain = new SubscriberMain();
    }
}
