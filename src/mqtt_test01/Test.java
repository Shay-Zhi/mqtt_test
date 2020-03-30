package mqtt_test01;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import java.text.DateFormat;
import java.util.Date;
import java.util.Random;

public class Test {
    private MqttClient client;
    private MqttConnectOptions options;
    private static String[] myTopics = { "6666" };
    private static int[] myQos = { 0 };
    
    public static void main(String[] args) {
    	// ≤‚ ‘App.java¿‡
        App myMqtt = new App("Java_Client_For_EMQX", myTopics, myQos);
    }
}
