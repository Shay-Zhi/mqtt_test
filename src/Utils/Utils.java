package Utils;

import java.text.DateFormat;
import java.util.*;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

/**
 * MQTTЭ�����OneNetƽ̨������
 * @author Shay Zhi
 * @version 2020-03-17
 */
public class Utils {
	/**
	 * ����OneNet����ķ�������Ϣ���˺���Ϣ
	 */
	private static String broker = "tcp://183.230.40.39";	// OneNet��������ַ���̶���
	private static Integer port = 6002;						// OneNet�������Ͽ����̶���
	private static String clientId = "588189007";	// �豸ID
	private static String userName = "326926";	// ��ƷID
	private static String password = "wlpub001";	// ��Ȩ��Ϣ
	
	/**
	 * �ͻ�������
	 */
	private MqttClient mqttClient;		// �ͻ��˶���
	private MqttConnectOptions options;	// ���Ӳ�������
	private MqttCallback clientCallback; // �ص�����
	
	/**
	 * �������ӣ���������OneNet������
	 */
	public void connect() {
		Date date = new Date();
		DateFormat df = DateFormat.getDateTimeInstance();
		
		// �ͻ��˶���Ϊ�գ������ͻ��˶���
		if(mqttClient == null) {
			// �������Ӳ�������
			options = new MqttConnectOptions();
			options.setUserName(userName);
			options.setPassword(password.toCharArray());
			options.setCleanSession(false);
			// �ڶ��������ִ�еĻص�����
			clientCallback = new MqttCallback() {
				@Override
				public void connectionLost(Throwable arg0) {
					// ���ӶϿ�ʱִ��
					System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] ���ӶϿ� ...");
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken arg0) {
					// publish�ɹ���ִ��
					System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] ��Ϣ���ͳɹ�");
				}

				@Override
				public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
					// ���յ�������Ϣ��ִ��
					System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] ������Ϣ��"+arg1);
				}
			};
			// ����MqttClient���󣨷�������ַ���ͻ���ID��
			try {
				mqttClient = new MqttClient(broker+":"+port, clientId);
				mqttClient.setCallback(clientCallback);
			} catch (MqttException e) {
				e.printStackTrace();
			}
		}
		// ������MqttClient�ͻ��˶����ͨ���ö������ӵ�OneNet������
		if(!mqttClient.isConnected()){
			try {
				mqttClient.connect(options);
				if(mqttClient.isConnected()) {
					System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] ���ӳɹ� ...");
				}
			} catch (Exception e) {
				System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] �޷����ӣ�������� ...");
			}
		}
	}
	
	/**
	 * �ͻ�������״̬
	 */
	public boolean isConnected() {
		return mqttClient.isConnected();
	}

	/**
	 * ��ȡ�豸ID
	 */
	public String getDeviceId() {
		return this.clientId;
	}
	
	/**
	 * ��ȡ��ƷID
	 */
	public String getProductId() {
		return this.userName;
	}
	
	/**
	 * ��������
	 */
	public void reConnect() {
		this.connect();
	}
	
	/**
	 * DataPoint ���ݸ�ʽ��װ
	 * ����payload�ַ����⣬������3���ֽ�
	 * 	1. �ϴ����ݵ���������ͣ��˴�Ϊ3��16����0x03��
	 * 	2. payload�ַ�������length�ĸ�λ�ֽڣ�8λ��
	 * 	3. payload�ַ�������length�ĵ�λ�ֽڣ�8λ��
	 */
	public byte[] string2dpType(String payload) {
		int length = payload.length();
		byte[] dpData = new byte[length+3];		// ��װ����ֽ������payload��3���ֽ�
		dpData[0] = 0x03;						// ��һ���ֽڹ̶�Ϊ����������
		dpData[1] = (byte) (length >> 8);		// �ڶ����ֽڹ̶�Ϊ��payload���ȵĸ�8λ
		dpData[2] = (byte) length;				// �������ֽڹ̶�Ϊ��payload���ȵĵ�8λ
		byte[] payloadBytes = payload.getBytes();
		for(int i=0; i<length; i++) {	// ��payload���ֽ�����׷�ӵ�dpData��
			dpData[3+i] = payloadBytes[i];
		}
		return dpData;
	}
	
	/**
	 * ���ݵ��ϴ���������Ҫ����OneNet�ĸ�ʽ��װ��
	 */
	public void publish(String topic, String payload) {
		try {
			mqttClient.publish(topic, string2dpType(payload), 0, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
