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
 * MQTT协议接入OneNet平台工具类
 * @author Shay Zhi
 * @version 2020-03-17
 */
public class Utils {
	/**
	 * 连接OneNet所需的服务器信息与账号信息
	 */
	private static String broker = "tcp://183.230.40.39";	// OneNet服务器地址（固定）
	private static Integer port = 6002;						// OneNet服务器断开（固定）
	private static String clientId = "588189007";	// 设备ID
	private static String userName = "326926";	// 产品ID
	private static String password = "wlpub001";	// 鉴权信息
	
	/**
	 * 客户端设置
	 */
	private MqttClient mqttClient;		// 客户端对象
	private MqttConnectOptions options;	// 连接参数对象
	private MqttCallback clientCallback; // 回调方法
	
	/**
	 * 创建连接，并连接至OneNet服务器
	 */
	public void connect() {
		Date date = new Date();
		DateFormat df = DateFormat.getDateTimeInstance();
		
		// 客户端对象为空，创建客户端对象
		if(mqttClient == null) {
			// 创建连接参数对象
			options = new MqttConnectOptions();
			options.setUserName(userName);
			options.setPassword(password.toCharArray());
			options.setCleanSession(false);
			// 在订阅主题后执行的回调方法
			clientCallback = new MqttCallback() {
				@Override
				public void connectionLost(Throwable arg0) {
					// 连接断开时执行
					System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] 连接断开 ...");
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken arg0) {
					// publish成功后执行
					System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] 消息推送成功");
				}

				@Override
				public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
					// 接收到订阅信息后执行
					System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] 接收消息："+arg1);
				}
			};
			// 创建MqttClient对象（服务器地址，客户端ID）
			try {
				mqttClient = new MqttClient(broker+":"+port, clientId);
				mqttClient.setCallback(clientCallback);
			} catch (MqttException e) {
				e.printStackTrace();
			}
		}
		// 创建了MqttClient客户端对象后，通过该对象连接到OneNet服务器
		if(!mqttClient.isConnected()){
			try {
				mqttClient.connect(options);
				if(mqttClient.isConnected()) {
					System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] 连接成功 ...");
				}
			} catch (Exception e) {
				System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] 无法连接，网络错误 ...");
			}
		}
	}
	
	/**
	 * 客户端连接状态
	 */
	public boolean isConnected() {
		return mqttClient.isConnected();
	}

	/**
	 * 获取设备ID
	 */
	public String getDeviceId() {
		return this.clientId;
	}
	
	/**
	 * 获取产品ID
	 */
	public String getProductId() {
		return this.userName;
	}
	
	/**
	 * 重新连接
	 */
	public void reConnect() {
		this.connect();
	}
	
	/**
	 * DataPoint 数据格式封装
	 * 除了payload字符串外，额外多加3个字节
	 * 	1. 上传数据点的数据类型：此处为3（16进制0x03）
	 * 	2. payload字符串长度length的高位字节（8位）
	 * 	3. payload字符串长度length的低位字节（8位）
	 */
	public byte[] string2dpType(String payload) {
		int length = payload.length();
		byte[] dpData = new byte[length+3];		// 封装后的字节数组比payload多3个字节
		dpData[0] = 0x03;						// 第一个字节固定为：数据类型
		dpData[1] = (byte) (length >> 8);		// 第二个字节固定为：payload长度的高8位
		dpData[2] = (byte) length;				// 第三个字节固定为：payload长度的低8位
		byte[] payloadBytes = payload.getBytes();
		for(int i=0; i<length; i++) {	// 将payload的字节数组追加到dpData中
			dpData[3+i] = payloadBytes[i];
		}
		return dpData;
	}
	
	/**
	 * 数据点上传（数据需要根据OneNet的格式封装）
	 */
	public void publish(String topic, String payload) {
		try {
			mqttClient.publish(topic, string2dpType(payload), 0, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
