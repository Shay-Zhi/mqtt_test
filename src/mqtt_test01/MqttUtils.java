package mqtt_test01;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttTopic;

/**
 * ����EMQXƽ̨�Ĺ�����
 * @author Shay Zhi
 * @version 2020-03-20
 */
public class MqttUtils {
	/**
	 * ����EMQX����ķ�������Ϣ
	 */
	private static String broker = "tcp://172.16.7.15";	
	private static Integer port = 1883;			
    private MqttTopic mqttTopic;						// �����������
    private String myTopic = "5122";					// Ĭ�ϵĶ�������
    private MqttMessage message;						// MQTT�������ݸ�ʽ����
    
	// EMQX��ʱ����Ҫ�˺�����
	private static String clientId = "588189007";	
	private static String userName = "326926";	
	private static String password = "wlpub001";	
	
	/**
	 * �ͻ�������
	 */
	private MqttClient mqttClient;		// �ͻ��˶���
	private MqttConnectOptions options;	// ���Ӳ�������
	private MqttCallback clientCallback; // �ص�����
	
	/**
	 * ���ݳ־û����
	 */
	private DBConn conn = new DBConn();
	
	/**
	 * MQTT���Ӳ���
	 * 
	 */
	
	/**
	 * �������ӣ���������EMQX������
	 */
	public MqttClient connect() {
		// �ͻ��˶���Ϊ�գ������ͻ��˶���
		if(mqttClient == null) {
			// �������Ӳ�������
			options = new MqttConnectOptions();
			options.setCleanSession(false);
			
			// ��ʱ����Ҫ�˺�����
			//options.setUserName(userName);
			//options.setPassword(password.toCharArray());
			
			// �ڶ��������ִ�еĻص�����
			clientCallback = new MqttCallback() {
				@Override
				public void connectionLost(Throwable arg0) {
					Date date = new Date();
					DateFormat df = DateFormat.getDateTimeInstance();
					// ���ӶϿ�ʱִ��
					//System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] ���ӶϿ� ...");
					System.out.println("["+df.format(date)+"] ["+broker+"] ���ӶϿ� ...");
                    conn.closeDB();
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken arg0) {
					Date date = new Date();
					DateFormat df = DateFormat.getDateTimeInstance();
					// publish�ɹ���ִ��
					//System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] ��Ϣ���ͳɹ�");
					System.out.println("["+df.format(date)+"] ["+broker+"] ��Ϣ���ͳɹ�");
                    conn.closeDB();
				}

				@Override
				public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
					Date date = new Date();
					DateFormat df = DateFormat.getDateTimeInstance();
					
					// ���յ�������Ϣ��ִ��
					//System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] ������Ϣ��"+arg1);
					System.out.println("["+df.format(date)+"] ["+broker+"] ������Ϣ��"+arg1.getPayload().toString());
					
					// ��������Ƶ����Ϣ�����ݳ־û�
                    Map<String,String> payload = new HashMap();
                    payload = parsePayload(arg1);
                    save2DB(payload);
                    System.out.println("["+df.format(date)+"] ["+broker+"] ��Ϣ�ѱ���");
                    
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
		// ������MqttClient�ͻ��˶����ͨ���ö������ӵ�EMQX������
		if(!mqttClient.isConnected()){
			Date date = new Date();
			DateFormat df = DateFormat.getDateTimeInstance();
			try {
				mqttClient.connect(options);
				if(mqttClient.isConnected()) {
					System.out.println("["+df.format(date)+"] ["+broker+"] ���ӳɹ� ...");
					return mqttClient;
				}
			} catch (Exception e) {
				System.out.println("["+df.format(date)+"] ["+broker+"] �޷����ӣ�������� ...");
				return null;
			}
		}
		return null;
	}

	/**
	 * �ͻ�������״̬
	 */
	public boolean isConnected() {
		return mqttClient.isConnected();
	}

	/**
	 * ��������
	 */
	public MqttClient reConnect() {
		return this.connect();
	}
	
	/**
	 * ���ݳ־û�����
	 * 
	 */
	
	/**
	 * �����Ӷ���������յ���payload����
	 * @param payload ������Ϣ
	 * @return �������payload
	 * @throws UnsupportedEncodingException
	 */
	private Map<String,String> parsePayload(MqttMessage payload) throws UnsupportedEncodingException {
    	// �������յ�payload����
        Map<String, String> payloadMap = new HashMap();
    	
        String msg = new String(payload.getPayload());
        String tmp = msg.replace(" ","").replace(":{", "|{").split("\\|")[1].replace("{","").replace("}","").replace("\\s","").replace("\n","");	// ɾ���س����еȿհ׷�
        //System.out.println(msg);
        String tmp1[] = tmp.split(",");
        
        for(int i = 0; i < tmp1.length; i ++) {
            tmp1[i] = tmp1[i].replace("\"", "");
        	String tmp2[] = tmp1[i].split(":");
        	tmp2[1] = new String (tmp2[1].getBytes("GBK"),"UTF-8");		// ת������
        	payloadMap.put(tmp2[0], tmp2[1]);
        	//System.out.println(tmp2[0] + " -- " + tmp2[1]);
        }
		
        // ��־���
	    //System.out.println(payloadMap); 
        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//�������ڸ�ʽ
	    System.out.print(df.format(new Date()));
	    for (Map.Entry<String, String> entry : payloadMap.entrySet()) {
		   System.out.print(entry.getKey() + "=" + entry.getValue() + " | "); 
	    }
	    System.out.print("\n");
		 
	    return payloadMap;
    }
    
	/**
	 * �����յĶ�����Ϣ���浽MySQL��
	 * @param payload ������Ϣ
	 */
    private void save2DB(Map<String, String> payload) {
    	String saveDetailInfoSql = "insert into `mqtt`(`x_coord`,`y_coord`,`z_coord`,`ip`,`a_speed`,`a_load`,`a_feed`,`x_coord1`,`y_coord1`,`z_coord1`,`b_feed`,`publish_date`,`publish_time`) " + 
        		"values ('" + payload.get("x_coord") + "','" + payload.get("y_coord") + "','" + payload.get("z_coord") + 
        		"','" + payload.get("ip") + "','" + payload.get("a_speed") + "','" + payload.get("a_load") + 
        		"','" + payload.get("a_feed") + "','" + payload.get("x_coord1") + "','" + payload.get("y_coord1") + 
        		"','" + payload.get("z_coord1") + "','" + payload.get("b_feed") + "',current_date(),current_time())";
        String updateStateSql = "update `device` set `state`='" + payload.get("state") + "' where `ip`='" + payload.get("ip") + "'";
    	//System.out.println(saveDetailInfoSql);
    	//System.out.println(updateStateSql);
    	
    	conn.executeSql(saveDetailInfoSql);
    	conn.executeSql(updateStateSql);
    }
    
    /**
     * ������ز���
     * 
     */
    
    /**
     * 
     * ��Ĭ������5122������Ϣ
     * @param msg ��Ϊpayload
     * @param client MqttClient�ͻ��˶���
     */
    public void sendMessage(MqttClient client, String msg) {
        sendMessage(client, myTopic, msg);
    }
    
    /**
     * ��ָ�����ⷢ����Ϣ
     * @param topic ָ����������
     * @param msg payload
     * @param client MqttClient�ͻ��˶���
     */
    public void sendMessage(MqttClient client, String topic, String msg){
        try {
            message = new MqttMessage();
            message.setQos(1);
            message.setRetained(true);
            message.setPayload(msg.getBytes());
            mqttTopic = client.getTopic(topic);
            MqttDeliveryToken token = mqttTopic.publish(message);//��������
            token.waitForCompletion();
        } catch (MqttPersistenceException e) {
            // TODO �Զ����ɵ� catch ��
            e.printStackTrace();
        } catch (MqttException e) {
            // TODO �Զ����ɵ� catch ��
            e.printStackTrace();
        }
    }
    
    /**
     * ��������
     * @param client MqttClient�ͻ��˶���
     * @param topicFilters ���ĵ���������
     * @param qos ��Ϣ�ȼ�
     */
    public void subscribe(MqttClient client, String[] topicFilters, int[] qos) {
        try {
            client.subscribe(topicFilters, qos);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

}
