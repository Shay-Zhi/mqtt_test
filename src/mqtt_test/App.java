package mqtt_test;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;


/**
 * MQTT�ͻ��˶���
 * ��װpublish��subscribe������
 * ��дmessageArrived�������������ݳ־û���
 */
public class App {
    //private String host = "tcp://218.17.87.115:1883";		// MQTT�����
    private String host = "tcp://172.16.7.15:1883";		// MQTT�����
    private String userName = "admin";					// ����MQTT������û���
    private String passWord = "password";				// ����MQTT���������
    private MqttClient client;							// MQTT�ͻ��������
    private String id;									// MQTT�ͻ��˵�ID
    private MqttTopic mqttTopic;						// �����������
    private String myTopic = "51220";					// Ĭ�ϵĶ�������
    private MqttMessage message;						// MQTT�������ݸ�ʽ����
    
    private DBConn conn = new DBConn();
    
    public App(String id, String[] topics, int[] qos) {
    	// ���췽��������App(id,callback,cleanSession)���췽��
        this(id, null, true, topics, qos);
    }
    
    public App(String id, MqttCallback callback, boolean cleanSession, String[] topics, int[] qos){
    	// ���췽��
        try {
            client = new MqttClient(host, id, new MemoryPersistence());		// idӦ�ñ���Ψһ��
            MqttConnectOptions options = new MqttConnectOptions();			// MQTT�ͻ����������ö���
            options.setConnectionTimeout(10);		// ���ӳ�ʱʱ�� 
            options.setKeepAliveInterval(20000);		// ���ӱ���ʱ��
            options.setAutomaticReconnect(true);	// �����Զ��������汾1.2��
            options.setCleanSession(cleanSession);	// �����session���Ա���������Լ�����������
            if (callback == null) {
                client.setCallback(new MqttCallbackExtended() {		
                	// ��д�ͻ��˻ص��������˴�ʵ��MqttCallbackExtended�ӿ����setAutomaticReconnect�ﵽ�Ͽ��������¶��ĵĹ���
                    @Override
                    public void connectionLost(Throwable arg0) {
                    	// ���Ӷ�ʧʱ��ִ�ж���
						Date date = new Date();
				        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//�������ڸ�ʽ
                        System.out.println(df.format(date) + id + " �����쳣�Ͽ�  " + arg0);
                        if(conn != null) {
                            //conn.closeDB();
                        }
                    }
    
                    @Override
                    public void deliveryComplete(IMqttDeliveryToken arg0) {
                    	// ���յ��Ѿ������� QoS 1 �� QoS 2 ��Ϣ�Ĵ�������ʱִ�еĶ���
                        System.out.println(id + " deliveryComplete " + arg0);
                        if(conn != null) {
                            //conn.closeDB();
                        }
                    }
    
                    @Override
                    public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
                    	// ���ն��ĵ���Ϣ��ִ�еĶ�������MQTT�ͻ��˶���ִ��subscribe������ִ��
                        System.out.println(id + " messageArrived: " + arg1.toString());
                        
                        // ��������Ƶ����Ϣ�����ݳ־û�
                        Map<String,String> payload = new HashMap();
                        //System.out.println("��ʼ��������");
                        payload = parsePayload(arg1);
                        if(payload == null) {
                        	System.out.println("��������ֹͣ���ݱ������ ...");
                        	return;
                        }
                        //System.out.println("��ʼ��������");
                        save2DB(payload);
                        
						Date date = new Date();
				        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//�������ڸ�ʽ
                        System.out.println(df.format(date)+"�����ѱ���");
                    }

					@Override
					public void connectComplete(boolean arg0, String arg1) {
						// �÷�����MqttCallbackExtended�ӿڣ�MqttCallback�ӿڵ���չ���ṩ�����ӳɹ���ִ�еĲ��������setAutomaticReconnect����ԶϿ��������ã�
						try {
							Date date = new Date();
					        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//�������ڸ�ʽ
							System.out.println(df.format(date)+"["+host+"] ���ӳɹ�����ʼ�������� " + topics[0].toString() + " qos��" + qos[0] + "...");
							if(conn==null) {
								System.out.println(df.format(date)+"["+host+"] �����������ݿ� ...");
								conn = new DBConn();
							}
							client.subscribe(topics, qos);
						} catch (MqttException e) {
							System.out.println(e);
						}
					}
                });
            } else {
                client.setCallback(callback);
            }
            client.connect(options);
        } catch (MqttException e) {
            // TODO �Զ����ɵ� catch ��
            e.printStackTrace();
        }
    }
    
    private Map<String,String> parsePayload(MqttMessage payload) throws UnsupportedEncodingException {
    	// �������յ�payload����
        Map<String, String> payloadMap = new HashMap();
    	
        String msg = new String(payload.getPayload());
        String tmp = msg.replace(" ","").replace(":{", "|{").split("\\|")[1].replace("{","").replace("}","").replace("\\s","").replace("\n","");	// ɾ���س����еȿհ׷�
        //System.out.println(msg);
        String tmp1[] = tmp.split(",");
        
        boolean failed = true;
        for(int i = 0; i < tmp1.length; i ++) {		// ��payload�������浽Map��
            tmp1[i] = tmp1[i].replace("\"", "");
        	String tmp2[] = tmp1[i].split(":");
        	if (tmp2[1]==null || tmp2[1].equals("")) {
        		failed = false;		// �޷����ܵ���ȷ������
        		continue;
        	}
        	//tmp2[1] = new String (tmp2[1].getBytes("GBK"),"UTF-8");		// ת������
        	//System.out.println(tmp2[0] + " -- " + tmp2[1]);
        	payloadMap.put(tmp2[0], tmp2[1]);
        	//System.out.println(tmp2[0] + " -- " + tmp2[1]);
        }
        
        if(!failed) {
        	System.out.println("���յ��������� ... ֹͣ���� ...");
        	return null;
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
    
    private void save2DB(Map<String, String> payload) {
    	// ���ݳ־û�
    	String saveDetailInfoSql = "insert into `mqtt_copy2`(`x_coord`,`y_coord`,`z_coord`,`ip`,`a_speed`,`a_load`,`a_feed`,`x_coord1`,`y_coord1`,`z_coord1`,`b_feed`,`publish_date`,`publish_time`,`b_Num`,`b_ToosNum`,`b_CNCapp`,`b_Runtime`,`state`) " + 
        		"values ('" + payload.get("x_coord") + "','" + payload.get("y_coord") + "','" + payload.get("z_coord") + 
        		"','" + payload.get("ip") + "','" + payload.get("a_speed") + "','" + payload.get("a_load") + 
        		"','" + payload.get("a_feed") + "','" + payload.get("x_coord1") + "','" + payload.get("y_coord1") + 
        		"','" + payload.get("z_coord1") + "','" + payload.get("b_feed") + "',current_date(),current_time()" +
        		",'" + payload.get("b_Num") + "','" + payload.get("b_ToosNum") + "','" + payload.get("b_CNCapp") + "','" + payload.get("b_Runtime") +
        		"','" + payload.get("state") + "')";
        String updateStateSql = "update `device` set `state`='" + payload.get("state") + "' where `ip`='" + payload.get("ip") + "'";
    	//System.out.println(saveDetailInfoSql);
    	//System.out.println(updateStateSql);
    	
    	conn.executeSql(saveDetailInfoSql);
    	conn.executeSql(updateStateSql);
    }
    
    public void sendMessage(String msg) {
        sendMessage(myTopic, msg);
    }
    
    public void sendMessage(String topic, String msg){
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
    
    public void subscribe(String[] topicFilters, int[] qos) {
        try {
        	System.out.println("���⣺" + topicFilters + " qos: " + qos.toString());
            client.subscribe(topicFilters, qos);
        } catch (MqttException e) {
            e.printStackTrace();
        }// ��������

    }
    
}