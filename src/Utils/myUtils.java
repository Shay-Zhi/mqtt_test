package Utils;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import mqtt_test.DBConn;

public class myUtils {
	private String host = "tcp://218.17.87.115:1883";		// MQTT�����
    private String id;									// MQTT�ͻ��˵�ID
    private String myTopic = "5122";					// Ĭ�ϵĶ�������
    private MqttConnectOptions options;
    
    private DBConn conn = new DBConn();
    
    public String mqttTopic() {
    	return this.myTopic;
    }
    
    public void setMqttTopic(String myTopic) {
    	this.myTopic = myTopic;
    }
    
    /**
     * �����ͻ���
     * @param myTopic
     * @param qos
     */
    public MqttClient createClient(String myTopic, int qos) {
    	try {
    		MqttCallback callback = null;		
    		MqttClient client = new MqttClient(host, id, new MemoryPersistence());		
            options = new MqttConnectOptions();			
            options.setConnectionTimeout(10);		
            options.setKeepAliveInterval(20000);		
            options.setAutomaticReconnect(true);	
            options.setCleanSession(false);	
            client.setCallback(new MqttCallbackExtended() {		
            	@Override
                public void connectionLost(Throwable arg0) {
                	Date date = new Date();
			        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");
                    System.out.println(df.format(date) + id + " �����쳣�Ͽ�  " + arg0);
                    if(conn != null) {
                        //conn.closeDB();
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken arg0) {
                    System.out.println(id + " deliveryComplete " + arg0);
                    if(conn != null) {
                        //conn.closeDB();
                    }
                }

                @Override
                public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
                	System.out.println(id + " messageArrived: " + arg1.toString());
                    
                    Map<String,String> payload = new HashMap();
                    System.out.println("��ʼ��������");
                    payload = parsePayload(arg1);
                    if(payload == null) {
                    	System.out.println("��������ֹͣ���ݱ������ ...");
                    	return;
                    }
                    System.out.println("��ʼ��������");
                    save2DB(payload);
                    
					Date date = new Date();
			        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//�������ڸ�ʽ
                    System.out.println(df.format(date)+"�����ѱ���");
                }

				@Override
				public void connectComplete(boolean arg0, String arg1) {
					Date date = new Date();
					SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//�������ڸ�ʽ
					System.out.println(df.format(date)+"["+host+"] ���ӳɹ�����ʼ�������� " + myTopic + " qos��" + qos + "...");
					if(conn==null) {
						System.out.println(df.format(date)+"["+host+"] �����������ݿ� ...");
						conn = new DBConn();
					}
					
					subscribe(client, myTopic, qos);
				}
            });
        	return client;
        } catch (MqttException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public void connect2Server(String topic, int qos) {
    	MqttClient client = createClient(topic, qos);
    	if(client == null) {
    		 new RuntimeException("�ͻ��˶���Ϊ��");
    	}
    	try {
			client.connect();
		} catch (MqttSecurityException e) {
			e.printStackTrace();
		} catch (MqttException e) {
			e.printStackTrace();
		}
    }
    
    public void reconnect(String topic, int qos) {
    	connect2Server(topic, qos);
    }
    
    
    
    /*
     * ����
     */
    public void subscribe(MqttClient client, String topicFilters, int qos) {
        try {
        	System.out.println("���⣺" + topicFilters + " qos: " + qos);
            client.subscribe(topicFilters, qos);
        } catch (MqttException e) {
            e.printStackTrace();
        }// ��������

    }
    
    /*
     * ���ݳ־û�
     */
    private void save2DB(Map<String, String> payload) {
    	String saveDetailInfoSql = "insert into `mqtt`(`x_coord`,`y_coord`,`z_coord`,`ip`,`a_speed`,`a_load`,`a_feed`,`x_coord1`,`y_coord1`,`z_coord1`,`b_feed`,`publish_date`,`publish_time`,`b_Num`,`b_ToosNum`,`b_CNCapp`,`b_Runtime`,`state`) " + 
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
        	tmp2[1] = new String (tmp2[1].getBytes("GBK"),"UTF-8");		// ת������
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
    
}
