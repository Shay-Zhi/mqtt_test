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
 * 接入EMQX平台的工具类
 * @author Shay Zhi
 * @version 2020-03-20
 */
public class MqttUtils {
	/**
	 * 连接EMQX所需的服务器信息
	 */
	private static String broker = "tcp://172.16.7.15";	
	private static Integer port = 1883;			
    private MqttTopic mqttTopic;						// 订阅主题对象
    private String myTopic = "5122";					// 默认的订阅主题
    private MqttMessage message;						// MQTT传输数据格式对象
    
	// EMQX暂时不需要账号连接
	private static String clientId = "588189007";	
	private static String userName = "326926";	
	private static String password = "wlpub001";	
	
	/**
	 * 客户端设置
	 */
	private MqttClient mqttClient;		// 客户端对象
	private MqttConnectOptions options;	// 连接参数对象
	private MqttCallback clientCallback; // 回调方法
	
	/**
	 * 数据持久化相关
	 */
	private DBConn conn = new DBConn();
	
	/**
	 * MQTT连接操作
	 * 
	 */
	
	/**
	 * 创建连接，并连接至EMQX服务器
	 */
	public MqttClient connect() {
		// 客户端对象为空，创建客户端对象
		if(mqttClient == null) {
			// 创建连接参数对象
			options = new MqttConnectOptions();
			options.setCleanSession(false);
			
			// 暂时不需要账号连接
			//options.setUserName(userName);
			//options.setPassword(password.toCharArray());
			
			// 在订阅主题后执行的回调方法
			clientCallback = new MqttCallback() {
				@Override
				public void connectionLost(Throwable arg0) {
					Date date = new Date();
					DateFormat df = DateFormat.getDateTimeInstance();
					// 连接断开时执行
					//System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] 连接断开 ...");
					System.out.println("["+df.format(date)+"] ["+broker+"] 连接断开 ...");
                    conn.closeDB();
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken arg0) {
					Date date = new Date();
					DateFormat df = DateFormat.getDateTimeInstance();
					// publish成功后执行
					//System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] 消息推送成功");
					System.out.println("["+df.format(date)+"] ["+broker+"] 消息推送成功");
                    conn.closeDB();
				}

				@Override
				public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
					Date date = new Date();
					DateFormat df = DateFormat.getDateTimeInstance();
					
					// 接收到订阅信息后执行
					//System.out.println("["+df.format(date)+"] ["+broker+":"+clientId+":"+userName+":"+password+"] 接收消息："+arg1);
					System.out.println("["+df.format(date)+"] ["+broker+"] 接收消息："+arg1.getPayload().toString());
					
					// 解析订阅频道信息与数据持久化
                    Map<String,String> payload = new HashMap();
                    payload = parsePayload(arg1);
                    save2DB(payload);
                    System.out.println("["+df.format(date)+"] ["+broker+"] 消息已保存");
                    
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
		// 创建了MqttClient客户端对象后，通过该对象连接到EMQX服务器
		if(!mqttClient.isConnected()){
			Date date = new Date();
			DateFormat df = DateFormat.getDateTimeInstance();
			try {
				mqttClient.connect(options);
				if(mqttClient.isConnected()) {
					System.out.println("["+df.format(date)+"] ["+broker+"] 连接成功 ...");
					return mqttClient;
				}
			} catch (Exception e) {
				System.out.println("["+df.format(date)+"] ["+broker+"] 无法连接，网络错误 ...");
				return null;
			}
		}
		return null;
	}

	/**
	 * 客户端连接状态
	 */
	public boolean isConnected() {
		return mqttClient.isConnected();
	}

	/**
	 * 重新连接
	 */
	public MqttClient reConnect() {
		return this.connect();
	}
	
	/**
	 * 数据持久化操作
	 * 
	 */
	
	/**
	 * 解析从订阅主题接收到的payload数据
	 * @param payload 订阅信息
	 * @return 解析后的payload
	 * @throws UnsupportedEncodingException
	 */
	private Map<String,String> parsePayload(MqttMessage payload) throws UnsupportedEncodingException {
    	// 解析接收的payload数据
        Map<String, String> payloadMap = new HashMap();
    	
        String msg = new String(payload.getPayload());
        String tmp = msg.replace(" ","").replace(":{", "|{").split("\\|")[1].replace("{","").replace("}","").replace("\\s","").replace("\n","");	// 删除回车换行等空白符
        //System.out.println(msg);
        String tmp1[] = tmp.split(",");
        
        for(int i = 0; i < tmp1.length; i ++) {
            tmp1[i] = tmp1[i].replace("\"", "");
        	String tmp2[] = tmp1[i].split(":");
        	tmp2[1] = new String (tmp2[1].getBytes("GBK"),"UTF-8");		// 转换编码
        	payloadMap.put(tmp2[0], tmp2[1]);
        	//System.out.println(tmp2[0] + " -- " + tmp2[1]);
        }
		
        // 日志输出
	    //System.out.println(payloadMap); 
        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//设置日期格式
	    System.out.print(df.format(new Date()));
	    for (Map.Entry<String, String> entry : payloadMap.entrySet()) {
		   System.out.print(entry.getKey() + "=" + entry.getValue() + " | "); 
	    }
	    System.out.print("\n");
		 
	    return payloadMap;
    }
    
	/**
	 * 将接收的订阅信息保存到MySQL中
	 * @param payload 订阅信息
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
     * 订阅相关操作
     * 
     */
    
    /**
     * 
     * 向默认主题5122发送消息
     * @param msg 即为payload
     * @param client MqttClient客户端对象
     */
    public void sendMessage(MqttClient client, String msg) {
        sendMessage(client, myTopic, msg);
    }
    
    /**
     * 向指定主题发送消息
     * @param topic 指定主题名称
     * @param msg payload
     * @param client MqttClient客户端对象
     */
    public void sendMessage(MqttClient client, String topic, String msg){
        try {
            message = new MqttMessage();
            message.setQos(1);
            message.setRetained(true);
            message.setPayload(msg.getBytes());
            mqttTopic = client.getTopic(topic);
            MqttDeliveryToken token = mqttTopic.publish(message);//发布主题
            token.waitForCompletion();
        } catch (MqttPersistenceException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        } catch (MqttException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
    }
    
    /**
     * 订阅主题
     * @param client MqttClient客户端对象
     * @param topicFilters 订阅的主题数组
     * @param qos 消息等级
     */
    public void subscribe(MqttClient client, String[] topicFilters, int[] qos) {
        try {
            client.subscribe(topicFilters, qos);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

}
