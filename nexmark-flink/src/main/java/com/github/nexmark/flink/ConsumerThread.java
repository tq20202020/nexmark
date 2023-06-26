package com.github.nexmark.flink;

//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/** Note:
 *  The work in the following class involving Kafka is largely based on the Microsoft tutorial for implementing Kafka with Event Hubs.
 *  Link: https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/event-hubs/event-hubs-java-get-started-send.md
 */
public class ConsumerThread implements Runnable {

    private final String TOPIC;
    
    //Each consumer needs a unique client ID per thread
    private static int id = 0;

    // Initializes ConsumerThread object
    public ConsumerThread(final String TOPIC){
        this.TOPIC = TOPIC;
    }

    @Override
    public void run() {
        final Consumer<Long, String> consumer = createConsumer();

        //Debugging Information - whether it successfully reached this stage (i.e. create consumer was successful, without exceptions)
        System.out.println("Polling");

        try {
            while (true) {
                // Includes updated syntax for consumer.poll() function which is depreciated if not used with Duration
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(1000));

                // Iterating through consumerRecords
                for(ConsumerRecord<Long, String> cr : consumerRecords) {
                    // Debugging information 
                    /** Example of Statement:
                     * Consumer Record:(1687819270433, "Auction{id=1211, itemName='dwoiiuq', description='hvqcartuxyddypkwrmusuogyvrwaesotztq dowirbjcaqsqczyrfambzwiwdfx', initialBid=939, reserve=1083, dateTime=2023-06-26T22:40:03.475Z, expires=2023-06-26T22:40:03.485Z, seller=1032, category=10, extra='ixhimsRQY]JSSH]VX`TSVISMTV]YUS_YHMJIMLLUWUezzbffvrzabqKURUaMbqlbyynnuwriwaocgwddweci^]\\NYJqqvfilVWV]ZSjlyflaINHZ`[SVMV^PP_SUW]X^YPV[]^WKTQnkznjhHIKWVUjozcduV_[_aaJVSX_Qkebsymaugnwhngtdvz`LYaIWzxaptv^NaRMafbudhnniwqgySQN\\SHLa][`aK\\KYJ[kaajfjK`XP^KkjabghUY[WLItjhwweHXYLXWywtxreuqhnrdrhnvzcsekcyriadunsSa_XONVaNaKPyewghxNVPJ^LOIIYJXUPX\\\\Kwudfkwtywofwbtydye\\TLXa`jxdkybTLRWN_T]M`KVrzwendRR^HaXykytdwQLVH`aevwmwiMP]YNKo'}", 0, 8102)
                     */
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", cr.key(), cr.value(), cr.partition(), cr.offset());
                }
                consumer.commitAsync();
            }
        } catch (CommitFailedException e) {
            System.out.println("CommitFailedException: " + e);
        } finally {
            consumer.close();
        }
    }

    // Creating the Consumer end of Kafka
    private Consumer<Long, String> createConsumer() {
        try {
            final Properties properties = new Properties();
            synchronized (ConsumerThread.class) {
                properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer#" + id);
                id++;
            }

            // Added from initial setup as process required a group ID
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "nexmarktests");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            //Get remaining properties from config file
            properties.load(new FileReader("C:\\Users\\t-thodan\\OneDrive - Microsoft\\nexmarkdatagen\\nexmark-flink\\src\\consumer.config"));

            // Create the consumer using properties.
            Consumer<Long, String> consumer = new KafkaConsumer<>(properties);

            // Subscribe to the topic.
            consumer.subscribe(Collections.singletonList(TOPIC));
            return consumer;
            
        } catch (FileNotFoundException e){
            System.out.println("FileNotFoundException: " + e);
            System.exit(1);

            // Requires a return statement
            return null;        
        } catch (IOException e){
            System.out.println("IOException: " + e);
            System.exit(1);

            // Requires a return statement
            return null;        
        }
    }
}
