### spark Streaming to read from Kafka topics and write to Kakfa topics

## To run the spark job use the below command:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 sparkStreaming.py


## Sample Test Case:

For sbsb_input_topic
{"SBSB_CK":10000,"GRGR_CK":900,"SBSB_ID":"900100201","SBSB_LAST_NAME":"TESTUSER1_FIRST","SBSB_FIRST_NAME":"TESTUSER1_LAST","SBSB_MID_INIT":"A","after":{"FNAME_AFTER":"TEST1","LNAME_AFTER":"TEST2"},"ts":"2021-03-11T13:31:48.447000"}

For meme_input_topic
{"SBSB_CK":10000,"MEME_CK":900,"MEME_LAST_NAME":"TESTUSER1_LAST","MEME_FIRST_NAME":"TESTUSER1_FIRST","MEME_MID_INIT":"A","MEME_REL":"M","ts":"2021-03-11T13:31:48.448000"}
{"SBSB_CK":10000,"MEME_CK":901,"MEME_LAST_NAME":"TESTUSER2_LAST","MEME_FIRST_NAME":"TESTUSER2_FIRST","MEME_MID_INIT":"B","MEME_REL":"W","ts":"2021-03-11T13:31:48.448000"}

Output is sent to sbsb_meme_output_topic
