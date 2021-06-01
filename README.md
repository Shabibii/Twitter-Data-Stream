# Twitter_Data_Stream
Finding top trending topics, hashtags and Tweets from a Twitter data stream in the past x minutes

Before running the program, it is important to set up a Kafka and Zookeeper broker which will run in the background.
Look at https://www.goavega.com/install-apache-kafka-on-windows/ for more information.

After the Kafka and Zookeeper broker are succesfully installed, they can be run in the order Zookeeper, then Kafka.
NOTE: It is necessary to create a topic in the Kafka cluster with the name "twitterStream_EN".
The command is for Windows is as follows: "bin/windows/kafka-topics.bat" --create --topic twitterStream_EN --bootstrap-server localhost:9092

Now, the application can be executed by opening the command prompt in the directory containing the following files and folder:
  producer_twitterStream.py
  consumer_twitterStream.py
  Visual/tlogo.png (Create a folder with the Visual, and use that as location for the Twitter mask file)
  
In the command prompt, type: python consumer_twitterStream.py
The program will run and prompt the user for the necessary information to activate certain features.
NOTE: the streaming time should not be too short, since some features might give errors because of the low amount of data to work with.

After the Word Cloud is presented, the application waits for the user to close down the image window.
Then, the program prompts the user for another run.
