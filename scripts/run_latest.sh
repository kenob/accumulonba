
cd ~/accumulonba

git pull

cd ~/hw5

echo "Updating source file..."
cp -r /home/guest/accumulonba/src/edu \
/home/guest/hw5/src/main/java/

mvn compile

echo "Generating updated jar..."
mvn package

echo "Copying latest team feed to HDFS..."

hadoop fs -rmr /user/guest/teamfeed

hadoop fs -copyFromLocal ~/accumulonba/scripts/teamfeed* /user/guest/teamfeed

~/cdse/accumulo/accumulo-1.4.2/bin/tool.sh /home/guest/hw5/target/hw5-1.0-SNAPSHOT.jar \
edu.cse.buffalo.cse587.Main acc guestvb \
/user/guest/teamfeed


