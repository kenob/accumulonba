cd ~/hw5/accumulonba/src

git pull

javac -d ../../bin -cp "/home/guest/cdse/accumulo/accumulo-1.4.2/lib/*" Main.java

cd ../..

ls

rm Wc.jar

echo "Generating jars...."

cd bin

jar cfe ../Wc.jar Main *.class