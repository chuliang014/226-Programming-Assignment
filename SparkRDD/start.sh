mvn package
spark-submit --master local --class edu.ucr.cs.cs226.czhan187.SparkRDD target/SparkRDD-1.0-SNAPSHOT.jar $1
#just run the command ./start.sh <path of nasa.tsv>
#eg. ./start.sh /home/download/nasa.tsv