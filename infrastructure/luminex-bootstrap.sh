# #!/bin/bash
#
# #
# # sudo wget https://search.maven.org/remotecontent?filepath=org/elasticsearch/elasticsearch/8.8.2/elasticsearch-8.8.2.jar -P /usr/lib/spark/jars/
#
# #
# sudo aws s3 cp s3://luminex/jars/elasticsearch-8.8.2.jar /usr/lib/spark/jars/
# # sudo yum install python-pip
# sudo pip3 install pyspark elasticsearch seaborn pandas boto3


#!/bin/bash
set -x -e

echo -e 'export PYSPARK_PYTHON=/usr/bin/python3
export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_JARS_DIR=/usr/lib/spark/jars
export SPARK_HOME=/usr/lib/spark' >> $HOME/.bashrc && source $HOME/.bashrc

sudo -H python3 -m pip install spark-nlp pandas boto3

set +x
exit 0
