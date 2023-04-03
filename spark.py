import os
from pyspark.sql import SparkSession

# Set the environment variables for Hadoop and Kerberos
os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
os.environ['HADOOP_CONF_DIR'] = '/usr/local/hadoop/etc/hadoop'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
os.environ['KRB5_CONFIG'] = '/etc/krb5.conf'

# Set the path to the keytab file
keytab_path = '/path/to/keytab/file'

# Create a Spark session with Kerberos authentication enabled
spark = SparkSession.builder \
    .appName('Read Hive Table with Kerberos Authentication') \
    .config('spark.executor.extraJavaOptions', '-Djava.security.auth.login.config=/path/to/jaas.conf') \
    .config('spark.driver.extraJavaOptions', '-Djava.security.auth.login.config=/path/to/jaas.conf') \
    .config('spark.hadoop.fs.defaultFS', 'hdfs://<name_node>:8020') \
    .config('spark.yarn.keytab', keytab_path) \
    .config('spark.yarn.principal', '<principal>') \
    .enableHiveSupport() \
    .getOrCreate()

# Read data from a Hive table using Spark SQL
df = spark.sql('SELECT * FROM <database_name>.<table_name>')

# Print the first 10 rows of the DataFrame
df.show(10)

# Stop the Spark session
spark.stop()
