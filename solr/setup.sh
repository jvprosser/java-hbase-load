
#Creating a Solr Collection

#Generate a sample schema.xml configuration file:
solrctl --zk jprosser-spot2-4.vpc.cloudera.com:2181 instancedir --generate $HOME/solrcfg

#Edit the schema.xml file in $HOME/solrcfg , specifying the fields we need for our collection. 

#Upload the Solr configurations to ZooKeeper:
solrctl --zk jprosser-spot2-4.vpc.cloudera.com:2181/solr instancedir --create imp1_collection $HOME/solrcfg

#Generate the Solr collection with 2 shards (-s 2) and 2 replicas (-r 2):
solrctl --zk jprosser-spot2-4.vpc.cloudera.com:2181/solr --solr jprosser-spot2-6.vpc.cloudera.com:8983/solr collection --create imp1_collection -s 2 -r 2


hbase-indexer add-indexer -n imp1_indexer -c indexer-config.xml -cp solr.zk=jprosser-spot2-4.vpc.cloudera.com:2181/solr -cp solr.collection=imp1_collection --zookeeper jprosser-spot2-4.vpc.cloudera.com:2181

hadoop --config /etc/hadoop/conf \
    jar /usr/lib/hbase-solr/tools/hbase-indexer-mr-*-job.jar \
    --conf /etc/hbase/conf/hbase-site.xml -D 'mapred.child.java.opts=-Xmx500m' \
    --hbase-indexer-file indexer-config.xml \
    --zk-host jprosser-spot2-4.vpc.cloudera.com:2181/solr --collection imp1-collection \
    --go-live --log4j src/test/resources/log4j.properties

