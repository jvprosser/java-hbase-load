import happybase, sys, os, string

# VARIABLES
# HBase Thrift server to connect to. Leave blank for localhost
server = "jprosser-spot2-5.vpc.cloudera.com"
namespace = "S92"
poolsize=3
prefix="04"
#rfilter="{'regexstring:*|175|0'}"

# RegexStringComparator
rfilter="(RowFilter (=, 'regexstring:312'))"
#rfilter="(RowFilter (=, 'binary:04')"
tab="event"


# HBase Thrift server to connect to. Leave blank for localhost
server = "jprosser-spot2-5.vpc.cloudera.com"

# Connect to server
connection = happybase.Connection(host=server,table_prefix=namespace, table_prefix_separator=':')


table = connection.table(tab)
print "table" + ": " + tab 
rowCount=0
colCount=0

# tail_num_009004


for key, data in table.scan(row_start="06", row_stop="08",filter=rfilter):
  rowCount += 1
  print "key is " + key
  print "LD data is " + data['f:LD']
  # Each column
  for col in data:
    colCount += 1

print "rows: %d, cols: %d" % (rowCount, colCount)
