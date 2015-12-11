import happybase, sys, os, string

# VARIABLES
# HBase Thrift server to connect to. Leave blank for localhost
server = "jprosser-spot2-5.vpc.cloudera.com"

# Connect to server
c = happybase.Connection(server)

# Get the full list of tables
tables = c.tables()

# For each table in the tables
for table in tables:
  t = c.table(table)

  print table + ": ",
  rowCount = 0
  colCount = 0

  # For each row key
  for prefix in string.printable:
    try:
      for key, data in t.scan(row_prefix=prefix):
        rowCount += 1

        # Each column
        for col in data:
          colCount += 1
    except:
        os.system("hbase-daemon.sh restart thrift")
        c = happybase.Connection(server)
        t = c.table(table)
        continue

  print "%d, %d" % (rowCount, colCount)
