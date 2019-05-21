# Class for loading raw event data
# Currently implemented for DMP customer-journey format only
# reads schemed avro files with events

class EventLoader:

  def __init__(self, _spark):
    self.spark = _spark

  def stats(self, org_id, from_ts, to_ts):
    hdfs_events_path = "{}".format(org_id)
    return self.spark.read.format("com.databricks.spark.avro").load(hdfs_events_path).count()

  def load(self, org_id, from_ts, to_ts):
    hdfs_events_path = "{}".format(org_id)
    return self.spark.read.format("com.databricks.spark.avro").load(hdfs_events_path)
    
  def load_all(self, org_id):
    hdfs_events_path = "{}".format(org_id)
    return self.spark.read.format("com.databricks.spark.avro").load(hdfs_events_path)