class EventLoader:
  def stats(self, org_id, from_ts, to_ts):
    hdfs_events_path = "{}".format(org_id)
    return spark.read.format("com.databricks.spark.avro").load(hdfs_events_path).count()
  def load(self, org_id, from_ts, to_ts):
    hdfs_events_path = "{}".format(org_id)
    return spark.read.format("com.databricks.spark.avro").load(hdfs_events_path)

class TrainsetGenerator:
  def __init__(self, raw_data):
    return self
  def groupByUser(self):
  def groupByUserSession(self):
  def generateTarget(self):
  def generateCases(self):
  
class EventModel:
  def optimize(self):
    pass
  def fit(self):
    pass
  def score(self):
    pass
  
class EventSequqentialModel(EventModel):
  def optimize(self):
  def fit(self):
  def score(self):
  
class EventStandardModel(EventModel):
  def optimize(self):
  def fit(self):
  def score(self):

class EventUploader:
  def upload(self):
  

# Use-case
if __name__=="__main__":
  
  # Events in Customer-Journey format (specific for DMP)
  eventLoader = EventLoader()
  raw_event_data = eventLoader.load("asdasd-asdasd-asdasd", "2010-01-01", "2020-01-01")
  
  generator = TrainsetGenerator()
  grouped_data = generator.groupByUser(raw_event_data)
  grouped_data = generator.groupByUserSessions(raw_event_data, session_interval=60*60*4)
  
  # sequence
  grouped_data = generator.groupByUser(raw_event_data, zipped=False)
  
  targets = generator.generateTarget
  dataset = generator.generateCases(grouped_data, targets, mode='last', pad=False)
  
  train_dataset, test_dataset = generator.generateTrainTest(dataset)
  
  prediction_model = EventSequentialModel()
  prediction_model.optimize_parameters()
  prediction_model.fit(train_dataset)