import EventLoader, EventModel, DataUploader, TrainsetGenerator

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
  train_tensors, scoring_tensors, tokenizer = prediction_model.preprocess_data([train_data, scoring_data])
  model = prediction_model.make_network(tokenizer)
  model = prediction_model.fit(train_tensors)
  model.score(scoring_tensors)
  prediction_model.optimize_parameters()
  prediction_model.fit(train_dataset)