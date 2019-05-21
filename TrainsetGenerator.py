class SequenceProcessor:
  
  def process(self, x):
    return [1,2,3,4,5]
    
  # Function to create Training Record from event list using event number
  def process_sequence1(id, event_sequence, session_close_event_num, targets, train_scoring_flag):
                return (
                        id,                                                               # FPC
                        event_sequence[1][0:session_close_event_num+1],                   # deltas
                        event_sequence[2][0:session_close_event_num+1],                   # urls
                        targets[session_close_event_num],                                 # Target
                        event_sequence[0][session_close_event_num],                       # TS
                        train_scoring_flag[session_close_event_num]                       # Train/Scoring Flag
                    )
    
  # Function to create Training Record from event list using event number
  def process_sequence2(id, event_sequence, session_close_event_num, targets, train_scoring_flag):
                    f1 = numpy.avg(event_sequence[0:session_close_event_num+1])
                    f2 = numpy.min(event_sequence[0:session_close_event_num+1])
                    f3 = f4 = f5 = 0
                    return (id, f1, f2, f3, f4, f5)
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    

class TrainsetGenerator:
  
  def __init__(self, raw_data):
    return self

  def compute_target(event_list, target_url_regexp, monitor_period):
                    
                    # Set initial Dummy next target TS
                    next_target_ts = 10000000000
                    
                    event_timestamps = event_list[0]
                    event_urls = event_list[2]
                    
                    # Construct Target Variables List
                    target_list = [-1] * len(event_timestamps)
                    
                    for t in range(len(event_list[0])-1,-1,-1):
                
                        # Set Target variable. event_list[0] - vector of event timestamps
                        target_list[t] = 0 if next_target_ts - event_timestamps[t] > monitor_period else 1
                        
                        # If we encounter Target Url, Update Next Target TS
                        if re.match(target_url_regexp, event_urls[t]):
                            next_target_ts=event_timestamps[t]
                    
                    return target_list
            
           
            
            
            # Create Multiple Session Records
            def process_event_list(spark_grouped_row, monitor_period, features_mode, split_mode):
                
                    customer_id = spark_grouped_row[0]
                    data_list = spark_grouped_row[1]
                    
                    # Sort events by timestamp
                    data_list = sorted(data_list, key=lambda y: y[0])
                    
                    # Divide event attributes into separate lists
                    event_lists = list(zip(*data_list))
                    deltas = event_lists[1]
                    timestamps = event_lists[0]
                    
                    # Generate Targets
                    targets = compute_target(event_lists, "^https://otus.ru/assessment/", monitor_period)
                    
                    # Mark Data Records For Training / Scoring (last record in event list goes to Scoring part)
                    train_scoring_flags = [1 if d == None else 0 for d in deltas]
                    
                    # Choose Session boundaries
                    if split_mode == "all":
                        # We seek large deltas and mark those points as session ends
                        session_coordinates = [i for i, x in enumerate(deltas) if x == None or x > 4]
                    else:
                        # We got one splitting point and generate one row preceding this TS
                        session_coordinates = [max([i for i,x in enumerate(timestamps) if x < split_dt])]
                        
                    # Choose a Function For Feature Generation
                    if features_mode == "seq":
                        process_function = process_sequence1
                    else:
                        process_function = process_sequence2
                    
                    return [process_function(customer_id, event_lists, session_close_event_num, targets, train_scoring_flags) for session_close_event_num in session_coordinates]
            
            
    
  def groupByUser(self, raw_data):
    sequenceProcessor = SequenceProcessor()
    train_scoring_df = raw_data.rdd.map(lambda x: (x['fpc'], (x['ts'], x['next'], x['link']))).\
        groupByKey().\
        flatMap(lambda x: sequenceProcessor.process(x))
        
  def groupByUserSession(self):
    train_scoring_df = raw_data.rdd.map(lambda x: (x['fpc'], (x['ts'], x['next'], x['link']))).\
        groupByKey().\
        flatMap(lambda x: sequenceProcessor.process(x))
    train_scoring_df.flatMap()
    
  def generateTarget(self):
  def generateCases(self):