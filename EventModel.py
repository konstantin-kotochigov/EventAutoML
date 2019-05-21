class EventModel:
  def optimize(self):
    pass
  def fit(self):
    pass
  def score(self):
    pass
    
class DataPreprocessor:
  
  def seq2tensor(self, vocablary_path):
    
        # Concatenate datasets to preprocess them together
        self.train_data['type']='train'
        self.scoring_data['type']='scoring'
        data = pandas.concat([self.train_data, self.scoring_data])
        
        # Preprocess "TS Deltas" Sequence
        data['dt'] = data.dt.apply(lambda x: list(x)[0:len(x)-1])
        data.loc[:, 'dt'] = data.dt.apply(lambda r: [0]*(32-len(r)) + r if pandas.notna(numpy.array(r).any()) else [0]*32 )
        data.dt = data.dt.apply(lambda r: numpy.log(numpy.array(r)+1))
        Max = numpy.max(data.dt.apply(lambda r: numpy.max(r)))
        data.dt = data.dt.apply(lambda r: r / Max)
        data.dt = data.dt.apply(lambda r: r if len(r)==32 else r[-32:])
        
        # Preprocess Urls Sequence
        if vocablary_path != None:
            tk = keras.preprocessing.text.Tokenizer(filters='', split=' ')
            tk.fit_on_texts(data.url.values)
            self.hdfs_client.write(self.vocabulary_path, data=pickle.dumps(tk), overwrite=True)
        else:
            with self.hdfs_client.read(self.vocabulary_path) as reader:
                tk = pickle.loads(reader.read())
                
        train_data = data[data.type=='train']
        scoring_data = data[data.type=='scoring']
        
        train_urls = tk.texts_to_sequences(train_data.url)
        train_urls = sequence.pad_sequences(train_urls, maxlen=32)
        
        scoring_urls = tk.texts_to_sequences(scoring_data.url)
        scoring_urls = sequence.pad_sequences(scoring_urls, maxlen=32)
        
        train_dt = numpy.concatenate(train_data.dt.values).reshape((len(train_data), 32, 1))
        scoring_dt = numpy.concatenate(scoring_data.dt.values).reshape((len(scoring_data), 32, 1))
        
        return (train_urls, train_dt, train_data['target'], scoring_urls, scoring_dt, tk)
    
class EventSequqentialModel(EventModel):

  def make_network(self, tokenizer):
    
        input_layer1 = Input(shape=(32,))
        layer11 = Embedding(len(tokenizer.word_index)+1, 4, input_length=32, trainable=True)(input_layer1)
        layer12 = LSTM(16, dropout_W=0.2, dropout_U=0.2, return_sequences=False)(layer11)
        
        input_layer2 = Input(shape=(32,1))
        layer21 = LSTM(16, dropout_W=0.2, dropout_U=0.2, return_sequences=False)(input_layer2)
        
        layer1 = Concatenate(axis=-1)([layer12, layer21])
        
        layer2 = Dense(16)(layer1)
        layer3 = Dense(4)(layer2)
        layer4 = Dense(1)(layer3)
        
        output_layer = Activation('sigmoid')(layer4)
        return_model = Model(inputs=[input_layer1, input_layer2], outputs=output_layer)
        
        return_model.compile(loss='binary_crossentropy',
                    optimizer='Adam',
                    metrics=['accuracy'])
        
        return return_model
        
  def optimize(self):
  
  def preprocess_data(self, train_data, scoring_data):
    preprocessor = DataPreprocessor()
    train_tensors, scoring_tensors, tokenizer = preprocessor.seq2tensor([train_data, scoring_data])
    return train_tensors, scoring_tensors, tokenizer
  
  def fit(self, train_tensors):
    
    model.fit([train_tensors['urls'], train_tensors['dt']], train_tensors['target'], epochs=1, batch_size=1024)
    
  
  def score(self, test_tensors):
    model.predict([scoring_tensors['urls'], scoring_tensors['dt']], batch_size=1024)
  
class EventStandardModel(EventModel):
  def optimize(self):
  def fit(self):
  def score(self):