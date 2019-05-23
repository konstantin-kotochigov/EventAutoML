# Class for loading raw event data
# Currently implemented for DMP customer-journey format only
# reads schemed avro files with events

class EventLoader:

  def __init__(self, _spark):
    self.spark = _spark

  def stats(self, org_id, from_ts, to_ts):
    hdfs_events_path = "{}".format(org_id)
    return self.spark.read.format("com.databricks.spark.avro").load(hdfs_events_path).count()

  def load_raw(self, org_id, from_ts, to_ts):
    hdfs_events_path = "{}".format(org_id)
    return self.spark.read.format("com.databricks.spark.avro").load(hdfs_events_path)
    
  @staticmethod
  def cj_id(cj_ids, arg_id, arg_key=-1):
        result = []
        for id in cj_ids['uids']:
            if id['id'] == arg_id and id['key'] == arg_key:
                result += [id['value']]
        return result
    
  @staticmethod
  def cj_attr(cj_attributes, arg_id, attr_type):
        result = []
        if cj_attributes is not None:
            for attr in cj_attributes:
                member_name = 'member' + str(attr_type)
                if attr is not None and member_name in attr:
                    if attr[member_name] is not None and 'id' in attr[member_name]:
                        if attr[member_name]['id'] == arg_id:
                            return attr[member_name]['value']
        return result
        
  def generateSQL(self, config, only_pageloads=True):
    sql_header = "SELECT "
    sql_lines = []
    for coltype in ["id","attr"]:
      for col in config[coltype]:
        sql_lines.append("cj_{}({},{},{}) as {}".format(coltype, col[0], col[1], coltype, col[2]))
    sql_columns = ", ".join(sql_lines)
    sql_footer = "FROM cj_raw_data"
    if only_pageloads:
        sql_footer += "WHERE pageload(attr, 10057, 100031) == True"
    return sql_header + sql_columns + sql_footer
        
  @staticmethod
  def cj_pageload(cj_attributes):
        result = False
        if cj_attributes is not None:
            for attr in cj_attributes:
                if attr is not None and 'member4' in attr:
                    if attr['member4'] is not None and 'id' in attr['member4']:
                        if attr['member4']['id'] == 10057 and attr['member4']['value']=='Page Load':
                            result = True
        return result
  
  def load(self, config):
    cj_raw_data = self.load_raw()
    cj_raw_data.registerTempTable("cj_raw_data")
    self.spark.udf.register("cj_id", self.cj_id, ArrayType(StringType()))
    self.spark.udf.register("cj_attr", self.cj_attr, StringType())
    self.spark.udf.register("cj_pageload", self.cj_pageload, BooleanType())
    sqltext = self.generateSQL(config)
    return self.spark.sql(sqltext)
    
  