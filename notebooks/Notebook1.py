# Databricks notebook source
# MAGIC %sh
# MAGIC 
# MAGIC pip install gremlinpython

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.text("storage_account_name", "")
# dbutils.widgets.text("containerName", "")
# dbutils.widgets.text("storage_access_key", "")
# dbutils.widgets.text("gremlin_account_name", "")
# dbutils.widgets.text("gremlin_database_name", "")
# dbutils.widgets.text("graph_name", "")
# dbutils.widgets.text("gremlin_access_key", "")
dbutils.widgets.text("storage_account_name", "nsyashdatasinkstorage2")
dbutils.widgets.text("containerName", "raw")
dbutils.widgets.text("containerName1", "transformed")
dbutils.widgets.text("storage_access_key", "ChJVw0TFFoqPiiBbnlY4+5AAG7PHRTT3uF3eTyy4m6fc9SeU1F9IuBwmAqWZ+N3sE8YNumEXEQLqX71OyAOvlg==")
dbutils.widgets.text("gremlin_account_name", "djaunscosgrem")
dbutils.widgets.text("gremlin_database_name", "djaunscosgremdb")
dbutils.widgets.text("graph_name", "demo")
dbutils.widgets.text("gremlin_access_key", "baTQKYY8gnwN1XNhkIkAYcRDPHnl9YRUtfAKAN7L1yiMfv0s8NlXQx2KZvQNl3NEXjhtQMwFZgbxkiS4qWkdfA==")

# COMMAND ----------

storage_account_name=dbutils.widgets.get("storage_account_name")
containerName=dbutils.widgets.get("containerName")
containerName1=dbutils.widgets.get("containerName1")
storage_access_key=dbutils.widgets.get("storage_access_key")
gremlin_account_name=dbutils.widgets.get("gremlin_account_name")
gremlin_database_name=dbutils.widgets.get("gremlin_database_name")
graph_name=dbutils.widgets.get("graph_name")
gremlin_access_key=dbutils.widgets.get("gremlin_access_key")

# COMMAND ----------

print(gremlin_account_name)

# COMMAND ----------

import sys
import traceback

from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def create_destination_node(Client, desAddress, columnInFile, idOfSrc):
    destinationNode = f"g.addV('Vertex').property('firstName', '{desAddress}'){columnInFile}"
    Client.submit(destinationNode)
    queryToFetchDestinationId = f"g.V().has('firstName', '{desAddress}')"
    fetchDestinationId = Client.submit(queryToFetchDestinationId).next()

    updateDestinationNode = f"g.V().has('firstName', '{desAddress}').property('sourceId', '{idOfSrc}')" \
                            f".property('selfId','{fetchDestinationId[-1]['id']}').values('firstName')"
    return Client.submit(updateDestinationNode)


def create_all_node(Client, srcAdd, desAddress, srcColName, desColName):
  try:
    checkExistenceOfNode = f"g.V().has('firstName', '{srcAdd}')"
    sourceNode = f"g.addV('Vertex').property('firstName', '{srcAdd}'){srcColName}"
    Client.submit(sourceNode)
    decisionNodeExistence = Client.submit(checkExistenceOfNode).next()
    sourceId = decisionNodeExistence[-1]['id']
    updateSourceNode = f"g.V().has('firstName', '{srcAdd}').property('sourceId', '').values('firstName')"
    Client.submit(updateSourceNode)
    destinationNode = f"g.addV('Vertex').property('firstName', '{desAddress}'){desColName}"
    Client.submit(destinationNode)
    queryToFetchDestinationId = f"g.V().has('firstName', '{desAddress}')"
    fetchDestinationId = Client.submit(queryToFetchDestinationId).next()
    updateDestinationNode = f"g.V().has('firstName', '{desAddress}').property('sourceId', '{sourceId}')" \
                            f".property('selfId', '{fetchDestinationId[-1]['id']}').values('firstName')"
    return Client.submit(updateDestinationNode)
  except StopIteration:
      print("No more suggestions...")


def create_relations(Client, srcAdd, desAdd, jobId):
    relations = f"g.V().hasLabel('Vertex').has('firstName', '{srcAdd}').addE('{jobId}').to(g.V()" \
                f".hasLabel('Vertex').has('firstName', '{desAdd}'))"
    return Client.submit(relations)


def load_data_to_neo4j(Client, src, des, nameOfOriginalColumn, nameOfChangedColumn):
    sourceExist = False
    checkExistenceOfSrcNode = f"g.V().has('firstName', '{src}')"
    abc = Client.submit(checkExistenceOfSrcNode)
    if abc.all().result():
        decisionSrcNodeExistence = Client.submit(checkExistenceOfSrcNode).next()
        sourceExist = True
        if sourceExist:
            print("Source Exist....Creating Destination Nodes....")
            create_destination_node(Client, des, nameOfChangedColumn, decisionSrcNodeExistence[-1]['id'])
            print("Yiiippppeee...Destination Node Created ✌")
        else:
            create_all_node(Client, src, des, nameOfOriginalColumn, nameOfChangedColumn)
            print("Yiiippppeee...All Node Created ✌")
    else:
        print("No Source Exist...Creating All Nodes...")
        create_all_node(Client, src, des, nameOfOriginalColumn, nameOfChangedColumn)
        print("Yiiippppeee...All Nodes Created ✌")
    print(f"Source & Destination Exist....Creating Relations between Source: {src} & Destination: {des}")
    create_relations(Client, src, des, spark.sparkContext.applicationId)
    print("Yiiippppeee...Created Relations Between Node ✌")


def create_column_name_string(df):
    b = [f"'column_{i}','{c}'" for i, c in enumerate(df.columns)]
    return b


def column_with_property(c):
    b = ""
    for i in c:
        a = f"property({i})"
        b = b + "." + a
    return b


def gremlin_client_connection(gremlin_account_name, gremlin_database_name, graph_name, gremlin_access_key):
    return client.Client(f"wss://{gremlin_account_name}.gremlin.cosmos.azure.com:443/", "g",
                         username=f"/dbs/{gremlin_database_name}/colls/{graph_name}",
                         password=gremlin_access_key,
                         message_serializer=serializer.GraphSONSerializersV2d0()
                         )


try:

    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_access_key)
    
    str3 = f"abfss://{containerName}@{storage_account_name}.dfs.core.windows.net/datasets/customers"
    str4 = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/customers"
    str5 = f"abfss://{containerName}@{storage_account_name}.dfs.core.windows.net/datasets/demographics"
    str6 = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/demographics"
    str7 = f"abfss://{containerName}@{storage_account_name}.dfs.core.windows.net/datasets/orders"
    str8 = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/orders"
    str9 = f"abfss://{containerName}@{storage_account_name}.dfs.core.windows.net/datasets/products"
    str10 = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/products"

    # Customer
    print("Customer Started.........")
    localDataset1 = spark.read.option("header", True).option("inferSchema", True).csv(str3)
    localDataset2 = localDataset1.na.drop()

    str11_temp = create_column_name_string(localDataset2)
    str11 = column_with_property(str11_temp)

    localDataset2.createOrReplaceTempView("customerView")
    localDataset3 = spark.sql("SELECT * FROM customerView")
    localDataset3.coalesce(1).write.mode("overwrite").option("header", True).option("inferSchema", True).csv(str4)

    str12_temp = create_column_name_string(localDataset3)
    str12 = column_with_property(str12_temp)

    print("Customer End.........")

    # Demographics
    print("Demographics Started.........")
    localDataset4 = spark.read.option("header", True).option("delimiter", "|").option("inferSchema", True).csv(str5)
    localDataset5 = localDataset4.na.drop()

    str13_temp = create_column_name_string(localDataset5)
    str13 = column_with_property(str13_temp)

    localDataset5.createOrReplaceTempView("demographicView")
    localDataset6 = spark.sql("SELECT * FROM demographicView")
    localDataset6.coalesce(1).write.mode("overwrite").option("header", True).option("inferSchema", True).csv(str6)

    str14_temp = create_column_name_string(localDataset6)
    str14 = column_with_property(str14_temp)

    print("Demographics End.........")

    # Product
    print("Product Started.........")
    localDataset7 = spark.read.option("header", True).option("inferSchema", True).csv(str9)
    localDataset8 = localDataset7.na.drop()
    
    str15_temp = create_column_name_string(localDataset8)
    str15 = column_with_property(str15_temp)
    
    localDataset8.createOrReplaceTempView("productView")
    localDataset9 = spark.sql("SELECT * FROM productView")
    localDataset9.coalesce(1).write.mode("overwrite").option("header", True).option("inferSchema", True).csv(str10)
    
    str16_temp = create_column_name_string(localDataset9)
    str16 = column_with_property(str16_temp)
    
    print("Product End.........")

    # Order
    print("Order Started.........")
    localDataset10 = spark.read.option("header", True).option("inferSchema", True).csv(str7)
    localDataset11 = localDataset10.na.drop()

    str17_temp = create_column_name_string(localDataset11)
    str17 = column_with_property(str17_temp)

    localDataset11.createOrReplaceTempView("orderView")
    localDataset12 = spark.sql("SELECT * FROM orderView")
    localDataset12.coalesce(1).write.mode("overwrite").option("header", True).option("inferSchema", True).csv(str8)

    str18_temp = create_column_name_string(localDataset12)
    str18 = column_with_property(str18_temp)

    print("Order End.........")
    
#     gremlin_client = gremlin_client_connection(gremlin_account_name, gremlin_database_name, graph_name, gremlin_access_key)
#     load_data_to_neo4j(gremlin_client, str3, str4, str11, str12)
    
#     load_data_to_neo4j(gremlin_client, str5, str6, str13, str14)
    
#     load_data_to_neo4j(gremlin_client, str9, str10, str15, str16)
    
#     load_data_to_neo4j(gremlin_client, str7, str8, str17, str18)
#     gremlin_client.close()

except GremlinServerError as e:
    print('Code: {0}, Attributes: {1}'.format(e.status_code, e.status_attributes))
    cosmos_status_code = e.status_attributes["x-ms-status-code"]
    if cosmos_status_code == 409:
        print('Conflict error!')
    elif cosmos_status_code == 412:
        print('Precondition error!')
    elif cosmos_status_code == 429:
        print('Throttling error!')
    elif cosmos_status_code == 1009:
        print('Request timeout error!')
    else:
        print("Default error handling")

    traceback.print_exc(file=sys.stdout)
    sys.exit(1)

# COMMAND ----------


