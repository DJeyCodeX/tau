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
dbutils.widgets.text("storage_account_name", "quickstartdatalake")
dbutils.widgets.text("containerName", "transformed")
dbutils.widgets.text("containerName1", "publish")
dbutils.widgets.text("storage_access_key", "i3efECABhQkCuCtkWyhke7BBqgMeQgWD9WRKmvjbb/1xtshvr8SjFI4DajcRmgApA/ngbOZd59LsbdJfMsXmSw==")
dbutils.widgets.text("gremlin_account_name", "djaunsgrem")
dbutils.widgets.text("gremlin_database_name", "djaunsgremdb")
dbutils.widgets.text("graph_name", "demo")
dbutils.widgets.text("gremlin_access_key", "vLLZNq7PtubbqztgQAFpKHjnJ4KSQOxmDudjprHHKSjMku8nT5lo8wNxUYTDEC4PEe4TSqteoXxJwCcjKOXwQQ==")

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

import sys
import traceback

from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def create_column_name_string(df):
    b = [f"'column_{i}','{c}'" for i, c in enumerate(df.columns)]
    return b


def column_with_property(column_array):
    b = ""
    for i in column_array:
        a = f"property({i})"
        b = b + "." + a
    return b


def create_destination_node(Client, desAddress, columnInFile, idOfSrc):
 
    # start
    queryToFetchDId = f"g.V().has('firstName', '{desAddress}')"
    Client.submit(queryToFetchDId)
    # end

    destinationNode = f"g.addV('Vertex').property('firstName', '{desAddress}'){columnInFile}"
    Client.submit(destinationNode).next()
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
    updateSourceNode = f"g.V().has('firstName', '{srcAdd}').property('sourceId', '0').values('firstName')"
    Client.submit(updateSourceNode)

    # change start
    destExist = False
    destId = None
    checkExistenceOfNode = f"g.V().has('firstName', '{desAddress}')"
    decisionSrcNodeExistence = Client.submit(checkExistenceOfNode)
    destExist = True
    destId = decisionSrcNodeExistence[-1]['id']
    # changes end

    destinationNode = f"g.addV('Vertex').property('firstName', '{desAddress}'){desColName}"
    Client.submit(destinationNode)
    queryToFetchDestinationId = f"g.V().has('firstName', '{desAddress}')"
    fetchDestinationId = Client.submit(queryToFetchDestinationId).next()

    updateDestinationNode = f"g.V().has('firstName', '{desAddress}').property('destId', '{destId}')" \
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
    sourceId = ""
    checkExistenceOfSrcNode = f"g.V().has('firstName', '{src}')"
    deciSrcNodeCheckExist = Client.submit(checkExistenceOfSrcNode)
    if deciSrcNodeCheckExist.all().result():
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


def gremlin_client_connection(gremlin_account_name, gremlin_database_name, graph_name, gremlin_access_key):
    return client.Client(f"wss://{gremlin_account_name}.gremlin.cosmos.azure.com:443/", 'g',
                         username=f"/dbs/{gremlin_database_name}/colls/{graph_name}",
                         password=gremlin_access_key,
                         message_serializer=serializer.GraphSONSerializersV2d0()
                         )

try:

    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_access_key)

    customer_src_url = f"abfss://{containerName}@{storage_account_name}.dfs.core.windows.net/customers"
    age_wise_expend_dist_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/age_wise_exp_dist_result_2"
    demographic_src_url = f"abfss://{containerName}@{storage_account_name}.dfs.core.windows.net/demographics"
    big_spenders_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/big_spenders_result"
    order_src_url = f"abfss://{containerName}@{storage_account_name}.dfs.core.windows.net/orders"
    gender_ord_by_mon_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/gender_orders_by_month_result"
    product_src_url = f"abfss://{containerName}@{storage_account_name}.dfs.core.windows.net/products"
    gender_prod_category_count_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/gender_product_category_count_result"
    house_hold_income_distribution_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/house_hold_income_distribution_result"
    loyal_customer_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/loyal_customer_result"
    order_dist_by_day_of_week_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/order_distribution_by_day_of_week_result"
    payment_type_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/payment_type_result"
    state_wise_order_dist_url = f"abfss://{containerName1}@{storage_account_name}.dfs.core.windows.net/state_wise_order_distribution_result"

    gremlin_client = gremlin_client_connection(gremlin_account_name, gremlin_database_name, graph_name, gremlin_access_key)
    # read csv from customer_src_url and create new coloumn name
    dataframe_customer = spark.read.option("header", True).option("inferSchema", True).csv(customer_src_url)
    nameOfCustomerColumn_temp = create_column_name_string(dataframe_customer)
    nameOfCustomerColumn = column_with_property(nameOfCustomerColumn_temp)

    # read csv from age_wise_expend_dist_url and create new coloumn name
    age_wise_expend_dist = spark.read.option("header", True).option("inferSchema", True).csv(age_wise_expend_dist_url)
    nameOfNewCustomerColumn_temp = create_column_name_string(age_wise_expend_dist)
    nameOfNewCustomerColumn = column_with_property(nameOfNewCustomerColumn_temp)

    load_data_to_neo4j(gremlin_client, customer_src_url, age_wise_expend_dist_url, nameOfCustomerColumn,
                       nameOfNewCustomerColumn)

    # read csv from order_src_url and create new column name
    dataframe_orders = spark.read.option("header", True).option("inferSchema", True).csv(order_src_url)
    nameOfordersColumn_temp = create_column_name_string(dataframe_orders)
    nameOfordersColumn = column_with_property(nameOfordersColumn_temp)

    load_data_to_neo4j(gremlin_client, order_src_url, age_wise_expend_dist_url, nameOfordersColumn,
                       nameOfNewCustomerColumn)

    dataframe_customer_big_spenders = spark.read.option("header", True).option("inferSchema", True).csv(
        customer_src_url)
    nameOfCustomerColumn = create_column_name_string(dataframe_customer_big_spenders)
    nameOfCustomerColumn_big_spenders = column_with_property(nameOfCustomerColumn)

    big_spenders1 = spark.read.option("header", True).option("inferSchema", True).csv(big_spenders_url)
    nameOfNewCustomer_big_temp = create_column_name_string(big_spenders1)
    nameOfNewCustomer_big_spenders_url = column_with_property(nameOfNewCustomer_big_temp)

    load_data_to_neo4j(gremlin_client, customer_src_url, big_spenders_url, nameOfCustomerColumn_big_spenders,
                       nameOfNewCustomer_big_spenders_url)

    dataframe_orders_big_spenders = spark.read.option("header", True).option("inferSchema", True).csv(order_src_url)
    nameOfordersColumn_big_temp = create_column_name_string(dataframe_orders_big_spenders)
    nameOfordersColumn_big_spenders = column_with_property(nameOfordersColumn_big_temp)

    load_data_to_neo4j(gremlin_client, order_src_url, big_spenders_url, nameOfordersColumn_big_spenders,
                       nameOfNewCustomer_big_spenders_url)

    dataframe_customer_gender_orders = spark.read.option("header", True).option("inferSchema", True).csv(
        customer_src_url)
    nameOfCustomerColumn_gender_temp = create_column_name_string(dataframe_customer_gender_orders)
    nameOfCustomerColumn_gender_orders = column_with_property(nameOfCustomerColumn_gender_temp)

    gender_orders1 = spark.read.option("header", True).option("inferSchema", True).csv(gender_ord_by_mon_url)
    nameOfNewCustomerColumn_gender_orders_temp = create_column_name_string(gender_orders1)
    nameOfNewCustomerColumn_gender_orders_by_month_url = column_with_property(
        nameOfNewCustomerColumn_gender_orders_temp)

    load_data_to_neo4j(gremlin_client, customer_src_url, gender_ord_by_mon_url, nameOfCustomerColumn_gender_orders,
                       nameOfNewCustomerColumn_gender_orders_by_month_url)

    dataframe_orders_gender_orders = spark.read.option("header", True).option("inferSchema", True).csv(order_src_url)
    nameOfordersColumn_gend_temp = create_column_name_string(dataframe_orders_gender_orders)
    nameOfordersColumn_gender_orders = column_with_property(nameOfordersColumn_gend_temp)

    load_data_to_neo4j(gremlin_client, order_src_url, gender_ord_by_mon_url, nameOfordersColumn_gender_orders,
                       nameOfNewCustomerColumn_gender_orders_by_month_url)

    dataframe_customer_gender_product = spark.read.option("header", True).option("inferSchema", True).csv(
        customer_src_url)
    nameOfCustomerColumn_gender_product_temp = create_column_name_string(dataframe_customer_gender_product)
    nameOfCustomerColumnForNeo4j_gender_product = column_with_property(nameOfCustomerColumn_gender_product_temp)

    gender_product1 = spark.read.option("header", True).option("inferSchema", True).csv(gender_prod_category_count_url)
    nameOfNewCustomerColumn_gender_temp = create_column_name_string(gender_product1)
    nameOfNewCustomerColumn_gender_product_category_count_url = column_with_property(
        nameOfNewCustomerColumn_gender_temp)

    load_data_to_neo4j(gremlin_client, customer_src_url, gender_prod_category_count_url,
                       nameOfCustomerColumnForNeo4j_gender_product,
                       nameOfNewCustomerColumn_gender_product_category_count_url)

    dataframe_orders_gender_product = spark.read.option("header", True).option("inferSchema", True).csv(order_src_url)
    nameOfordersColumn_gender_temp = create_column_name_string(dataframe_orders_gender_product)
    nameOfordersColumn_gender_product = column_with_property(nameOfordersColumn_gender_temp)

    gender_product2 = spark.read.option("header", True).option("inferSchema", True).csv(gender_prod_category_count_url)
    nameOfNewordersColumn_gender_temp = create_column_name_string(gender_product2)
    nameOfNewordersColumn_gender_product_category_count_url = column_with_property(nameOfNewordersColumn_gender_temp)

    load_data_to_neo4j(gremlin_client, order_src_url, gender_prod_category_count_url, nameOfordersColumn_gender_product,
                       nameOfNewordersColumn_gender_product_category_count_url)

    dataframe_product_gender_product = spark.read.option("header", True).option("inferSchema", True).csv(
        product_src_url)
    nameOfproductColumn_gender_temp = create_column_name_string(dataframe_product_gender_product)
    nameOfproductColumn_gender_product = column_with_property(nameOfproductColumn_gender_temp)

    gender_product3 = spark.read.option("header", True).option("inferSchema", True).csv(gender_prod_category_count_url)
    nameOfNewproductColumn_temp = create_column_name_string(gender_product3)
    nameOfNewproductColumn_gender_product_category_count_url = column_with_property(nameOfNewproductColumn_temp)

    load_data_to_neo4j(gremlin_client, product_src_url, gender_prod_category_count_url,
                       nameOfproductColumn_gender_product, nameOfNewproductColumn_gender_product_category_count_url)

    dataframe_customer_loyal_customer = spark.read.option("header", True).option("inferSchema", True).csv(
        customer_src_url)
    nameOfCustomerColumn_loyal_temp = create_column_name_string(dataframe_customer_loyal_customer)
    nameOfCustomerColumn_loyal_customer = column_with_property(nameOfCustomerColumn_loyal_temp)

    loyal_customer1 = spark.read.option("header", True).option("inferSchema", True).csv(loyal_customer_url)
    nameOfNewCustomerColumn_loyal_temp = create_column_name_string(loyal_customer1)
    nameOfNewCustomerColumn_loyal_customer = column_with_property(nameOfNewCustomerColumn_loyal_temp)

    load_data_to_neo4j(gremlin_client, customer_src_url, loyal_customer_url, nameOfCustomerColumn_loyal_customer,
                       nameOfNewCustomerColumn_loyal_customer)

    dataframe_orders_loyal_customer = spark.read.option("header", True).option("inferSchema", True).csv(order_src_url)
    nameOfordersColumn_loyal_temp = create_column_name_string(dataframe_orders_loyal_customer)
    nameOfordersColumn_loyal_customer = column_with_property(nameOfordersColumn_loyal_temp)

    load_data_to_neo4j(gremlin_client, order_src_url, loyal_customer_url, nameOfordersColumn_loyal_customer,
                       nameOfNewCustomerColumn_loyal_customer)

    dataframe_orders_order_distribution = spark.read.option("header", True).option("inferSchema", True).csv(
        order_src_url)
    nameOfordersColumn_order_temp = create_column_name_string(dataframe_orders_order_distribution)
    nameOfordersColumn_order_distribution = column_with_property(nameOfordersColumn_order_temp)

    order_distribution2 = spark.read.option("header", True).option("inferSchema", True).csv(
        order_dist_by_day_of_week_url)
    nameOfNewordersColumn_order_temp = create_column_name_string(order_distribution2)
    nameOfNewordersColumn_order_distribution = column_with_property(nameOfNewordersColumn_order_temp)

    load_data_to_neo4j(gremlin_client, order_src_url, order_dist_by_day_of_week_url,
                       nameOfordersColumn_order_distribution, nameOfNewordersColumn_order_distribution)

    dataframe_orders_payment_type = spark.read.option("header", True).option("inferSchema", True).csv(order_src_url)
    nameOfordersColumn_payment_temp = create_column_name_string(dataframe_orders_payment_type)
    nameOfordersColumn_payment_type = column_with_property(nameOfordersColumn_payment_temp)

    payment_type1 = spark.read.option("header", True).option("inferSchema", True).csv(payment_type_url)
    nameOfNewordersColumn_temp = create_column_name_string(payment_type1)
    nameOfNewordersColumn_payment_type = column_with_property(nameOfNewordersColumn_temp)

    load_data_to_neo4j(gremlin_client, order_src_url, payment_type_url, nameOfordersColumn_payment_type,
                       nameOfNewordersColumn_payment_type)

    dataframe_customer_state_wise_order_distribution = spark.read.option("header", True).option("inferSchema",
                                                                                                True).csv(
        customer_src_url)
    nameOfCustomerColumn_state_temp = create_column_name_string(dataframe_customer_state_wise_order_distribution)
    nameOfCustomerColumn_state_wise_order_distribution = column_with_property(nameOfCustomerColumn_state_temp)

    state_wise_order_distribution1 = spark.read.option("header", True).option("inferSchema", True).csv(
        state_wise_order_dist_url)
    nameOfNewCustomerColumn_stemp = create_column_name_string(state_wise_order_distribution1)
    nameOfNewCustomerColumn_state_wise_order_distribution = column_with_property(nameOfNewCustomerColumn_stemp)

    load_data_to_neo4j(gremlin_client, customer_src_url, state_wise_order_dist_url,
                       nameOfCustomerColumn_state_wise_order_distribution,
                       nameOfNewCustomerColumn_state_wise_order_distribution)

    dataframe_orders_state_wise_order_distribution = spark.read.option("header", True).option("inferSchema", True).csv(
        order_src_url)
    nameOfordersColumn_stemp = create_column_name_string(dataframe_orders_state_wise_order_distribution)
    nameOfordersColumn_state_wise_order_distribution = column_with_property(nameOfordersColumn_stemp)

    load_data_to_neo4j(gremlin_client, order_src_url, state_wise_order_dist_url,
                       nameOfordersColumn_state_wise_order_distribution,
                       nameOfNewCustomerColumn_state_wise_order_distribution)

    gremlin_client.close()

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


