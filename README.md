# Retail_Data_Analysis
Real time Retail Data Analysis with the help of Spark Streaming.


Broadly, we will perform the following tasks in this project:
1) Reading the sales data from the Kafka server.
2) Preprocessing the data to calculate additional derived columns such as total_cost etc.
3) Calculating the time-based KPIs and time and country-based KPIs.
4) Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis.


The data contains the following information:

1) Invoice number: Identifier of the invoice
2) Country: Country where the order is placed
3) Timestamp: Time at which the order is placed
4) Type: Whether this is a new order or a return order
5) SKU (Stock Keeping Unit): Identifier of the product being ordered
6) Title: Name of the product is ordered
7) Unit price: Price of a single unit of the product
8) Quantity: Quantity of the product being ordered



Reading input data from Kafka 
Code to take raw Stream data from Kafka server

1) Bootstrap Server - 18.211.252.152
2) Port - 9092
3) Topic - real-time-project
