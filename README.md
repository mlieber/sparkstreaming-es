# sparkstreaming-es

The purpose of the applications described is to stream files generated by IoT sensors, and generate various analytics about the telemetric device measurements, such as minimum, maximum, average of heart rate, and heart rate variances for the patients. The results are then visualized through a dashboard in graphical & tabular formats.

Data ingestion
This component provides the mechanism for:
•	Streaming data files near real-time and processing analytics on them for later querying, via the Spark Streaming ingestion API.

Visualization/ Reporting
The visualization layer provides a simple interface to filter and retrieve the different attributes to interact with, and provide a graphical analysis of the data with various charting and plotting options in an interactive way. 
Technology stack

	Technology
1	Data/File streaming	Spark-Streaming
2	Storage/Indexing	Elasticsearch
3	Visualization	Kibana

High level Architecture


Spark Streaming Application

Data ingestion configuration
The spark streaming application takes data from either over the network, with the netcat utility to transfer data over the network over TCP, or from a folder. The option is configurable from the config/medtronicProperties file (see Installation document), with full configuration of netcat (server, port) and folder location. Once running, the application is looping, polling for data.

Data storage configuration
The application expects the data store to have been initialized; here the Search store index. The application will take care of creating the relevant tables and/or index types needed to store the data automatically. 
Ingestion
Once data is found, the streaming application ingests it, whether in an index store (Elastic Search).
Parsing of the data
Once the data file has been fully ingested, it is parsed and filtered according to the filtering category found in the configuration file. By default, that is set to filtering on the proper sub-category. 
The data from the file is first stored in an index / table, under the name ‘trialrecord’. This essentially holds the raw data from the data file, so as to be searched or retrieved later. 


Choice of technology
Elasticsearch

Overview
Elasticsearch is a flexible and powerful open source distributed real-time search and analytics engine designed from the ground up to be distributed and scalable. Data in Elasticsearch is stored as structured JSON documents.  All fields are indexed by default, and all the indices can be used in a single query. Elasticsearch by default is schema-less; it will try to detect the data structure within a JSON document to index the data and make it searchable. In our design a schema is provided for added robustness.
Elasticsearch clusters are resilient – they will detect and remove failed nodes, and reorganize themselves automatically.



Spark

Overview
Apache Spark is an open source cluster-computing framework. Spark’s in-memory primitives provide high performance for data batch processing applications. Spark Streaming is a sub-project of Apache Spark. 
Spark Streaming is a real-time processing tool that runs on top of the Spark engine. In Spark Streaming, batches of Resilient Distributed Datasets (RDDs) are passed to Spark Streaming, which processes these batches using the Spark Engine and returns a processed stream of batches. Spark Streaming allows stateful computations—maintaining a state based on data coming in a stream.

It is worth noting that this utilizes the new mapWithState() function (in Spark 1.6) as opposed to updateStateByKey ().
