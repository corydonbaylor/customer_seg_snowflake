# Customer Segmentation in Snowflake

Customer segmentation enables companies to tailor their products and services to different groups of customers. Graph algorithms offer a powerful approach to solving customer segmentation challenges by leveraging the inherent relationships and interactions within customer data. Before **Neo4j Graph Analytics for Snowflake**, leveraging these powerful algorithms was exceedingly difficult for data stored in snowflake. Let's jump into how we can deploy these algorithms for snowflake data.

## Getting and Cleaning our Data

We will use the H&M sample data from Kaggle as a starting point for our analysis. Specifically, we will be using a `transaction.csv` as a starting point. You can download this data like so:

```python
transaction_df = pd.read_csv('https://storage.googleapis.com/neo4j-workshop-data/genai-hm/transaction.csv')
```

Next you will need to load this into snowflake as a table. I created a database called `HM_SEG` to store the various tables we will be creating. I am not going to walk through how to clean and load the needed tables (you can take a look at `load_data.py` to see how I did this if you are curious), but Graph Analytics for Snowflake does require a few conditions in order to run.

Namely:

- You will need two tables, one for *Nodes* and one for *Relationships* in order to create a projection
- The first row in the *Nodes* table must be the `NodeId` for the nodes
- In the *Relationships* table, you must name the columns: `SOURCENODEID` and `TARGETNODEID` 

In our case, the *Nodes* table contains the ids for both the customers who purchased the product and the ids for the product. And the *Relationships* table contains a mapping of what customers bought which products.

## Setting Up the Environment

Now that we have our tables in snowflake, it is time to create and project a graph. But before we do that, there is a little bit of house keeping we must complete in order to have access to graph analytics app:

```sql
CREATE ROLE IF NOT EXISTS gds_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_user TO ROLE gds_role;
-- Create a consumer role for administrators of the GDS application
CREATE ROLE IF NOT EXISTS gds_admin_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_admin TO ROLE gds_admin_role;

-- Grant access to consumer data
-- The application reads consumer data to build a graph object, and it also writes results into new tables.
-- We therefore need to grant the right permissions to give the application access.
GRANT USAGE ON DATABASE HM_SEG TO APPLICATION neo4j_graph_analytics;
GRANT USAGE ON SCHEMA HM_SEG.PUBLIC TO APPLICATION neo4j_graph_analytics;

-- required to read view data into a graph
GRANT SELECT ON ALL VIEWS IN SCHEMA HM_SEG.PUBLIC TO APPLICATION neo4j_graph_analytics;
GRANT SELECT ON ALL TABLES IN SCHEMA HM_SEG.PUBLIC TO APPLICATION neo4j_graph_analytics;
-- required to write computation results into a table
GRANT CREATE TABLE ON SCHEMA HM_SEG.PUBLIC TO APPLICATION neo4j_graph_analytics;
-- optional, ensuring the consumer role has access to tables created by the application
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA HM_SEG.PUBLIC TO ROLE gds_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA HM_SEG.PUBLIC TO ROLE accountadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA HM_SEG.PUBLIC TO ROLE accountadmin;

-- Spin up Neo4j
CALL neo4j_graph_analytics.gds.create_session('CPU_X64_M');
```

## Creating a Graph and Running Algos

Ok now the graph analytics app is up and running and accessible in our notebook. Let's create a graph:

```sql
-- create 
SELECT neo4j_graph_analytics.gds.graph_project('graph', {
    'nodeTable': 'HM_SEG.public.nodes',
    'relationshipTable': 'HM_SEG.public.relationships',
    'readConcurrency': 28
});
```

The projection here is pretty simple. We are using the node and relationship tables as (you guessed it), the nodes and relationships within our projection. Before we go any further, we need to check and see how connected our graph really is.

### Checking How Connected Our Graph Is

We will run weakly connected components to immediately check. If there are no products in common between different customers than each pair of customer and purchase will get their own "component community". If there is a lot customers bought the same items, than we should expect a web of connections linking various customers together. 

In the first scenario, there is no amount of clustering technique that will create anything meaningful from our data. Let's run WCC and find out:

```sql
SELECT neo4j_graph_analytics.gds.wcc('graph', {
    'mutateProperty': 'wcc_groups'
});
```

Our results:

```
"componentCount": 131,
    "componentDistribution": {
      "max": 13901,
      "mean": 109.54961832061069,
      "min": 2,
      "p1": 2,
      "p10": 2,
      "p25": 2,
      "p5": 2,
      "p50": 3,
      "p75": 3,
      "p90": 6,
      "p95": 8,
      "p99": 32,
      "p999": 13901
    },
```

A few things to highlight here:

We had 131 different communities according to WCC, and the average size of those communities is 109 nodes. As you can see, at the tail end of the distribution, there is one huge community with 13,901 nodes within it. And on the other end, there are a lot of smaller communities with only two member nodes. 

### Running Similarity and Louvain

Next we will run node similarity to create relationships between nodes that are similar to each other:

```sql
SELECT neo4j_graph_analytics.gds.node_similarity('graph', {
    'mutateRelationshipType': 'SIMILAR_NODES_TO',
    'mutateProperty': 'similarity',
    'concurrency': 28
});
```

And then just using this new relationship (called `SIMILAR_NODES_TO`) we will use Louvain to find communities based on modularity&mdash; which is a measure of how internally connected clusters are to each other compared to externally connected they are to other clusters.

```sql
SELECT neo4j_graph_analytics.gds.louvain('graph', {
    'relationshipTypes': ['SIMILAR_NODES_TO'],
    'relationshipWeightProperty': 'similarity',
    'mutateProperty': 'cohort'
});
```

Unfortunately, almost every node is now in its own community. That means that there was not many nodes that had a connection based on their similarity scores. We can double check this by running WCC, which gives similarly grim results:

``` sql
-- it appears that the graph is largely unconnected
SELECT neo4j_graph_analytics.gds.wcc('graph', {
    'relationshipTypes': ['SIMILAR_NODES_TO'],
    'relationshipWeightProperty': 'similarity',
    'mutateProperty': 'cohort2'
});
```

