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

-- fail if missing
SELECT neo4j_graph_analytics.gds.graph_drop('graph', { 'failIfMissing': false });

select * from HM_SEG.public.nodes;

-- Q: can you not have ANY non numeric variables?

-- removing nodetype
-- ALTER TABLE HM_SEG.public.nodes DROP COLUMN original_id;
-- ALTER TABLE HM_SEG.public.nodes DROP COLUMN unitprice;
select * from HM_SEG.public.nodes;
select * from RETAIL_EXAMPLE.public.relationships;


-- create 
SELECT neo4j_graph_analytics.gds.graph_project('graph', {
    'nodeTable': 'HM_SEG.public.nodes',
    'relationshipTable': 'HM_SEG.public.relationships',
    'readConcurrency': 28
});


-- run node similarity to find similar patients by encounter procedures
SELECT neo4j_graph_analytics.gds.node_similarity('graph', {
    'mutateRelationshipType': 'SIMILAR_NODES_TO',
    'mutateProperty': 'similarity',
    'concurrency': 28
});
-- first we will creates a similarity score which will connect different customer
-- then we will filter down to just that relationship and customers and run louvain
SELECT neo4j_graph_analytics.gds.louvain('graph', {
    'relationshipTypes': ['SIMILAR_NODES_TO'],
    'relationshipWeightProperty': 'similarity',
    'mutateProperty': 'cohort'
});

-- it appears that the graph is largely unconnected
SELECT neo4j_graph_analytics.gds.wcc('graph', {
    'relationshipTypes': ['SIMILAR_NODES_TO'],
    'relationshipWeightProperty': 'similarity',
    'mutateProperty': 'cohort2'
});
