import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    print("Starting main function...")
    
    # Step 1: Load the transactions table into a DataFrame
    print("Step 1: Loading transactions table...")
    transactions_df = session.table("transactions")
    print(f"Transactions table loaded. Row count: {transactions_df.count()}")
    
    # Step 2: Create the 'nodes' DataFrame
    print("Step 2: Creating nodes DataFrame...")
    # Extract unique customerId and articleId values
    customer_ids = transactions_df.select(col("customerId").cast("STRING").alias("nodeid")).distinct()
    article_ids = transactions_df.select(col("articleId").cast("STRING").alias("nodeid")).distinct()
    
    print(f"Unique customer IDs count: {customer_ids.count()}")
    print(f"Unique article IDs count: {article_ids.count()}")
    
    # Combine customerId and articleId into a single column called 'nodeid'
    nodes_df = customer_ids.union_all(article_ids).distinct()
    
    print(f"Total unique nodes count: {nodes_df.count()}")
    
    # Step 3: Create the 'relationships' DataFrame
    print("Step 3: Creating relationships DataFrame...")
    relationships_df = transactions_df.select(
        col("customerId").cast("STRING").alias("fromNodeid"),
        col("articleId").cast("STRING").alias("toNodeid")
    )
    
    print(f"Relationships DataFrame row count: {relationships_df.count()}")
    
    # Step 4: Create tables in Snowflake
    print("Step 4: Dropping existing tables if they exist...")
    session.sql("DROP TABLE IF EXISTS nodes").collect()
    session.sql("DROP TABLE IF EXISTS relationships").collect()
    
    print("Creating new tables...")
    # Create the 'nodes' table
    session.sql("""
        CREATE TABLE nodes (
            nodeid VARCHAR(256) NOT NULL PRIMARY KEY
        )
    """).collect()
    
    # Create the 'relationships' table
    session.sql("""
        CREATE TABLE relationships (
            fromNodeid VARCHAR(256) NOT NULL,
            toNodeid VARCHAR(256) NOT NULL,
            FOREIGN KEY (fromNodeid) REFERENCES nodes(nodeid),
            FOREIGN KEY (toNodeid) REFERENCES nodes(nodeid)
        )
    """).collect()
    
    print("Tables created successfully.")
    
    # Step 5: Write DataFrames into the respective tables
    print("Step 5: Writing data to tables...")
    nodes_df.write.save_as_table("nodes", mode="overwrite")
    print("Nodes table populated.")
    
    relationships_df.write.save_as_table("relationships", mode="overwrite")
    print("Relationships table populated.")
    
    # Display success messages
    print("Nodes and Relationships tables have been successfully created and populated.")
    
    # Verify table contents
    print("\nVerifying table contents:")
    print("Nodes table sample:")
    session.sql("SELECT * FROM nodes LIMIT 5").show()
    
    print("\nRelationships table sample:")
    session.sql("SELECT * FROM relationships LIMIT 5").show()
    
    # Return a sample of the nodes DataFrame for verification
    return nodes_df.limit(10)
