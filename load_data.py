import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, row_number, lit
from snowflake.snowpark.window import Window

def main(session: snowpark.Session): 
    print("Starting main function...")
    
    # Step 1: Load the transactions table into a DataFrame
    print("Step 1: Loading transactions table...")
    transactions_df = session.table("transactions")
    print(f"Transactions table loaded. Row count: {transactions_df.count()}")
    
    # Step 2: Create unique IDs for customerId and articleId
    print("Step 2: Creating unique numeric IDs for customerId and articleId...")
    
    # Extract unique customer IDs and assign a numeric ID starting from 1
    customer_ids = transactions_df.select(col("customerId").cast("STRING").alias("original_id")).distinct()
    customer_ids = customer_ids.with_column(
        "nodeid", row_number().over(Window.order_by("original_id"))
    )
    
    # Extract unique article IDs and assign a numeric ID starting from 1,000,000
    article_ids = transactions_df.select(col("articleId").cast("STRING").alias("original_id")).distinct()
    article_ids = article_ids.with_column(
        "nodeid", row_number().over(Window.order_by("original_id")) + lit(1000000)
    )

    # Combine both into a single nodes DataFrame
    nodes_df = customer_ids.union_all(article_ids)

    # drop original id
    nodes_df = nodes_df.drop('original_id')    
    print(f"Total unique nodes count: {nodes_df.count()}")
    
    # Step 3: Create the 'relationships' DataFrame
    print("Step 3: Creating relationships DataFrame...")
    
    # Join transactions with the numeric IDs for customers and articles
    relationships_df = transactions_df.join(
        customer_ids,
        transactions_df["customerId"] == customer_ids["original_id"],
        "inner"
    ).select(
        col("nodeid").alias("sourcenodeid"),
        col("articleId")
    ).join(
        article_ids,
        col("articleId") == article_ids["original_id"],
        "inner"
    ).select(
        col("sourcenodeid"),
        col("nodeid").alias("targetnodeid")
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
            nodeid INT NOT NULL PRIMARY KEY
            )
    """).collect()
    
    # Create the 'relationships' table
    session.sql("""
        CREATE TABLE relationships (
            sourcenodeid INT NOT NULL,
            targetnodeid INT NOT NULL,
            FOREIGN KEY (sourcenodeid) REFERENCES nodes(nodeid),
            FOREIGN KEY (targetnodeid) REFERENCES nodes(nodeid)
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
    # keeping this for debugging
    session.sql("SELECT * FROM nodes LIMIT 5").show()
    # then dropping original id
    session.sql("ALTER TABLE nodes DROP COLUMN original_id")

    print("\nRelationships table sample:")
    
    # Return a sample of the nodes DataFrame for verification
    return nodes_df.limit(10)
