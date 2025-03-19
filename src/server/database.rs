use omnipaxos_kv::common::kv::KVCommand;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};
use std::collections::HashMap;
use std::env;
use log::*;

#[derive(Debug, Clone)]
pub enum QueryResult {
    SingleValue(Option<String>),
    Rows(Vec<Vec<String>>),
    Success, // For non-query operations
}

pub struct Database {
    db: HashMap<String, String>,              // Keep the original KV store for compatibility
    sql_pool: Option<Pool<Postgres>>,         // SQL connection pool
}

impl Database {
    pub async fn new() -> Self {
        // Try to connect to PostgreSQL
        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@postgres:5432/omnipaxos_db".to_string());
        
        info!("Attempting to connect to PostgreSQL at {}", database_url);
        
        let sql_pool = match PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
        {
            Ok(pool) => {
                // Initialize database with a simple table for testing
                match Self::init_database(&pool).await {
                    Ok(_) => info!("Database initialized successfully"),
                    Err(e) => error!("Failed to initialize database: {}", e),
                }
                
                info!("Connected to PostgreSQL");
                Some(pool)
            }
            Err(e) => {
                error!("Failed to connect to PostgreSQL: {}", e);
                None
            }
        };

        Self {
            db: HashMap::new(),
            sql_pool,
        }
    }

    // Initialize the database with tables
    async fn init_database(pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
        info!("Initializing database tables");
        
        // Create a simple test table if it doesn't exist
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS data (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )"
        )
        .execute(pool)
        .await?;

        info!("Created data table");
        Ok(())
    }

    // Handle KV and SQL commands
    pub async fn handle_command(&mut self, command: KVCommand) -> Option<Result<QueryResult, String>> {
        match command {
            // Handle original KV commands
            KVCommand::Put(key, value) => {
                debug!("KV PUT: {} = {}", key, value);
                
                // Also store in SQL database if connected
                if let Some(pool) = &self.sql_pool {
                    match sqlx::query("INSERT INTO data (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2")
                        .bind(&key)
                        .bind(&value)
                        .execute(pool)
                        .await
                    {
                        Ok(_) => debug!("Inserted into SQL database: {} = {}", key, value),
                        Err(e) => error!("Error inserting into SQL database: {}", e),
                    }
                }
                
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                debug!("KV DELETE: {}", key);
                
                // Also delete from SQL database if connected
                if let Some(pool) = &self.sql_pool {
                    match sqlx::query("DELETE FROM data WHERE key = $1")
                        .bind(&key)
                        .execute(pool)
                        .await
                    {
                        Ok(_) => debug!("Deleted from SQL database: {}", key),
                        Err(e) => error!("Error deleting from SQL database: {}", e),
                    }
                }
                
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => {
                debug!("KV GET: {}", key);
                
                // Try to get from SQL database if connected
                if let Some(pool) = &self.sql_pool {
                    match sqlx::query("SELECT value FROM data WHERE key = $1")
                        .bind(&key)
                        .fetch_optional(pool)
                        .await
                    {
                        Ok(row) => {
                            if let Some(row) = row {
                                let value: String = row.get(0);
                                debug!("Retrieved from SQL database: {} = {}", key, value);
                                return Some(Ok(QueryResult::SingleValue(Some(value))));
                            }
                        }
                        Err(e) => error!("Error retrieving from SQL database: {}", e),
                    }
                }
                
                // Fall back to memory store
                Some(Ok(QueryResult::SingleValue(self.db.get(&key).cloned())))
            }
            
            // Handle SQL commands
            KVCommand::ExecuteQuery(query) => {
                info!("Executing SQL query: {}", query);
                Some(self.execute_sql(&query).await)
            }
            KVCommand::QueryRead(query) => {
                info!("Reading SQL query: {}", query);
                Some(self.query_sql(&query).await)
            }
        }
    }

    // Read directly from the database without going through the log (for local reads)
    pub async fn direct_read(&self, query: &str) -> Result<QueryResult, String> {
        info!("Direct SQL read: {}", query);
        self.query_sql(query).await
    }

    // Execute a SQL query that modifies data
    async fn execute_sql(&self, query: &str) -> Result<QueryResult, String> {
        if let Some(pool) = &self.sql_pool {
            match sqlx::query(query).execute(pool).await {
                Ok(result) => {
                    info!("SQL execute success: {} rows affected", result.rows_affected());
                    Ok(QueryResult::Success)
                },
                Err(e) => {
                    error!("SQL execution error: {}", e);
                    Err(format!("SQL execution error: {}", e))
                },
            }
        } else {
            error!("SQL database not connected");
            Err("SQL database not connected".to_string())
        }
    }

    // Execute a SQL query that reads data
    async fn query_sql(&self, query: &str) -> Result<QueryResult, String> {
        if let Some(pool) = &self.sql_pool {
            match sqlx::query(query).fetch_all(pool).await {
                Ok(rows) => {
                    let mut result = Vec::new();
                    
                    // Process each row
                    for row in rows {
                        let mut row_values = Vec::new();
                        
                        // Get column values as strings
                        for i in 0..row.len() {
                            let value: Option<String> = row.try_get(i)
                                .unwrap_or_else(|_| None);
                            
                            row_values.push(value.unwrap_or_else(|| "NULL".to_string()));
                        }
                        
                        result.push(row_values);
                    }
                    
                    info!("SQL query returned {} rows", result.len());
                    Ok(QueryResult::Rows(result))
                },
                Err(e) => {
                    error!("SQL query error: {}", e);
                    Err(format!("SQL query error: {}", e))
                },
            }
        } else {
            error!("SQL database not connected");
            Err("SQL database not connected".to_string())
        }
    }

    // Check if connected to SQL database
    pub fn is_sql_connected(&self) -> bool {
        self.sql_pool.is_some()
    }
}