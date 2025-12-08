# 📘 Query Lineage Exploration (QLE)

**Query Lineage Exploration (QLE)** is an interactive system for capturing, visualizing, and reusing **SQL query lineage** during exploratory data analysis.  
This project was developed as a final project for **CS 6530 — Advanced Databases** at the University of Utah.

QLE helps analysts navigate branching SQL analysis paths by:

- Logging all executed SQL queries  
- Tracking dependencies between queries  
- Extracting referenced tables  
- Visualizing lineage as a directed graph  
- Allowing reuse through pinned materialized views  
- Supporting replay and preview of past queries  

---

## ✨ Features

### Query Logging
Every executed query is stored along with:
- SQL text  
- Runtime  
- Row count  
- Error message (if any)  
- Tables referenced  
- Parent queries (lineage edges)

### Lineage Visualization
A directed graph (NetworkX + Matplotlib) shows how queries derive from one another.

### Materialized View Pinning  
Any query can be *pinned* as a materialized view for fast reuse downstream.  
Metadata stored includes:
- View name  
- Byte size  
- Originating query  

### Result Preview
The interface always shows the **most recently executed** query’s output, even after UI reruns.

### Metadata Management
You can:
- Delete an individual query (and its lineage/pinned view)  
- Clear all QLE metadata (IDs reset to 1)  
- Preserve your IMDB dataset at all times  

---


