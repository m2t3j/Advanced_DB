# Query Lineage Exploration (QLE)

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

## Features

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

## How to Use QLE

####  NOTE: This was built on my own personal PSQL server and database. The database configuration can be changed by editing this line of code in app.py: DSN = "dbname=imdb user=postgres password=uromastyx host=localhost port=5432"


### 1. **Run SQL Queries**
- Type SQL into the query editor in the right panel.
- Click **Run query**.
- The results of the most recent query appear at the top-right and persist across app refreshes.
- Queried tables, runtime, row count, and any errors are automatically logged.

---

### 2. **Explore the Lineage Graph**
- The center panel displays a directed graph of query lineage.
- Each node is a query (`Q1`, `Q2`, …).
- Edges indicate parent → child relationships based on query derivation.
- This helps visualize exploratory branching paths.

---

### 3. **Start Derived Queries**
To create a new query linked to an earlier one:

1. Select a query from the **Query History** panel.
2. Click **Start new query from selected**.
3. The SQL text from that query is copied into the editor.
4. When you run a new query, it is recorded as a *child* in the lineage graph.

This allows you to build and visualize branching analytical workflows.

---

### 4. **Pin Materialized Views**
Any query can be converted into a reusable physical materialized view:

1. Select a query.
2. Click **Pin as materialized view**.
3. QLE stores:
   - The view name (`qle_view_<id>`)
   - Storage size
   - Originating query ID
   - Creation timestamp

Pinned views appear in the **Pinned Views** section where you can:
- Preview their contents
- Insert them into the SQL editor with **Use in new query**

---

### 5. **Delete Queries**
- Select a query from the history list.
- Click **Delete this query**.
- QLE removes the query, its lineage edges, and any pinned view created from it.

The lineage graph updates automatically.

---

### 6. **Reset All Metadata**
If you want a totally fresh start:

- Scroll to **Maintenance**
- Click **Clear ALL QLE history (irreversible)**

This deletes:
- All logged queries  
- All lineage edges  
- All pinned views  
- Resets query numbering back to 1  

Your underlying IMDB database is **not touched**.

---

### 7. **Previewing Pinned Views**
Under the **Pinned Views** section:

- Click **Preview** to see the first rows of a materialized view.
- Click **Use in new query** to automatically generate:

```sql
SELECT * FROM qle_view_<id> LIMIT 100;


