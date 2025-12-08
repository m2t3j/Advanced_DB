# app.py
import streamlit as st
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

import qle_backend as qle

st.set_page_config(layout="wide", page_title="Query Lineage Exploration")
st.title("Query Lineage Exploration (QLE)")

# Initialize session state
if "sql_input" not in st.session_state:
    st.session_state["sql_input"] = ""
if "parent_ids" not in st.session_state:
    st.session_state["parent_ids"] = []

# Try to fetch history to check DB connection
try:
    history = qle.get_query_history(limit=50)
    db_error = None
except Exception as e:
    history = []
    db_error = str(e)

left_col, center_col, right_col = st.columns([2, 3, 3])

# ---------------- LEFT: Query History ----------------
with left_col:
    st.subheader("Query History")

    if db_error:
        st.error(f"Could not connect to database: {db_error}")
        selected_id = None
    else:
        if not history:
            st.write("No queries logged yet.")
            selected_id = None
        else:
            df_hist = pd.DataFrame(history)
            st.dataframe(
                df_hist[
                    [
                        "query_id",
                        "executed_at",
                        "runtime_ms",
                        "row_count",
                        "tables",
                        "error_message",
                    ]
                ]
            )

            selected_id = st.selectbox(
                "Select query to inspect",
                options=[row["query_id"] for row in history],
                format_func=lambda qid: f"Q{qid}",
            )

# ---------------- CENTER: Lineage Graph ----------------
with center_col:
    st.subheader("Lineage Graph")

    if db_error:
        st.error("No lineage: database connection failed.")
    else:
        nodes, edges = qle.get_lineage_graph()
        if nodes:
            G = nx.DiGraph()
            node_ids = [n["query_id"] for n in nodes]
            G.add_nodes_from(node_ids)

            for e in edges:
                G.add_edge(e["parent_query_id"], e["child_query_id"])

            pos = nx.spring_layout(G, seed=42)

            fig, ax = plt.subplots()
            nx.draw(G, pos, with_labels=True, ax=ax, arrows=True)
            st.pyplot(fig)
        else:
            st.write("No lineage yet. Run some queries.")

# ---------------- RIGHT: Query Editor / Details / Pinned Views ----------------
with right_col:
    st.subheader("Query Editor / Details")

    if db_error:
        st.error("Cannot show query details: database connection failed.")
    else:
        # ----- Selected query details -----
        if history and selected_id is not None:
            q_details, tables, pinned = qle.get_query_details(selected_id)

            st.markdown(f"**Selected Query Q{selected_id}**")
            st.code(q_details["sql_text"], language="sql")
            st.write(
                f"Runtime: {q_details['runtime_ms']} ms, "
                f"Rows: {q_details['row_count']}"
            )
            st.write(f"Tables: {tables}")
            if q_details["error_message"]:
                st.error(f"Error: {q_details['error_message']}")

            if pinned:
                st.success(
                    f"Pinned as view {pinned['view_name']} "
                    f"({pinned['storage_bytes']} bytes)"
                )

            # Buttons for selected query
            col_a, col_b, col_c = st.columns(3)

            # Start new query from selected (for lineage)
            with col_a:
                if st.button("Start new query from selected"):
                    st.session_state["sql_input"] = q_details["sql_text"]
                    st.session_state["parent_ids"] = [selected_id]
                    st.rerun()

            # Pin this query as materialized view
            with col_b:
                if st.button("Pin as materialized view"):
                    try:
                        view_id, view_name, size_bytes = qle.pin_query_as_view(
                            selected_id
                        )
                        st.success(
                            f"Pinned as view {view_name} ({size_bytes} bytes)"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to pin view: {e}")

            # Delete this query
            with col_c:
                if st.button("Delete this query"):
                    try:
                        qle.delete_query(selected_id)
                        st.success(
                            f"Deleted query Q{selected_id} and associated metadata."
                        )
                        # Clear editor / parent info
                        st.session_state["sql_input"] = ""
                        st.session_state["parent_ids"] = []
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to delete query: {e}")

        st.markdown("---")
        st.markdown("**New / Modified Query**")

        # Show current parent_ids info
        if st.session_state["parent_ids"]:
            st.caption(
                "New queries will be recorded as children of: "
                + ", ".join("Q" + str(pid) for pid in st.session_state["parent_ids"])
            )
        else:
            st.caption(
                "No parent selected: new queries will appear as roots in the lineage graph."
            )

        # Query editor
        sql_input = st.text_area(
            "SQL", value=st.session_state["sql_input"], height=200, key="sql_editor"
        )
        st.session_state["sql_input"] = sql_input  # keep in sync

        # Run query button (NO rerun here so results stay visible)
        if st.button("Run query"):
            if not sql_input.strip():
                st.warning("Please enter SQL.")
            else:
                try:
                    qid, rows, cols, err = qle.run_query(
                        sql_input, parent_query_ids=st.session_state["parent_ids"]
                    )
                    # After using parent_ids once, clear them by default
                    st.session_state["parent_ids"] = []

                    if err:
                        st.error(f"Query Q{qid} failed: {err}")
                    else:
                        st.success(f"Query Q{qid} succeeded.")
                        if rows:
                            st.markdown("**Results**")
                            df_res = pd.DataFrame(rows, columns=cols)
                            st.dataframe(df_res.head(50), use_container_width=True)
                except Exception as e:
                    st.error(f"Error executing query: {e}")

        st.markdown("---")
        st.subheader("Pinned Views")

        if not db_error:
            pinned_views = qle.list_pinned_views()
            if not pinned_views:
                st.write("No materialized views pinned yet.")
            else:
                for pv in pinned_views:
                    with st.expander(
                        f"{pv['view_name']} (from Q{pv['query_id']})"
                    ):
                        st.write(f"Created: {pv['created_at']}")
                        st.write(f"Storage: {pv['storage_bytes']} bytes")
                        st.markdown("**Originating SQL:**")
                        st.code(pv["sql_text"], language="sql")

                        col_v1, col_v2 = st.columns(2)

                        # Preview button
                        with col_v1:
                            if st.button(
                                f"Preview {pv['view_name']}",
                                key=f"preview_{pv['view_id']}",
                            ):
                                try:
                                    rows, cols = qle.preview_view(pv["view_name"])
                                    if rows:
                                        st.dataframe(
                                            pd.DataFrame(rows, columns=cols),
                                            use_container_width=True,
                                        )
                                    else:
                                        st.write("View is empty.")
                                except Exception as e:
                                    st.error(f"Failed to preview view: {e}")

                        # Use view in new query
                        with col_v2:
                            if st.button(
                                f"Use in new query",
                                key=f"use_{pv['view_id']}",
                            ):
                                st.session_state[
                                    "sql_input"
                                ] = f"SELECT * FROM {pv['view_name']} LIMIT 100;"
                                # Treat as a new root query: no parent
                                st.session_state["parent_ids"] = []
                                st.rerun()

        st.markdown("---")
        st.subheader("Maintenance")

        # ⚠ Clear ALL history button
        if st.button("⚠ Clear ALL QLE history (irreversible)"):
            try:
                qle.clear_history()
                st.session_state["sql_input"] = ""
                st.session_state["parent_ids"] = []
                st.success(
                    "Cleared all query history, lineage, and pinned views. "
                    "Note: underlying IMDB tables are untouched."
                )
                st.rerun()
            except Exception as e:
                st.error(f"Failed to clear history: {e}")
