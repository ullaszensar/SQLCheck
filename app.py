import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
import json

from sql_analyzer import SQLAnalyzer
from excel_processor import ExcelProcessor
from utils import format_analysis_results, create_summary_charts

# Page configuration
st.set_page_config(
    page_title="SQL Query Analysis Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'metadata_loaded' not in st.session_state:
    st.session_state.metadata_loaded = False
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = []
if 'table_metadata' not in st.session_state:
    st.session_state.table_metadata = None

def main():
    st.title("🔍 SQL Query Analysis Tool")
    st.markdown("---")
    
    # Sidebar for file upload and configuration
    with st.sidebar:
        st.header("📁 Data Input")
        
        # Excel file upload
        st.subheader("1. Upload Table Metadata")
        uploaded_file = st.file_uploader(
            "Upload Excel file with table and field details",
            type=['xlsx', 'xls'],
            help="Excel file should contain table names, field names, and data types"
        )
        
        if uploaded_file is not None:
            try:
                excel_processor = ExcelProcessor()
                metadata = excel_processor.process_excel_file(uploaded_file)
                st.session_state.table_metadata = metadata
                st.session_state.metadata_loaded = True
                st.success("✅ Metadata loaded successfully!")
                
                # Display metadata summary
                st.subheader("📊 Metadata Summary")
                st.write(f"**Tables found:** {len(metadata)}")
                for table_name, fields in metadata.items():
                    st.write(f"- {table_name}: {len(fields)} fields")
                    
            except Exception as e:
                st.error(f"❌ Error processing Excel file: {str(e)}")
                st.session_state.metadata_loaded = False
        
        st.markdown("---")
        
        # Analysis options
        st.subheader("⚙️ Analysis Options")
        include_temp_tables = st.checkbox("Detect temporary tables", value=True)
        detailed_join_analysis = st.checkbox("Detailed join analysis", value=True)
        column_usage_tracking = st.checkbox("Track column usage", value=True)
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📝 SQL Query Input")
        
        # SQL query input methods
        input_method = st.radio(
            "Choose input method:",
            ["Single Query", "Multiple Queries", "Upload SQL File"],
            horizontal=True
        )
        
        queries = []
        
        if input_method == "Single Query":
            query = st.text_area(
                "Enter your SQL query:",
                height=200,
                placeholder="SELECT * FROM users WHERE active = 1..."
            )
            if query.strip():
                queries = [query.strip()]
                
        elif input_method == "Multiple Queries":
            query_text = st.text_area(
                "Enter multiple SQL queries (separate with semicolon):",
                height=300,
                placeholder="SELECT * FROM users; INSERT INTO logs...; UPDATE..."
            )
            if query_text.strip():
                # Split by semicolon and clean up
                queries = [q.strip() for q in query_text.split(';') if q.strip()]
                
        else:  # Upload SQL File
            sql_file = st.file_uploader(
                "Upload SQL file:",
                type=['sql', 'txt'],
                help="Upload a file containing SQL queries"
            )
            if sql_file is not None:
                content = sql_file.read().decode('utf-8')
                queries = [q.strip() for q in content.split(';') if q.strip()]
                st.success(f"✅ Loaded {len(queries)} queries from file")
        
        # Analysis button
        if st.button("🔍 Analyze SQL Queries", type="primary", disabled=not queries):
            if not st.session_state.metadata_loaded:
                st.warning("⚠️ Please upload table metadata first for comprehensive analysis")
            
            with st.spinner("Analyzing SQL queries..."):
                analyzer = SQLAnalyzer(
                    table_metadata=st.session_state.table_metadata,
                    include_temp_tables=include_temp_tables,
                    detailed_join_analysis=detailed_join_analysis,
                    column_usage_tracking=column_usage_tracking
                )
                
                results = []
                for i, query in enumerate(queries):
                    try:
                        analysis = analyzer.analyze_query(query, query_id=f"Query_{i+1}")
                        results.append(analysis)
                    except Exception as e:
                        st.error(f"❌ Error analyzing Query {i+1}: {str(e)}")
                
                st.session_state.analysis_results = results
                
                if results:
                    st.success(f"✅ Successfully analyzed {len(results)} queries!")
    
    with col2:
        if st.session_state.analysis_results:
            st.header("📊 Quick Stats")
            
            # Calculate quick statistics
            total_queries = len(st.session_state.analysis_results)
            crud_counts = {}
            join_count = 0
            temp_table_count = 0
            
            for result in st.session_state.analysis_results:
                query_type = result.get('query_type', 'UNKNOWN')
                crud_counts[query_type] = crud_counts.get(query_type, 0) + 1
                
                if result.get('has_joins', False):
                    join_count += 1
                    
                if result.get('temp_tables'):
                    temp_table_count += len(result['temp_tables'])
            
            # Display metrics
            st.metric("Total Queries", total_queries)
            st.metric("Queries with Joins", join_count)
            st.metric("Temporary Tables", temp_table_count)
            
            # CRUD distribution chart
            if crud_counts:
                fig = px.pie(
                    values=list(crud_counts.values()),
                    names=list(crud_counts.keys()),
                    title="Query Type Distribution"
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
    
    # Results section
    if st.session_state.analysis_results:
        st.markdown("---")
        st.header("📋 Analysis Results")
        
        # Tabs for different views
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Summary Table", "📈 Visualizations", "🔍 Detailed Analysis", "📋 Excel Comparison", "📄 SQL File Analysis", "📥 Export"])
        
        with tab1:
            # Create summary DataFrame
            summary_data = []
            for result in st.session_state.analysis_results:
                summary_data.append({
                    'Query ID': result.get('query_id', 'N/A'),
                    'Type': result.get('query_type', 'UNKNOWN'),
                    'Tables': ', '.join(result.get('tables', [])),
                    'Columns': len(result.get('columns', [])),
                    'Has Joins': '✅' if result.get('has_joins', False) else '❌',
                    'Join Type': ', '.join(result.get('join_types', [])) if result.get('join_types') else 'None',
                    'Temp Tables': len(result.get('temp_tables', [])),
                    'Complexity': result.get('complexity_score', 0)
                })
            
            df_summary = pd.DataFrame(summary_data)
            st.dataframe(df_summary, use_container_width=True)
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                # Complexity distribution
                complexity_scores = [result.get('complexity_score', 0) for result in st.session_state.analysis_results]
                fig_complexity = px.histogram(
                    x=complexity_scores,
                    nbins=10,
                    title="Query Complexity Distribution",
                    labels={'x': 'Complexity Score', 'y': 'Number of Queries'}
                )
                st.plotly_chart(fig_complexity, use_container_width=True)
            
            with col2:
                # Table usage frequency
                table_usage = {}
                for result in st.session_state.analysis_results:
                    for table in result.get('tables', []):
                        table_usage[table] = table_usage.get(table, 0) + 1
                
                if table_usage:
                    fig_tables = px.bar(
                        x=list(table_usage.keys()),
                        y=list(table_usage.values()),
                        title="Table Usage Frequency",
                        labels={'x': 'Table Name', 'y': 'Usage Count'}
                    )
                    fig_tables.update_xaxes(tickangle=45)
                    st.plotly_chart(fig_tables, use_container_width=True)
        
        with tab3:
            # Detailed analysis for each query
            for i, result in enumerate(st.session_state.analysis_results):
                with st.expander(f"🔍 {result.get('query_id', f'Query {i+1}')} - {result.get('query_type', 'UNKNOWN')}"):
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("📊 Query Details")
                        st.write(f"**Type:** {result.get('query_type', 'UNKNOWN')}")
                        st.write(f"**Complexity Score:** {result.get('complexity_score', 0)}")
                        st.write(f"**Tables:** {', '.join(result.get('tables', []))}")
                        st.write(f"**Has Joins:** {'Yes' if result.get('has_joins', False) else 'No'}")
                        
                        if result.get('join_types'):
                            st.write(f"**Join Types:** {', '.join(result.get('join_types', []))}")
                        
                        if result.get('temp_tables'):
                            st.write(f"**Temporary Tables:** {', '.join(result.get('temp_tables', []))}")
                    
                    with col2:
                        st.subheader("📝 Columns & Operations")
                        if result.get('columns'):
                            st.write("**Columns Used:**")
                            for col in result.get('columns', []):
                                st.write(f"- {col}")
                        
                        if result.get('operations'):
                            st.write("**Operations:**")
                            for op in result.get('operations', []):
                                st.write(f"- {op}")
                    
                    # Excel Metadata Comparison
                    if st.session_state.metadata_loaded:
                        st.subheader("🔍 Excel Metadata Comparison")
                        
                        # Tables and Fields from Query vs Excel
                        query_tables = result.get('tables', [])
                        query_columns = result.get('columns', [])
                        
                        # Create comparison data
                        comparison_data = []
                        
                        for table in query_tables:
                            if table in st.session_state.table_metadata:
                                excel_fields = [field['name'] for field in st.session_state.table_metadata[table]]
                                matching_fields = [col for col in query_columns if col in excel_fields]
                                missing_fields = [col for col in query_columns if col not in excel_fields and any(table.lower() in col.lower() or col.lower() in field.lower() for field in excel_fields)]
                                
                                comparison_data.append({
                                    'Table': table,
                                    'Status': '✅ Found in Excel',
                                    'Excel Fields Count': len(excel_fields),
                                    'Matching Fields': ', '.join(matching_fields) if matching_fields else 'None',
                                    'Potential Missing': ', '.join(missing_fields) if missing_fields else 'None'
                                })
                            else:
                                comparison_data.append({
                                    'Table': table,
                                    'Status': '❌ Not in Excel',
                                    'Excel Fields Count': 0,
                                    'Matching Fields': 'N/A',
                                    'Potential Missing': 'All fields'
                                })
                        
                        if comparison_data:
                            df_comparison = pd.DataFrame(comparison_data)
                            st.dataframe(df_comparison, use_container_width=True)
                        
                        # Detailed Field Matching Table
                        st.subheader("📋 Detailed Field Analysis")
                        field_analysis = []
                        
                        for table in query_tables:
                            if table in st.session_state.table_metadata:
                                excel_fields = st.session_state.table_metadata[table]
                                for field in excel_fields:
                                    field_analysis.append({
                                        'Table': table,
                                        'Field Name': field['name'],
                                        'Data Type': field.get('data_type', 'unknown'),
                                        'Used in Query': '✅' if field['name'] in query_columns else '❌',
                                        'Description': field.get('description', '')[:50] + '...' if len(field.get('description', '')) > 50 else field.get('description', '')
                                    })
                        
                        if field_analysis:
                            df_fields = pd.DataFrame(field_analysis)
                            st.dataframe(df_fields, use_container_width=True)
                    
                    # Original query
                    if result.get('original_query'):
                        st.subheader("📄 Original Query")
                        st.code(result.get('original_query'), language='sql')
                    
                    # Areas needing changes
                    if result.get('change_areas'):
                        st.subheader("⚠️ Areas Needing Changes")
                        for area in result.get('change_areas', []):
                            st.warning(f"• {area}")
        
        with tab4:
            # Excel Comparison Tables
            if st.session_state.metadata_loaded:
                st.header("📋 SQL Query vs Excel Metadata Comparison")
                
                # Table 1: Query-Excel Table Matching Summary
                st.subheader("🔍 Table 1: Query-Excel Table Matching Summary")
                
                all_query_tables = set()
                all_excel_tables = set(st.session_state.table_metadata.keys())
                
                for result in st.session_state.analysis_results:
                    all_query_tables.update(result.get('tables', []))
                
                table_matching_data = []
                
                # All tables found in queries (including those not in Excel)
                for table in all_query_tables:
                    query_count = sum(1 for result in st.session_state.analysis_results if table in result.get('tables', []))
                    if table in all_excel_tables:
                        excel_field_count = len(st.session_state.table_metadata[table])
                        table_matching_data.append({
                            'Table Name': table,
                            'Found in Excel': 'Yes',
                            'Used in Queries': query_count,
                            'Excel Fields Count': excel_field_count,
                            'Status': 'Matched'
                        })
                    else:
                        table_matching_data.append({
                            'Table Name': table,
                            'Found in Excel': 'No',
                            'Used in Queries': query_count,
                            'Excel Fields Count': 0,
                            'Status': 'Missing in Excel'
                        })
                
                # Tables only in Excel (not used in queries)
                unused_tables = all_excel_tables - all_query_tables
                for table in unused_tables:
                    excel_field_count = len(st.session_state.table_metadata[table])
                    table_matching_data.append({
                        'Table Name': table,
                        'Found in Excel': 'Yes',
                        'Used in Queries': 0,
                        'Excel Fields Count': excel_field_count,
                        'Status': 'Unused in Queries'
                    })
                
                if table_matching_data:
                    df_table_matching = pd.DataFrame(table_matching_data)
                    st.dataframe(df_table_matching, use_container_width=True)
                
                st.markdown("---")
                
                # Table 1.5: Tables in SQL but Missing from Excel
                st.subheader("⚠️ Table 1.5: Tables Found in SQL but Missing from Excel")
                
                missing_tables_data = []
                missing_tables = all_query_tables - all_excel_tables
                
                if missing_tables:
                    for table in missing_tables:
                        query_count = sum(1 for result in st.session_state.analysis_results if table in result.get('tables', []))
                        
                        # Find which queries use this missing table
                        queries_using_table = []
                        for result in st.session_state.analysis_results:
                            if table in result.get('tables', []):
                                queries_using_table.append(result.get('query_id', 'Unknown'))
                        
                        missing_tables_data.append({
                            'Table Name': table,
                            'Used in Queries Count': query_count,
                            'Query IDs Using This Table': ', '.join(queries_using_table),
                            'Impact Level': 'High' if query_count > 2 else 'Medium' if query_count > 1 else 'Low',
                            'Action Required': 'Add to Excel metadata or verify table name spelling'
                        })
                    
                    df_missing_tables = pd.DataFrame(missing_tables_data)
                    st.dataframe(df_missing_tables, use_container_width=True)
                    
                    # Alert summary
                    st.error(f"⚠️ Found {len(missing_tables)} tables in SQL queries that are not documented in Excel metadata")
                else:
                    st.success("✅ All tables from SQL queries are documented in Excel metadata")
                
                st.markdown("---")
                
                # Table 2: Detailed Query Analysis with Field Matching
                st.subheader("🔍 Table 2: Detailed Query Analysis with Field Matching")
                
                detailed_query_data = []
                
                for result in st.session_state.analysis_results:
                    query_id = result.get('query_id', 'N/A')
                    query_type = result.get('query_type', 'UNKNOWN')
                    query_tables = result.get('tables', [])
                    query_columns = result.get('columns', [])
                    
                    # Count matching fields for this query
                    total_matching_fields = 0
                    total_excel_fields = 0
                    matched_tables = 0
                    matched_table_names = []
                    matched_field_names = []
                    
                    for table in query_tables:
                        if table in st.session_state.table_metadata:
                            matched_tables += 1
                            matched_table_names.append(table)
                            excel_fields = [field['name'] for field in st.session_state.table_metadata[table]]
                            total_excel_fields += len(excel_fields)
                            matching_fields = [col for col in query_columns if col in excel_fields]
                            total_matching_fields += len(matching_fields)
                            matched_field_names.extend(matching_fields)
                    
                    detailed_query_data.append({
                        'Query ID': query_id,
                        'Query Type': query_type,
                        'Table Names in Query': ', '.join(query_tables),
                        'Tables in Query': len(query_tables),
                        'Tables Matched in Excel': matched_tables,
                        'Table Field Matching in Excel': ', '.join(set(matched_field_names)) if matched_field_names else 'None',
                        'Fields in Query': len(query_columns),
                        'Fields Matched in Excel': total_matching_fields,
                        'Has Joins': '✅' if result.get('has_joins', False) else '❌',
                        'Temp Tables': len(result.get('temp_tables', [])),
                        'Complexity Score': result.get('complexity_score', 0),
                        'Match Percentage': f"{(total_matching_fields/len(query_columns)*100):.1f}%" if query_columns else "0%"
                    })
                
                if detailed_query_data:
                    df_detailed_query = pd.DataFrame(detailed_query_data)
                    st.dataframe(df_detailed_query, use_container_width=True)
                
                st.markdown("---")
                
                # Table 2.5: All Tables Found in SQL Files (Non-Temporary)
                st.subheader("📋 Table 2.5: All Tables Found in SQL Files")
                
                sql_tables_data = []
                
                # Get all tables from SQL queries (excluding temporary tables)
                all_sql_tables = set()
                for result in st.session_state.analysis_results:
                    tables = result.get('tables', [])
                    temp_tables = result.get('temp_tables', [])
                    # Only include non-temporary tables
                    non_temp_tables = [table for table in tables if table not in temp_tables]
                    all_sql_tables.update(non_temp_tables)
                
                if all_sql_tables:
                    for table_name in sorted(all_sql_tables):
                        # Count how many queries use this table
                        query_usage_count = sum(1 for result in st.session_state.analysis_results if table_name in result.get('tables', []))
                        
                        # Get which queries use this table
                        queries_using_table = []
                        for result in st.session_state.analysis_results:
                            if table_name in result.get('tables', []):
                                queries_using_table.append(result.get('query_id', 'Unknown'))
                        
                        # Check if table exists in Excel
                        in_excel = table_name in st.session_state.table_metadata if st.session_state.metadata_loaded else False
                        excel_fields_count = len(st.session_state.table_metadata.get(table_name, [])) if in_excel else 0
                        
                        sql_tables_data.append({
                            'Table Name': table_name,
                            'Query Usage Count': query_usage_count,
                            'Found in Excel': '✅' if in_excel else '❌',
                            'Excel Fields Count': excel_fields_count,
                            'Queries Using This Table': ', '.join(queries_using_table),
                            'Usage Frequency': 'High' if query_usage_count > 3 else 'Medium' if query_usage_count > 1 else 'Low',
                            'Documentation Status': 'Documented' if in_excel else 'Missing from Excel'
                        })
                
                if sql_tables_data:
                    df_sql_tables = pd.DataFrame(sql_tables_data)
                    
                    # Add filtering options
                    col1, col2 = st.columns(2)
                    with col1:
                        show_undocumented_only = st.checkbox("Show only undocumented tables", key="sql_undoc_filter")
                    with col2:
                        min_usage = st.number_input("Minimum usage count:", min_value=0, value=0, key="sql_min_usage")
                    
                    # Apply filters
                    filtered_sql_df = df_sql_tables.copy()
                    
                    if show_undocumented_only:
                        filtered_sql_df = filtered_sql_df[filtered_sql_df['Found in Excel'] == '❌']
                    
                    if min_usage > 0:
                        filtered_sql_df = filtered_sql_df[filtered_sql_df['Query Usage Count'] >= min_usage]
                    
                    st.dataframe(filtered_sql_df, use_container_width=True)
                    
                    # Summary for SQL tables
                    total_sql_tables = len(df_sql_tables)
                    undocumented_tables = len(df_sql_tables[df_sql_tables['Found in Excel'] == '❌'])
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Tables in SQL", total_sql_tables)
                    with col2:
                        st.metric("Undocumented Tables", undocumented_tables)
                    with col3:
                        st.metric("Documentation Rate", f"{((total_sql_tables-undocumented_tables)/total_sql_tables*100):.1f}%")
                
                else:
                    st.info("No non-temporary tables found in SQL queries")
                
                st.markdown("---")
                
                # New Table: SQL File Tables with Query Details
                st.subheader("📋 SQL File Tables - Complete List with Query Details")
                
                sql_file_tables_data = []
                
                # Process each query to extract table usage details
                for result in st.session_state.analysis_results:
                    query_id = result.get('query_id', 'Unknown')
                    query_type = result.get('query_type', 'UNKNOWN')
                    tables = result.get('tables', [])
                    temp_tables = result.get('temp_tables', [])
                    
                    # For each table in this query
                    for table_name in tables:
                        # Skip temporary tables
                        if table_name not in temp_tables:
                            # Create Query ID based on table name for SELECT queries
                            if query_type == 'SELECT':
                                custom_query_id = f"SELECT_{table_name}"
                            else:
                                custom_query_id = f"{query_type}_{table_name}"
                            
                            # Check if table exists in Excel
                            in_excel = table_name in st.session_state.table_metadata if st.session_state.metadata_loaded else False
                            excel_fields_count = len(st.session_state.table_metadata.get(table_name, [])) if in_excel else 0
                            
                            # Get sample Excel fields if available
                            sample_fields = []
                            if in_excel:
                                fields = st.session_state.table_metadata.get(table_name, [])
                                sample_fields = [field['name'] for field in fields[:3]]
                            
                            sql_file_tables_data.append({
                                'Original Query ID': query_id,
                                'Table-Based Query ID': custom_query_id,
                                'Query Type': query_type,
                                'Table Name': table_name,
                                'Found in Excel': '✅' if in_excel else '❌',
                                'Excel Fields Count': excel_fields_count,
                                'Sample Excel Fields': ', '.join(sample_fields) if sample_fields else 'N/A',
                                'Documentation Status': 'Documented' if in_excel else 'Missing'
                            })
                
                if sql_file_tables_data:
                    df_sql_file_tables = pd.DataFrame(sql_file_tables_data)
                    
                    # Add filtering options
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        filter_query_type = st.selectbox("Filter by Query Type:", ['All'] + list(df_sql_file_tables['Query Type'].unique()), key="sql_file_type_filter")
                    with col2:
                        show_undocumented = st.checkbox("Show only undocumented", key="sql_file_undoc")
                    with col3:
                        show_select_only = st.checkbox("Show only SELECT queries", key="sql_file_select")
                    
                    # Apply filters
                    filtered_df = df_sql_file_tables.copy()
                    
                    if filter_query_type != 'All':
                        filtered_df = filtered_df[filtered_df['Query Type'] == filter_query_type]
                    
                    if show_undocumented:
                        filtered_df = filtered_df[filtered_df['Found in Excel'] == '❌']
                    
                    if show_select_only:
                        filtered_df = filtered_df[filtered_df['Query Type'] == 'SELECT']
                    
                    st.dataframe(filtered_df, use_container_width=True)
                    
                    # Summary statistics
                    total_entries = len(df_sql_file_tables)
                    select_queries = len(df_sql_file_tables[df_sql_file_tables['Query Type'] == 'SELECT'])
                    undocumented = len(df_sql_file_tables[df_sql_file_tables['Found in Excel'] == '❌'])
                    unique_tables = len(df_sql_file_tables['Table Name'].unique())
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Table References", total_entries)
                    with col2:
                        st.metric("SELECT Query Tables", select_queries)
                    with col3:
                        st.metric("Undocumented References", undocumented)
                    with col4:
                        st.metric("Unique Tables", unique_tables)
                
                else:
                    st.info("No table references found in SQL queries")
                
                st.markdown("---")
                
                # Table 3: Complete Field Inventory
                st.subheader("🔍 Table 3: Complete Field Inventory from Excel")
                
                field_inventory_data = []
                
                for table_name, fields in st.session_state.table_metadata.items():
                    # Check if this table is used in any query
                    table_used_in_queries = any(table_name in result.get('tables', []) for result in st.session_state.analysis_results)
                    
                    for field in fields:
                        # Check if this field is used in any query
                        field_used_in_queries = any(field['name'] in result.get('columns', []) for result in st.session_state.analysis_results)
                        
                        # Count in how many queries this field appears
                        field_usage_count = sum(1 for result in st.session_state.analysis_results if field['name'] in result.get('columns', []))
                        
                        field_inventory_data.append({
                            'Table Name': table_name,
                            'Field Name': field['name'],
                            'Data Type': field.get('data_type', 'unknown'),
                            'Table Used in Queries': '✅' if table_used_in_queries else '❌',
                            'Field Used in Queries': '✅' if field_used_in_queries else '❌',
                            'Usage Count': field_usage_count,
                            'Description': field.get('description', '')[:100] + '...' if len(field.get('description', '')) > 100 else field.get('description', '')
                        })
                
                if field_inventory_data:
                    df_field_inventory = pd.DataFrame(field_inventory_data)
                    
                    # Add filters
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        show_unused_only = st.checkbox("Show only unused fields")
                    with col2:
                        selected_table = st.selectbox("Filter by table:", ['All'] + list(st.session_state.table_metadata.keys()))
                    with col3:
                        min_usage = st.number_input("Minimum usage count:", min_value=0, value=0)
                    
                    # Apply filters
                    filtered_df = df_field_inventory.copy()
                    
                    if show_unused_only:
                        filtered_df = filtered_df[filtered_df['Field Used in Queries'] == '❌']
                    
                    if selected_table != 'All':
                        filtered_df = filtered_df[filtered_df['Table Name'] == selected_table]
                    
                    if min_usage > 0:
                        filtered_df = filtered_df[filtered_df['Usage Count'] >= min_usage]
                    
                    st.dataframe(filtered_df, use_container_width=True)
                    
                    # Summary statistics
                    st.subheader("📊 Summary Statistics")
                    total_fields = len(df_field_inventory)
                    unused_fields = len(df_field_inventory[df_field_inventory['Field Used in Queries'] == '❌'])
                    unused_tables = len(df_field_inventory[df_field_inventory['Table Used in Queries'] == '❌']['Table Name'].unique())
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Fields in Excel", total_fields)
                    with col2:
                        st.metric("Unused Fields", unused_fields)
                    with col3:
                        st.metric("Unused Tables", unused_tables)
                    with col4:
                        st.metric("Field Usage Rate", f"{((total_fields-unused_fields)/total_fields*100):.1f}%")
            
            else:
                st.warning("⚠️ Please upload Excel metadata first to see comparison tables")
        
        with tab5:
            # SQL File Analysis Tab - Tables only from SQL files
            st.header("📄 SQL File Analysis")
            st.subheader("All Tables Used in SQL Files")
            
            # Table 1: Complete List of Tables from SQL Files
            st.markdown("### 📋 Table 1: All Tables in SQL Queries")
            
            sql_only_tables_data = []
            
            # Get all tables from SQL queries
            all_sql_tables = set()
            for result in st.session_state.analysis_results:
                tables = result.get('tables', [])
                temp_tables = result.get('temp_tables', [])
                # Include both regular and temporary tables
                all_sql_tables.update(tables)
            
            if all_sql_tables:
                for table_name in sorted(all_sql_tables):
                    # Count usage across all queries
                    query_usage_count = sum(1 for result in st.session_state.analysis_results if table_name in result.get('tables', []))
                    
                    # Check if it's a temporary table
                    is_temp = any(table_name in result.get('temp_tables', []) for result in st.session_state.analysis_results)
                    
                    # Get queries that use this table
                    queries_using_table = []
                    query_types_using_table = []
                    for result in st.session_state.analysis_results:
                        if table_name in result.get('tables', []):
                            queries_using_table.append(result.get('query_id', 'Unknown'))
                            query_types_using_table.append(result.get('query_type', 'UNKNOWN'))
                    
                    sql_only_tables_data.append({
                        'Table Name': table_name,
                        'Table Type': 'Temporary' if is_temp else 'Regular',
                        'Usage Count': query_usage_count,
                        'Query Types Using': ', '.join(set(query_types_using_table)),
                        'Query IDs Using': ', '.join(queries_using_table),
                        'Usage Frequency': 'High' if query_usage_count > 3 else 'Medium' if query_usage_count > 1 else 'Low'
                    })
            
            if sql_only_tables_data:
                df_sql_only_tables = pd.DataFrame(sql_only_tables_data)
                
                # Add filtering options
                col1, col2, col3 = st.columns(3)
                with col1:
                    table_type_filter = st.selectbox("Filter by Table Type:", ['All', 'Regular', 'Temporary'], key="sql_table_type")
                with col2:
                    usage_filter = st.selectbox("Filter by Usage:", ['All', 'High', 'Medium', 'Low'], key="sql_usage_freq")
                with col3:
                    min_usage_count = st.number_input("Min usage count:", min_value=0, value=0, key="sql_min_usage_count")
                
                # Apply filters
                filtered_sql_only = df_sql_only_tables.copy()
                
                if table_type_filter != 'All':
                    filtered_sql_only = filtered_sql_only[filtered_sql_only['Table Type'] == table_type_filter]
                
                if usage_filter != 'All':
                    filtered_sql_only = filtered_sql_only[filtered_sql_only['Usage Frequency'] == usage_filter]
                
                if min_usage_count > 0:
                    filtered_sql_only = filtered_sql_only[filtered_sql_only['Usage Count'] >= min_usage_count]
                
                st.dataframe(filtered_sql_only, use_container_width=True)
                
                # Statistics
                total_tables = len(df_sql_only_tables)
                regular_tables = len(df_sql_only_tables[df_sql_only_tables['Table Type'] == 'Regular'])
                temp_tables = len(df_sql_only_tables[df_sql_only_tables['Table Type'] == 'Temporary'])
                high_usage = len(df_sql_only_tables[df_sql_only_tables['Usage Frequency'] == 'High'])
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Tables", total_tables)
                with col2:
                    st.metric("Regular Tables", regular_tables)
                with col3:
                    st.metric("Temporary Tables", temp_tables)
                with col4:
                    st.metric("High Usage Tables", high_usage)
            
            st.markdown("---")
            
            # Table 2: Query-Table Relationship
            st.markdown("### 🔗 Table 2: Query-Table Relationships")
            
            query_table_data = []
            
            for result in st.session_state.analysis_results:
                query_id = result.get('query_id', 'Unknown')
                query_type = result.get('query_type', 'UNKNOWN')
                tables = result.get('tables', [])
                temp_tables = result.get('temp_tables', [])
                has_joins = result.get('has_joins', False)
                join_types = result.get('join_types', [])
                complexity = result.get('complexity_score', 0)
                
                for table in tables:
                    is_temp = table in temp_tables
                    
                    query_table_data.append({
                        'Query ID': query_id,
                        'Query Type': query_type,
                        'Table Name': table,
                        'Table Type': 'Temporary' if is_temp else 'Regular',
                        'Has Joins': '✅' if has_joins else '❌',
                        'Join Types': ', '.join(join_types) if join_types else 'None',
                        'Complexity Score': complexity
                    })
            
            if query_table_data:
                df_query_table = pd.DataFrame(query_table_data)
                
                # Filtering options for query-table relationships
                col1, col2 = st.columns(2)
                with col1:
                    query_type_filter = st.selectbox("Filter by Query Type:", ['All'] + list(df_query_table['Query Type'].unique()), key="qt_query_type")
                with col2:
                    joins_filter = st.selectbox("Filter by Joins:", ['All', 'With Joins', 'Without Joins'], key="qt_joins")
                
                # Apply filters
                filtered_qt = df_query_table.copy()
                
                if query_type_filter != 'All':
                    filtered_qt = filtered_qt[filtered_qt['Query Type'] == query_type_filter]
                
                if joins_filter == 'With Joins':
                    filtered_qt = filtered_qt[filtered_qt['Has Joins'] == '✅']
                elif joins_filter == 'Without Joins':
                    filtered_qt = filtered_qt[filtered_qt['Has Joins'] == '❌']
                
                st.dataframe(filtered_qt, use_container_width=True)
                
                # Summary for query-table relationships
                total_relationships = len(df_query_table)
                select_relationships = len(df_query_table[df_query_table['Query Type'] == 'SELECT'])
                join_relationships = len(df_query_table[df_query_table['Has Joins'] == '✅'])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Query-Table Relations", total_relationships)
                with col2:
                    st.metric("SELECT Query Relations", select_relationships)
                with col3:
                    st.metric("Relations with Joins", join_relationships)
            
            else:
                st.info("No query-table relationships found")
        
        with tab6:
            # Export options
            st.subheader("📥 Export Analysis Results")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Export as Excel
                if st.button("📊 Export to Excel", type="primary"):
                    try:
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            # Summary sheet
                            df_summary.to_excel(writer, sheet_name='Summary', index=False)
                            
                            # Detailed results sheet
                            detailed_data = []
                            for result in st.session_state.analysis_results:
                                detailed_data.append({
                                    'Query ID': result.get('query_id', 'N/A'),
                                    'Query Type': result.get('query_type', 'UNKNOWN'),
                                    'Tables': '; '.join(result.get('tables', [])),
                                    'Columns': '; '.join(result.get('columns', [])),
                                    'Join Types': '; '.join(result.get('join_types', [])),
                                    'Temp Tables': '; '.join(result.get('temp_tables', [])),
                                    'Complexity Score': result.get('complexity_score', 0),
                                    'Change Areas': '; '.join(result.get('change_areas', [])),
                                    'Original Query': result.get('original_query', '')
                                })
                            
                            df_detailed = pd.DataFrame(detailed_data)
                            df_detailed.to_excel(writer, sheet_name='Detailed Analysis', index=False)
                        
                        output.seek(0)
                        st.download_button(
                            label="📥 Download Excel Report",
                            data=output.getvalue(),
                            file_name="sql_analysis_report.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        
                    except Exception as e:
                        st.error(f"❌ Error creating Excel file: {str(e)}")
            
            with col2:
                # Export as JSON
                if st.button("📄 Export to JSON"):
                    json_data = json.dumps(st.session_state.analysis_results, indent=2)
                    st.download_button(
                        label="📥 Download JSON Report",
                        data=json_data,
                        file_name="sql_analysis_report.json",
                        mime="application/json"
                    )

if __name__ == "__main__":
    main()
