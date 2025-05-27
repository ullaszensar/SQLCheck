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
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Summary Table", "📈 Visualizations", "🔍 Detailed Analysis", "📥 Export"])
        
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
