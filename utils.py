import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Any, Optional
import streamlit as st
from collections import defaultdict, Counter

def format_analysis_results(results: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Format analysis results into a structured DataFrame for display.
    
    Args:
        results: List of analysis result dictionaries
        
    Returns:
        Formatted DataFrame
    """
    if not results:
        return pd.DataFrame()
    
    formatted_data = []
    
    for result in results:
        formatted_row = {
            'Query ID': result.get('query_id', 'N/A'),
            'Type': result.get('query_type', 'UNKNOWN'),
            'Tables Count': len(result.get('tables', [])),
            'Tables': ', '.join(result.get('tables', [])),
            'Columns Count': len(result.get('columns', [])),
            'Has Joins': 'Yes' if result.get('has_joins', False) else 'No',
            'Join Types': ', '.join(result.get('join_types', [])),
            'Subqueries': result.get('subqueries', 0),
            'Temp Tables': len(result.get('temp_tables', [])),
            'Functions': ', '.join(result.get('functions', [])),
            'Complexity Score': result.get('complexity_score', 0),
            'Change Areas': len(result.get('change_areas', [])),
            'Status': 'Needs Review' if result.get('change_areas', []) and 
                     not any('No obvious issues' in area for area in result.get('change_areas', [])) else 'OK'
        }
        
        formatted_data.append(formatted_row)
    
    return pd.DataFrame(formatted_data)

def create_summary_charts(results: List[Dict[str, Any]]) -> Dict[str, go.Figure]:
    """
    Create summary charts for the analysis results.
    
    Args:
        results: List of analysis result dictionaries
        
    Returns:
        Dictionary of plotly figures
    """
    if not results:
        return {}
    
    charts = {}
    
    # 1. Query type distribution
    query_types = [result.get('query_type', 'UNKNOWN') for result in results]
    type_counts = Counter(query_types)
    
    charts['query_types'] = px.pie(
        values=list(type_counts.values()),
        names=list(type_counts.keys()),
        title="Distribution of Query Types",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    # 2. Complexity distribution
    complexity_scores = [result.get('complexity_score', 0) for result in results]
    
    charts['complexity'] = px.histogram(
        x=complexity_scores,
        nbins=min(10, len(set(complexity_scores))),
        title="Query Complexity Distribution",
        labels={'x': 'Complexity Score', 'y': 'Number of Queries'},
        color_discrete_sequence=['#3498db']
    )
    
    # 3. Table usage frequency
    all_tables = []
    for result in results:
        all_tables.extend(result.get('tables', []))
    
    if all_tables:
        table_counts = Counter(all_tables)
        most_used = dict(table_counts.most_common(10))  # Top 10 most used tables
        
        charts['table_usage'] = px.bar(
            x=list(most_used.keys()),
            y=list(most_used.values()),
            title="Most Frequently Used Tables (Top 10)",
            labels={'x': 'Table Name', 'y': 'Usage Count'},
            color_discrete_sequence=['#e74c3c']
        )
        charts['table_usage'].update_xaxes(tickangle=45)
    
    # 4. Join analysis
    join_data = {
        'Has Joins': sum(1 for result in results if result.get('has_joins', False)),
        'No Joins': sum(1 for result in results if not result.get('has_joins', False))
    }
    
    charts['joins'] = px.pie(
        values=list(join_data.values()),
        names=list(join_data.keys()),
        title="Queries with Joins vs No Joins",
        color_discrete_sequence=['#f39c12', '#2ecc71']
    )
    
    # 5. Function usage
    all_functions = []
    for result in results:
        all_functions.extend(result.get('functions', []))
    
    if all_functions:
        function_counts = Counter(all_functions)
        
        charts['functions'] = px.bar(
            x=list(function_counts.keys()),
            y=list(function_counts.values()),
            title="SQL Functions Usage",
            labels={'x': 'Function Name', 'y': 'Usage Count'},
            color_discrete_sequence=['#9b59b6']
        )
    
    return charts

def calculate_change_impact(results: List[Dict[str, Any]], metadata: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Calculate the overall impact and areas requiring changes.
    
    Args:
        results: List of analysis result dictionaries
        metadata: Optional table metadata for enhanced analysis
        
    Returns:
        Dictionary with impact analysis
    """
    if not results:
        return {}
    
    impact_analysis = {
        'total_queries': len(results),
        'queries_needing_changes': 0,
        'high_complexity_queries': 0,
        'queries_with_joins': 0,
        'queries_with_temp_tables': 0,
        'queries_with_subqueries': 0,
        'most_common_issues': [],
        'tables_at_risk': set(),
        'recommended_actions': []
    }
    
    all_change_areas = []
    
    for result in results:
        change_areas = result.get('change_areas', [])
        
        # Skip queries with no issues
        if change_areas and not any('No obvious issues' in area for area in change_areas):
            impact_analysis['queries_needing_changes'] += 1
            all_change_areas.extend(change_areas)
        
        # Count high complexity queries
        if result.get('complexity_score', 0) > 15:
            impact_analysis['high_complexity_queries'] += 1
        
        # Count queries with joins
        if result.get('has_joins', False):
            impact_analysis['queries_with_joins'] += 1
        
        # Count queries with temp tables
        if result.get('temp_tables', []):
            impact_analysis['queries_with_temp_tables'] += 1
        
        # Count queries with subqueries
        if result.get('subqueries', 0) > 0:
            impact_analysis['queries_with_subqueries'] += 1
        
        # Collect tables that might be at risk
        if result.get('complexity_score', 0) > 10:
            impact_analysis['tables_at_risk'].update(result.get('tables', []))
    
    # Analyze most common issues
    if all_change_areas:
        issue_counts = Counter(all_change_areas)
        impact_analysis['most_common_issues'] = issue_counts.most_common(5)
    
    # Generate recommendations
    impact_analysis['recommended_actions'] = _generate_recommendations(impact_analysis)
    
    # Convert set to list for JSON serialization
    impact_analysis['tables_at_risk'] = list(impact_analysis['tables_at_risk'])
    
    return impact_analysis

def _generate_recommendations(impact_analysis: Dict[str, Any]) -> List[str]:
    """
    Generate actionable recommendations based on impact analysis.
    
    Args:
        impact_analysis: Impact analysis results
        
    Returns:
        List of recommendation strings
    """
    recommendations = []
    
    total_queries = impact_analysis['total_queries']
    
    if impact_analysis['queries_needing_changes'] > 0:
        percentage = (impact_analysis['queries_needing_changes'] / total_queries) * 100
        recommendations.append(
            f"{percentage:.1f}% of queries need attention - prioritize based on business impact"
        )
    
    if impact_analysis['high_complexity_queries'] > 0:
        recommendations.append(
            f"Consider refactoring {impact_analysis['high_complexity_queries']} high-complexity queries"
        )
    
    if impact_analysis['queries_with_temp_tables'] > 0:
        recommendations.append(
            "Review temporary table usage - consider CTEs or materialized views as alternatives"
        )
    
    if impact_analysis['queries_with_subqueries'] > impact_analysis['total_queries'] * 0.3:
        recommendations.append(
            "High subquery usage detected - consider using JOINs or CTEs for better performance"
        )
    
    if len(impact_analysis['tables_at_risk']) > 0:
        recommendations.append(
            f"Focus testing efforts on {len(impact_analysis['tables_at_risk'])} high-impact tables"
        )
    
    if not recommendations:
        recommendations.append("Overall query health looks good - continue monitoring")
    
    return recommendations

def export_detailed_report(results: List[Dict[str, Any]], metadata: Optional[Dict] = None) -> Dict[str, pd.DataFrame]:
    """
    Create detailed report DataFrames for export.
    
    Args:
        results: List of analysis result dictionaries
        metadata: Optional table metadata
        
    Returns:
        Dictionary of DataFrames for different report sections
    """
    report_data = {}
    
    # 1. Executive Summary
    impact = calculate_change_impact(results, metadata)
    summary_data = {
        'Metric': [
            'Total Queries Analyzed',
            'Queries Needing Changes',
            'High Complexity Queries',
            'Queries with Joins',
            'Queries with Temp Tables',
            'Queries with Subqueries',
            'Tables at Risk'
        ],
        'Value': [
            impact['total_queries'],
            impact['queries_needing_changes'],
            impact['high_complexity_queries'],
            impact['queries_with_joins'],
            impact['queries_with_temp_tables'],
            impact['queries_with_subqueries'],
            len(impact['tables_at_risk'])
        ]
    }
    report_data['Executive_Summary'] = pd.DataFrame(summary_data)
    
    # 2. Detailed Query Analysis
    report_data['Detailed_Analysis'] = format_analysis_results(results)
    
    # 3. Change Areas Summary
    all_change_areas = []
    for result in results:
        for area in result.get('change_areas', []):
            all_change_areas.append({
                'Query_ID': result.get('query_id', 'N/A'),
                'Change_Area': area,
                'Complexity_Score': result.get('complexity_score', 0),
                'Priority': 'High' if result.get('complexity_score', 0) > 15 else 
                          'Medium' if result.get('complexity_score', 0) > 8 else 'Low'
            })
    
    if all_change_areas:
        report_data['Change_Areas'] = pd.DataFrame(all_change_areas)
    
    # 4. Table Usage Analysis
    table_usage_data = []
    all_tables = []
    for result in results:
        all_tables.extend(result.get('tables', []))
    
    if all_tables:
        table_counts = Counter(all_tables)
        for table, count in table_counts.items():
            table_usage_data.append({
                'Table_Name': table,
                'Usage_Count': count,
                'In_Metadata': 'Yes' if metadata and table in metadata else 'No',
                'Risk_Level': 'High' if count > len(results) * 0.5 else 
                            'Medium' if count > len(results) * 0.2 else 'Low'
            })
        
        report_data['Table_Usage'] = pd.DataFrame(table_usage_data)
    
    return report_data

def create_complexity_heatmap(results: List[Dict[str, Any]]) -> go.Figure:
    """
    Create a heatmap showing query complexity across different dimensions.
    
    Args:
        results: List of analysis result dictionaries
        
    Returns:
        Plotly heatmap figure
    """
    if not results:
        return go.Figure()
    
    # Prepare data for heatmap
    dimensions = ['Tables', 'Joins', 'Subqueries', 'Functions', 'Temp Tables']
    query_ids = [result.get('query_id', f'Query_{i+1}') for i, result in enumerate(results)]
    
    heatmap_data = []
    for result in results:
        row = [
            len(result.get('tables', [])),
            len(result.get('join_types', [])),
            result.get('subqueries', 0),
            len(result.get('functions', [])),
            len(result.get('temp_tables', []))
        ]
        heatmap_data.append(row)
    
    # Transpose for proper orientation
    heatmap_data = list(map(list, zip(*heatmap_data)))
    
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data,
        x=query_ids,
        y=dimensions,
        colorscale='RdYlBu_r',
        hoverongaps=False,
        hovertemplate='Query: %{x}<br>Dimension: %{y}<br>Count: %{z}<extra></extra>'
    ))
    
    fig.update_layout(
        title="Query Complexity Heatmap",
        xaxis_title="Query ID",
        yaxis_title="Complexity Dimensions",
        height=400
    )
    
    return fig

def validate_sql_syntax(query: str) -> Dict[str, Any]:
    """
    Basic SQL syntax validation.
    
    Args:
        query: SQL query string
        
    Returns:
        Dictionary with validation results
    """
    validation_result = {
        'is_valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not query or not query.strip():
        validation_result['is_valid'] = False
        validation_result['errors'].append("Query is empty")
        return validation_result
    
    # Basic syntax checks
    query_upper = query.upper().strip()
    
    # Check for basic SQL statement start
    valid_starts = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'WITH']
    if not any(query_upper.startswith(start) for start in valid_starts):
        validation_result['warnings'].append("Query doesn't start with a recognized SQL keyword")
    
    # Check for balanced parentheses
    paren_count = query.count('(') - query.count(')')
    if paren_count != 0:
        validation_result['errors'].append(f"Unbalanced parentheses (difference: {paren_count})")
        validation_result['is_valid'] = False
    
    # Check for basic quote balance
    single_quote_count = query.count("'")
    if single_quote_count % 2 != 0:
        validation_result['warnings'].append("Potentially unbalanced single quotes")
    
    double_quote_count = query.count('"')
    if double_quote_count % 2 != 0:
        validation_result['warnings'].append("Potentially unbalanced double quotes")
    
    return validation_result
