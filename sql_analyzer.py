import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Token, Keyword, Name
import re
from typing import Dict, List, Set, Optional, Any
from collections import defaultdict

class SQLAnalyzer:
    """
    Comprehensive SQL query analyzer that extracts various components and metrics from SQL queries.
    """
    
    def __init__(self, table_metadata: Optional[Dict] = None, include_temp_tables: bool = True,
                 detailed_join_analysis: bool = True, column_usage_tracking: bool = True):
        """
        Initialize the SQL analyzer.
        
        Args:
            table_metadata: Dictionary containing table and field information from Excel
            include_temp_tables: Whether to detect temporary table creation
            detailed_join_analysis: Whether to perform detailed join analysis
            column_usage_tracking: Whether to track column usage patterns
        """
        self.table_metadata = table_metadata or {}
        self.include_temp_tables = include_temp_tables
        self.detailed_join_analysis = detailed_join_analysis
        self.column_usage_tracking = column_usage_tracking
        
        # SQL keywords for different operations
        self.crud_keywords = {
            'SELECT': ['SELECT'],
            'INSERT': ['INSERT'],
            'UPDATE': ['UPDATE'],
            'DELETE': ['DELETE'],
            'CREATE': ['CREATE'],
            'DROP': ['DROP'],
            'ALTER': ['ALTER'],
            'MERGE': ['MERGE'],
            'UPSERT': ['UPSERT']
        }
        
        self.join_keywords = [
            'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
            'CROSS JOIN', 'NATURAL JOIN', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN',
            'FULL OUTER JOIN'
        ]
        
        self.temp_table_patterns = [
            r'CREATE\s+(?:TEMPORARY|TEMP)\s+TABLE',
            r'CREATE\s+TABLE\s+#\w+',
            r'WITH\s+\w+\s+AS\s*\(',
            r'INTO\s+#\w+'
        ]
    
    def analyze_query(self, query: str, query_id: str = None) -> Dict[str, Any]:
        """
        Analyze a single SQL query and extract comprehensive information.
        
        Args:
            query: The SQL query string to analyze
            query_id: Optional identifier for the query
            
        Returns:
            Dictionary containing analysis results
        """
        if not query or not query.strip():
            return self._create_empty_result(query_id)
        
        try:
            # Parse the SQL query
            parsed = sqlparse.parse(query)[0]
            
            # Initialize result structure
            result = {
                'query_id': query_id or 'unknown',
                'original_query': query.strip(),
                'query_type': self._determine_query_type(query),
                'tables': self._extract_tables(parsed),
                'columns': self._extract_columns(parsed),
                'has_joins': self._detect_joins(query),
                'join_types': self._extract_join_types(query),
                'temp_tables': self._extract_temp_tables(query) if self.include_temp_tables else [],
                'operations': self._extract_operations(parsed),
                'complexity_score': 0,
                'change_areas': [],
                'subqueries': self._count_subqueries(parsed),
                'functions': self._extract_functions(parsed),
                'conditions': self._extract_conditions(parsed)
            }
            
            # Calculate complexity score
            result['complexity_score'] = self._calculate_complexity(result)
            
            # Identify areas needing changes
            result['change_areas'] = self._identify_change_areas(result)
            
            # Add detailed join analysis if enabled
            if self.detailed_join_analysis and result['has_joins']:
                result['join_details'] = self._analyze_joins_detailed(query, parsed)
            
            # Add column usage analysis if enabled
            if self.column_usage_tracking:
                result['column_usage'] = self._analyze_column_usage(result)
            
            return result
            
        except Exception as e:
            return self._create_error_result(query_id, str(e), query)
    
    def _create_empty_result(self, query_id: str) -> Dict[str, Any]:
        """Create an empty result structure for invalid queries."""
        return {
            'query_id': query_id or 'unknown',
            'original_query': '',
            'query_type': 'EMPTY',
            'tables': [],
            'columns': [],
            'has_joins': False,
            'join_types': [],
            'temp_tables': [],
            'operations': [],
            'complexity_score': 0,
            'change_areas': ['Query is empty or invalid'],
            'subqueries': 0,
            'functions': [],
            'conditions': []
        }
    
    def _create_error_result(self, query_id: str, error: str, query: str) -> Dict[str, Any]:
        """Create an error result structure for failed parsing."""
        return {
            'query_id': query_id or 'unknown',
            'original_query': query,
            'query_type': 'ERROR',
            'tables': [],
            'columns': [],
            'has_joins': False,
            'join_types': [],
            'temp_tables': [],
            'operations': [],
            'complexity_score': 0,
            'change_areas': [f'Parsing error: {error}'],
            'subqueries': 0,
            'functions': [],
            'conditions': []
        }
    
    def _determine_query_type(self, query: str) -> str:
        """Determine the primary CRUD operation type of the query."""
        query_upper = query.upper().strip()
        
        # Check for each CRUD type
        for crud_type, keywords in self.crud_keywords.items():
            for keyword in keywords:
                if query_upper.startswith(keyword):
                    return crud_type
        
        # Check for WITH clauses (Common Table Expressions)
        if query_upper.startswith('WITH'):
            return 'CTE'
        
        return 'UNKNOWN'
    
    def _extract_tables(self, parsed) -> List[str]:
        """Extract table names specifically from FROM and JOIN clauses."""
        tables = set()
        
        # Convert tokens to string for easier parsing
        query_str = str(parsed).upper()
        tokens = list(parsed.flatten())
        
        # Find FROM and JOIN clauses more precisely
        in_from_clause = False
        in_join_clause = False
        next_is_table = False
        
        for i, token in enumerate(tokens):
            token_value = token.value.upper().strip()
            
            # Reset flags when encountering certain keywords
            if token.ttype is Keyword and token_value in ['WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION', 'SELECT']:
                in_from_clause = False
                in_join_clause = False
                next_is_table = False
                continue
            
            # Detect FROM clause
            if token.ttype is Keyword and token_value == 'FROM':
                in_from_clause = True
                next_is_table = True
                continue
            
            # Detect JOIN clauses
            if token.ttype is Keyword and 'JOIN' in token_value:
                in_join_clause = True
                next_is_table = True
                continue
            
            # Skip ON keyword in joins
            if token.ttype is Keyword and token_value == 'ON':
                next_is_table = False
                continue
            
            # Extract table names when in FROM or JOIN context
            if (in_from_clause or in_join_clause) and next_is_table:
                if token.ttype in (Name, None) and token_value not in ['AS', 'ON', 'WHERE', 'AND', 'OR']:
                    # Clean the table name
                    cleaned_name = token.value.strip('`"[](),').strip()
                    
                    # Additional validation for table names
                    if (cleaned_name and 
                        not cleaned_name.upper() in ['AS', 'ON', 'WHERE', 'AND', 'OR', 'SELECT', 'FROM', 'JOIN'] and
                        len(cleaned_name) > 0 and
                        not cleaned_name.isdigit() and
                        '.' not in cleaned_name or len(cleaned_name.split('.')) <= 2):  # Allow schema.table format
                        
                        # Remove schema prefix if present (keep only table name)
                        if '.' in cleaned_name:
                            cleaned_name = cleaned_name.split('.')[-1]
                        
                        tables.add(cleaned_name)
                        next_is_table = False  # Found table, reset flag
            
            # Handle aliases (AS keyword)
            if token.ttype is Keyword and token_value == 'AS':
                next_is_table = False
        
        return list(tables)
    
    def _is_likely_table_name(self, name: str) -> bool:
        """Determine if a name is likely a table name rather than a column or keyword."""
        if not name or len(name) < 2:
            return False
        
        # Common SQL keywords that aren't table names
        sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'AS', 'AND', 'OR', 'IN', 'EXISTS',
            'GROUP', 'ORDER', 'BY', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'DISTINCT',
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        }
        
        if name.upper() in sql_keywords:
            return False
        
        # If we have metadata, check against known tables
        if self.table_metadata:
            return name in self.table_metadata
        
        # Basic heuristics for table names
        return not ('.' in name and len(name.split('.')) > 2)  # Avoid complex column references
    
    def _extract_columns(self, parsed) -> List[str]:
        """Extract column names from the parsed SQL."""
        columns = set()
        
        # This is a simplified extraction - in practice, this would be much more complex
        for token in parsed.flatten():
            if token.ttype is Name:
                # Filter out obvious non-column names
                value = token.value.strip('`"[]')
                if value and not self._is_likely_table_name(value) and value.upper() not in [
                    'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'AS'
                ]:
                    columns.add(value)
        
        return list(columns)
    
    def _detect_joins(self, query: str) -> bool:
        """Detect if the query contains JOIN operations."""
        query_upper = query.upper()
        return any(join_type in query_upper for join_type in self.join_keywords)
    
    def _extract_join_types(self, query: str) -> List[str]:
        """Extract the types of JOINs used in the query."""
        query_upper = query.upper()
        found_joins = []
        
        for join_type in self.join_keywords:
            if join_type in query_upper:
                found_joins.append(join_type)
        
        return found_joins
    
    def _extract_temp_tables(self, query: str) -> List[str]:
        """Extract temporary table names from the query."""
        temp_tables = []
        
        for pattern in self.temp_table_patterns:
            matches = re.finditer(pattern, query, re.IGNORECASE)
            for match in matches:
                # Extract table name after the pattern
                remaining = query[match.end():].strip()
                table_name = remaining.split()[0] if remaining.split() else ''
                if table_name:
                    temp_tables.append(table_name.strip('();,'))
        
        # Also look for CTE (Common Table Expressions)
        cte_pattern = r'WITH\s+(\w+)\s+AS\s*\('
        cte_matches = re.finditer(cte_pattern, query, re.IGNORECASE)
        for match in cte_matches:
            temp_tables.append(match.group(1))
        
        return list(set(temp_tables))  # Remove duplicates
    
    def _extract_operations(self, parsed) -> List[str]:
        """Extract the types of operations performed in the query."""
        operations = []
        
        for token in parsed.flatten():
            if token.ttype is Keyword:
                keyword = token.value.upper()
                if keyword in ['INSERT', 'UPDATE', 'DELETE', 'SELECT', 'CREATE', 'DROP', 'ALTER']:
                    operations.append(keyword)
        
        return list(set(operations))  # Remove duplicates
    
    def _count_subqueries(self, parsed) -> int:
        """Count the number of subqueries in the SQL."""
        subquery_count = 0
        paren_depth = 0
        
        for token in parsed.flatten():
            if token.value == '(':
                paren_depth += 1
            elif token.value == ')':
                paren_depth -= 1
            elif token.ttype is Keyword and token.value.upper() == 'SELECT' and paren_depth > 0:
                subquery_count += 1
        
        return subquery_count
    
    def _extract_functions(self, parsed) -> List[str]:
        """Extract SQL functions used in the query."""
        functions = set()
        
        for token in parsed.flatten():
            if token.ttype is Name and token.value.upper() in [
                'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'COALESCE', 'ISNULL', 'CASE',
                'CAST', 'CONVERT', 'SUBSTRING', 'CONCAT', 'UPPER', 'LOWER', 'TRIM'
            ]:
                functions.add(token.value.upper())
        
        return list(functions)
    
    def _extract_conditions(self, parsed) -> List[str]:
        """Extract WHERE conditions and other filtering criteria."""
        conditions = []
        in_where = False
        current_condition = []
        
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() == 'WHERE':
                in_where = True
                continue
            elif token.ttype is Keyword and token.value.upper() in ['GROUP', 'ORDER', 'HAVING', 'LIMIT']:
                if current_condition and in_where:
                    conditions.append(' '.join(current_condition).strip())
                in_where = False
                current_condition = []
                continue
            
            if in_where and token.value.strip():
                current_condition.append(token.value)
        
        if current_condition and in_where:
            conditions.append(' '.join(current_condition).strip())
        
        return conditions
    
    def _calculate_complexity(self, result: Dict[str, Any]) -> int:
        """Calculate a complexity score for the query."""
        score = 0
        
        # Base score for query type
        if result['query_type'] == 'SELECT':
            score += 1
        elif result['query_type'] in ['INSERT', 'UPDATE', 'DELETE']:
            score += 2
        elif result['query_type'] in ['CREATE', 'ALTER', 'DROP']:
            score += 3
        
        # Add points for various complexity factors
        score += len(result['tables']) * 1
        score += len(result['columns']) * 0.5
        score += len(result['join_types']) * 2
        score += len(result['temp_tables']) * 3
        score += result['subqueries'] * 4
        score += len(result['functions']) * 1
        score += len(result['conditions']) * 1
        
        return int(score)
    
    def _identify_change_areas(self, result: Dict[str, Any]) -> List[str]:
        """Identify areas of the query that may need changes based on analysis."""
        change_areas = []
        
        # Check for potential issues
        if len(result['tables']) > 5:
            change_areas.append("Query involves many tables - consider breaking into smaller queries")
        
        if len(result['join_types']) > 3:
            change_areas.append("Complex join structure - review for optimization opportunities")
        
        if result['subqueries'] > 2:
            change_areas.append("Multiple subqueries detected - consider using CTEs for better readability")
        
        if result['complexity_score'] > 20:
            change_areas.append("High complexity score - consider refactoring for maintainability")
        
        if result['temp_tables']:
            change_areas.append("Temporary tables used - ensure proper cleanup and consider alternatives")
        
        # Check against metadata if available
        if self.table_metadata:
            for table in result['tables']:
                if table not in self.table_metadata:
                    change_areas.append(f"Table '{table}' not found in provided metadata")
        
        if not change_areas:
            change_areas.append("No obvious issues detected")
        
        return change_areas
    
    def _analyze_joins_detailed(self, query: str, parsed) -> Dict[str, Any]:
        """Perform detailed analysis of JOIN operations."""
        join_details = {
            'join_count': len(self._extract_join_types(query)),
            'join_tables': [],
            'join_conditions': [],
            'cartesian_product_risk': False
        }
        
        # This would be expanded with more sophisticated join analysis
        # For now, provide basic information
        
        return join_details
    
    def _analyze_column_usage(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze column usage patterns."""
        column_usage = {
            'total_columns': len(result['columns']),
            'unique_columns': len(set(result['columns'])),
            'repeated_columns': [],
            'metadata_matched': 0
        }
        
        # Check for repeated column usage
        column_counts = defaultdict(int)
        for col in result['columns']:
            column_counts[col] += 1
        
        column_usage['repeated_columns'] = [
            col for col, count in column_counts.items() if count > 1
        ]
        
        # Check against metadata
        if self.table_metadata:
            for table_name, fields in self.table_metadata.items():
                for col in result['columns']:
                    if col in fields:
                        column_usage['metadata_matched'] += 1
        
        return column_usage
