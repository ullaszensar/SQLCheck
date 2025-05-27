import pandas as pd
import streamlit as st
from typing import Dict, List, Any, Optional
import io

class ExcelProcessor:
    """
    Processes Excel files containing table metadata and field information.
    """
    
    def __init__(self):
        self.supported_extensions = ['.xlsx', '.xls']
        self.expected_columns = {
            'table_name': ['table_name', 'table', 'tablename', 'table_nm', 'tableName'],
            'field_name': ['field_name', 'field', 'column', 'column_name', 'fieldname', 'fieldsql', 'FieldSQL'],
            'data_type': ['data_type', 'datatype', 'type', 'field_type'],
            'description': ['description', 'desc', 'comment', 'remarks']
        }
    
    def process_excel_file(self, uploaded_file) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process uploaded Excel file and extract table metadata.
        
        Args:
            uploaded_file: Streamlit uploaded file object
            
        Returns:
            Dictionary with table names as keys and field information as values
        """
        try:
            # Read Excel file
            if uploaded_file.name.endswith('.xlsx'):
                excel_data = pd.read_excel(uploaded_file, sheet_name=None, engine='openpyxl')
            else:
                excel_data = pd.read_excel(uploaded_file, sheet_name=None)
            
            metadata = {}
            
            # Process each sheet
            for sheet_name, df in excel_data.items():
                sheet_metadata = self._process_sheet(df, sheet_name)
                if sheet_metadata:
                    metadata.update(sheet_metadata)
            
            if not metadata:
                raise ValueError("No valid table metadata found in any sheet")
            
            return metadata
            
        except Exception as e:
            raise Exception(f"Error processing Excel file: {str(e)}")
    
    def _process_sheet(self, df: pd.DataFrame, sheet_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process a single Excel sheet to extract table metadata.
        
        Args:
            df: DataFrame containing the sheet data
            sheet_name: Name of the Excel sheet
            
        Returns:
            Dictionary with table metadata
        """
        if df.empty:
            return {}
        
        # Clean column names
        df.columns = df.columns.str.strip().str.lower()
        
        # Map columns to expected schema
        column_mapping = self._map_columns(df.columns.tolist())
        
        if not column_mapping.get('table_name') or not column_mapping.get('field_name'):
            # If no table/field columns found, treat sheet name as table and columns as fields
            return self._process_schema_sheet(df, sheet_name)
        
        # Process metadata sheet
        return self._process_metadata_sheet(df, column_mapping)
    
    def _map_columns(self, columns: List[str]) -> Dict[str, str]:
        """
        Map actual column names to expected column types.
        
        Args:
            columns: List of column names from the Excel sheet
            
        Returns:
            Dictionary mapping expected column types to actual column names
        """
        mapping = {}
        
        for expected_type, possible_names in self.expected_columns.items():
            for col in columns:
                if any(possible in col.lower() for possible in possible_names):
                    mapping[expected_type] = col
                    break
        
        return mapping
    
    def _process_metadata_sheet(self, df: pd.DataFrame, column_mapping: Dict[str, str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process a metadata sheet with table and field information.
        
        Args:
            df: DataFrame containing metadata
            column_mapping: Mapping of expected columns to actual columns
            
        Returns:
            Dictionary with table metadata
        """
        metadata = {}
        
        try:
            # Get required columns
            table_col = column_mapping['table_name']
            field_col = column_mapping['field_name']
            type_col = column_mapping.get('data_type', None)
            desc_col = column_mapping.get('description', None)
            
            # Group by table name
            for table_name, group in df.groupby(table_col):
                if pd.isna(table_name) or str(table_name).strip() == '':
                    continue
                
                table_name = str(table_name).strip()
                fields = []
                
                for _, row in group.iterrows():
                    field_name = row[field_col]
                    if pd.isna(field_name) or str(field_name).strip() == '':
                        continue
                    
                    field_info = {
                        'name': str(field_name).strip(),
                        'data_type': str(row[type_col]).strip() if type_col and not pd.isna(row[type_col]) else 'unknown',
                        'description': str(row[desc_col]).strip() if desc_col and not pd.isna(row[desc_col]) else ''
                    }
                    
                    fields.append(field_info)
                
                if fields:
                    metadata[table_name] = fields
            
            return metadata
            
        except Exception as e:
            st.warning(f"Error processing metadata sheet: {str(e)}")
            return {}
    
    def _process_schema_sheet(self, df: pd.DataFrame, sheet_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process a sheet as a table schema where sheet name is table name and columns are fields.
        
        Args:
            df: DataFrame containing the data
            sheet_name: Name of the sheet (used as table name)
            
        Returns:
            Dictionary with table metadata
        """
        table_name = sheet_name.strip()
        fields = []
        
        for col in df.columns:
            if col and str(col).strip():
                # Infer data type from the column data
                data_type = self._infer_data_type(df[col])
                
                field_info = {
                    'name': str(col).strip(),
                    'data_type': data_type,
                    'description': f'Field from {sheet_name} sheet'
                }
                
                fields.append(field_info)
        
        return {table_name: fields} if fields else {}
    
    def _infer_data_type(self, series: pd.Series) -> str:
        """
        Infer data type from a pandas Series.
        
        Args:
            series: Pandas Series to analyze
            
        Returns:
            String representation of the inferred data type
        """
        try:
            # Remove null values for analysis
            non_null_series = series.dropna()
            
            if non_null_series.empty:
                return 'unknown'
            
            # Check pandas dtype
            if pd.api.types.is_integer_dtype(series):
                return 'integer'
            elif pd.api.types.is_float_dtype(series):
                return 'float'
            elif pd.api.types.is_bool_dtype(series):
                return 'boolean'
            elif pd.api.types.is_datetime64_any_dtype(series):
                return 'datetime'
            elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
                # Check if it's numeric data stored as string
                try:
                    pd.to_numeric(non_null_series.iloc[:min(100, len(non_null_series))])
                    return 'numeric_string'
                except:
                    pass
                
                # Check if it's date data stored as string
                try:
                    pd.to_datetime(non_null_series.iloc[:min(10, len(non_null_series))])
                    return 'date_string'
                except:
                    pass
                
                # Check average length to distinguish between varchar and text
                avg_length = non_null_series.astype(str).str.len().mean()
                if avg_length > 255:
                    return 'text'
                else:
                    return f'varchar({int(non_null_series.astype(str).str.len().max())})'
            
            return 'unknown'
            
        except Exception:
            return 'unknown'
    
    def validate_metadata(self, metadata: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Validate the extracted metadata and provide summary information.
        
        Args:
            metadata: Extracted metadata dictionary
            
        Returns:
            Dictionary with validation results and summary
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'summary': {
                'total_tables': len(metadata),
                'total_fields': sum(len(fields) for fields in metadata.values()),
                'tables_with_no_fields': [],
                'data_type_distribution': {}
            }
        }
        
        # Check for tables with no fields
        for table_name, fields in metadata.items():
            if not fields:
                validation_result['summary']['tables_with_no_fields'].append(table_name)
                validation_result['warnings'].append(f"Table '{table_name}' has no fields")
        
        # Analyze data type distribution
        data_type_counts = {}
        for table_name, fields in metadata.items():
            for field in fields:
                data_type = field.get('data_type', 'unknown')
                data_type_counts[data_type] = data_type_counts.get(data_type, 0) + 1
        
        validation_result['summary']['data_type_distribution'] = data_type_counts
        
        # Check for potential issues
        if validation_result['summary']['total_tables'] == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("No tables found in the metadata")
        
        if validation_result['summary']['total_fields'] == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("No fields found in any table")
        
        return validation_result
    
    def export_metadata_sample(self) -> pd.DataFrame:
        """
        Create a sample DataFrame showing the expected format for metadata input.
        
        Returns:
            DataFrame with sample metadata structure
        """
        sample_data = {
            'table_name': ['users', 'users', 'orders', 'orders', 'order_items'],
            'field_name': ['user_id', 'username', 'order_id', 'user_id', 'item_id'],
            'data_type': ['integer', 'varchar(50)', 'integer', 'integer', 'integer'],
            'description': ['Primary key', 'User login name', 'Primary key', 'Foreign key to users', 'Foreign key to items']
        }
        
        return pd.DataFrame(sample_data)
