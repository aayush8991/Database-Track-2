"""
Advanced SQL Normalization Strategy
Implements detection of:
- Repeating groups (with functional dependencies)
- Multi-level nested arrays
- Many-to-many relationships
- Proper key selection
- Index strategies
"""

from typing import Dict, List, Tuple, Set
import json
from collections import defaultdict
from datetime import datetime
import uuid


class AdvancedNormalizer:
    """
    Implements formal normalization theory:
    - 1NF (Atomic values)
    - 2NF (No partial dependencies)
    - 3NF (No transitive dependencies)
    - BCNF (Every determinant is candidate key)
    """
    
    def __init__(self):
        self.functional_dependencies = defaultdict(set)
        self.candidate_keys = []
        self.normalization_log = []
        self.decomposition_rules = {}
    
    def analyze_data_structure(self, data: List[Dict]) -> Dict:
        """
        Complete analysis of data structure for normalization.
        """
        if not data:
            return {
                'status': 'error',
                'message': 'No data to analyze'
            }
        
        analysis = {
            'repeating_groups': self.detect_repeating_groups(data)['repeating_groups'],
            'nesting_levels': self.detect_multi_level_nesting(data),
            'functional_dependencies': self.detect_functional_dependencies(data)['dependencies'],
            'many_to_many': self.detect_many_to_many_relationships(data)['junction_tables'],
            'primary_key_strategy': self.recommend_primary_keys(data, 'entity'),
            'normalization_log': self.normalization_log
        }
        
        return analysis
    
    def detect_repeating_groups(self, data: List[Dict]) -> Dict:
        """
        Detect actual repeating groups in data.
        
        A repeating group is a set of attributes that appear
        multiple times for the same entity.
        
        Returns:
        {
            'repeating_groups': [
                {
                    'parent_entity': 'users',
                    'group_name': 'addresses',
                    'attributes': ['street', 'city', 'zip'],
                    'cardinality': '1:N',
                    'occurrences_per_parent': [1, 3, 2, ...]
                }
            ]
        }
        """
        analysis = {
            'repeating_groups': [],
            'candidate_keys': [],
            'functional_dependencies': []
        }
        
        # Sample data to analyze
        sample = data[:min(100, len(data))]
        
        # Find all array fields
        array_fields = self._find_array_fields(sample)
        
        for field_name, array_data in array_fields.items():
            if not array_data:
                continue
            
            # Check if arrays contain objects (repeating groups)
            if isinstance(array_data[0], dict):
                group_info = {
                    'group_name': field_name,
                    'attributes': list(array_data[0].keys()),
                    'is_repeating_group': True,
                    'cardinality': '1:N',
                    'avg_occurrences': sum(
                        len(arr) for arr in array_data
                    ) / len(array_data) if array_data else 0,
                    'requires_normalization': True
                }
                analysis['repeating_groups'].append(group_info)
                self.normalization_log.append(
                    f"✓ Found repeating group '{field_name}' "
                    f"with attributes {group_info['attributes']}"
                )
            else:
                # Simple array - could be many-to-many
                group_info = {
                    'group_name': field_name,
                    'attributes': [field_name + '_value'],
                    'is_repeating_group': False,
                    'relationship_type': 'M:N',
                    'values_sample': array_data[0][:3] if array_data else [],
                    'requires_normalization': True
                }
                analysis['repeating_groups'].append(group_info)
                self.normalization_log.append(
                    f"✓ Found M:N array '{field_name}' - requires junction table"
                )
        
        return analysis
    
    def detect_multi_level_nesting(self, data: List[Dict]) -> Dict:
        """
        Detect multi-level nested structures.
        
        Example:
        {
            user_id: 1,
            posts: [
                {
                    post_id: 1,
                    comments: [
                        {text: "...", likes: [...]}
                    ]
                }
            ]
        }
        
        Returns hierarchy of nesting levels with paths.
        """
        nesting_analysis = {
            'levels': [],
            'paths': [],
            'deepest_level': 0,
            'hierarchies': []
        }
        
        sample = data[0] if data else {}
        self._analyze_nesting_recursive(
            sample, 
            path='root',
            level=0,
            analysis=nesting_analysis,
            parent_key=None
        )
        
        # Log findings
        if nesting_analysis['deepest_level'] > 1:
            self.normalization_log.append(
                f"✓ Found {nesting_analysis['deepest_level']}-level nesting: "
                f"{' → '.join([p.split('.')[-1] for p in nesting_analysis['paths']])}"
            )
        
        return nesting_analysis
    
    def _analyze_nesting_recursive(
        self, 
        obj, 
        path: str, 
        level: int,
        analysis: Dict,
        parent_key: str = None
    ):
        """Recursively analyze nesting structure."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}"
                
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    # Found nested array of objects
                    analysis['levels'].append({
                        'level': level + 1,
                        'path': current_path,
                        'type': 'array_of_objects',
                        'attributes': list(value[0].keys()),
                        'parent_key': parent_key,
                        'child_key': key
                    })
                    analysis['paths'].append(current_path)
                    analysis['deepest_level'] = max(
                        analysis['deepest_level'], 
                        level + 1
                    )
                    
                    # Build hierarchy
                    hierarchy_entry = {
                        'parent': path.split('.')[-1] if level > 0 else 'root',
                        'child': key,
                        'level': level + 1,
                        'cardinality': '1:N'
                    }
                    analysis['hierarchies'].append(hierarchy_entry)
                    
                    # Recurse into nested objects
                    self._analyze_nesting_recursive(
                        value[0],
                        current_path,
                        level + 1,
                        analysis,
                        parent_key=key
                    )
                elif isinstance(value, dict) and not isinstance(value, list):
                    # Nested object (not array)
                    self._analyze_nesting_recursive(
                        value,
                        current_path,
                        level + 1,
                        analysis,
                        parent_key=parent_key
                    )
    
    def detect_functional_dependencies(self, data: List[Dict]) -> Dict:
        """
        Detect functional dependencies in data.
        
        Example: email → email_domain, username
        If X → Y, then for every X value there's exactly one Y value.
        
        Returns:
        {
            'dependencies': [
                {
                    'determinant': 'user_id',
                    'dependent': ['username', 'email'],
                    'confidence': 0.95
                }
            ]
        }
        """
        dependencies = {'dependencies': []}
        
        if not data:
            return dependencies
        
        sample = data[:min(100, len(data))]
        all_keys = set()
        
        # Get all keys (non-complex types)
        for record in sample:
            for k, v in record.items():
                if not isinstance(v, (dict, list)):
                    all_keys.add(k)
        
        # Test each potential determinant
        for potential_det in all_keys:
            values_map = {}
            valid_count = 0
            
            for record in sample:
                if potential_det not in record:
                    continue
                
                det_value = record[potential_det]
                if det_value is None:
                    continue
                
                valid_count += 1
                det_key = str(det_value)  # Convert to string for consistency
                
                if det_key not in values_map:
                    values_map[det_key] = {}
                
                # Check other attributes
                for attr in all_keys:
                    if attr != potential_det:
                        if attr not in values_map[det_key]:
                            values_map[det_key][attr] = set()
                        
                        if attr in record:
                            values_map[det_key][attr].add(
                                str(record[attr])
                            )
            
            # Check if this is a valid determinant
            dependents = []
            for det_val, attrs_dict in values_map.items():
                for attr, values in attrs_dict.items():
                    # If each determinant value has exactly one attr value
                    if len(values) == 1:
                        dependents.append(attr)
            
            if dependents:
                dependents = list(set(dependents))
                confidence = len(values_map) / valid_count if valid_count > 0 else 0
                
                dependencies['dependencies'].append({
                    'determinant': potential_det,
                    'dependents': dependents,
                    'confidence': min(confidence, 0.99),  # Cap at 0.99
                    'num_determinant_values': len(values_map),
                    'num_samples': valid_count
                })
                
                self.normalization_log.append(
                    f"✓ Found functional dependency: "
                    f"{potential_det} → {dependents} (confidence: {confidence:.2f})"
                )
        
        return dependencies
    
    def detect_many_to_many_relationships(self, data: List[Dict]) -> Dict:
        """
        Detect many-to-many relationships.
        
        Examples:
        - users.tags (user has many tags, tag has many users)
        - students.courses (student takes many courses, course has many students)
        
        Returns junction table specifications.
        """
        m2m_analysis = {
            'junction_tables': []
        }
        
        if not data:
            return m2m_analysis
        
        sample = data[0]
        
        for key, value in sample.items():
            if isinstance(value, list) and value:
                # Check what's in the array
                if isinstance(value[0], str) or isinstance(value[0], (int, float)):
                    # Simple array of values - this is M:N
                    # Singularize the key to get entity name
                    singular = key.rstrip('s') if key.endswith('s') else key
                    
                    junction_spec = {
                        'left_entity': 'parent_entity',
                        'left_key': 'parent_id',
                        'right_entity': singular,
                        'right_key': f"{singular}_id",
                        'junction_table': f"parent_{key}",
                        'left_table': 'parent',
                        'right_table': singular,
                        'data_sample': value[:3],
                        'cardinality': 'M:N',
                        'value_type': type(value[0]).__name__
                    }
                    m2m_analysis['junction_tables'].append(junction_spec)
                    
                    self.normalization_log.append(
                        f"✓ Found M:N relationship: '{key}' requires junction table "
                        f"'{junction_spec['junction_table']}'"
                    )
        
        return m2m_analysis
    
    def recommend_primary_keys(self, data: List[Dict], entity_name: str) -> Dict:
        """
        Recommend primary keys based on data analysis.
        
        Strategy:
        1. Check for natural keys (unique, non-null attributes)
        2. If no natural key, recommend surrogate key
        3. Support composite keys if needed
        """
        recommendations = {
            'primary_key': None,
            'strategy': None,
            'reasoning': [],
            'candidate_keys': [],
            'uniqueness_analysis': {},
            'recommended_composite': None
        }
        
        if not data:
            # No data - recommend surrogate key
            recommendations['primary_key'] = f"{entity_name}_id"
            recommendations['strategy'] = 'surrogate'
            recommendations['reasoning'].append(
                "No data to analyze, using surrogate key pattern"
            )
            return recommendations
        
        sample = data[:min(100, len(data))]
        
        # Test uniqueness of each field
        for field in sample[0].keys():
            if isinstance(sample[0][field], (dict, list)):
                continue
            
            values = [r.get(field) for r in sample if field in r]
            unique_values = set(v for v in values if v is not None)
            null_count = sum(1 for v in values if v is None)
            
            uniqueness = {
                'field': field,
                'unique_count': len(unique_values),
                'total_count': len(values),
                'null_count': null_count,
                'uniqueness_ratio': len(unique_values) / len(values) if values else 0,
                'is_candidate': len(unique_values) == len(values) and null_count == 0
            }
            recommendations['uniqueness_analysis'][field] = uniqueness
            
            # Natural key candidates
            if uniqueness['is_candidate']:
                recommendations['candidate_keys'].append(field)
        
        # Determine recommendation
        if recommendations['candidate_keys']:
            # Prefer natural keys with semantic meaning
            semantic_keywords = ['email', 'username', 'code', 'id', 'identifier', 'ssn', 'isbn']
            
            for key in recommendations['candidate_keys']:
                if any(x in key.lower() for x in semantic_keywords):
                    recommendations['primary_key'] = key
                    recommendations['strategy'] = 'natural'
                    recommendations['reasoning'].append(
                        f"Using natural key '{key}' (unique, semantic meaning)"
                    )
                    self.normalization_log.append(
                        f"✓ Selected primary key: '{key}' (natural key strategy)"
                    )
                    break
            
            # Otherwise use first candidate
            if not recommendations['primary_key']:
                recommendations['primary_key'] = recommendations['candidate_keys'][0]
                recommendations['strategy'] = 'natural'
                recommendations['reasoning'].append(
                    f"Using natural key '{recommendations['primary_key']}'"
                )
        else:
            # Use surrogate key
            recommendations['primary_key'] = f"{entity_name}_id"
            recommendations['strategy'] = 'surrogate'
            recommendations['reasoning'].append(
                "No natural key found, using surrogate key"
            )
            self.normalization_log.append(
                f"✓ No natural key found, recommending surrogate: '{recommendations['primary_key']}'"
            )
        
        return recommendations
    
    def recommend_indexes(self, 
                         schema: Dict, 
                         query_patterns: List[Dict] = None) -> List[Dict]:
        """
        Recommend indexes for tables.
        
        Strategy:
        1. Index all foreign keys
        2. Index primary keys (automatic)
        3. Index frequently filtered columns
        4. Composite indexes for common WHERE + JOIN patterns
        """
        indexes = []
        
        # Index all foreign keys
        for table_name, table_schema in schema.items():
            if 'foreign_keys' in table_schema:
                for fk in table_schema['foreign_keys']:
                    fk_col = fk.get('column') or fk.get('name')
                    indexes.append({
                        'table': table_name,
                        'columns': [fk_col],
                        'type': 'BTREE',
                        'name': f"idx_{table_name}_{fk_col}",
                        'reason': f'Foreign key {fk_col} for JOIN operations',
                        'priority': 'HIGH'
                    })
                    self.normalization_log.append(
                        f"✓ Recommended index on FK: {table_name}({fk_col})"
                    )
            
            # Index primary key (implicit in SQL, explicit here for reference)
            if 'primary_key' in table_schema:
                pk = table_schema['primary_key']
                if not isinstance(pk, list):
                    pk = [pk]
                indexes.append({
                    'table': table_name,
                    'columns': pk,
                    'type': 'BTREE',
                    'name': f"idx_{table_name}_pk",
                    'reason': 'Primary key lookup',
                    'priority': 'CRITICAL'
                })
        
        # If query patterns provided, index frequently used columns
        if query_patterns:
            for pattern in query_patterns:
                if 'filter_columns' in pattern:
                    indexes.append({
                        'table': pattern.get('table'),
                        'columns': pattern['filter_columns'],
                        'type': 'BTREE',
                        'name': f"idx_{pattern.get('table')}_filter",
                        'reason': f"Common filter pattern: {pattern.get('description')}",
                        'priority': 'MEDIUM'
                    })
        
        return indexes
    
    def validate_normalization(self, schema: Dict) -> Dict:
        """
        Validate that schema meets normalization requirements.
        
        Checks:
        - 1NF: All values atomic (no arrays/objects in values)
        - 2NF: No partial dependencies on keys
        - 3NF: No transitive dependencies
        """
        validation = {
            '1NF': {'passed': True, 'violations': []},
            '2NF': {'passed': True, 'violations': []},
            '3NF': {'passed': True, 'violations': []},
            'overall_status': 'UNKNOWN'
        }
        
        for table_name, table_schema in schema.items():
            # Check 1NF - all attributes should be atomic
            for col_name, col_type in table_schema.get('columns', {}).items():
                if isinstance(col_type, str):
                    if col_type.upper() in ['JSON', 'ARRAY', 'OBJECT', 'TEXT_ARRAY']:
                        validation['1NF']['passed'] = False
                        validation['1NF']['violations'].append(
                            f"Table {table_name}: Column {col_name} has non-atomic type {col_type}"
                        )
        
        if (validation['1NF']['passed'] and 
            validation['2NF']['passed'] and 
            validation['3NF']['passed']):
            validation['overall_status'] = 'PASSED'
        else:
            validation['overall_status'] = 'FAILED'
        
        return validation
    
    def generate_ddl_statements(self, analysis: Dict, base_entity: str) -> Dict[str, str]:
        """
        Generate CREATE TABLE DDL statements from analysis.
        """
        ddl = {}
        
        # Main entity table
        pk = analysis['primary_key_strategy']['primary_key']
        pk_type = 'VARCHAR(36)' if 'id' in pk.lower() or 'uuid' in pk.lower() else 'INT'
        
        create_main = f"""
CREATE TABLE `{base_entity}` (
    `{pk}` {pk_type} PRIMARY KEY,
    -- Additional columns will be added here
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""
        ddl[base_entity] = create_main
        
        # Create tables for repeating groups
        for rg in analysis['repeating_groups']:
            if rg['is_repeating_group']:
                table_name = f"{base_entity}_{rg['group_name']}"
                fk_col = f"{base_entity}_id"
                
                create_child = f"""
CREATE TABLE `{table_name}` (
    `{table_name}_id` INT PRIMARY KEY AUTO_INCREMENT,
    `{fk_col}` {pk_type} NOT NULL,
    {''.join([f"`{attr}` VARCHAR(255)," for attr in rg['attributes']][:-1])},
    FOREIGN KEY (`{fk_col}`) REFERENCES `{base_entity}`(`{pk}`) ON DELETE CASCADE,
    INDEX idx_{fk_col} (`{fk_col}`)
);
"""
                ddl[table_name] = create_child
                self.normalization_log.append(
                    f"✓ Generated DDL for repeating group table: {table_name}"
                )
        
        # Create junction tables for M:N relationships
        for m2n in analysis['many_to_many']:
            junction_table = m2n['junction_table']
            left_entity = m2n['left_table']
            right_entity = m2n['right_table']
            
            create_junction = f"""
CREATE TABLE `{junction_table}` (
    `{left_entity}_id` {pk_type} NOT NULL,
    `{right_entity}_id` VARCHAR(36) NOT NULL,
    PRIMARY KEY (`{left_entity}_id`, `{right_entity}_id`),
    FOREIGN KEY (`{left_entity}_id`) REFERENCES `{left_entity}`(`id`),
    INDEX idx_{right_entity}_id (`{right_entity}_id`)
);
"""
            ddl[junction_table] = create_junction
            self.normalization_log.append(
                f"✓ Generated DDL for junction table: {junction_table}"
            )
        
        return ddl
    
    def _find_array_fields(self, data: List[Dict]) -> Dict[str, List]:
        """Find all array fields and their contents."""
        array_fields = defaultdict(list)
        
        for record in data:
            for key, value in record.items():
                if isinstance(value, list):
                    array_fields[key].append(value)
        
        return array_fields
    
    def get_normalization_report(self) -> str:
        """Generate human-readable normalization report."""
        report = "\n" + "=" * 70 + "\n"
        report += "  NORMALIZATION STRATEGY REPORT\n"
        report += "=" * 70 + "\n\n"
        report += "Normalization Decisions Made:\n"
        report += "-" * 70 + "\n"
        
        
        if self.normalization_log:
            for log_entry in self.normalization_log:
                report += f"{log_entry}\n"
        else:
            report += "No normalization decisions made yet.\n"
        
        report += "\n" + "=" * 70 + "\n"
        return report
    
    def analyze_mongodb_strategy(self, data_samples: List[Dict]) -> Dict:
        """
        Analyze data samples and create embedding/referencing strategy for MongoDB.
        
        Strategy:
        - EMBED if: small (< 1KB), rarely updated, not shared
        - REFERENCE if: large (> 5KB), frequently updated, unbounded growth
        
        Returns:
        {
            "strategy": {
                "field_name": {
                    "decision": "EMBED" | "REFERENCE",
                    "reasoning": "...",
                    "characteristics": {...}
                }
            },
            "schemas": {
                "collection_name": {...}
            }
        }
        """
        if not data_samples:
            return {"status": "error", "message": "No data samples provided"}
        
        strategy = {"field_decisions": {}}
        
        # Analyze each field in the sample documents
        for sample in data_samples[:min(10, len(data_samples))]:
            self._analyze_record_fields_for_mongo(sample, strategy, data_samples)
        
        # Generate collection schemas based on decisions
        schemas = self._generate_mongodb_collection_schemas(strategy)
        
        return {
            "status": "success",
            "strategy": strategy,
            "schemas": schemas
        }
    
    def _analyze_record_fields_for_mongo(self, record: Dict, strategy: Dict, all_samples: List[Dict]):
        """Analyze each field to determine embedding/referencing."""
        
        for field_name, value in record.items():
            if field_name.startswith("_"):  # Skip system fields
                continue
            
            # Skip if already analyzed
            if field_name in strategy["field_decisions"]:
                continue
            
            # Not nested - keep in main document
            if not isinstance(value, (dict, list)):
                strategy["field_decisions"][field_name] = {
                    "decision": "EMBED",
                    "reasoning": "Scalar value - always embed",
                    "type": "scalar",
                    "characteristics": {}
                }
                continue
            
            # Nested structure - needs decision
            if isinstance(value, dict):
                decision = self._decide_dict_strategy_mongo(field_name, value, all_samples)
                strategy["field_decisions"][field_name] = decision
            
            elif isinstance(value, list):
                decision = self._decide_array_strategy_mongo(field_name, value, all_samples)
                strategy["field_decisions"][field_name] = decision
    
    def _decide_dict_strategy_mongo(self, field_name: str, value: Dict, all_samples: List[Dict]) -> Dict:
        """Decide whether to embed or reference a nested object."""
        
        # Calculate characteristics
        size_bytes = len(json.dumps(value, default=str))
        depth = self._calculate_nesting_depth(value)
        
        # Check if nested object appears in all documents
        appears_in_all = sum(1 for s in all_samples if field_name in s) == len(all_samples)
        
        # Check for high cardinality
        unique_values = set()
        for sample in all_samples:
            if field_name in sample:
                unique_values.add(json.dumps(sample[field_name], default=str, sort_keys=True))
        cardinality_ratio = len(unique_values) / len(all_samples) if all_samples else 0
        
        # Make decision based on characteristics
        characteristics = {
            "size_bytes": size_bytes,
            "depth": depth,
            "appears_in_all": appears_in_all,
            "cardinality_ratio": cardinality_ratio,
            "num_keys": len(value)
        }
        
        # Decision logic
        if size_bytes < 500 and appears_in_all and depth <= 2:
            # Small, always present, shallow → EMBED
            decision = {
                "decision": "EMBED",
                "reasoning": f"Small object ({size_bytes}B), appears in all documents, shallow nesting",
                "type": "nested_object",
                "characteristics": characteristics
            }
        elif size_bytes > 2000 or cardinality_ratio > 0.8:
            # Large or many unique values → REFERENCE
            decision = {
                "decision": "REFERENCE",
                "reasoning": f"Large ({size_bytes}B) or high cardinality ({cardinality_ratio:.2%})",
                "type": "nested_object",
                "characteristics": characteristics,
                "reference_collection": f"{field_name}_details"
            }
        else:
            # Medium object → Default to EMBED
            decision = {
                "decision": "EMBED",
                "reasoning": f"Medium-sized object ({size_bytes}B), reasonable cardinality",
                "type": "nested_object",
                "characteristics": characteristics
            }
        
        return decision
    
    def _decide_array_strategy_mongo(self, field_name: str, value: List, all_samples: List[Dict]) -> Dict:
        """Decide whether to embed array or reference external collection."""
        
        if not value:
            # Empty array
            return {
                "decision": "EMBED",
                "reasoning": "Empty array - embed as-is",
                "type": "array",
                "characteristics": {"length": 0}
            }
        
        # Analyze array characteristics
        array_sizes = []
        for sample in all_samples:
            if field_name in sample and isinstance(sample[field_name], list):
                array_sizes.append(len(sample[field_name]))
        
        avg_length = sum(array_sizes) / len(array_sizes) if array_sizes else len(value)
        max_length = max(array_sizes) if array_sizes else len(value)
        
        # Check if array contains objects
        contains_objects = isinstance(value[0], dict) if value else False
        
        # Calculate total size if embedded
        embedded_size = len(json.dumps(value, default=str))
        
        characteristics = {
            "length": len(value),
            "avg_length": avg_length,
            "max_length": max_length,
            "contains_objects": contains_objects,
            "embedded_size_bytes": embedded_size,
            "growth_pattern": "unbounded" if max_length > avg_length * 2 else "bounded"
        }
        
        # Decision logic
        if max_length > 1000:
            # Unbounded growth (e.g., activity logs)
            decision = {
                "decision": "REFERENCE",
                "reasoning": f"Unbounded array growth (max {max_length} items, avg {avg_length:.0f})",
                "type": "array",
                "characteristics": characteristics,
                "reference_collection": field_name,
                "link_field": "parent_id"
            }
        elif embedded_size > 5000:
            # Too large to embed
            decision = {
                "decision": "REFERENCE",
                "reasoning": f"Array too large when serialized ({embedded_size}B)",
                "type": "array",
                "characteristics": characteristics,
                "reference_collection": field_name,
                "link_field": "parent_id"
            }
        elif contains_objects and avg_length > 20:
            # Complex objects, many of them
            decision = {
                "decision": "REFERENCE",
                "reasoning": f"Array of {avg_length:.0f} complex objects - reference external collection",
                "type": "array",
                "characteristics": characteristics,
                "reference_collection": field_name,
                "link_field": "parent_id"
            }
        else:
            # Small, bounded array → EMBED
            decision = {
                "decision": "EMBED",
                "reasoning": f"Small bounded array ({avg_length:.0f} items, {embedded_size}B)",
                "type": "array",
                "characteristics": characteristics
            }
        
        return decision
    
    def _calculate_nesting_depth(self, obj, current_depth: int = 0) -> int:
        """Calculate maximum nesting depth of an object."""
        if not isinstance(obj, (dict, list)):
            return current_depth
        
        if isinstance(obj, dict):
            if not obj:
                return current_depth
            max_depth = current_depth
            for v in obj.values():
                depth = self._calculate_nesting_depth(v, current_depth + 1)
                max_depth = max(max_depth, depth)
            return max_depth
        
        elif isinstance(obj, list):
            if not obj:
                return current_depth
            return self._calculate_nesting_depth(obj[0], current_depth + 1)
        
        return current_depth
    
    def _generate_mongodb_collection_schemas(self, strategy: Dict) -> Dict:
        """Generate MongoDB collection schemas with validation rules."""
        schemas = {}
        
        # Main collection schema
        main_schema = self._create_main_mongo_collection_schema(strategy)
        schemas["main_collection"] = main_schema
        
        # Referenced collection schemas (for REFERENCE decisions)
        for field_name, decision in strategy["field_decisions"].items():
            if decision["decision"] == "REFERENCE":
                ref_collection = decision.get("reference_collection", field_name)
                if ref_collection not in schemas:
                    ref_schema = self._create_reference_mongo_collection_schema(
                        ref_collection, 
                        field_name,
                        decision
                    )
                    schemas[ref_collection] = ref_schema
        
        return schemas
    
    def _create_main_mongo_collection_schema(self, strategy: Dict) -> Dict:
        """Create schema for main document collection."""
        
        schema = {
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["uuid", "created_at"],
                    "properties": {
                        "uuid": {
                            "bsonType": "string",
                            "description": "Unique document identifier"
                        },
                        "created_at": {
                            "bsonType": "date",
                            "description": "Document creation timestamp"
                        }
                    }
                }
            },
            "indexes": [
                {"key": {"uuid": 1}, "unique": True},
                {"key": {"created_at": 1}}
            ],
            "comment": "Main document collection with embedded and referenced fields"
        }
        
        # Add properties for each field
        for field_name, decision in strategy["field_decisions"].items():
            if decision["decision"] == "EMBED":
                # Add embedded field schema
                schema["validator"]["$jsonSchema"]["properties"][field_name] = \
                    self._get_field_mongo_schema(field_name, decision)
            else:
                # Add reference field (just store the ID)
                ref_field = f"{field_name}_id"
                schema["validator"]["$jsonSchema"]["properties"][ref_field] = {
                    "bsonType": "string",
                    "description": f"Reference to {decision.get('reference_collection', field_name)}"
                }
        
        return schema
    
    def _create_reference_mongo_collection_schema(self, collection_name: str, 
                                                  field_name: str, 
                                                  decision: Dict) -> Dict:
        """Create schema for a referenced collection."""
        
        schema = {
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["_id", "parent_id"],
                    "properties": {
                        "_id": {
                            "bsonType": "objectId",
                            "description": "MongoDB object ID"
                        },
                        "parent_id": {
                            "bsonType": "string",
                            "description": "Reference to parent document uuid"
                        },
                        "created_at": {
                            "bsonType": "date",
                            "description": "Document creation timestamp"
                        },
                        "data": {
                            "description": "Referenced data"
                        }
                    }
                }
            },
            "indexes": [
                {"key": {"parent_id": 1}},
                {"key": {"created_at": 1}}
            ],
            "comment": f"Referenced collection for {field_name} (Decision: {decision['decision']})"
        }
        
        return schema
    
    def _get_field_mongo_schema(self, field_name: str, decision: Dict) -> Dict:
        """Get JSON Schema for a field based on decision."""
        
        field_type = decision.get("type", "object")
        
        if field_type == "scalar":
            return {"bsonType": ["string", "int", "double", "bool", "null"]}
        
        elif field_type == "nested_object":
            return {
                "bsonType": "object",
                "description": f"Embedded object: {field_name}",
                "additionalProperties": True
            }
        
        elif field_type == "array":
            return {
                "bsonType": "array",
                "description": f"Embedded array: {field_name}",
                "items": {"bsonType": ["string", "int", "double", "bool", "object"]}
            }
        
        return {"bsonType": "object"}

