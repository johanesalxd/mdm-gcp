"""
Streaming MDM Processor
Implements 4-way matching for real-time entity resolution
"""

import re
import time
from typing import Any, Dict, List, Optional, Tuple
import uuid

from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
import pandas as pd


class StreamingMDMProcessor:
    """4-way streaming MDM processor with real-time matching"""

    def __init__(self, spanner_helper, embedding_model=None):
        self.spanner_helper = spanner_helper
        self.embedding_model = embedding_model

        # Matching weights (4-strategy, no AI)
        self.weights = {
            'exact': 0.40,      # Increased from 30% (no AI to compensate)
            'fuzzy': 0.30,      # Increased from 25%
            'vector': 0.20,     # Same
            'business': 0.10    # Decreased from 15%
        }

        # Confidence thresholds
        self.auto_merge_threshold = 0.85
        self.create_new_threshold = 0.65

    def standardize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize incoming record (matching BigQuery patterns)"""
        standardized = record.copy()

        # Standardize name
        if record.get('full_name'):
            standardized['full_name_clean'] = re.sub(
                r'[^a-zA-Z\s]', '', record['full_name']).strip().upper()

        # Standardize email
        if record.get('email'):
            standardized['email_clean'] = record['email'].lower().strip()

        # Standardize phone (digits only)
        if record.get('phone'):
            standardized['phone_clean'] = re.sub(
                r'[^0-9]', '', record['phone'])

        # Standardize address
        if record.get('address'):
            addr = record['address'].upper().strip()
            addr = re.sub(r'\bSTREET\b', 'ST', addr)
            addr = re.sub(r'\bAVENUE\b', 'AVE', addr)
            addr = re.sub(r'\bBOULEVARD\b', 'BLVD', addr)
            addr = re.sub(r'\bROAD\b', 'RD', addr)
            addr = re.sub(r'\bDRIVE\b', 'DR', addr)
            standardized['address_clean'] = addr

        # Standardize city/state
        if record.get('city'):
            standardized['city_clean'] = record['city'].upper().strip()
        if record.get('state'):
            standardized['state_clean'] = record['state'].upper().strip()

        return standardized

    def generate_embedding(self, record: Dict[str, Any]) -> List[float]:
        """Generate embedding for the record (placeholder for now)"""
        # For demo purposes, create a simple hash-based embedding
        # In production, this would call Vertex AI
        content = ' '.join([
            record.get('full_name_clean') or '',
            record.get('email_clean') or '',
            record.get('address_clean') or '',
            record.get('city_clean') or '',
            record.get('company') or ''
        ])

        # Simple hash-based embedding (768 dimensions)
        import hashlib
        hash_obj = hashlib.md5(content.encode())
        hash_hex = hash_obj.hexdigest()

        # Convert to 768-dimensional vector
        embedding = []
        for i in range(768):
            byte_val = int(hash_hex[i % len(hash_hex)], 16)
            embedding.append(float(byte_val) / 15.0)  # Normalize to 0-1

        return embedding

    def find_exact_matches(self, record: Dict[str, Any]) -> List[Tuple[str, float, str]]:
        """Find exact matches using indexed fields"""
        matches = []

        # Email exact match
        if record.get('email_clean'):
            query = """
            SELECT entity_id, 1.0 as score, 'email' as match_type
            FROM golden_entities
            WHERE master_email = @email
            """
            params = {'email': record['email_clean']}
            param_types_dict = {'email': param_types.STRING}

            results = self.spanner_helper.execute_sql(
                query, params, param_types_dict)
            for _, row in results.iterrows():
                matches.append((row['col_0'], row['col_1'], row['col_2']))

        # Phone exact match
        if record.get('phone_clean'):
            query = """
            SELECT entity_id, 1.0 as score, 'phone' as match_type
            FROM golden_entities
            WHERE master_phone = @phone
            """
            params = {'phone': record['phone_clean']}
            param_types_dict = {'phone': param_types.STRING}

            results = self.spanner_helper.execute_sql(
                query, params, param_types_dict)
            for _, row in results.iterrows():
                matches.append((row['col_0'], row['col_1'], row['col_2']))

        return matches

    def find_fuzzy_matches(self, record: Dict[str, Any]) -> List[Tuple[str, float, str]]:
        """Find fuzzy matches using string similarity"""
        matches = []

        if not record.get('full_name_clean'):
            return matches

        # Get candidates with similar name prefixes
        name_prefix = record['full_name_clean'][:3] if len(
            record['full_name_clean']) >= 3 else record['full_name_clean']

        query = """
        SELECT entity_id, master_name, master_address
        FROM golden_entities
        WHERE STARTS_WITH(master_name, @prefix)
        LIMIT 20
        """
        params = {'prefix': name_prefix}
        param_types_dict = {'prefix': param_types.STRING}

        results = self.spanner_helper.execute_sql(
            query, params, param_types_dict)

        for _, row in results.iterrows():
            entity_id = row['col_0']
            master_name = row['col_1'] or ''
            master_address = row['col_2'] or ''

            # Calculate name similarity
            name_score = self.calculate_string_similarity(
                record['full_name_clean'], master_name)

            # Calculate address similarity
            address_score = 0.0
            if record.get('address_clean') and master_address:
                address_score = self.calculate_string_similarity(
                    record['address_clean'], master_address)

            # Combined fuzzy score
            fuzzy_score = max(name_score, address_score)

            if fuzzy_score > 0.6:  # Threshold for fuzzy matches
                matches.append((entity_id, fuzzy_score, 'fuzzy'))

        return matches

    def calculate_string_similarity(self, str1: str, str2: str) -> float:
        """Calculate string similarity using edit distance"""
        if not str1 or not str2:
            return 0.0

        # Simple edit distance calculation
        len1, len2 = len(str1), len(str2)
        if len1 == 0:
            return 0.0 if len2 > 0 else 1.0
        if len2 == 0:
            return 0.0

        # Create matrix
        matrix = [[0] * (len2 + 1) for _ in range(len1 + 1)]

        # Initialize first row and column
        for i in range(len1 + 1):
            matrix[i][0] = i
        for j in range(len2 + 1):
            matrix[0][j] = j

        # Fill matrix
        for i in range(1, len1 + 1):
            for j in range(1, len2 + 1):
                if str1[i-1] == str2[j-1]:
                    matrix[i][j] = matrix[i-1][j-1]
                else:
                    matrix[i][j] = min(
                        matrix[i-1][j] + 1,      # deletion
                        matrix[i][j-1] + 1,      # insertion
                        matrix[i-1][j-1] + 1     # substitution
                    )

        # Convert to similarity score
        edit_distance = matrix[len1][len2]
        max_len = max(len1, len2)
        similarity = 1.0 - (edit_distance / max_len)

        return max(0.0, similarity)

    def find_vector_matches(self, record: Dict[str, Any], embedding: List[float]) -> List[Tuple[str, float, str]]:
        """Find vector similarity matches (simplified for demo)"""
        matches = []

        # For demo purposes, we'll use a simple approach
        # In production, this would use Spanner's vector search capabilities

        # Get a sample of entities to compare against
        query = """
        SELECT entity_id, embedding
        FROM golden_entities
        WHERE embedding IS NOT NULL
        LIMIT 50
        """

        results = self.spanner_helper.execute_sql(query)

        for _, row in results.iterrows():
            entity_id = row['col_0']
            stored_embedding = row['col_1']

            if stored_embedding:
                # Calculate cosine similarity
                similarity = self.calculate_cosine_similarity(
                    embedding, stored_embedding)

                if similarity > 0.8:  # Threshold for vector matches
                    matches.append((entity_id, similarity, 'vector'))

        return matches

    def calculate_cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        if len(vec1) != len(vec2):
            return 0.0

        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = sum(a * a for a in vec1) ** 0.5
        magnitude2 = sum(b * b for b in vec2) ** 0.5

        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0

        return dot_product / (magnitude1 * magnitude2)

    def apply_business_rules(self, record: Dict[str, Any]) -> List[Tuple[str, float, str]]:
        """Apply business rules matching"""
        matches = []

        # Same company rule
        if record.get('company'):
            query = """
            SELECT entity_id, 0.3 as score, 'company' as match_type
            FROM golden_entities
            WHERE master_company = @company
            """
            params = {'company': record['company']}
            param_types_dict = {'company': param_types.STRING}

            results = self.spanner_helper.execute_sql(
                query, params, param_types_dict)
            for _, row in results.iterrows():
                matches.append((row['col_0'], row['col_1'], row['col_2']))

        # Same location rule
        if record.get('city_clean') and record.get('state_clean'):
            query = """
            SELECT entity_id, 0.2 as score, 'location' as match_type
            FROM golden_entities
            WHERE master_city = @city AND master_state = @state
            """
            params = {
                'city': record['city_clean'],
                'state': record['state_clean']
            }
            param_types_dict = {
                'city': param_types.STRING,
                'state': param_types.STRING
            }

            results = self.spanner_helper.execute_sql(
                query, params, param_types_dict)
            for _, row in results.iterrows():
                matches.append((row['col_0'], row['col_1'], row['col_2']))

        return matches

    def combine_scores(self, exact_matches: List, fuzzy_matches: List,
                       vector_matches: List, business_matches: List) -> Dict[str, Any]:
        """Combine scores from all 4 strategies"""

        # Collect all unique entity IDs
        all_entities = set()
        for matches in [exact_matches, fuzzy_matches, vector_matches, business_matches]:
            for entity_id, _, _ in matches:
                all_entities.add(entity_id)

        if not all_entities:
            return {'best_match': None, 'combined_score': 0.0, 'strategy_scores': {}}

        # Calculate combined scores for each entity
        entity_scores = {}

        for entity_id in all_entities:
            scores = {
                'exact': 0.0,
                'fuzzy': 0.0,
                'vector': 0.0,
                'business': 0.0
            }

            # Get best score from each strategy
            for entity_id_match, score, _ in exact_matches:
                if entity_id_match == entity_id:
                    scores['exact'] = max(scores['exact'], score)

            for entity_id_match, score, _ in fuzzy_matches:
                if entity_id_match == entity_id:
                    scores['fuzzy'] = max(scores['fuzzy'], score)

            for entity_id_match, score, _ in vector_matches:
                if entity_id_match == entity_id:
                    scores['vector'] = max(scores['vector'], score)

            for entity_id_match, score, _ in business_matches:
                if entity_id_match == entity_id:
                    scores['business'] = max(scores['business'], score)

            # Calculate weighted combined score
            combined_score = (
                self.weights['exact'] * scores['exact'] +
                self.weights['fuzzy'] * scores['fuzzy'] +
                self.weights['vector'] * scores['vector'] +
                self.weights['business'] * scores['business']
            )

            entity_scores[entity_id] = {
                'combined_score': combined_score,
                'strategy_scores': scores
            }

        # Find best match
        best_entity = max(entity_scores.keys(),
                          key=lambda x: entity_scores[x]['combined_score'])
        best_score_info = entity_scores[best_entity]

        return {
            'best_match': best_entity,
            'combined_score': best_score_info['combined_score'],
            'strategy_scores': best_score_info['strategy_scores'],
            'all_candidates': entity_scores
        }

    def make_decision(self, combined_score: float) -> Dict[str, str]:
        """Make matching decision based on combined score"""
        if combined_score >= self.auto_merge_threshold:
            return {
                'action': 'AUTO_MERGE',
                'confidence': 'HIGH',
                'decision': 'auto_merge'
            }
        elif combined_score >= self.create_new_threshold:
            return {
                'action': 'AUTO_MERGE',  # For demo, treat medium as merge too
                'confidence': 'MEDIUM',
                'decision': 'auto_merge'
            }
        else:
            return {
                'action': 'CREATE_NEW',
                'confidence': 'LOW',
                'decision': 'create_new'
            }

    def process_record(self, record: Dict[str, Any], record_num: int, total_records: int) -> Dict[str, Any]:
        """Process a single streaming record with 4-way matching"""

        start_time = time.time()

        print(f"📨 Record {record_num}/{total_records}: {record.get('full_name', 'Unknown')} ({record.get('email', 'No email')}) - {record.get('source_system', 'Unknown')} Source")

        # Step 1: Standardize
        standardized = self.standardize_record(record)

        # Step 2: Generate embedding
        embedding = self.generate_embedding(standardized)

        # Step 3: Run 4-way matching
        exact_matches = self.find_exact_matches(standardized)
        print(f"  ⚡ Exact matching: {len(exact_matches)} matches found")

        fuzzy_matches = self.find_fuzzy_matches(standardized)
        print(f"  🔍 Fuzzy matching: {len(fuzzy_matches)} matches found")

        vector_matches = self.find_vector_matches(standardized, embedding)
        print(f"  🧮 Vector matching: {len(vector_matches)} matches found")

        business_matches = self.apply_business_rules(standardized)
        print(f"  📋 Business rules: {len(business_matches)} matches found")

        # Step 4: Combine scores
        match_result = self.combine_scores(
            exact_matches, fuzzy_matches, vector_matches, business_matches)

        # Step 5: Make decision
        decision = self.make_decision(match_result['combined_score'])

        print(
            f"  📊 Combined score: {match_result['combined_score']:.2f} ({decision['confidence']} confidence) → {decision['action']}")

        # Step 6: Execute action
        if decision['action'] == 'AUTO_MERGE' and match_result['best_match']:
            entity_id = self.update_golden_record(
                match_result['best_match'], standardized, embedding)
            action_detail = f"merged with existing {match_result['best_match']}"
        else:
            entity_id = self.create_new_golden_record(standardized, embedding)
            action_detail = "new record created"

        processing_time = (time.time() - start_time) * 1000

        print(
            f"  🗃️ → {decision['action']} Spanner (entity_id: {entity_id[:8]}..., {action_detail})")
        print(f"  ⏱️ Processing time: {processing_time:.0f}ms")

        return {
            'record_id': record.get('record_id', str(uuid.uuid4())),
            'entity_id': entity_id,
            'action': decision['action'],
            'confidence': decision['confidence'],
            'combined_score': match_result['combined_score'],
            'strategy_scores': match_result['strategy_scores'],
            'processing_time_ms': processing_time,
            'matched_entity': match_result['best_match']
        }

    def create_new_golden_record(self, record: Dict[str, Any], embedding: List[float]) -> str:
        """Create a new golden record"""
        entity_id = str(uuid.uuid4())

        def insert_record(transaction):
            transaction.execute_update(
                """
                INSERT INTO golden_entities (
                    entity_id, source_record_ids, source_record_count, source_systems,
                    master_name, master_email, master_phone, master_address,
                    master_city, master_state, master_company, master_income,
                    master_segment, embedding, confidence_score, processing_path,
                    created_at, updated_at
                ) VALUES (
                    @entity_id, @source_record_ids, @source_record_count, @source_systems,
                    @master_name, @master_email, @master_phone, @master_address,
                    @master_city, @master_state, @master_company, @master_income,
                    @master_segment, @embedding, @confidence_score, @processing_path,
                    PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()
                )
                """,
                params={
                    'entity_id': entity_id,
                    'source_record_ids': [record.get('record_id', entity_id)],
                    'source_record_count': 1,
                    'source_systems': [record.get('source_system', 'unknown')],
                    'master_name': record.get('full_name_clean'),
                    'master_email': record.get('email_clean'),
                    'master_phone': record.get('phone_clean'),
                    'master_address': record.get('address_clean'),
                    'master_city': record.get('city_clean'),
                    'master_state': record.get('state_clean'),
                    'master_company': record.get('company'),
                    'master_income': record.get('annual_income'),
                    'master_segment': record.get('customer_segment'),
                    'embedding': embedding,
                    'confidence_score': 0.8,  # New record confidence
                    'processing_path': 'stream'
                },
                param_types={
                    'entity_id': param_types.STRING,
                    'source_record_ids': param_types.Array(param_types.STRING),
                    'source_record_count': param_types.INT64,
                    'source_systems': param_types.Array(param_types.STRING),
                    'master_name': param_types.STRING,
                    'master_email': param_types.STRING,
                    'master_phone': param_types.STRING,
                    'master_address': param_types.STRING,
                    'master_city': param_types.STRING,
                    'master_state': param_types.STRING,
                    'master_company': param_types.STRING,
                    'master_income': param_types.INT64,
                    'master_segment': param_types.STRING,
                    'embedding': param_types.Array(param_types.FLOAT64),
                    'confidence_score': param_types.FLOAT64,
                    'processing_path': param_types.STRING
                }
            )

        self.spanner_helper.database.run_in_transaction(insert_record)
        return entity_id

    def update_golden_record(self, entity_id: str, record: Dict[str, Any], embedding: List[float]) -> str:
        """Update existing golden record with survivorship rules"""

        def update_record(transaction):
            # Get current record
            current_query = """
            SELECT master_name, master_email, master_phone, master_address,
                   source_record_ids, source_systems, source_record_count
            FROM golden_entities
            WHERE entity_id = @entity_id
            """

            current_result = transaction.execute_sql(
                current_query,
                params={'entity_id': entity_id},
                param_types={'entity_id': param_types.STRING}
            )

            current_row = list(current_result)[0]

            # Apply survivorship rules (most recent/complete wins)
            updated_name = record.get('full_name_clean') or current_row[0]
            updated_email = record.get('email_clean') or current_row[1]
            updated_phone = record.get('phone_clean') or current_row[2]
            updated_address = record.get('address_clean') or current_row[3]

            # Update source tracking
            current_source_ids = current_row[4] or []
            current_sources = current_row[5] or []
            current_count = current_row[6] or 0

            new_source_ids = current_source_ids + \
                [record.get('record_id', entity_id)]
            new_sources = list(
                set(current_sources + [record.get('source_system', 'unknown')]))
            new_count = current_count + 1

            # Update the record
            transaction.execute_update(
                """
                UPDATE golden_entities
                SET master_name = @master_name,
                    master_email = @master_email,
                    master_phone = @master_phone,
                    master_address = @master_address,
                    source_record_ids = @source_record_ids,
                    source_systems = @source_systems,
                    source_record_count = @source_record_count,
                    embedding = @embedding,
                    updated_at = PENDING_COMMIT_TIMESTAMP()
                WHERE entity_id = @entity_id
                """,
                params={
                    'entity_id': entity_id,
                    'master_name': updated_name,
                    'master_email': updated_email,
                    'master_phone': updated_phone,
                    'master_address': updated_address,
                    'source_record_ids': new_source_ids,
                    'source_systems': new_sources,
                    'source_record_count': new_count,
                    'embedding': embedding
                },
                param_types={
                    'entity_id': param_types.STRING,
                    'master_name': param_types.STRING,
                    'master_email': param_types.STRING,
                    'master_phone': param_types.STRING,
                    'master_address': param_types.STRING,
                    'source_record_ids': param_types.Array(param_types.STRING),
                    'source_systems': param_types.Array(param_types.STRING),
                    'source_record_count': param_types.INT64,
                    'embedding': param_types.Array(param_types.FLOAT64)
                }
            )

        self.spanner_helper.database.run_in_transaction(update_record)
        return entity_id
