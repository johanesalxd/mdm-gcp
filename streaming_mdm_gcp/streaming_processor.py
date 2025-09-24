"""
Streaming MDM Processor
Implements 4-way matching for real-time entity resolution
"""

import hashlib
import re
import time
from typing import Any, Dict, List, Optional, Tuple
import uuid

from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
import pandas as pd


class StreamingMDMProcessor:
    """4-way streaming MDM processor with real-time matching"""

    def __init__(self, spanner_helper):
        self.spanner_helper = spanner_helper

        # Matching weights (4-strategy for all records - aligned with real-world patterns)
        # Vector matching searches existing embeddings, doesn't generate new ones
        self.weights = {
            'exact': 0.33,      # 33% - exact matches (email, phone, ID)
            'fuzzy': 0.28,      # 28% - fuzzy string similarity
            # 22% - semantic similarity (existing embeddings)
            'vector': 0.22,
            'business': 0.17    # 17% - business rules and domain logic
        }

        # Confidence thresholds (aligned with batch)
        self.auto_merge_threshold = 0.8     # >= 0.8 for auto-merge
        self.human_review_threshold = 0.6   # >= 0.6 for human review
        self.create_new_threshold = 0.6     # < 0.6 for create new

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
        """
        Find vector similarity matches using existing embeddings only

        🚧 CURRENT LIMITATION: Vector matching is architecturally supported but operationally
        limited due to lack of real-time embedding generation for new streaming records.

        📋 ROADMAP: Full 4-way matching will be enabled when Vertex AI integration is added
        for on-the-fly embedding generation (+200-500ms latency cost per record).

        Current behavior: Always returns empty for new streaming records (no embeddings).
        """
        # Skip vector matching if no embedding provided (streaming records don't generate embeddings)
        if not embedding:
            print(
                f"  🧮 Vector matching: Deferred (real-time embedding generation pending)")
            print(f"  📋 Note: Full 4-way matching requires Vertex AI integration")
            return []

        try:
            # Use existing spanner function for native COSINE_DISTANCE search
            similar_entities = self.spanner_helper.query_similar_embeddings(
                query_embedding=embedding,
                limit=10
            )

            matches = []
            for _, row in similar_entities.iterrows():
                entity_id = row['entity_id']
                similarity = row['similarity']

                # Threshold for vector matches (lowered for better recall)
                if similarity > 0.7:
                    matches.append((entity_id, similarity, 'vector'))

            return matches

        except Exception as e:
            # Fail gracefully
            print(f"  ⚠️ Vector search failed: {e}")
            return []

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

            # Calculate weighted combined score using 4-way weights
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
        """Make matching decision based on combined score (aligned with batch)"""
        if combined_score >= self.auto_merge_threshold:  # >= 0.8
            return {
                'action': 'AUTO_MERGE',
                'confidence': 'HIGH',
                'decision': 'auto_merge'
            }
        elif combined_score >= self.human_review_threshold:  # >= 0.6
            return {
                'action': 'HUMAN_REVIEW',  # Changed from AUTO_MERGE
                'confidence': 'MEDIUM',
                'decision': 'human_review'  # Changed to match batch
            }
        else:  # < 0.6
            return {
                'action': 'CREATE_NEW',
                'confidence': 'LOW',
                'decision': 'no_match'  # Changed to match batch terminology
            }

    def generate_deterministic_entity_id(self, record: Dict[str, Any],
                                         matched_entity_id: Optional[str] = None) -> str:
        """
        Generate deterministic entity ID for consistency between batch and streaming.

        Priority:
        1. If matching existing entity, use that ID
        2. If has email, use hash of cleaned email
        3. If has phone, use hash of cleaned phone
        4. Otherwise, use UUID (for truly new records without identifiers)
        """
        # If matching existing entity, keep the same ID
        if matched_entity_id:
            return matched_entity_id

        # Try to create deterministic ID from best identifier
        if record.get('email_clean'):
            # Use email as primary identifier
            hash_input = f"email:{record['email_clean']}"
            return hashlib.sha256(hash_input.encode()).hexdigest()[:36]

        elif record.get('phone_clean'):
            # Use phone as secondary identifier
            hash_input = f"phone:{record['phone_clean']}"
            return hashlib.sha256(hash_input.encode()).hexdigest()[:36]

        else:
            # No good identifier, generate UUID
            return str(uuid.uuid4())

    def process_record(self, record: Dict[str, Any], record_num: int, total_records: int, include_match_details: bool = False) -> Dict[str, Any]:
        """
        Process a single streaming record with traditional 4-way matching.

        Args:
            record: The input record to process
            record_num: Current record number for display
            total_records: Total number of records for display
            include_match_details: If True, include detailed match counts in result

        Returns:
            Dictionary with processing results including match details if requested
        """
        start_time = time.time()

        print(f"📨 Record {record_num}/{total_records}: {record.get('full_name', 'Unknown')} ({record.get('email', 'No email')}) - {record.get('source_system', 'Unknown')} Source")

        try:
            # Step 1: Standardize record
            standardized = self.standardize_record(record)

            # Step 2: Run ALL 4 strategies for every record (no gatekeeper)
            exact_matches = self.find_exact_matches(standardized)
            fuzzy_matches = self.find_fuzzy_matches(standardized)
            vector_matches = self.find_vector_matches(
                standardized, [])  # Use existing embeddings only
            business_matches = self.apply_business_rules(standardized)

            print(f"  ⚡ Exact matching: {len(exact_matches)} matches found")
            print(f"  🔍 Fuzzy matching: {len(fuzzy_matches)} matches found")
            print(f"  🧮 Vector matching: {len(vector_matches)} matches found")
            print(f"  📋 Business rules: {len(business_matches)} matches found")

            # Step 3: Combine scores from all 4 strategies
            match_result = self.combine_scores(
                exact_matches, fuzzy_matches, vector_matches, business_matches)

            # Step 4: Make decision
            decision = self.make_decision(match_result['combined_score'])

            print(
                f"  📊 Combined score: {match_result['combined_score']:.2f} ({decision['confidence']} confidence) → {decision['action']}")

            # Step 5: Execute action
            embedding = []  # No embedding generated for streaming records

            if decision['action'] in ['AUTO_MERGE', 'HUMAN_REVIEW'] and match_result['best_match']:
                # Merge with existing entity
                entity_id = self.update_golden_record(
                    match_result['best_match'], standardized, embedding)
                action_detail = f"merged with existing {match_result['best_match'][:8]}..."
            else:
                # Create new entity
                entity_id = self.create_new_golden_record(
                    standardized, embedding)
                action_detail = "new record created"

            processing_time = (time.time() - start_time) * 1000

            print(
                f"  🗃️ → {decision['action']} Spanner (entity_id: {entity_id[:8]}..., {action_detail})")
            print(f"  ⏱️ Processing time: {processing_time:.0f}ms")

            # Build result
            result = {
                'record_id': record.get('record_id', str(uuid.uuid4())),
                'record_num': record_num,
                'entity_id': entity_id,
                'action': decision['action'],
                'confidence': decision['confidence'],
                'combined_score': match_result['combined_score'],
                'strategy_scores': match_result['strategy_scores'],
                'processing_time_ms': processing_time,
                'matched_entity': match_result['best_match']
            }

            # Add match details if requested
            if include_match_details:
                result.update({
                    'exact_count': len(exact_matches),
                    'fuzzy_count': len(fuzzy_matches),
                    'vector_count': len(vector_matches),
                    'business_count': len(business_matches),
                    'standardized_record': standardized
                })

            return result

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            print(f"  ❌ Error processing record {record_num}: {e}")

            # Return error result
            result = {
                'record_id': record.get('record_id', 'unknown'),
                'record_num': record_num,
                'action': 'ERROR',
                'confidence': 'LOW',
                'combined_score': 0.0,
                'processing_time_ms': processing_time,
                'entity_id': None,
                'strategy_scores': {},
                'matched_entity': None,
                'gatekeeper_skip': False
            }

            # Add match details if requested (with zeros)
            if include_match_details:
                result.update({
                    'exact_count': 0,
                    'fuzzy_count': 0,
                    'vector_count': 0,
                    'business_count': 0,
                    'standardized_record': {}
                })

            return result

    def create_new_golden_record(self, record: Dict[str, Any], embedding: List[float]) -> str:
        """Create a new golden record with deterministic ID (UPSERT logic)"""
        entity_id = self.generate_deterministic_entity_id(record)

        def upsert_record(transaction):
            # Check if entity already exists
            check_query = """
            SELECT entity_id, source_record_ids, source_systems, source_record_count
            FROM golden_entities
            WHERE entity_id = @entity_id
            """

            existing_result = transaction.execute_sql(
                check_query,
                params={'entity_id': entity_id},
                param_types={'entity_id': param_types.STRING}
            )

            existing_rows = list(existing_result)

            if existing_rows:
                # Entity exists - update it (merge logic)
                current_row = existing_rows[0]
                current_source_ids = current_row[1] or []
                current_sources = current_row[2] or []
                current_count = current_row[3] or 0

                # Add new source information
                new_source_ids = current_source_ids + \
                    [record.get('record_id', entity_id)]
                new_sources = list(
                    set(current_sources + [record.get('source_system', 'unknown')]))
                new_count = current_count + 1

                # Update existing record
                transaction.execute_update(
                    """
                    UPDATE golden_entities
                    SET source_record_ids = @source_record_ids,
                        source_systems = @source_systems,
                        source_record_count = @source_record_count,
                        master_name = COALESCE(@master_name, master_name),
                        master_email = COALESCE(@master_email, master_email),
                        master_phone = COALESCE(@master_phone, master_phone),
                        master_address = COALESCE(@master_address, master_address),
                        master_city = COALESCE(@master_city, master_city),
                        master_state = COALESCE(@master_state, master_state),
                        master_company = COALESCE(@master_company, master_company),
                        embedding = @embedding,
                        processing_path = 'stream_updated',
                        updated_at = PENDING_COMMIT_TIMESTAMP()
                    WHERE entity_id = @entity_id
                    """,
                    params={
                        'entity_id': entity_id,
                        'source_record_ids': new_source_ids,
                        'source_systems': new_sources,
                        'source_record_count': new_count,
                        'master_name': record.get('full_name_clean'),
                        'master_email': record.get('email_clean'),
                        'master_phone': record.get('phone_clean'),
                        'master_address': record.get('address_clean'),
                        'master_city': record.get('city_clean'),
                        'master_state': record.get('state_clean'),
                        'master_company': record.get('company'),
                        'embedding': embedding
                    },
                    param_types={
                        'entity_id': param_types.STRING,
                        'source_record_ids': param_types.Array(param_types.STRING),
                        'source_systems': param_types.Array(param_types.STRING),
                        'source_record_count': param_types.INT64,
                        'master_name': param_types.STRING,
                        'master_email': param_types.STRING,
                        'master_phone': param_types.STRING,
                        'master_address': param_types.STRING,
                        'master_city': param_types.STRING,
                        'master_state': param_types.STRING,
                        'master_company': param_types.STRING,
                        'embedding': param_types.Array(param_types.FLOAT64)
                    }
                )
            else:
                # Entity doesn't exist - insert new record
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

        self.spanner_helper.database.run_in_transaction(upsert_record)

        # Stage new entity for future batch processing (golden master sync)
        try:
            golden_record_data = {
                'entity_id': entity_id,
                'master_name': record.get('full_name_clean'),
                'master_email': record.get('email_clean'),
                'master_phone': record.get('phone_clean'),
                'master_address': record.get('address_clean'),
                'master_city': record.get('city_clean'),
                'master_state': record.get('state_clean'),
                'master_company': record.get('company'),
                'source_system': record.get('source_system', 'streaming'),
                'processing_path': 'stream'
            }
            self.spanner_helper.stage_new_entity(
                entity_id, golden_record_data, record.get('source_system', 'streaming'))
        except Exception as e:
            print(f"  ⚠️ Failed to stage entity for batch processing: {e}")

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

    def store_match_result(self, record: Dict[str, Any], result: Dict[str, Any]) -> str:
        """Store match result in Spanner for analysis"""
        match_id = str(uuid.uuid4())

        def insert_match(transaction):
            transaction.execute_update(
                """
                INSERT INTO match_results (
                    match_id, record1_id, record2_id,
                    source1, source2,
                    exact_score, fuzzy_score, vector_score, business_score,
                    combined_score, confidence_level, match_decision,
                    matched_at, processing_time_ms
                ) VALUES (
                    @match_id, @record1_id, @record2_id,
                    @source1, @source2,
                    @exact_score, @fuzzy_score, @vector_score, @business_score,
                    @combined_score, @confidence_level, @match_decision,
                    PENDING_COMMIT_TIMESTAMP(), @processing_time_ms
                )
                """,
                params={
                    'match_id': match_id,
                    'record1_id': result.get('record_id'),
                    'record2_id': result.get('matched_entity') or 'none',
                    'source1': record.get('source_system', 'unknown'),
                    'source2': 'golden_entity' if result.get('matched_entity') else 'none',
                    'exact_score': float(result.get('strategy_scores', {}).get('exact', 0.0)),
                    'fuzzy_score': float(result.get('strategy_scores', {}).get('fuzzy', 0.0)),
                    'vector_score': float(result.get('strategy_scores', {}).get('vector', 0.0)),
                    'business_score': float(result.get('strategy_scores', {}).get('business', 0.0)),
                    'combined_score': float(result.get('combined_score', 0.0)),
                    'confidence_level': result.get('confidence', 'LOW'),
                    'match_decision': result.get('action', 'ERROR'),
                    'processing_time_ms': int(result.get('processing_time_ms', 0))
                },
                param_types={
                    'match_id': param_types.STRING,
                    'record1_id': param_types.STRING,
                    'record2_id': param_types.STRING,
                    'source1': param_types.STRING,
                    'source2': param_types.STRING,
                    'exact_score': param_types.FLOAT64,
                    'fuzzy_score': param_types.FLOAT64,
                    'vector_score': param_types.FLOAT64,
                    'business_score': param_types.FLOAT64,
                    'combined_score': param_types.FLOAT64,
                    'confidence_level': param_types.STRING,
                    'match_decision': param_types.STRING,
                    'processing_time_ms': param_types.INT64
                }
            )

        self.spanner_helper.database.run_in_transaction(insert_match)
        return match_id

    @staticmethod
    def add_realistic_variations(existing_df):
        """Add realistic variations to existing records to simulate data drift."""
        import random
        import re

        varied_records = []

        for _, row in existing_df.iterrows():
            # Create variations of the existing record
            base_record = {
                # 32 chars max
                'record_id': str(uuid.uuid4()).replace('-', '')[:32],
                'full_name': row['full_name'],
                'email': row['email'],
                'phone': row['phone'],
                'address': row.get('address', ''),
                'city': row.get('city', ''),
                'state': row.get('state', ''),
                'company': row.get('company', ''),
                'source_system': 'streaming_variation'
            }

            # Apply realistic variations (simulate data drift)
            variation_type = random.choice(
                ['email', 'phone', 'name', 'address'])

            if variation_type == 'email' and base_record['email']:
                # Change email domain: john@gmail.com -> john@outlook.com
                email_parts = base_record['email'].split('@')
                if len(email_parts) == 2:
                    new_domains = ['outlook.com', 'yahoo.com',
                                   'hotmail.com', 'icloud.com']
                    base_record['email'] = f"{email_parts[0]}@{random.choice(new_domains)}"

            elif variation_type == 'phone' and base_record['phone']:
                # Change phone formatting: 555-1234 -> (555) 123-4567
                phone = re.sub(r'[^0-9]', '', base_record['phone'])
                if len(phone) >= 10:
                    base_record['phone'] = f"({phone[:3]}) {phone[3:6]}-{phone[6:10]}"

            elif variation_type == 'name' and base_record['full_name']:
                # Name variations: John Smith -> J. Smith or John A. Smith
                name_parts = base_record['full_name'].split()
                if len(name_parts) >= 2:
                    if random.choice([True, False]):
                        # Abbreviate first name: John -> J.
                        base_record['full_name'] = f"{name_parts[0][0]}. {' '.join(name_parts[1:])}"
                    else:
                        # Add middle initial: John Smith -> John A. Smith
                        middle_initial = random.choice(
                            'ABCDEFGHIJKLMNOPQRSTUVWXYZ')
                        base_record[
                            'full_name'] = f"{name_parts[0]} {middle_initial}. {' '.join(name_parts[1:])}"

            elif variation_type == 'address' and base_record['address']:
                # Address variations: 123 Main St -> 123 Main Street, Apt 2
                address = base_record['address']
                if 'St' in address and 'Street' not in address:
                    address = address.replace(' St', ' Street')
                if random.choice([True, False]):
                    apt_num = random.randint(1, 20)
                    address += f", Apt {apt_num}"
                base_record['address'] = address

            varied_records.append(base_record)

        return varied_records
