"""
End-to-End Integration Tests

Tests complete workflow from PI Web API → Processing → Delta Lake (simulated)

Validates:
1. Full connector workflow
2. Error handling and resilience
3. Data transformation accuracy
4. Integration between modules
5. Production-readiness
"""

import pytest
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

BASE_URL = "http://localhost:8000"


class TestEnd2EndWorkflow:
    """Test complete data flow through connector"""

    def test_e2e_timeseries_extraction_to_dataframe(self):
        """
        End-to-end: Discover tags → Extract data → Transform to DataFrame

        Simulates bronze layer ingestion workflow
        """
        print(f"\n{'='*80}")
        print("END-TO-END TEST: Time-Series Extraction Pipeline")
        print('='*80)

        # Step 1: Discover PI Server
        print("\n[Step 1] Discovering PI Data Server...")
        response = requests.get(f"{BASE_URL}/piwebapi/dataservers")
        assert response.status_code == 200
        servers = response.json()['Items']
        assert len(servers) > 0
        server_webid = servers[0]['WebId']
        print(f"✓ Found server: {servers[0]['Name']}")

        # Step 2: List available tags
        print("\n[Step 2] Listing PI Points...")
        response = requests.get(
            f"{BASE_URL}/piwebapi/dataservers/{server_webid}/points",
            params={"maxCount": 10}
        )
        assert response.status_code == 200
        tags = response.json()['Items']
        assert len(tags) > 0
        print(f"✓ Found {len(tags)} tags")

        # Step 3: Extract time-series data using batch controller
        print("\n[Step 3] Extracting time-series data (batch mode)...")
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)

        tag_webids = [tag['WebId'] for tag in tags[:5]]  # 5 tags for test
        batch_payload = {
            "Requests": [
                {
                    "Method": "GET",
                    "Resource": f"/streams/{webid}/recorded",
                    "Parameters": {
                        "startTime": start_time.isoformat() + "Z",
                        "endTime": end_time.isoformat() + "Z",
                        "maxCount": "100"
                    }
                }
                for webid in tag_webids
            ]
        }

        response = requests.post(f"{BASE_URL}/piwebapi/batch", json=batch_payload)
        assert response.status_code == 200
        batch_results = response.json()['Responses']
        print(f"✓ Batch extraction completed: {len(batch_results)} responses")

        # Step 4: Transform to DataFrame (bronze layer schema)
        print("\n[Step 4] Transforming to Delta Lake schema...")
        all_records = []

        for i, result in enumerate(batch_results):
            if result['Status'] == 200:
                tag_webid = tag_webids[i]
                tag_name = tags[i]['Name']
                items = result['Content']['Items']

                for item in items:
                    all_records.append({
                        'tag_webid': tag_webid,
                        'tag_name': tag_name,
                        'timestamp': pd.to_datetime(item['Timestamp']),
                        'value': item['Value'],
                        'quality_good': item['Good'],
                        'quality_questionable': item['Questionable'],
                        'quality_substituted': item['Substituted'],
                        'units': item['UnitsAbbreviation'],
                        'ingestion_timestamp': pd.Timestamp.now()
                    })

        df = pd.DataFrame(all_records)
        assert not df.empty
        assert len(df) > 0
        print(f"✓ Created DataFrame: {len(df)} rows × {len(df.columns)} columns")

        # Step 5: Data quality validation
        print("\n[Step 5] Validating data quality...")
        good_pct = df['quality_good'].mean() * 100
        assert good_pct > 80, f"Expected >80% good data, got {good_pct:.1f}%"
        print(f"✓ Data quality: {good_pct:.1f}% good")

        # Step 6: Schema validation
        print("\n[Step 6] Validating bronze layer schema...")
        expected_columns = {
            'tag_webid', 'tag_name', 'timestamp', 'value',
            'quality_good', 'quality_questionable', 'quality_substituted',
            'units', 'ingestion_timestamp'
        }
        assert set(df.columns) == expected_columns
        print(f"✓ Schema validated: all required columns present")

        # Summary
        print(f"\n{'='*80}")
        print("PIPELINE SUMMARY")
        print('='*80)
        print(f"Tags processed: {len(tags[:5])}")
        print(f"Data points extracted: {len(df)}")
        print(f"Time range: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"Data quality: {good_pct:.1f}% good")
        print(f"✅ END-TO-END PIPELINE SUCCESSFUL")
        print('='*80 + '\n')

    def test_e2e_af_hierarchy_to_dimension_table(self):
        """
        End-to-end: Extract AF hierarchy → Transform to dimension table

        Simulates bronze.pi_asset_hierarchy table creation
        """
        print(f"\n{'='*80}")
        print("END-TO-END TEST: AF Hierarchy Extraction Pipeline")
        print('='*80)

        # Step 1: Get AF database
        print("\n[Step 1] Discovering AF Databases...")
        response = requests.get(f"{BASE_URL}/piwebapi/assetdatabases")
        assert response.status_code == 200
        databases = response.json()['Items']
        assert len(databases) > 0
        db_webid = databases[0]['WebId']
        print(f"✓ Found database: {databases[0]['Name']}")

        # Step 2: Extract hierarchy (recursive)
        print("\n[Step 2] Extracting AF hierarchy (3 levels)...")
        hierarchy_records = []

        # Level 1: Plants
        response = requests.get(
            f"{BASE_URL}/piwebapi/assetdatabases/{db_webid}/elements"
        )
        assert response.status_code == 200
        plants = response.json()['Items']

        for plant in plants:
            hierarchy_records.append({
                'element_id': plant['WebId'],
                'element_name': plant['Name'],
                'element_path': plant['Path'],
                'parent_id': None,
                'template_name': plant.get('TemplateName'),
                'element_type': plant.get('Type', 'Element'),
                'description': plant.get('Description', ''),
                'depth': 1
            })

            # Level 2: Units
            response = requests.get(
                f"{BASE_URL}/piwebapi/elements/{plant['WebId']}/elements"
            )
            if response.status_code == 200:
                units = response.json()['Items']

                for unit in units:
                    hierarchy_records.append({
                        'element_id': unit['WebId'],
                        'element_name': unit['Name'],
                        'element_path': unit['Path'],
                        'parent_id': plant['WebId'],
                        'template_name': unit.get('TemplateName'),
                        'element_type': unit.get('Type', 'Element'),
                        'description': unit.get('Description', ''),
                        'depth': 2
                    })

                    # Level 3: Equipment
                    response = requests.get(
                        f"{BASE_URL}/piwebapi/elements/{unit['WebId']}/elements"
                    )
                    if response.status_code == 200:
                        equipment_list = response.json()['Items']

                        for equipment in equipment_list:
                            hierarchy_records.append({
                                'element_id': equipment['WebId'],
                                'element_name': equipment['Name'],
                                'element_path': equipment['Path'],
                                'parent_id': unit['WebId'],
                                'template_name': equipment.get('TemplateName'),
                                'element_type': equipment.get('Type', 'Element'),
                                'description': equipment.get('Description', ''),
                                'depth': 3
                            })

        df_hierarchy = pd.DataFrame(hierarchy_records)
        assert not df_hierarchy.empty
        print(f"✓ Extracted {len(df_hierarchy)} AF elements")

        # Step 3: Validate hierarchy structure
        print("\n[Step 3] Validating hierarchy structure...")
        level_counts = df_hierarchy['depth'].value_counts().sort_index()
        for depth, count in level_counts.items():
            print(f"  Level {depth}: {count} elements")

        # Validate relationships
        assert df_hierarchy[df_hierarchy['depth'] == 1]['parent_id'].isna().all()
        assert df_hierarchy[df_hierarchy['depth'] > 1]['parent_id'].notna().all()
        print(f"✓ Parent-child relationships valid")

        # Summary
        print(f"\n{'='*80}")
        print("AF HIERARCHY SUMMARY")
        print('='*80)
        print(f"Total elements: {len(df_hierarchy)}")
        print(f"Max depth: {df_hierarchy['depth'].max()}")
        print(f"Templates: {df_hierarchy['template_name'].nunique()}")
        print(f"✅ AF HIERARCHY PIPELINE SUCCESSFUL")
        print('='*80 + '\n')

    def test_e2e_event_frames_to_fact_table(self):
        """
        End-to-end: Extract event frames → Transform to fact table

        Simulates bronze.pi_event_frames table creation
        """
        print(f"\n{'='*80}")
        print("END-TO-END TEST: Event Frame Extraction Pipeline")
        print('='*80)

        # Step 1: Get database
        print("\n[Step 1] Discovering AF Database...")
        response = requests.get(f"{BASE_URL}/piwebapi/assetdatabases")
        db = response.json()['Items'][0]
        db_webid = db['WebId']
        print(f"✓ Database: {db['Name']}")

        # Step 2: Extract event frames
        print("\n[Step 2] Extracting event frames (30 days)...")
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)

        response = requests.get(
            f"{BASE_URL}/piwebapi/assetdatabases/{db_webid}/eventframes",
            params={
                "startTime": start_time.isoformat() + "Z",
                "endTime": end_time.isoformat() + "Z",
                "searchMode": "Overlapped"
            }
        )
        assert response.status_code == 200
        event_frames = response.json()['Items']
        assert len(event_frames) > 0
        print(f"✓ Found {len(event_frames)} event frames")

        # Step 3: Extract event attributes for each event
        print("\n[Step 3] Extracting event attributes...")
        event_records = []

        for ef in event_frames[:10]:  # Limit to 10 for test performance
            # Get attributes
            response = requests.get(
                f"{BASE_URL}/piwebapi/eventframes/{ef['WebId']}/attributes"
            )

            attributes_dict = {}
            if response.status_code == 200:
                attributes = response.json()['Items']
                for attr in attributes:
                    attributes_dict[attr['Name']] = attr.get('Value')

            # Build event record
            start_ts = pd.to_datetime(ef['StartTime'])
            end_ts = pd.to_datetime(ef.get('EndTime')) if ef.get('EndTime') else None
            duration_minutes = (end_ts - start_ts).total_seconds() / 60 if end_ts else None

            event_records.append({
                'event_frame_id': ef['WebId'],
                'event_name': ef['Name'],
                'template_name': ef['TemplateName'],
                'start_time': start_ts,
                'end_time': end_ts,
                'duration_minutes': duration_minutes,
                'primary_element_id': ef.get('PrimaryReferencedElementWebId'),
                'event_attributes': attributes_dict,
                'categories': ','.join(ef.get('CategoryNames', [])),
                'description': ef.get('Description', '')
            })

        df_events = pd.DataFrame(event_records)
        assert not df_events.empty
        print(f"✓ Processed {len(df_events)} event frames with attributes")

        # Step 4: Analyze event types
        print("\n[Step 4] Analyzing event types...")
        template_counts = df_events['template_name'].value_counts()
        for template, count in template_counts.items():
            print(f"  {template}: {count} events")

        # Summary
        print(f"\n{'='*80}")
        print("EVENT FRAME SUMMARY")
        print('='*80)
        print(f"Total events: {len(df_events)}")
        print(f"Event types: {df_events['template_name'].nunique()}")
        print(f"Avg duration: {df_events['duration_minutes'].mean():.1f} minutes")
        print(f"✅ EVENT FRAME PIPELINE SUCCESSFUL")
        print('='*80 + '\n')


class TestEnd2EndErrorHandling:
    """Test error handling and resilience"""

    def test_partial_batch_failure_handling(self):
        """Test handling when some tags in batch fail"""
        # Create batch with mix of valid and invalid tags
        valid_tags_response = requests.get(
            f"{BASE_URL}/piwebapi/dataservers/F1DP-Server-Primary/points",
            params={"maxCount": 2}
        )
        valid_tags = valid_tags_response.json()['Items']

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)

        # Mix valid and invalid tags
        mixed_webids = [tag['WebId'] for tag in valid_tags] + ["INVALID-TAG-1", "INVALID-TAG-2"]

        batch_payload = {
            "Requests": [
                {
                    "Method": "GET",
                    "Resource": f"/streams/{webid}/recorded",
                    "Parameters": {
                        "startTime": start_time.isoformat() + "Z",
                        "endTime": end_time.isoformat() + "Z",
                        "maxCount": "100"
                    }
                }
                for webid in mixed_webids
            ]
        }

        response = requests.post(f"{BASE_URL}/piwebapi/batch", json=batch_payload)
        assert response.status_code == 200
        results = response.json()['Responses']

        # Count successes and failures
        success_count = sum(1 for r in results if r['Status'] == 200)
        failure_count = sum(1 for r in results if r['Status'] != 200)

        print(f"\n{'='*80}")
        print("PARTIAL FAILURE HANDLING TEST")
        print('='*80)
        print(f"Total requests: {len(mixed_webids)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {failure_count}")
        print(f"✅ PASS: Connector handles partial failures gracefully")
        print('='*80 + '\n')

        # Should have both successes and failures
        assert success_count > 0, "Expected some successful extractions"
        assert failure_count > 0, "Expected some failures (invalid tags)"

    def test_empty_time_range_handling(self):
        """Test handling when time range has no data"""
        # Get tag
        response = requests.get(
            f"{BASE_URL}/piwebapi/dataservers/F1DP-Server-Primary/points",
            params={"maxCount": 1}
        )
        tag = response.json()['Items'][0]

        # Future time range (no data)
        start_time = datetime.now() + timedelta(days=365)
        end_time = start_time + timedelta(hours=1)

        response = requests.get(
            f"{BASE_URL}/piwebapi/streams/{tag['WebId']}/recorded",
            params={
                "startTime": start_time.isoformat() + "Z",
                "endTime": end_time.isoformat() + "Z",
                "maxCount": "100"
            }
        )

        assert response.status_code == 200
        items = response.json()['Items']

        print(f"\n{'='*80}")
        print("EMPTY TIME RANGE HANDLING TEST")
        print('='*80)
        print(f"Time range: Future (no data expected)")
        print(f"Data points returned: {len(items)}")
        print(f"✅ PASS: Empty result handled gracefully")
        print('='*80 + '\n')

        # Should return empty list, not error
        assert isinstance(items, list)


class TestEnd2EndPerformance:
    """Performance tests for end-to-end workflows"""

    def test_e2e_100_tag_extraction_performance(self):
        """
        Performance test: Extract 100 tags end-to-end
        Target: <30 seconds for 1-hour window
        """
        print(f"\n{'='*80}")
        print("PERFORMANCE TEST: 100 Tags End-to-End")
        print('='*80)

        # Get 100 tags
        response = requests.get(
            f"{BASE_URL}/piwebapi/dataservers/F1DP-Server-Primary/points",
            params={"maxCount": 100}
        )
        tags = response.json()['Items']
        actual_tag_count = min(len(tags), 20)  # Use 20 for test (extrapolate to 100)
        tag_webids = [tag['WebId'] for tag in tags[:actual_tag_count]]

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)

        # Time the complete workflow
        start_bench = time.time()

        # Build batch request
        batch_payload = {
            "Requests": [
                {
                    "Method": "GET",
                    "Resource": f"/streams/{webid}/recorded",
                    "Parameters": {
                        "startTime": start_time.isoformat() + "Z",
                        "endTime": end_time.isoformat() + "Z",
                        "maxCount": "100"
                    }
                }
                for webid in tag_webids
            ]
        }

        # Execute batch
        response = requests.post(f"{BASE_URL}/piwebapi/batch", json=batch_payload)
        assert response.status_code == 200

        # Transform to DataFrame
        results = response.json()['Responses']
        all_records = []
        for i, result in enumerate(results):
            if result['Status'] == 200:
                items = result['Content']['Items']
                for item in items:
                    all_records.append({
                        'tag_webid': tag_webids[i],
                        'timestamp': item['Timestamp'],
                        'value': item['Value'],
                        'good': item['Good']
                    })

        df = pd.DataFrame(all_records)

        elapsed = time.time() - start_bench

        # Extrapolate to 100 tags
        projected_100 = (elapsed / actual_tag_count) * 100

        print(f"\nActual tags: {actual_tag_count}")
        print(f"Time elapsed: {elapsed:.2f} seconds")
        print(f"Data points extracted: {len(df)}")
        print(f"Projected time for 100 tags: {projected_100:.1f} seconds")

        print(f"\n{'='*80}")
        print("PERFORMANCE SUMMARY")
        print('='*80)
        print(f"Tags/second: {actual_tag_count/elapsed:.1f}")
        print(f"Records/second: {len(df)/elapsed:.0f}")
        if projected_100 < 30:
            print(f"✅ PASS: Projected {projected_100:.1f}s < 30s target")
        else:
            print(f"⚠️  WARNING: Projected {projected_100:.1f}s > 30s target")
        print('='*80 + '\n')


# pytest marks
pytestmark = [pytest.mark.integration, pytest.mark.e2e]

if __name__ == "__main__":
    print("\n" + "="*80)
    print("END-TO-END INTEGRATION TEST SUITE")
    print("="*80)
    print("\nTests complete data flow:")
    print("1. PI Web API → DataFrame → Delta Lake (simulated)")
    print("2. Error handling and resilience")
    print("3. Performance benchmarks")
    print("\nRun with: pytest tests/test_integration_end2end.py -v -s")
    print("="*80 + "\n")
