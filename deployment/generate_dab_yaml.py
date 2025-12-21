#!/usr/bin/env python3
"""
PI Web API DAB YAML Generator

Generates Databricks Asset Bundle (DAB) YAML configuration from CSV input.
Creates multiple DLT pipelines and scheduled jobs for load-balanced PI data ingestion.

Usage:
    python generate_dab_yaml.py examples/pi_tags_config.csv --project my_pi_project

CSV Format:
    tag_name,tag_webid,pi_server_url,connection_name,target_catalog,target_schema,pipeline_group,schedule,start_time_offset_days
    Plant1_Temp,F1DP-TAG-001,https://pi.company.com/piwebapi,pi_conn,main,bronze,1,0 */15 * * * ?,30

Output:
    - resources/pipelines.yml: DLT pipeline configurations
    - resources/jobs.yml: Scheduled job configurations
"""

import yaml
import pandas as pd
import os
import argparse
from typing import Dict, List
from collections import defaultdict


def create_pipelines(df: pd.DataFrame, project_name: str) -> Dict:
    """
    Create DLT pipeline YAML configuration from dataframe.

    Each pipeline_group becomes a separate DLT pipeline with its assigned tags.
    """
    pipelines = {}

    # Group tags by pipeline_group
    for pipeline_group, group_df in df.groupby('pipeline_group'):
        pipeline_name = f"{project_name}_pipeline_pi_ingestion_{pipeline_group}"

        # Get common configuration from first row
        first_row = group_df.iloc[0]
        catalog = first_row['target_catalog']
        schema = first_row['target_schema']

        # Build tag list for this pipeline
        tags = group_df['tag_webid'].tolist()

        # Build libraries reference for connector code
        libraries = [
            {'notebook': {'path': f'../src/notebooks/pi_ingestion_pipeline.py'}}
        ]

        pipelines[pipeline_name] = {
            'name': f"{project_name}_pi_ingestion_group_{pipeline_group}",
            'catalog': catalog,
            'target': schema,
            'clusters': [{
                'label': 'default',
                'num_workers': 2
            }],
            'libraries': libraries,
            'configuration': {
                'pi.tags': ','.join(tags),
                'pi.server.url': first_row['pi_server_url'],
                'pi.connection.name': first_row['connection_name'],
                'pi.target.catalog': catalog,
                'pi.target.schema': schema,
                'pi.start_time_offset_days': str(first_row['start_time_offset_days'])
            }
        }

    return {'resources': {'pipelines': pipelines}}


def create_jobs(df: pd.DataFrame, project_name: str) -> Dict:
    """
    Create scheduled job YAML configuration from dataframe.

    Each pipeline gets a scheduled job to run at specified intervals.
    """
    jobs = {}

    # Group by pipeline and schedule
    for (pipeline_group, schedule), group_df in df.groupby(['pipeline_group', 'schedule']):
        job_name = f"{project_name}_job_pi_scheduler_{pipeline_group}"
        pipeline_ref_name = f"{project_name}_pipeline_pi_ingestion_{pipeline_group}"

        jobs[job_name] = {
            'name': f"{project_name}_pi_scheduler_group_{pipeline_group}",
            'schedule': {
                'quartz_cron_expression': schedule,
                'timezone_id': 'UTC'
            },
            'tasks': [{
                'task_key': f'run_pi_pipeline_group_{pipeline_group}',
                'pipeline_task': {
                    'pipeline_id': f'${{resources.pipelines.{pipeline_ref_name}.id}}'
                }
            }]
        }

    return {'resources': {'jobs': jobs}}


def generate_yaml_files(
    df: pd.DataFrame,
    project_name: str,
    output_dir: str = 'resources'
):
    """
    Generate DAB YAML files from input dataframe.

    Args:
        df: Input dataframe with required columns:
            - tag_name: Display name for the tag
            - tag_webid: PI Web API WebID for the tag
            - pi_server_url: PI Web API base URL
            - connection_name: Databricks connection name (for secrets)
            - target_catalog: Target Unity Catalog catalog
            - target_schema: Target schema for Delta tables
            - pipeline_group: Pipeline group number (1, 2, 3, etc.)
            - schedule: Quartz cron expression for scheduling
            - start_time_offset_days: Days to lookback for initial load
        project_name: Project name prefix for all resources
        output_dir: Output directory for YAML files
    """
    # Validate required columns
    required_cols = [
        'tag_name', 'tag_webid', 'pi_server_url', 'connection_name',
        'target_catalog', 'target_schema', 'pipeline_group', 'schedule',
        'start_time_offset_days'
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Generate YAML configurations
    pipelines_yaml = create_pipelines(df, project_name)
    jobs_yaml = create_jobs(df, project_name)

    # Write YAML files
    pipeline_file = os.path.join(output_dir, 'pipelines.yml')
    jobs_file = os.path.join(output_dir, 'jobs.yml')

    with open(pipeline_file, 'w') as f:
        yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)

    with open(jobs_file, 'w') as f:
        yaml.dump(jobs_yaml, f, default_flow_style=False, sort_keys=False)

    # Print summary
    num_pipelines = len(pipelines_yaml['resources']['pipelines'])
    num_jobs = len(jobs_yaml['resources']['jobs'])
    total_tags = len(df)

    print("=" * 70)
    print("DAB YAML Generation Complete")
    print("=" * 70)
    print(f"\n✓ Generated {num_pipelines} DLT pipelines")
    print(f"✓ Generated {num_jobs} scheduled jobs")
    print(f"✓ Total tags configured: {total_tags}")
    print(f"\nOutput files:")
    print(f"  - {pipeline_file}")
    print(f"  - {jobs_file}")
    print(f"\nPipeline distribution:")

    for pipeline_group, group_df in df.groupby('pipeline_group'):
        tag_count = len(group_df)
        schedule = group_df.iloc[0]['schedule']
        print(f"  Pipeline {pipeline_group}: {tag_count} tags (schedule: {schedule})")

    print(f"\nNext steps:")
    print(f"  1. Review generated YAML files in {output_dir}/")
    print(f"  2. Update databricks.yml to include these resources")
    print(f"  3. Deploy: databricks bundle deploy -t dev")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Generate DAB YAML for PI Web API ingestion pipelines',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate from CSV
  python generate_dab_yaml.py examples/pi_tags_config.csv --project my_pi_project

  # Specify custom output directory
  python generate_dab_yaml.py examples/pi_tags_config.csv --project pi_prod --output ../dab_config

CSV Format:
  tag_name,tag_webid,pi_server_url,connection_name,target_catalog,target_schema,pipeline_group,schedule,start_time_offset_days
  Plant1_Temp,F1DP-TAG-001,https://pi.company.com/piwebapi,pi_conn,main,bronze,1,0 */15 * * * ?,30
        """
    )

    parser.add_argument(
        'csv_file',
        help='Path to CSV configuration file'
    )
    parser.add_argument(
        '--project', '-p',
        required=True,
        help='Project name prefix for all resources'
    )
    parser.add_argument(
        '--output', '-o',
        default='resources',
        help='Output directory for YAML files (default: resources)'
    )

    args = parser.parse_args()

    # Validate CSV file exists
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file not found: {args.csv_file}")
        return 1

    # Load CSV
    print(f"Reading configuration from: {args.csv_file}")
    df = pd.read_csv(args.csv_file)
    print(f"Loaded {len(df)} tags from CSV\n")

    # Generate YAML files
    try:
        generate_yaml_files(
            df=df,
            project_name=args.project,
            output_dir=args.output
        )
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
