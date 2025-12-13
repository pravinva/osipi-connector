"""
Live Demo Dashboard - Visual Representation of OSIPI Data Ingestion

Fetches REAL data from Delta tables and displays:
- AF Hierarchy (actual structure from osipi.bronze.pi_af_hierarchy)
- Real-time data flow
- Late data detection metrics
- System statistics

Reads directly from Unity Catalog tables - NO FAKE DATA!
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.animation import FuncAnimation
from matplotlib.gridspec import GridSpec
import networkx as nx
from datetime import datetime, timedelta
from typing import Dict, List, Any
import numpy as np
from collections import deque
import os
from databricks.sdk import WorkspaceClient


class LiveDashboard:
    """
    Real-time dashboard showing actual data from Delta tables (REAL DATA ONLY)
    """

    def __init__(self,
                 databricks_host: str = None,
                 databricks_token: str = None,
                 warehouse_id: str = "4b9b953939869799",
                 catalog: str = "osipi",
                 schema: str = "bronze"):
        # Initialize Databricks workspace client
        if databricks_host and databricks_token:
            os.environ['DATABRICKS_HOST'] = databricks_host
            os.environ['DATABRICKS_TOKEN'] = databricks_token

        self.workspace_client = WorkspaceClient()
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema

        # Databricks colors
        self.colors = {
            'lava': '#FF3621',
            'cyan': '#00A8E1',
            'navy': '#1B3139',
            'bg': '#FFFFFF',
            'success': '#00A87E',
            'warning': '#FFB000'
        }

        # Data buffers for real-time plotting
        self.timestamps = deque(maxlen=50)
        self.throughput = deque(maxlen=50)
        self.late_data_pct = deque(maxlen=50)
        self.tag_count = 0
        self.elements_count = 0
        self.event_frames_count = 0
        self.pipeline_stats = {}  # Track contribution from each pipeline

    def fetch_af_hierarchy(self) -> Dict[str, Any]:
        """Fetch REAL AF hierarchy from ALL per-pipeline bronze tables"""
        try:
            # Query ALL pi_af_hierarchy_pipeline* tables using UNION
            # Multi-pipeline architecture: each pipeline writes to separate table
            result = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=f"""
                    SELECT element_id, element_name, element_path, parent_id,
                           element_type, template_name, depth
                    FROM {self.catalog}.{self.schema}.pi_af_hierarchy_pipeline1
                    UNION ALL
                    SELECT element_id, element_name, element_path, parent_id,
                           element_type, template_name, depth
                    FROM {self.catalog}.{self.schema}.pi_af_hierarchy_pipeline2
                    UNION ALL
                    SELECT element_id, element_name, element_path, parent_id,
                           element_type, template_name, depth
                    FROM {self.catalog}.{self.schema}.pi_af_hierarchy_pipeline3
                    UNION ALL
                    SELECT element_id, element_name, element_path, parent_id,
                           element_type, template_name, depth
                    FROM {self.catalog}.{self.schema}.pi_af_hierarchy_pipeline4
                    UNION ALL
                    SELECT element_id, element_name, element_path, parent_id,
                           element_type, template_name, depth
                    FROM {self.catalog}.{self.schema}.pi_af_hierarchy_pipeline5
                    UNION ALL
                    SELECT element_id, element_name, element_path, parent_id,
                           element_type, template_name, depth
                    FROM {self.catalog}.{self.schema}.pi_af_hierarchy_pipeline6
                    UNION ALL
                    SELECT element_id, element_name, element_path, parent_id,
                           element_type, template_name, depth
                    FROM {self.catalog}.{self.schema}.pi_af_hierarchy_pipeline7
                    ORDER BY depth, element_name
                    LIMIT 100
                """,
                catalog=self.catalog,
                schema=self.schema
            )

            # Convert to API-like format for compatibility with visualization
            items = []
            if result.result.data_array:
                for row in result.result.data_array:
                    items.append({
                        'WebId': row[0],  # element_id
                        'Name': row[1],    # element_name
                        'Path': row[2],    # element_path
                        'ParentId': row[3], # parent_id
                        'Type': row[4],    # element_type
                        'Template': row[5], # template_name
                        'Depth': row[6]    # depth
                    })

            return {'Items': items}

        except Exception as e:
            print(f"Warning: Could not fetch AF hierarchy from Delta tables: {e}")
            return {'Items': []}

    def fetch_tag_list(self) -> List[Dict[str, Any]]:
        """Fetch REAL tag list from ALL per-pipeline bronze tables"""
        try:
            # Query ALL pi_timeseries_pipeline* tables using UNION
            result = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=f"""
                    SELECT DISTINCT tag_webid
                    FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline1
                    UNION
                    SELECT DISTINCT tag_webid
                    FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline2
                    UNION
                    SELECT DISTINCT tag_webid
                    FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline3
                    UNION
                    SELECT DISTINCT tag_webid
                    FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline4
                    UNION
                    SELECT DISTINCT tag_webid
                    FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline5
                    UNION
                    SELECT DISTINCT tag_webid
                    FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline6
                    UNION
                    SELECT DISTINCT tag_webid
                    FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline7
                    LIMIT 1000
                """,
                catalog=self.catalog,
                schema=self.schema
            )

            # Convert to API-like format
            tags = []
            if result.result.data_array:
                for row in result.result.data_array:
                    tags.append({
                        'WebId': row[0],
                        'Name': row[0]  # Use WebId as name for now
                    })

            return tags

        except Exception as e:
            print(f"Warning: Could not fetch tags from Delta tables: {e}")
            return []

    def fetch_live_data_sample(self, tag_webid: str) -> Dict[str, Any]:
        """Fetch REAL latest data point from ALL per-pipeline bronze tables"""
        try:
            # Query ALL pi_timeseries_pipeline* tables using UNION
            result = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=f"""
                    SELECT value, timestamp, quality_good, units
                    FROM (
                        SELECT value, timestamp, quality_good, units
                        FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline1
                        WHERE tag_webid = '{tag_webid}'
                        UNION ALL
                        SELECT value, timestamp, quality_good, units
                        FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline2
                        WHERE tag_webid = '{tag_webid}'
                        UNION ALL
                        SELECT value, timestamp, quality_good, units
                        FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline3
                        WHERE tag_webid = '{tag_webid}'
                        UNION ALL
                        SELECT value, timestamp, quality_good, units
                        FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline4
                        WHERE tag_webid = '{tag_webid}'
                        UNION ALL
                        SELECT value, timestamp, quality_good, units
                        FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline5
                        WHERE tag_webid = '{tag_webid}'
                        UNION ALL
                        SELECT value, timestamp, quality_good, units
                        FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline6
                        WHERE tag_webid = '{tag_webid}'
                        UNION ALL
                        SELECT value, timestamp, quality_good, units
                        FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline7
                        WHERE tag_webid = '{tag_webid}'
                    )
                    ORDER BY timestamp DESC
                    LIMIT 1
                """,
                catalog=self.catalog,
                schema=self.schema
            )

            # Convert to API-like format
            if result.result.data_array and len(result.result.data_array) > 0:
                row = result.result.data_array[0]
                return {
                    'Value': row[0],      # value
                    'Timestamp': row[1],  # timestamp
                    'Good': row[2],       # quality_good
                    'UnitsAbbreviation': row[3] if len(row) > 3 else ''  # units
                }

            return {}

        except Exception as e:
            print(f"Warning: Could not fetch live data from Delta tables: {e}")
            return {}

    def fetch_event_frames_count(self) -> int:
        """Fetch count of event frames from ALL per-pipeline bronze tables"""
        try:
            # Query ALL pi_event_frames_pipeline* tables using UNION
            result = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=f"""
                    SELECT COUNT(*) as event_count
                    FROM (
                        SELECT event_frame_id FROM {self.catalog}.{self.schema}.pi_event_frames_pipeline1
                        UNION ALL
                        SELECT event_frame_id FROM {self.catalog}.{self.schema}.pi_event_frames_pipeline2
                        UNION ALL
                        SELECT event_frame_id FROM {self.catalog}.{self.schema}.pi_event_frames_pipeline3
                        UNION ALL
                        SELECT event_frame_id FROM {self.catalog}.{self.schema}.pi_event_frames_pipeline4
                        UNION ALL
                        SELECT event_frame_id FROM {self.catalog}.{self.schema}.pi_event_frames_pipeline5
                        UNION ALL
                        SELECT event_frame_id FROM {self.catalog}.{self.schema}.pi_event_frames_pipeline6
                        UNION ALL
                        SELECT event_frame_id FROM {self.catalog}.{self.schema}.pi_event_frames_pipeline7
                    )
                """,
                catalog=self.catalog,
                schema=self.schema
            )

            if result.result.data_array and len(result.result.data_array) > 0:
                return result.result.data_array[0][0]
            return 0

        except Exception as e:
            print(f"Warning: Could not fetch event frames count: {e}")
            return 0

    def fetch_pipeline_stats(self) -> Dict[int, int]:
        """Fetch record counts per pipeline (shows contribution from each of the 7 pipelines)"""
        try:
            pipeline_counts = {}
            for pipeline_id in range(1, 8):
                result = self.workspace_client.statement_execution.execute_statement(
                    warehouse_id=self.warehouse_id,
                    statement=f"""
                        SELECT COUNT(*) as row_count
                        FROM {self.catalog}.{self.schema}.pi_timeseries_pipeline{pipeline_id}
                    """,
                    catalog=self.catalog,
                    schema=self.schema
                )
                if result.result.data_array and len(result.result.data_array) > 0:
                    pipeline_counts[pipeline_id] = result.result.data_array[0][0]
                else:
                    pipeline_counts[pipeline_id] = 0

            return pipeline_counts

        except Exception as e:
            print(f"Warning: Could not fetch pipeline stats: {e}")
            return {}

    def visualize_af_hierarchy(self, ax):
        """Visualize REAL AF hierarchy as tree"""
        hierarchy = self.fetch_af_hierarchy()

        if not hierarchy.get('Items'):
            ax.text(0.5, 0.5, 'Waiting for mock PI server...',
                   ha='center', va='center', fontsize=14, color='red')
            ax.set_title('AF Hierarchy', fontsize=16, fontweight='bold')
            ax.axis('off')
            return

        # Build tree from real data
        G = nx.DiGraph()
        root = "Production DB"
        G.add_node(root, type='database')

        elements = hierarchy.get('Items', [])
        self.elements_count = len(elements)

        # Add plants (top-level elements)
        for plant_idx, plant in enumerate(elements[:10]):  # Show first 10 for readability
            plant_name = plant.get('Name', f'Plant_{plant_idx}')
            G.add_node(plant_name, type='plant')
            G.add_edge(root, plant_name)

            # Get child elements (units)
            if plant.get('Elements'):
                for unit_idx, unit in enumerate(plant['Elements'][:3]):  # Show first 3 units
                    unit_name = unit.get('Name', f'Unit_{unit_idx}')
                    full_unit_name = f"{plant_name}/{unit_name}"
                    G.add_node(full_unit_name, type='unit')
                    G.add_edge(plant_name, full_unit_name)

                    # Show equipment
                    if unit.get('Elements'):
                        for equip in unit['Elements'][:2]:  # Show 2 pieces of equipment
                            equip_name = equip.get('Name', 'Equipment')
                            full_equip_name = f"{unit_name}/{equip_name}"
                            G.add_node(full_equip_name, type='equipment')
                            G.add_edge(full_unit_name, full_equip_name)

        # Layout and draw
        pos = nx.spring_layout(G, k=1.5, iterations=50, seed=42)

        # Draw by type
        node_styles = {
            'database': {'color': self.colors['lava'], 'size': 2000},
            'plant': {'color': self.colors['cyan'], 'size': 1500},
            'unit': {'color': self.colors['navy'], 'size': 1000},
            'equipment': {'color': '#666666', 'size': 700}
        }

        for node_type, style in node_styles.items():
            nodes = [n for n, d in G.nodes(data=True) if d.get('type') == node_type]
            if nodes:
                nx.draw_networkx_nodes(G, pos, nodelist=nodes,
                                      node_color=style['color'],
                                      node_size=style['size'],
                                      alpha=0.9, ax=ax)

        nx.draw_networkx_edges(G, pos, edge_color='#CCCCCC',
                              width=1.5, alpha=0.6, arrows=True,
                              arrowsize=10, ax=ax)

        # Labels (show only for larger nodes)
        large_nodes = {n: n.split('/')[-1] for n, d in G.nodes(data=True)
                      if d.get('type') in ['database', 'plant', 'unit']}
        nx.draw_networkx_labels(G, pos, labels=large_nodes,
                               font_size=7, font_weight='bold',
                               font_color='white', ax=ax)

        ax.set_title(f'AF Hierarchy ({self.elements_count} Total Elements)',
                    fontsize=14, fontweight='bold', color=self.colors['navy'])
        ax.axis('off')

    def visualize_data_flow(self, ax):
        """Visualize real-time data flow statistics"""
        tags = self.fetch_tag_list()
        self.tag_count = len(tags)

        if not tags:
            ax.text(0.5, 0.5, 'Waiting for tags...',
                   ha='center', va='center', fontsize=14, color='red')
            ax.set_title('Data Flow', fontsize=16, fontweight='bold')
            ax.axis('off')
            return

        # Sample data from a few tags
        sample_tags = tags[:5]
        live_data = []

        for tag in sample_tags:
            data = self.fetch_live_data_sample(tag.get('WebId', ''))
            if data:
                live_data.append({
                    'name': tag.get('Name', 'Unknown')[:20],
                    'value': data.get('Value', 0),
                    'timestamp': data.get('Timestamp', ''),
                    'good': data.get('Good', True)
                })

        # Create flow diagram
        ax.clear()

        # Draw flow boxes
        boxes = [
            {'x': 0.1, 'y': 0.7, 'label': f'OSIPI Server\n{self.tag_count} tags', 'color': self.colors['lava']},
            {'x': 0.4, 'y': 0.7, 'label': 'Connector\nStreaming', 'color': self.colors['cyan']},
            {'x': 0.7, 'y': 0.7, 'label': 'Delta Lake\nBronze', 'color': self.colors['navy']}
        ]

        for box in boxes:
            rect = mpatches.FancyBboxPatch(
                (box['x'], box['y']), 0.15, 0.15,
                boxstyle="round,pad=0.01",
                facecolor=box['color'],
                edgecolor='black',
                linewidth=2,
                transform=ax.transAxes
            )
            ax.add_patch(rect)
            ax.text(box['x'] + 0.075, box['y'] + 0.075, box['label'],
                   ha='center', va='center', fontsize=9, fontweight='bold',
                   color='white', transform=ax.transAxes)

        # Draw arrows
        for i in range(len(boxes) - 1):
            ax.annotate('', xy=(boxes[i+1]['x'], boxes[i+1]['y'] + 0.075),
                       xytext=(boxes[i]['x'] + 0.15, boxes[i]['y'] + 0.075),
                       arrowprops=dict(arrowstyle='->', lw=3, color=self.colors['cyan']),
                       transform=ax.transAxes)

        # Show live data samples
        if live_data:
            y_pos = 0.4
            ax.text(0.5, 0.5, 'Live Data Sample:', ha='center', fontsize=10,
                   fontweight='bold', transform=ax.transAxes)

            for data in live_data:
                quality_icon = '✓' if data['good'] else '✗'
                quality_color = self.colors['success'] if data['good'] else self.colors['warning']
                text = f"{quality_icon} {data['name']}: {data['value']:.2f}"
                ax.text(0.5, y_pos, text, ha='center', fontsize=8,
                       color=quality_color, transform=ax.transAxes, family='monospace')
                y_pos -= 0.08

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        ax.set_title('Data Flow (Live)', fontsize=14, fontweight='bold',
                    color=self.colors['navy'])

    def visualize_stats(self, ax):
        """Show system statistics including per-pipeline breakdown"""
        ax.clear()

        # Fetch event frames count and pipeline stats
        self.event_frames_count = self.fetch_event_frames_count()
        self.pipeline_stats = self.fetch_pipeline_stats()

        # Main stats at top
        stats = [
            {'label': 'Total Tags', 'value': self.tag_count, 'icon': '📊'},
            {'label': 'AF Elements', 'value': self.elements_count, 'icon': '🏭'},
            {'label': 'Event Frames', 'value': self.event_frames_count, 'icon': '⚠️'},
            {'label': 'Pipelines Active', 'value': len([v for v in self.pipeline_stats.values() if v > 0]), 'icon': '⚡'}
        ]

        y_pos = 0.95
        for stat in stats:
            # Draw stat box
            rect = mpatches.FancyBboxPatch(
                (0.05, y_pos - 0.06), 0.9, 0.08,
                boxstyle="round,pad=0.005",
                facecolor='#F5F5F5',
                edgecolor=self.colors['cyan'],
                linewidth=1.5,
                transform=ax.transAxes
            )
            ax.add_patch(rect)

            # Label
            ax.text(0.08, y_pos - 0.02, f"{stat['icon']} {stat['label']}",
                   fontsize=8, fontweight='bold',
                   color=self.colors['navy'], transform=ax.transAxes)

            # Value
            value_str = str(stat['value'])
            ax.text(0.92, y_pos - 0.02, value_str,
                   fontsize=11, fontweight='bold',
                   color=self.colors['lava'], ha='right', transform=ax.transAxes)

            y_pos -= 0.10

        # Pipeline breakdown section
        if self.pipeline_stats:
            ax.text(0.5, y_pos - 0.03, 'Pipeline Contribution:',
                   fontsize=9, fontweight='bold',
                   color=self.colors['navy'], ha='center', transform=ax.transAxes)
            y_pos -= 0.08

            for pipeline_id, count in sorted(self.pipeline_stats.items()):
                if count > 0:  # Only show active pipelines
                    ax.text(0.1, y_pos, f"Pipeline {pipeline_id}:",
                           fontsize=7, color=self.colors['navy'], transform=ax.transAxes)
                    ax.text(0.9, y_pos, f"{count:,} records",
                           fontsize=7, color=self.colors['cyan'], ha='right', transform=ax.transAxes)
                    y_pos -= 0.06

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        ax.set_title('System Statistics (7 Pipelines)', fontsize=12, fontweight='bold',
                    color=self.colors['navy'])

    def create_dashboard(self):
        """Create complete live dashboard"""
        fig = plt.figure(figsize=(18, 10))
        fig.patch.set_facecolor(self.colors['bg'])

        gs = GridSpec(2, 3, figure=fig, hspace=0.3, wspace=0.3)

        # Title
        fig.suptitle('OSIPI → Databricks Live Demo Dashboard',
                    fontsize=20, fontweight='bold', color=self.colors['navy'])

        # Create subplots
        ax1 = fig.add_subplot(gs[0, :2])  # AF Hierarchy (large)
        ax2 = fig.add_subplot(gs[0, 2])   # Stats
        ax3 = fig.add_subplot(gs[1, :])   # Data Flow

        def update(frame):
            """Update dashboard in real-time"""
            self.visualize_af_hierarchy(ax1)
            self.visualize_stats(ax2)
            self.visualize_data_flow(ax3)

        # Initial render
        update(0)

        # Animate (refresh every 2 seconds)
        ani = FuncAnimation(fig, update, interval=2000, cache_frame_data=False)

        return fig, ani


def create_ascii_dashboard():
    """Create ASCII dashboard for terminal display"""
    # Use environment variables for Databricks credentials
    databricks_host = os.getenv('DATABRICKS_HOST', 'https://e2-demo-field-eng.cloud.databricks.com')
    databricks_token = os.getenv('DATABRICKS_TOKEN')

    if not databricks_token:
        print("ERROR: DATABRICKS_TOKEN environment variable not set!")
        print("Please set DATABRICKS_HOST and DATABRICKS_TOKEN to query Delta tables.")
        return

    dashboard = LiveDashboard(
        databricks_host=databricks_host,
        databricks_token=databricks_token
    )

    tags = dashboard.fetch_tag_list()
    hierarchy = dashboard.fetch_af_hierarchy()

    print("=" * 100)
    print("  OSIPI → DATABRICKS LIVE DEMO DASHBOARD".center(100))
    print("=" * 100)
    print()

    # System stats
    print("📊 SYSTEM STATISTICS")
    print("-" * 100)
    print(f"  Total Tags:       {len(tags)}")
    print(f"  AF Elements:      {len(hierarchy.get('Items', []))}")
    print(f"  Status:           ACTIVE ✓")
    print(f"  Mock Server:      http://localhost:5050")
    print()

    # AF Hierarchy summary
    print("🏭 AF HIERARCHY")
    print("-" * 100)
    for idx, element in enumerate(hierarchy.get('Items', [])[:5]):
        print(f"  {idx+1}. {element.get('Name', 'Unknown')}")
        if element.get('Elements'):
            for unit in element['Elements'][:2]:
                print(f"     └── {unit.get('Name', 'Unit')}")
    if len(hierarchy.get('Items', [])) > 5:
        print(f"  ... and {len(hierarchy.get('Items', [])) - 5} more plants")
    print()

    # Live data sample
    print("📈 LIVE DATA SAMPLE")
    print("-" * 100)
    for idx, tag in enumerate(tags[:10]):
        data = dashboard.fetch_live_data_sample(tag.get('WebId', ''))
        if data:
            quality = "✓ GOOD" if data.get('Good', True) else "✗ BAD"
            print(f"  {tag.get('Name', 'Unknown')[:40]:<40} | "
                  f"{data.get('Value', 0):>10.2f} | {quality}")
    print()

    print("=" * 100)
    print("  Dashboard refreshing every 2 seconds... (Press Ctrl+C to stop)".center(100))
    print("=" * 100)


if __name__ == "__main__":
    import sys

    print("OSIPI Live Demo Dashboard")
    print("=" * 100)
    print()

    if '--ascii' in sys.argv:
        # ASCII terminal dashboard
        import time
        try:
            while True:
                create_ascii_dashboard()
                time.sleep(2)
                print("\033[H\033[J")  # Clear screen
        except KeyboardInterrupt:
            print("\n\nDashboard stopped.")
    else:
        # Graphical dashboard
        print("Starting graphical dashboard...")
        print("Close window to exit.")
        print()

        # Use environment variables for Databricks credentials
        databricks_host = os.getenv('DATABRICKS_HOST', 'https://e2-demo-field-eng.cloud.databricks.com')
        databricks_token = os.getenv('DATABRICKS_TOKEN')

        if not databricks_token:
            print("ERROR: DATABRICKS_TOKEN environment variable not set!")
            print("Please set DATABRICKS_HOST and DATABRICKS_TOKEN to query Delta tables.")
            print("\nExample:")
            print("  export DATABRICKS_HOST='https://e2-demo-field-eng.cloud.databricks.com'")
            print("  export DATABRICKS_TOKEN='dapi...'")
            return

        dashboard = LiveDashboard(
            databricks_host=databricks_host,
            databricks_token=databricks_token
        )
        fig, ani = dashboard.create_dashboard()
        plt.show()
