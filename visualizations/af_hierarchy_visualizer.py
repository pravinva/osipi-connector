"""
AF Hierarchy Visualizer

Creates visual tree representation of OSIPI AF hierarchy for demo purposes.
Uses matplotlib and networkx for interactive visualization.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import networkx as nx
from typing import Dict, List, Any
import requests
import json


class AFHierarchyVisualizer:
    """
    Visualize OSIPI AF hierarchy as interactive tree diagram
    """

    def __init__(self, pi_server_url: str = "http://localhost:5050"):
        self.pi_server_url = pi_server_url
        self.color_scheme = {
            'database': '#FF3621',  # Databricks Lava
            'element': '#00A8E1',   # Databricks Cyan
            'attribute': '#1B3139', # Databricks Navy
            'background': '#FFFFFF',
            'text': '#1B3139'
        }

    def fetch_hierarchy(self) -> Dict[str, Any]:
        """Fetch AF hierarchy from PI server or mock"""
        try:
            response = requests.get(
                f"{self.pi_server_url}/piwebapi/assetservers",
                timeout=5
            )
            if response.status_code == 200:
                return response.json()
        except:
            pass

        # Return mock data if server not available
        return self._mock_hierarchy()

    def _mock_hierarchy(self) -> Dict[str, Any]:
        """Generate mock hierarchy for visualization"""
        return {
            'name': 'OSIPI Asset Server',
            'databases': [
                {
                    'name': 'Manufacturing',
                    'elements': [
                        {
                            'name': 'Plant1',
                            'attributes': ['Temperature', 'Pressure', 'Flow'],
                            'children': [
                                {
                                    'name': 'Reactor_A',
                                    'attributes': ['Temp_In', 'Temp_Out', 'Level']
                                },
                                {
                                    'name': 'Compressor_B',
                                    'attributes': ['Pressure', 'Vibration', 'Speed']
                                }
                            ]
                        },
                        {
                            'name': 'Plant2',
                            'attributes': ['Status', 'Power'],
                            'children': [
                                {
                                    'name': 'Boiler_C',
                                    'attributes': ['Steam_Pressure', 'Fuel_Rate']
                                }
                            ]
                        }
                    ]
                },
                {
                    'name': 'Utilities',
                    'elements': [
                        {
                            'name': 'Cooling_Tower',
                            'attributes': ['Water_Temp', 'Fan_Speed']
                        }
                    ]
                }
            ]
        }

    def build_graph(self, hierarchy: Dict[str, Any]) -> nx.DiGraph:
        """Build networkx graph from hierarchy"""
        G = nx.DiGraph()

        # Add root
        root = hierarchy['name']
        G.add_node(root, type='root', label=root)

        node_id = 1
        for db in hierarchy.get('databases', []):
            db_node = f"db_{node_id}"
            G.add_node(db_node, type='database', label=db['name'])
            G.add_edge(root, db_node)
            node_id += 1

            for elem in db.get('elements', []):
                elem_node = f"elem_{node_id}"
                G.add_node(elem_node, type='element', label=elem['name'])
                G.add_edge(db_node, elem_node)
                node_id += 1

                # Add attributes
                for attr in elem.get('attributes', []):
                    attr_node = f"attr_{node_id}"
                    G.add_node(attr_node, type='attribute', label=attr)
                    G.add_edge(elem_node, attr_node)
                    node_id += 1

                # Add child elements
                for child in elem.get('children', []):
                    child_node = f"elem_{node_id}"
                    G.add_node(child_node, type='element', label=child['name'])
                    G.add_edge(elem_node, child_node)
                    node_id += 1

                    for attr in child.get('attributes', []):
                        attr_node = f"attr_{node_id}"
                        G.add_node(attr_node, type='attribute', label=attr)
                        G.add_edge(child_node, attr_node)
                        node_id += 1

        return G

    def visualize(self, save_path: str = None, show: bool = True):
        """Create and display AF hierarchy visualization"""
        hierarchy = self.fetch_hierarchy()
        G = self.build_graph(hierarchy)

        # Create figure
        fig, ax = plt.subplots(figsize=(16, 10))
        fig.patch.set_facecolor(self.color_scheme['background'])
        ax.set_facecolor(self.color_scheme['background'])

        # Compute layout
        pos = nx.spring_layout(G, k=2, iterations=50, seed=42)

        # Draw nodes by type
        node_types = {
            'root': {'color': '#555555', 'size': 3000, 'shape': 's'},
            'database': {'color': self.color_scheme['database'], 'size': 2500, 'shape': 's'},
            'element': {'color': self.color_scheme['element'], 'size': 2000, 'shape': 'o'},
            'attribute': {'color': self.color_scheme['attribute'], 'size': 1500, 'shape': 'o'}
        }

        for node_type, style in node_types.items():
            nodes = [n for n, d in G.nodes(data=True) if d.get('type') == node_type]
            if nodes:
                nx.draw_networkx_nodes(
                    G, pos, nodelist=nodes,
                    node_color=style['color'],
                    node_size=style['size'],
                    node_shape=style['shape'],
                    alpha=0.9,
                    ax=ax
                )

        # Draw edges
        nx.draw_networkx_edges(
            G, pos,
            edge_color='#CCCCCC',
            width=2,
            alpha=0.6,
            arrows=True,
            arrowsize=15,
            ax=ax
        )

        # Draw labels
        labels = {n: d['label'] for n, d in G.nodes(data=True)}
        nx.draw_networkx_labels(
            G, pos, labels,
            font_size=9,
            font_weight='bold',
            font_color='white',
            ax=ax
        )

        # Add legend
        legend_elements = [
            mpatches.Patch(color='#555555', label='Asset Server'),
            mpatches.Patch(color=self.color_scheme['database'], label='AF Database'),
            mpatches.Patch(color=self.color_scheme['element'], label='AF Element'),
            mpatches.Patch(color=self.color_scheme['attribute'], label='Attribute (Tag)')
        ]
        ax.legend(
            handles=legend_elements,
            loc='upper left',
            fontsize=12,
            framealpha=0.95
        )

        # Add title
        ax.set_title(
            'OSIPI AF Hierarchy - Connected to Databricks Lakehouse',
            fontsize=18,
            fontweight='bold',
            color=self.color_scheme['text'],
            pad=20
        )

        # Add stats
        stats_text = (
            f"Databases: {sum(1 for n, d in G.nodes(data=True) if d.get('type') == 'database')}\n"
            f"Elements: {sum(1 for n, d in G.nodes(data=True) if d.get('type') == 'element')}\n"
            f"Attributes: {sum(1 for n, d in G.nodes(data=True) if d.get('type') == 'attribute')}"
        )
        ax.text(
            0.02, 0.02, stats_text,
            transform=ax.transAxes,
            fontsize=11,
            verticalalignment='bottom',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8)
        )

        ax.axis('off')
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"Saved hierarchy visualization to {save_path}")

        if show:
            plt.show()

        return fig


class AFHierarchyTreeVisualizer:
    """
    Alternative: ASCII tree visualization for terminal display
    """

    def __init__(self, pi_server_url: str = "http://localhost:5050"):
        self.pi_server_url = pi_server_url

    def visualize_tree(self) -> str:
        """Generate ASCII tree visualization"""
        hierarchy = AFHierarchyVisualizer(self.pi_server_url)._mock_hierarchy()

        lines = []
        lines.append("=" * 80)
        lines.append(f"  {hierarchy['name']}")
        lines.append("=" * 80)

        for db_idx, db in enumerate(hierarchy.get('databases', [])):
            is_last_db = db_idx == len(hierarchy['databases']) - 1
            db_prefix = "└── " if is_last_db else "├── "
            lines.append(f"{db_prefix}📊 {db['name']} (Database)")

            for elem_idx, elem in enumerate(db.get('elements', [])):
                is_last_elem = elem_idx == len(db['elements']) - 1
                elem_prefix = "    └── " if is_last_db else "│   └── " if is_last_elem else "│   ├── "
                if is_last_db and is_last_elem:
                    elem_prefix = "    └── "
                elif is_last_db:
                    elem_prefix = "    ├── "

                lines.append(f"{elem_prefix}🏭 {elem['name']} (Element)")

                # Attributes
                for attr_idx, attr in enumerate(elem.get('attributes', [])):
                    is_last_attr = attr_idx == len(elem['attributes']) - 1 and not elem.get('children')
                    if is_last_db and is_last_elem and is_last_attr:
                        attr_prefix = "        └── "
                    elif is_last_db and is_last_elem:
                        attr_prefix = "        ├── "
                    elif is_last_db and is_last_attr and not elem.get('children'):
                        attr_prefix = "        └── "
                    elif is_last_elem and is_last_attr and not elem.get('children'):
                        attr_prefix = "│       └── "
                    elif is_last_elem:
                        attr_prefix = "│       ├── "
                    else:
                        attr_prefix = "│   │   ├── "

                    lines.append(f"{attr_prefix}📈 {attr}")

                # Child elements
                for child_idx, child in enumerate(elem.get('children', [])):
                    is_last_child = child_idx == len(elem['children']) - 1
                    if is_last_db and is_last_elem and is_last_child:
                        child_prefix = "        └── "
                    elif is_last_db and is_last_elem:
                        child_prefix = "        ├── "
                    elif is_last_elem and is_last_child:
                        child_prefix = "│       └── "
                    elif is_last_elem:
                        child_prefix = "│       ├── "
                    else:
                        child_prefix = "│   │   ├── "

                    lines.append(f"{child_prefix}🏭 {child['name']} (Element)")

                    for attr_idx, attr in enumerate(child.get('attributes', [])):
                        is_last_attr = attr_idx == len(child['attributes']) - 1
                        if is_last_db and is_last_elem and is_last_child and is_last_attr:
                            attr_prefix = "            └── "
                        elif is_last_db and is_last_elem and is_last_child:
                            attr_prefix = "            ├── "
                        elif is_last_elem and is_last_child and is_last_attr:
                            attr_prefix = "│           └── "
                        elif is_last_elem and is_last_child:
                            attr_prefix = "│           ├── "
                        else:
                            attr_prefix = "│   │   │   ├── "

                        lines.append(f"{attr_prefix}📈 {attr}")

        lines.append("=" * 80)
        lines.append(f"Total: {sum(len(db['elements']) for db in hierarchy['databases'])} elements")
        lines.append("=" * 80)

        return "\n".join(lines)


if __name__ == "__main__":
    import sys

    print("OSIPI AF Hierarchy Visualization")
    print("=" * 80)

    # ASCII tree visualization
    print("\n1. ASCII Tree View:\n")
    tree_viz = AFHierarchyTreeVisualizer()
    print(tree_viz.visualize_tree())

    # Graphical visualization
    print("\n2. Generating graphical visualization...")
    viz = AFHierarchyVisualizer()
    viz.visualize(
        save_path='visualizations/output/af_hierarchy.png',
        show='--show' in sys.argv
    )

    print("\n✅ Visualizations generated!")
    print("   - ASCII tree: Displayed above")
    print("   - Graph: visualizations/output/af_hierarchy.png")
