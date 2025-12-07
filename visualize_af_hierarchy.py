"""
AF Hierarchy Visual HTML Generator

Creates an interactive HTML tree view of the PI Asset Framework hierarchy.
No external dependencies needed - pure HTML/CSS/JavaScript.
"""

import requests
import json

def create_html_tree_view():
    """Generate interactive HTML tree view of AF hierarchy."""

    # Fetch AF hierarchy from mock server
    print("Fetching AF hierarchy from http://localhost:8010...")
    response = requests.get('http://localhost:8010/piwebapi/assetdatabases/F1DP-DB-Production/elements')
    data = response.json()

    plants = data.get('Items', [])
    print(f"Found {len(plants)} plants")

    # Build HTML with embedded CSS and JavaScript
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PI Asset Framework Hierarchy - Visual Tree</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            padding: 20px;
            color: #333;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }

        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }

        .header h1 {
            font-size: 32px;
            margin-bottom: 10px;
        }

        .header p {
            font-size: 16px;
            opacity: 0.9;
        }

        .stats {
            display: flex;
            justify-content: space-around;
            padding: 20px;
            background: #f8f9fa;
            border-bottom: 2px solid #e9ecef;
        }

        .stat-box {
            text-align: center;
        }

        .stat-number {
            font-size: 36px;
            font-weight: bold;
            color: #667eea;
        }

        .stat-label {
            font-size: 14px;
            color: #6c757d;
            margin-top: 5px;
        }

        .tree-container {
            padding: 30px;
            max-height: 800px;
            overflow-y: auto;
        }

        .tree-node {
            margin: 10px 0;
        }

        .node-content {
            display: flex;
            align-items: center;
            padding: 12px 15px;
            background: #f8f9fa;
            border-left: 4px solid #667eea;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s ease;
            margin-bottom: 5px;
        }

        .node-content:hover {
            background: #e9ecef;
            transform: translateX(5px);
        }

        .toggle-icon {
            margin-right: 10px;
            font-weight: bold;
            color: #667eea;
            width: 20px;
            text-align: center;
        }

        .node-icon {
            margin-right: 10px;
            font-size: 20px;
        }

        .node-name {
            font-weight: 600;
            color: #2c3e50;
            flex: 1;
        }

        .node-template {
            font-size: 12px;
            color: #6c757d;
            background: white;
            padding: 4px 10px;
            border-radius: 12px;
            margin-left: 10px;
        }

        .node-path {
            font-size: 11px;
            color: #95a5a6;
            margin-left: 40px;
            font-family: monospace;
        }

        .children {
            margin-left: 30px;
            display: none;
            border-left: 2px dashed #dee2e6;
            padding-left: 20px;
        }

        .children.expanded {
            display: block;
        }

        .plant-node .node-content {
            border-left-color: #e74c3c;
            background: linear-gradient(90deg, #fff5f5 0%, #f8f9fa 100%);
        }

        .unit-node .node-content {
            border-left-color: #3498db;
            background: linear-gradient(90deg, #f0f8ff 0%, #f8f9fa 100%);
        }

        .equipment-node .node-content {
            border-left-color: #2ecc71;
            background: linear-gradient(90deg, #f0fff4 0%, #f8f9fa 100%);
        }

        .collapsed {
            opacity: 0.7;
        }

        .expand-all-btn {
            position: fixed;
            bottom: 30px;
            right: 30px;
            background: #667eea;
            color: white;
            border: none;
            padding: 15px 25px;
            border-radius: 50px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            box-shadow: 0 5px 20px rgba(102, 126, 234, 0.4);
            transition: all 0.3s ease;
            z-index: 1000;
        }

        .expand-all-btn:hover {
            background: #764ba2;
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(102, 126, 234, 0.6);
        }

        .legend {
            display: flex;
            justify-content: center;
            gap: 30px;
            padding: 20px;
            background: #f8f9fa;
            border-top: 2px solid #e9ecef;
        }

        .legend-item {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .legend-color {
            width: 20px;
            height: 20px;
            border-radius: 4px;
        }

        .plant-color { background: #e74c3c; }
        .unit-color { background: #3498db; }
        .equipment-color { background: #2ecc71; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏭 PI Asset Framework Hierarchy</h1>
            <p>Interactive Tree View - Mock PI Server (10,000 Tags)</p>
        </div>

        <div class="stats">
            <div class="stat-box">
                <div class="stat-number" id="plant-count">0</div>
                <div class="stat-label">Plants</div>
            </div>
            <div class="stat-box">
                <div class="stat-number" id="unit-count">0</div>
                <div class="stat-label">Units</div>
            </div>
            <div class="stat-box">
                <div class="stat-number" id="equipment-count">0</div>
                <div class="stat-label">Equipment</div>
            </div>
            <div class="stat-box">
                <div class="stat-number" id="total-count">0</div>
                <div class="stat-label">Total Elements</div>
            </div>
        </div>

        <div class="tree-container" id="tree-container">
            <!-- Tree will be injected here -->
        </div>

        <div class="legend">
            <div class="legend-item">
                <div class="legend-color plant-color"></div>
                <span>Plant</span>
            </div>
            <div class="legend-item">
                <div class="legend-color unit-color"></div>
                <span>Unit</span>
            </div>
            <div class="legend-item">
                <div class="legend-color equipment-color"></div>
                <span>Equipment</span>
            </div>
        </div>
    </div>

    <button class="expand-all-btn" onclick="expandAll()">Expand All</button>

    <script>
        // AF Hierarchy Data
        const hierarchyData = """ + json.dumps(plants, indent=2) + """;

        let plantCount = 0;
        let unitCount = 0;
        let equipmentCount = 0;

        function getIcon(templateName) {
            const icons = {
                'PlantTemplate': '🏭',
                'ProcessUnitTemplate': '⚙️',
                'PumpTemplate': '💧',
                'CompressorTemplate': '🔄',
                'HeatExchangerTemplate': '🔥',
                'ReactorTemplate': '⚗️'
            };
            return icons[templateName] || '📦';
        }

        function getNodeType(templateName) {
            if (templateName === 'PlantTemplate') return 'plant';
            if (templateName === 'ProcessUnitTemplate') return 'unit';
            return 'equipment';
        }

        function createNode(element, level = 0) {
            const nodeType = getNodeType(element.TemplateName);
            const hasChildren = element.Elements && element.Elements.length > 0;

            // Count elements
            if (nodeType === 'plant') plantCount++;
            else if (nodeType === 'unit') unitCount++;
            else if (nodeType === 'equipment') equipmentCount++;

            let html = `
                <div class="tree-node ${nodeType}-node">
                    <div class="node-content" onclick="toggleNode(this)">
                        <span class="toggle-icon">${hasChildren ? '▶' : '•'}</span>
                        <span class="node-icon">${getIcon(element.TemplateName)}</span>
                        <span class="node-name">${element.Name}</span>
                        <span class="node-template">${element.TemplateName.replace('Template', '')}</span>
                    </div>
                    <div class="node-path">${element.Path}</div>
            `;

            if (hasChildren) {
                html += '<div class="children">';
                element.Elements.forEach(child => {
                    html += createNode(child, level + 1);
                });
                html += '</div>';
            }

            html += '</div>';
            return html;
        }

        function toggleNode(element) {
            const parent = element.parentElement;
            const children = parent.querySelector('.children');
            const icon = element.querySelector('.toggle-icon');

            if (children) {
                children.classList.toggle('expanded');
                icon.textContent = children.classList.contains('expanded') ? '▼' : '▶';
            }
        }

        function expandAll() {
            const allChildren = document.querySelectorAll('.children');
            const allIcons = document.querySelectorAll('.toggle-icon');
            const isExpanding = Array.from(allChildren).some(c => !c.classList.contains('expanded'));

            allChildren.forEach(children => {
                if (isExpanding) {
                    children.classList.add('expanded');
                } else {
                    children.classList.remove('expanded');
                }
            });

            allIcons.forEach(icon => {
                if (icon.textContent !== '•') {
                    icon.textContent = isExpanding ? '▼' : '▶';
                }
            });

            document.querySelector('.expand-all-btn').textContent =
                isExpanding ? 'Collapse All' : 'Expand All';
        }

        // Render tree
        const container = document.getElementById('tree-container');
        hierarchyData.forEach(plant => {
            container.innerHTML += createNode(plant);
        });

        // Update stats
        document.getElementById('plant-count').textContent = plantCount;
        document.getElementById('unit-count').textContent = unitCount;
        document.getElementById('equipment-count').textContent = equipmentCount;
        document.getElementById('total-count').textContent = plantCount + unitCount + equipmentCount;
    </script>
</body>
</html>"""

    # Save HTML file
    output_file = 'af_hierarchy_tree.html'
    with open(output_file, 'w') as f:
        f.write(html)

    print(f"\n✅ Visual tree generated: {output_file}")
    print(f"   Plants: {len(plants)}")

    total_units = sum(len(plant.get('Elements', [])) for plant in plants)
    total_equipment = sum(
        len(unit.get('Elements', []))
        for plant in plants
        for unit in plant.get('Elements', [])
    )

    print(f"   Units: {total_units}")
    print(f"   Equipment: {total_equipment}")
    print(f"   Total Elements: {len(plants) + total_units + total_equipment}")
    print(f"\nOpening in browser...")

    return output_file

if __name__ == "__main__":
    import os
    import webbrowser

    output_file = create_html_tree_view()

    # Open in browser
    file_path = os.path.abspath(output_file)
    webbrowser.open(f'file://{file_path}')

    print(f"✅ Browser opened with visual tree!")
    print(f"\nFile location: {file_path}")
