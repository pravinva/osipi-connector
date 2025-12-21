# Pipeline Configuration Examples

## Overview

The mock API now supports **10 plants** with **1,000 tags each** (10,000 total tags). This provides maximum flexibility for creating any number of pipelines.

## Configuration Strategy

### Plants Available
```python
AVAILABLE_PLANTS = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
                   "Darwin", "Hobart", "Canberra", "Newcastle", "Wollongong"]
TAGS_PER_PLANT = 1000
TOTAL_TAGS = 10000
```

### Flexible Pipeline Distribution

You can create pipelines in two ways:
1. **By Plant** - Each pipeline handles ONE complete plant
2. **By Tag Range** - Split a plant's tags across multiple pipelines

## Example Configurations

### Example 1: 3 Pipelines (Plant-Based)

**Configuration:**
```python
NUM_PIPELINES = 3
PLANTS_TO_USE = ["Sydney", "Melbourne", "Brisbane"]
```

**Result:**
- Pipeline 1: Sydney plant (1,000 tags)
- Pipeline 2: Melbourne plant (1,000 tags)
- Pipeline 3: Brisbane plant (1,000 tags)
- **Total:** 3,000 tags ingested

**Code:**
```python
for plant in PLANTS_TO_USE:
    response = requests.get(f"{MOCK_API_URL}/piwebapi/dataservers/F1DP-Server-Primary/points",
                            params={"nameFilter": f"{plant}_*", "maxCount": 10000})
    tags = response.json()['Items']

    create_pipeline(
        name=f"osipi_pipeline_{plant}",
        plant=plant,
        tags=[t['WebId'] for t in tags]
    )
```

### Example 2: 5 Pipelines (Plant-Based)

**Configuration:**
```python
NUM_PIPELINES = 5
PLANTS_TO_USE = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]
```

**Result:**
- Pipeline 1: Sydney (1,000 tags)
- Pipeline 2: Melbourne (1,000 tags)
- Pipeline 3: Brisbane (1,000 tags)
- Pipeline 4: Perth (1,000 tags)
- Pipeline 5: Adelaide (1,000 tags)
- **Total:** 5,000 tags ingested

### Example 3: 10 Pipelines (All Plants)

**Configuration:**
```python
NUM_PIPELINES = 10
PLANTS_TO_USE = AVAILABLE_PLANTS  # All 10 plants
```

**Result:**
- One pipeline per plant (10 pipelines × 1,000 tags each)
- **Total:** 10,000 tags ingested

### Example 4: 15 Pipelines (Plant + Tag Range Split)

**Configuration:**
```python
NUM_PIPELINES = 15
PLANTS_TO_USE = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]  # 5 plants
PIPELINES_PER_PLANT = 3
TAGS_PER_PIPELINE = 1000 // PIPELINES_PER_PLANT  # ~333 tags per pipeline
```

**Result:**
- Sydney_Range1 (tags 1-333)
- Sydney_Range2 (tags 334-666)
- Sydney_Range3 (tags 667-1000)
- Melbourne_Range1 (tags 1-333)
- ... (same for each of 5 plants)
- **Total:** 15 pipelines handling 5,000 tags

**Code:**
```python
for plant in PLANTS_TO_USE:
    # Get all tags for this plant
    response = requests.get(f"{MOCK_API_URL}/piwebapi/dataservers/F1DP-Server-Primary/points",
                            params={"nameFilter": f"{plant}_*", "maxCount": 10000})
    all_plant_tags = response.json()['Items']

    # Split into ranges
    tags_per_range = len(all_plant_tags) // PIPELINES_PER_PLANT
    for range_id in range(PIPELINES_PER_PLANT):
        start_idx = range_id * tags_per_range
        end_idx = start_idx + tags_per_range if range_id < PIPELINES_PER_PLANT - 1 else len(all_plant_tags)
        range_tags = all_plant_tags[start_idx:end_idx]

        create_pipeline(
            name=f"osipi_pipeline_{plant}_range{range_id+1}",
            plant=plant,
            range_id=range_id+1,
            tags=[t['WebId'] for t in range_tags]
        )
```

### Example 5: 20 Pipelines (All Plants × 2 Ranges)

**Configuration:**
```python
NUM_PIPELINES = 20
PLANTS_TO_USE = AVAILABLE_PLANTS  # All 10 plants
PIPELINES_PER_PLANT = 2
TAGS_PER_PIPELINE = 1000 // 2  # 500 tags per pipeline
```

**Result:**
- Sydney_Range1 (tags 1-500)
- Sydney_Range2 (tags 501-1000)
- Melbourne_Range1 (tags 1-500)
- Melbourne_Range2 (tags 501-1000)
- ... (same for all 10 plants)
- **Total:** 20 pipelines handling 10,000 tags

### Example 6: 7 Pipelines (Mixed Approach)

**Configuration:**
```python
NUM_PIPELINES = 7

# 5 plants get full pipeline each
# 2 additional plants split into 1 pipeline each (using just half the data)
FULL_PLANTS = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]  # 5
PARTIAL_PLANTS = ["Darwin", "Hobart"]  # 2
```

**Result:**
- 5 pipelines × 1,000 tags (full plants)
- 2 pipelines × 1,000 tags (partial plants)
- **Total:** 7 pipelines handling 7,000 tags

## Implementation in generate_pipelines Notebook

### Flexible Pipeline Generator Function

```python
def generate_pipeline_configs(num_pipelines, mock_api_url):
    """
    Generate pipeline configurations for any number of pipelines.

    Strategy:
    - If num_pipelines <= 10: Use one pipeline per plant (simple)
    - If num_pipelines > 10: Split plants into multiple ranges
    """
    AVAILABLE_PLANTS = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
                       "Darwin", "Hobart", "Canberra", "Newcastle", "Wollongong"]

    pipeline_configs = []

    if num_pipelines <= 10:
        # Simple: One pipeline per plant
        plants_to_use = AVAILABLE_PLANTS[:num_pipelines]

        for plant in plants_to_use:
            # Query API for this plant's tags
            response = requests.get(
                f"{mock_api_url}/piwebapi/dataservers/F1DP-Server-Primary/points",
                params={"nameFilter": f"{plant}_*", "maxCount": 10000}
            )
            tags = response.json()['Items']

            pipeline_configs.append({
                'name': f"osipi_pipeline_{plant}",
                'plant': plant,
                'range': None,
                'tags': [t['WebId'] for t in tags],
                'tag_count': len(tags)
            })

    else:
        # Advanced: Split plants into multiple ranges
        pipelines_per_plant = (num_pipelines + 9) // 10  # Round up
        plants_needed = (num_pipelines + pipelines_per_plant - 1) // pipelines_per_plant
        plants_to_use = AVAILABLE_PLANTS[:plants_needed]

        for plant in plants_to_use:
            # Get all tags for this plant
            response = requests.get(
                f"{mock_api_url}/piwebapi/dataservers/F1DP-Server-Primary/points",
                params={"nameFilter": f"{plant}_*", "maxCount": 10000}
            )
            all_plant_tags = response.json()['Items']

            # Split into ranges
            tags_per_range = len(all_plant_tags) // pipelines_per_plant
            for range_id in range(pipelines_per_plant):
                if len(pipeline_configs) >= num_pipelines:
                    break  # Stop if we've reached desired pipeline count

                start_idx = range_id * tags_per_range
                end_idx = start_idx + tags_per_range if range_id < pipelines_per_plant - 1 else len(all_plant_tags)
                range_tags = all_plant_tags[start_idx:end_idx]

                pipeline_configs.append({
                    'name': f"osipi_pipeline_{plant}_range{range_id+1}",
                    'plant': plant,
                    'range': range_id+1,
                    'tags': [t['WebId'] for t in range_tags],
                    'tag_count': len(range_tags)
                })

    return pipeline_configs
```

### Usage Example

```python
# Configure desired number of pipelines
NUM_PIPELINES = 7  # User configurable

# Generate configurations
configs = generate_pipeline_configs(NUM_PIPELINES, MOCK_API_URL)

print(f"Generated {len(configs)} pipeline configurations:")
for config in configs:
    if config['range']:
        print(f"  {config['name']}: {config['plant']} (Range {config['range']}) - {config['tag_count']} tags")
    else:
        print(f"  {config['name']}: {config['plant']} - {config['tag_count']} tags")

# Create DLT pipeline YAML for each config
for config in configs:
    create_dlt_pipeline_yaml(
        name=config['name'],
        tags=config['tags'],
        plant=config['plant'],
        range_id=config.get('range')
    )
```

## DLT Pipeline Configuration

Each pipeline receives:

```yaml
configuration:
  pi.plant.name: "Sydney"           # Plant identifier
  pi.range.id: "1"                  # Optional: Range within plant (if split)
  pi.tags: "tag1,tag2,tag3,..."     # Comma-separated tag WebIDs
  pi.server.url: "https://..."      # Mock API URL
  pi.target.catalog: "osipi"
  pi.target.schema: "bronze"
```

## Benefits of This Approach

✅ **Complete Flexibility**
   - Create any number of pipelines (1, 3, 5, 10, 15, 20, ...)
   - Automatic distribution strategy

✅ **Realistic OT Architecture**
   - Plant-based partitioning when possible
   - Graceful degradation to tag ranges when needed

✅ **No Ownership Conflicts**
   - Each pipeline handles unique data
   - All write to same Unity Catalog table

✅ **Easy Scaling**
   - Need more throughput? Increase pipeline count
   - Need to add new plant? Just increment count

## Summary Table

| Pipelines | Strategy | Plants Used | Tags/Pipeline | Total Tags |
|-----------|----------|-------------|---------------|------------|
| 1         | By Plant | 1           | 1,000         | 1,000      |
| 3         | By Plant | 3           | 1,000         | 3,000      |
| 5         | By Plant | 5           | 1,000         | 5,000      |
| 10        | By Plant | 10          | 1,000         | 10,000     |
| 15        | Plant+Range | 5 × 3 ranges | ~333       | 5,000      |
| 20        | Plant+Range | 10 × 2 ranges | 500       | 10,000     |
| 30        | Plant+Range | 10 × 3 ranges | ~333      | 10,000     |

## Next Steps

Update `notebooks/generate_pipelines_from_mock_api.py` with the flexible generator function above, then set `NUM_PIPELINES` to whatever you need!
