"""
Sample PI Web API responses for deterministic testing.

This module provides realistic, production-quality fixture data that
simulates actual PI Web API responses for use in unit and integration tests.

Each fixture includes:
- Proper timestamp formatting (ISO 8601 UTC)
- Realistic sensor values with units
- Quality flags (Good, Questionable, Substituted)
- WebId formats matching PI standards
- Proper response structure matching PI Web API spec
"""

# Sample time-series recorded data response
# Represents temperature readings from a single PI Point over ~1 hour interval
SAMPLE_RECORDED_RESPONSE = {
    "Items": [
        {
            "Timestamp": "2025-01-08T10:00:00Z",
            "Value": 75.5,
            "UnitsAbbreviation": "degC",
            "Good": True,
            "Questionable": False,
            "Substituted": False
        },
        {
            "Timestamp": "2025-01-08T10:01:00Z",
            "Value": 75.3,
            "UnitsAbbreviation": "degC",
            "Good": True,
            "Questionable": False,
            "Substituted": False
        },
        {
            "Timestamp": "2025-01-08T10:02:00Z",
            "Value": 75.8,
            "UnitsAbbreviation": "degC",
            "Good": True,
            "Questionable": False,
            "Substituted": False
        },
        {
            "Timestamp": "2025-01-08T10:03:00Z",
            "Value": 76.1,
            "UnitsAbbreviation": "degC",
            "Good": True,
            "Questionable": False,
            "Substituted": False
        },
        {
            "Timestamp": "2025-01-08T10:04:00Z",
            "Value": 75.9,
            "UnitsAbbreviation": "degC",
            "Good": False,
            "Questionable": True,
            "Substituted": False
        }
    ],
    "Links": {}
}

# Sample Asset Framework element with proper hierarchy attributes
# Represents a pump in the PI AF model (Rotating Equipment asset)
SAMPLE_AF_ELEMENT = {
    "WebId": "F1DP-Element-Pump101",
    "Name": "Pump-101",
    "TemplateName": "PumpTemplate",
    "Type": "Element",
    "Description": "Main process pump",
    "CategoryNames": ["Rotating Equipment", "Critical"],
    "Elements": [],  # Children elements (empty for leaf node)
    "Parent": {
        "WebId": "F1DP-UnitA",
        "Name": "Unit A",
        "Type": "Element"
    },
    "Attributes": [
        {
            "WebId": "F1DP-Attr-Pump101-Speed",
            "Name": "Speed",
            "Type": "Double",
            "DefaultUnitsName": "RPM"
        },
        {
            "WebId": "F1DP-Attr-Pump101-Vibration",
            "Name": "Vibration",
            "Type": "Double",
            "DefaultUnitsName": "mm/s"
        }
    ],
    "ExtendedProperties": {},
    "SecurityIdentity": "DOMAIN\\Username"
}

# Sample Event Frame representing a production batch run
# Includes time window, product grade, and process parameters
SAMPLE_EVENT_FRAME = {
    "WebId": "F1DP-EF-Batch001",
    "Name": "Batch-2025-01-08-001",
    "TemplateName": "BatchRunTemplate",
    "StartTime": "2025-01-08T10:00:00Z",
    "EndTime": "2025-01-08T12:30:00Z",
    "PrimaryReferencedElementWebId": "F1DP-Unit1",
    "CategoryNames": ["Production"],
    "Description": "Grade A production run",
    "IsConfirmed": True,
    "IsAcknowledged": False,
    "ExtendedProperties": {
        "ProductGrade": "A",
        "Operator": "John Smith",
        "Yield%": 94.5
    },
    "Attributes": [
        {
            "WebId": "F1DP-EFAttr-Batch001-Duration",
            "Name": "Duration",
            "Value": 150.0,
            "Timestamp": "2025-01-08T12:30:00Z"
        },
        {
            "WebId": "F1DP-EFAttr-Batch001-Quality",
            "Name": "Quality",
            "Value": 98.2,
            "Timestamp": "2025-01-08T12:30:00Z"
        }
    ]
}

# Sample batch request response containing multiple PI API calls
# Demonstrates the batch controller pattern for efficient API usage
SAMPLE_BATCH_RESPONSE = {
    "Responses": [
        {
            "Status": 200,
            "Content": SAMPLE_RECORDED_RESPONSE
        },
        {
            "Status": 200,
            "Content": SAMPLE_RECORDED_RESPONSE
        },
        {
            "Status": 200,
            "Content": {
                "Items": [
                    {
                        "Timestamp": "2025-01-08T10:00:00Z",
                        "Value": 42.7,
                        "UnitsAbbreviation": "bar",
                        "Good": True,
                        "Questionable": False,
                        "Substituted": False
                    },
                    {
                        "Timestamp": "2025-01-08T10:01:00Z",
                        "Value": 42.8,
                        "UnitsAbbreviation": "bar",
                        "Good": True,
                        "Questionable": False,
                        "Substituted": False
                    }
                ],
                "Links": {}
            }
        }
    ]
}

# Additional fixtures for extended test scenarios

# Sample AF Database containing the asset hierarchy
SAMPLE_AF_DATABASE = {
    "WebId": "F1DP-DB1",
    "Name": "Production",
    "Description": "Main production database",
    "Type": "Database",
    "IsReadOnly": False,
    "InstanceCount": 150,
    "ElementCount": 842,
    "EventFrameCount": 1250,
    "ExtendedProperties": {},
    "SecurityIdentity": "DOMAIN\\PIReaders"
}

# Sample AF Element with children (hierarchical structure)
SAMPLE_AF_ELEMENT_WITH_CHILDREN = {
    "WebId": "F1DP-Site1",
    "Name": "Sydney_Plant",
    "TemplateName": "PlantTemplate",
    "Type": "Element",
    "Description": "Main production facility",
    "CategoryNames": ["Production Site"],
    "Elements": [
        {
            "WebId": "F1DP-Unit1",
            "Name": "Unit_1",
            "TemplateName": "UnitTemplate",
            "Type": "Element"
        },
        {
            "WebId": "F1DP-Unit2",
            "Name": "Unit_2",
            "TemplateName": "UnitTemplate",
            "Type": "Element"
        }
    ]
}

# Sample AF Element List Response
# Represents multiple child elements under a parent
SAMPLE_AF_ELEMENTS_LIST = {
    "Items": [
        {
            "WebId": "F1DP-Unit1",
            "Name": "Unit-1",
            "TemplateName": "UnitTemplate",
            "Type": "Element",
            "Description": "Production Unit 1",
            "CategoryNames": ["Manufacturing"]
        },
        {
            "WebId": "F1DP-Unit2",
            "Name": "Unit-2",
            "TemplateName": "UnitTemplate",
            "Type": "Element",
            "Description": "Production Unit 2",
            "CategoryNames": ["Manufacturing"]
        },
        {
            "WebId": "F1DP-Unit3",
            "Name": "Unit-3",
            "TemplateName": "UnitTemplate",
            "Type": "Element",
            "Description": "Production Unit 3",
            "CategoryNames": ["Manufacturing"]
        }
    ],
    "Links": {}
}

# Sample Event Frame List Response
# Represents multiple events within a time window
SAMPLE_EVENT_FRAMES_LIST = {
    "Items": [
        {
            "WebId": "F1DP-EF-Batch001",
            "Name": "Batch-2025-01-08-001",
            "TemplateName": "BatchRunTemplate",
            "StartTime": "2025-01-08T10:00:00Z",
            "EndTime": "2025-01-08T12:30:00Z",
            "PrimaryReferencedElementWebId": "F1DP-Unit1",
            "CategoryNames": ["Production"]
        },
        {
            "WebId": "F1DP-EF-Batch002",
            "Name": "Batch-2025-01-08-002",
            "TemplateName": "BatchRunTemplate",
            "StartTime": "2025-01-08T13:00:00Z",
            "EndTime": "2025-01-08T14:45:00Z",
            "PrimaryReferencedElementWebId": "F1DP-Unit2",
            "CategoryNames": ["Production"]
        },
        {
            "WebId": "F1DP-EF-Maintenance001",
            "Name": "Maintenance-2025-01-08-001",
            "TemplateName": "MaintenanceTemplate",
            "StartTime": "2025-01-08T15:00:00Z",
            "EndTime": "2025-01-08T16:30:00Z",
            "PrimaryReferencedElementWebId": "F1DP-Unit1",
            "CategoryNames": ["Maintenance"]
        }
    ],
    "Links": {}
}

# Sample Attribute List Response
# Attributes are data references attached to elements
SAMPLE_ATTRIBUTES_LIST = {
    "Items": [
        {
            "WebId": "F1DP-Attr1",
            "Name": "Temperature",
            "DataReferencePlugIn": "PI Point",
            "DefaultUnitsName": "degC",
            "Type": "Double",
            "DataReference": {
                "PointWebId": "PI-Point-Temp001"
            }
        },
        {
            "WebId": "F1DP-Attr2",
            "Name": "Pressure",
            "DataReferencePlugIn": "PI Point",
            "DefaultUnitsName": "bar",
            "Type": "Double",
            "DataReference": {
                "PointWebId": "PI-Point-Press001"
            }
        },
        {
            "WebId": "F1DP-Attr3",
            "Name": "Flow Rate",
            "DataReferencePlugIn": "PI Point",
            "DefaultUnitsName": "m3/h",
            "Type": "Double",
            "DataReference": {
                "PointWebId": "PI-Point-Flow001"
            }
        }
    ],
    "Links": {}
}

# Sample error response (404 Not Found)
SAMPLE_ERROR_RESPONSE_404 = {
    "Status": 404,
    "Errors": [
        "The requested resource was not found."
    ],
    "StatusCode": 404,
    "Content": {
        "Errors": ["The requested resource was not found."]
    }
}

# Sample error response (500 Internal Server Error)
SAMPLE_ERROR_RESPONSE_500 = {
    "Status": 500,
    "Errors": [
        "An unexpected error occurred processing the request."
    ],
    "StatusCode": 500,
    "Content": {
        "Errors": ["An unexpected error occurred processing the request."]
    }
}

# Sample authentication challenge response
SAMPLE_AUTH_CHALLENGE = {
    "Status": 401,
    "Headers": {
        "WWW-Authenticate": "Basic realm=\"PI Web API\""
    }
}

# Sample paged response with continuation link
# Demonstrates pagination for large result sets
SAMPLE_PAGED_RESPONSE = {
    "Items": [
        {
            "Timestamp": f"2025-01-08T{i//60:02d}:{i%60:02d}:00Z",
            "Value": 75.0 + (i % 10) * 0.5,
            "UnitsAbbreviation": "degC",
            "Good": True,
            "Questionable": False,
            "Substituted": False
        }
        for i in range(100)
    ],
    "Links": {
        "Next": "/piwebapi/streams/F1DP-Tag1/recorded?startTime=2025-01-08T01:40:00Z&maxCount=100"
    }
}

# Sample stream query for real-time data subscriptions
SAMPLE_STREAM_RESPONSE = {
    "Items": [
        {
            "WebId": "F1DP-Tag1",
            "Name": "Plant1_Temperature",
            "Value": 75.5,
            "Timestamp": "2025-01-08T10:30:00Z",
            "UnitsAbbreviation": "degC",
            "Good": True,
            "Questionable": False
        },
        {
            "WebId": "F1DP-Tag2",
            "Name": "Plant1_Pressure",
            "Value": 42.3,
            "Timestamp": "2025-01-08T10:30:00Z",
            "UnitsAbbreviation": "bar",
            "Good": True,
            "Questionable": False
        }
    ],
    "Links": {}
}

# Sample element template (defines structure for elements)
SAMPLE_ELEMENT_TEMPLATE = {
    "WebId": "F1DP-Template-Pump",
    "Name": "PumpTemplate",
    "Type": "ElementTemplate",
    "Description": "Standard pump asset template",
    "CategoryNames": ["Equipment"],
    "AllowElementToExtend": True,
    "BaseTemplate": {
        "WebId": "F1DP-Template-Equipment",
        "Name": "EquipmentTemplate"
    },
    "AttributeTemplates": [
        {
            "Name": "Speed",
            "Type": "Double",
            "DefaultUnitsName": "RPM"
        },
        {
            "Name": "Vibration",
            "Type": "Double",
            "DefaultUnitsName": "mm/s"
        },
        {
            "Name": "Temperature",
            "Type": "Double",
            "DefaultUnitsName": "degC"
        }
    ]
}

# Sample empty response (for scenarios with no data)
SAMPLE_EMPTY_RESPONSE = {
    "Items": [],
    "Links": {}
}

# Sample AF hierarchy depth 3 (Site -> Unit -> Equipment)
SAMPLE_DEEP_HIERARCHY = {
    "Items": [
        {
            "WebId": "F1DP-Site1",
            "Name": "Sydney_Plant",
            "TemplateName": "SiteTemplate",
            "Type": "Element",
            "Description": "Main Sydney production facility",
            "CategoryNames": ["Plant", "Active"],
            "Path": "\\\\Production Database\\Sydney_Plant"
        },
        {
            "WebId": "F1DP-Unit1",
            "Name": "Unit-1",
            "TemplateName": "UnitTemplate",
            "Type": "Element",
            "Description": "Primary production unit",
            "CategoryNames": ["Manufacturing"],
            "Path": "\\\\Production Database\\Sydney_Plant\\Unit-1"
        },
        {
            "WebId": "F1DP-Equipment1",
            "Name": "Pump-101",
            "TemplateName": "PumpTemplate",
            "Type": "Element",
            "Description": "Primary circulation pump",
            "CategoryNames": ["Rotating Equipment", "Critical"],
            "Path": "\\\\Production Database\\Sydney_Plant\\Unit-1\\Pump-101"
        }
    ],
    "Links": {}
}

# Sample response with quality issues (mixed good/bad data)
SAMPLE_RECORDED_RESPONSE_WITH_QUALITY_ISSUES = {
    "Items": [
        {
            "Timestamp": "2025-01-08T10:00:00Z",
            "Value": 75.5,
            "UnitsAbbreviation": "degC",
            "Good": True,
            "Questionable": False,
            "Substituted": False
        },
        {
            "Timestamp": "2025-01-08T10:01:00Z",
            "Value": None,
            "UnitsAbbreviation": "degC",
            "Good": False,
            "Questionable": True,
            "Substituted": False
        },
        {
            "Timestamp": "2025-01-08T10:02:00Z",
            "Value": 75.8,
            "UnitsAbbreviation": "degC",
            "Good": True,
            "Questionable": False,
            "Substituted": True
        },
        {
            "Timestamp": "2025-01-08T10:03:00Z",
            "Value": 76.1,
            "UnitsAbbreviation": "degC",
            "Good": False,
            "Questionable": False,
            "Substituted": False
        }
    ],
    "Links": {}
}

# Sample summary data (interpolated/compressed)
SAMPLE_SUMMARY_RESPONSE = {
    "Items": [
        {
            "Timestamp": "2025-01-08T10:00:00Z",
            "Total": 1,
            "Average": 75.5,
            "Maximum": 75.5,
            "Minimum": 75.5,
            "StdDev": 0.0,
            "Count": 1
        },
        {
            "Timestamp": "2025-01-08T10:05:00Z",
            "Total": 5,
            "Average": 75.72,
            "Maximum": 76.1,
            "Minimum": 75.3,
            "StdDev": 0.31,
            "Count": 5
        },
        {
            "Timestamp": "2025-01-08T10:10:00Z",
            "Total": 5,
            "Average": 75.84,
            "Maximum": 76.5,
            "Minimum": 75.4,
            "StdDev": 0.38,
            "Count": 5
        }
    ],
    "Links": {}
}

# Sample event frame that is still active (no EndTime)
SAMPLE_ACTIVE_EVENT_FRAME = {
    "WebId": "F1DP-EF-Active001",
    "Name": "Batch-2025-01-08-Active",
    "TemplateName": "BatchRunTemplate",
    "StartTime": "2025-01-08T14:00:00Z",
    "EndTime": None,  # Active event, still running
    "PrimaryReferencedElementWebId": "F1DP-Unit3",
    "CategoryNames": ["Production", "Active"],
    "Description": "Currently running production batch",
    "IsConfirmed": False,
    "IsAcknowledged": False
}

# Sample batch response with mixed success/error statuses
SAMPLE_BATCH_RESPONSE_WITH_ERRORS = {
    "Responses": [
        {
            "Status": 200,
            "Content": SAMPLE_RECORDED_RESPONSE
        },
        {
            "Status": 404,
            "Content": {
                "Errors": ["The requested resource was not found."]
            }
        },
        {
            "Status": 200,
            "Content": SAMPLE_RECORDED_RESPONSE
        },
        {
            "Status": 500,
            "Content": {
                "Errors": ["An unexpected error occurred."]
            }
        }
    ]
}
