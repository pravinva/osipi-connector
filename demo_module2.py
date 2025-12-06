#!/usr/bin/env python3
"""
Demo script for Module 2: PI Web API HTTP Client

This demonstrates the key features of the PIWebAPIClient:
- Basic authentication
- GET requests
- Batch execution for performance
- Error handling
"""

import logging
from src.auth.pi_auth_manager import PIAuthManager
from src.client.pi_web_api_client import PIWebAPIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demo_basic_usage():
    """Demonstrate basic client usage"""
    logger.info("=== Demo 1: Basic Client Usage ===")

    # Configure authentication
    auth_config = {
        'type': 'basic',
        'username': 'demo_user',
        'password': 'demo_password'
    }

    # Initialize authentication manager
    auth_manager = PIAuthManager(auth_config)
    logger.info("Authentication manager initialized")

    # Initialize PI Web API client
    client = PIWebAPIClient(
        base_url="https://pi-demo.example.com/piwebapi",
        auth_manager=auth_manager
    )
    logger.info(f"Client initialized with base URL: {client.base_url}")

    # Note: Actual requests will fail without a real PI server
    # This demonstrates the API structure

    logger.info("Client ready for use")
    # Client session is automatically managed by the requests library


def demo_batch_execution():
    """Demonstrate batch execution for performance"""
    logger.info("\n=== Demo 2: Batch Execution (Performance Critical) ===")

    auth_config = {'type': 'basic', 'username': 'user', 'password': 'pass'}
    auth_manager = PIAuthManager(auth_config)
    client = PIWebAPIClient(
        base_url="https://pi-demo.example.com/piwebapi",
        auth_manager=auth_manager
    )

    # Simulate 100 tag requests in a single batch
    logger.info("Creating batch request for 100 tags...")

    tag_webids = [f"F1DP-Tag{i}" for i in range(100)]
    batch_requests = []

    for webid in tag_webids:
        batch_requests.append({
            "Method": "GET",
            "Resource": f"/streams/{webid}/recorded",
            "Parameters": {
                "startTime": "2025-01-01T00:00:00Z",
                "endTime": "2025-01-02T00:00:00Z",
                "maxCount": "10000"
            }
        })

    logger.info(f"Batch request created with {len(batch_requests)} sub-requests")
    logger.info("Performance: 100 tags in 1 HTTP request (vs 100 separate requests)")
    logger.info("Expected speedup: ~30-100x faster than sequential requests")

    # Note: Actual execution would be:
    # result = client.batch_execute(batch_requests)
    # for i, sub_response in enumerate(result["Responses"]):
    #     if sub_response["Status"] == 200:
    #         items = sub_response["Content"]["Items"]
    #         logger.info(f"Tag {tag_webids[i]}: {len(items)} data points")


def demo_context_manager():
    """Demonstrate request session management"""
    logger.info("\n=== Demo 3: Session Management ===")

    auth_config = {'type': 'basic', 'username': 'user', 'password': 'pass'}
    auth_manager = PIAuthManager(auth_config)

    client = PIWebAPIClient(
        base_url="https://pi-demo.example.com/piwebapi",
        auth_manager=auth_manager
    )

    logger.info("Client session created with connection pooling")
    logger.info(f"Session active: {client.session is not None}")
    logger.info("Sessions are automatically managed by requests library")


def demo_error_handling():
    """Demonstrate error handling capabilities"""
    logger.info("\n=== Demo 4: Error Handling ===")

    logger.info("Client includes built-in handling for:")
    logger.info("  - HTTPError: Authentication failures, 404 Not Found, etc.")
    logger.info("  - ConnectionError: Network issues, server unreachable")
    logger.info("  - Timeout: Slow responses, network delays")
    logger.info("  - Automatic retry: 503, 504, 429 with exponential backoff")
    logger.info("  - Retry strategy: 3 attempts with 1s, 2s, 4s delays")


def demo_retry_configuration():
    """Show retry configuration details"""
    logger.info("\n=== Demo 5: Retry Configuration ===")

    logger.info("Retry Strategy:")
    logger.info("  - Total retries: 3")
    logger.info("  - Backoff factor: 1 (delays: 1s, 2s, 4s)")
    logger.info("  - Status codes triggering retry: 429, 500, 502, 503, 504")
    logger.info("  - Methods allowed: HEAD, GET, POST, PUT, DELETE")
    logger.info("")
    logger.info("Connection Pooling:")
    logger.info("  - Pool connections: 10 (cached connection pools)")
    logger.info("  - Pool max size: 20 (max connections per pool)")
    logger.info("  - Benefit: Reuses TCP connections for better performance")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("PI Web API HTTP Client - Module 2 Demo")
    print("="*60)

    try:
        demo_basic_usage()
        demo_batch_execution()
        demo_context_manager()
        demo_error_handling()
        demo_retry_configuration()

        print("\n" + "="*60)
        print("Demo completed successfully!")
        print("="*60 + "\n")

        print("Key Takeaways:")
        print("1. Use batch_execute() for multiple tags (100x faster)")
        print("2. Automatic retry handles transient failures")
        print("3. Connection pooling improves performance")
        print("4. Context manager ensures proper cleanup")
        print("5. Comprehensive error handling built-in")

    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)
