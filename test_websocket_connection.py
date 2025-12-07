"""Test WebSocket connection to mock PI server"""
import asyncio
import websockets
import json

async def test_websocket():
    uri = "ws://localhost:8010/piwebapi/streams/channel"

    print(f"Connecting to {uri}...")

    try:
        async with websockets.connect(uri) as websocket:
            print("✅ Connected successfully!")

            # Subscribe to a tag
            tag_webid = "F1DP-Loy_Yang_A-U001-Temp-000001"
            subscribe_msg = {
                'Action': 'Subscribe',
                'Resource': f'streams/{tag_webid}/value',
                'Parameters': {'updateRate': 1000}
            }

            await websocket.send(json.dumps(subscribe_msg))
            print(f"📡 Sent subscription request for {tag_webid}")

            # Receive updates for 10 seconds
            print("\n👂 Listening for updates (10 seconds)...\n")

            for i in range(10):
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                    data = json.loads(message)

                    if 'Items' in data and data['Items']:
                        item = data['Items'][0]
                        value = item.get('Value')
                        uom = item.get('UnitsAbbreviation', '')
                        timestamp = item.get('Timestamp', '')
                        good = item.get('Good', False)

                        quality_icon = '✅' if good else '❌'
                        print(f"[{i+1}] {quality_icon} {value} {uom} at {timestamp[:19]}")
                    elif 'Action' in data and data['Action'] == 'Subscribed':
                        print(f"✅ Subscription confirmed: {data.get('Status')}")

                except asyncio.TimeoutError:
                    print(f"[{i+1}] ⏱️  Timeout waiting for message")

            print("\n✅ Test complete!")

    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket())
