import json
import datetime
from typing import List, Dict, Any

def load_json_file(filename: str) -> Dict[str, Any]:
    """Load and parse a JSON file."""
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: File {filename} not found.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in file {filename}.")
        return {}

def iso_to_milliseconds(iso_timestamp: str) -> int:
    """
    Convert ISO timestamp string to milliseconds since epoch.
    Example: "2023-10-15T14:30:25.123Z" -> 1697372225123
    """
    try:
        # Parse ISO timestamp and convert to datetime object
        dt = datetime.datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
        # Convert to milliseconds since epoch
        return int(dt.timestamp() * 1000)
    except ValueError:
        print(f"Error: Invalid ISO timestamp format: {iso_timestamp}")
        return 0

def transform_format1_to_unified(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    IMPLEMENT: Transform data-1.json format to unified format.

    Input format (data-1.json):
    {
      "telemetry": [
        {
          "device_id": "sensor_001",
          "timestamp": "2023-10-15T14:30:25.123Z",  # ISO format
          "temperature": 23.5,
          "humidity": 65.2,
          "pressure": 1013.25
        }
      ]
    }

    Output format (unified):
    [
      {
        "device_id": "sensor_001",
        "timestamp": 1697372225123,  # milliseconds since epoch
        "temperature": 23.5,
        "humidity": 65.2,
        "pressure": 1013.25
      }
    ]
    """
    unified_data = []

    if 'telemetry' not in data:
        return unified_data

    for entry in data['telemetry']:
        # Convert ISO timestamp to milliseconds
        timestamp_ms = iso_to_milliseconds(entry['timestamp'])

        # Create unified format entry
        unified_entry = {
            'device_id': entry['device_id'],
            'timestamp': timestamp_ms,
            'temperature': entry['temperature'],
            'humidity': entry['humidity'],
            'pressure': entry['pressure']
        }

        unified_data.append(unified_entry)

    return unified_data

def transform_format2_to_unified(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    IMPLEMENT: Transform data-2.json format to unified format.

    Input format (data-2.json):
    {
      "sensors": [
        {
          "id": "sensor_001",
          "ts": 1697372225123,  # already in milliseconds
          "temp": 23.5,
          "hum": 65.2,
          "press": 1013.25
        }
      ]
    }

    Output format (unified):
    [
      {
        "device_id": "sensor_001",
        "timestamp": 1697372225123,
        "temperature": 23.5,
        "humidity": 65.2,
        "pressure": 1013.25
      }
    ]
    """
    unified_data = []

    if 'sensors' not in data:
        return unified_data

    for entry in data['sensors']:
        # Map field names to unified format
        unified_entry = {
            'device_id': entry['id'],      # id -> device_id
            'timestamp': entry['ts'],      # ts -> timestamp (already in milliseconds)
            'temperature': entry['temp'],  # temp -> temperature
            'humidity': entry['hum'],      # hum -> humidity
            'pressure': entry['press']     # press -> pressure
        }

        unified_data.append(unified_entry)

    return unified_data

def save_unified_data(unified_data: List[Dict[str, Any]], filename: str) -> None:
    """Save unified data to JSON file."""
    output = {"unified_telemetry": unified_data}

    try:
        with open(filename, 'w') as file:
            json.dump(output, file, indent=2)
        print(f"Unified data saved to {filename}")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def run_tests() -> bool:
    """Run automated tests to verify the solution."""
    print("Running automated tests...")

    # Load expected result
    expected_data = load_json_file('data-result.json')
    if not expected_data:
        print("❌ Test failed: Could not load expected result file")
        return False

    # Test format 1 transformation
    print("\n📊 Testing format 1 transformation (ISO timestamps)...")
    data1 = load_json_file('data-1.json')
    if not data1:
        print("❌ Test failed: Could not load data-1.json")
        return False

    unified1 = transform_format1_to_unified(data1)
    print(f"✅ Transformed {len(unified1)} records from format 1")

    # Test format 2 transformation
    print("\n📊 Testing format 2 transformation (millisecond timestamps)...")
    data2 = load_json_file('data-2.json')
    if not data2:
        print("❌ Test failed: Could not load data-2.json")
        return False

    unified2 = transform_format2_to_unified(data2)
    print(f"✅ Transformed {len(unified2)} records from format 2")

    # Combine all unified data
    all_unified = unified1 + unified2

    # Compare with expected result
    expected_records = expected_data.get('unified_telemetry', [])

    if len(all_unified) != len(expected_records):
        print(f"❌ Test failed: Expected {len(expected_records)} records, got {len(all_unified)}")
        return False

    # Check each record
    for i, (actual, expected) in enumerate(zip(all_unified, expected_records)):
        if actual != expected:
            print(f"❌ Test failed: Record {i+1} doesn't match expected result")
            print(f"Expected: {expected}")
            print(f"Actual: {actual}")
            return False

    print("✅ All tests passed! Data transformation is working correctly.")
    return True

def main():
    """Main function to run the data transformation and tests."""
    print("🚀 Python Data Transformation Tool")
    print("=" * 50)

    # Load both data formats
    print("📁 Loading telemetry data files...")
    data1 = load_json_file('data-1.json')
    data2 = load_json_file('data-2.json')

    if not data1 or not data2:
        print("❌ Error: Could not load required data files")
        return

    print("✅ Data files loaded successfully")

    # Transform both formats to unified format
    print("\n🔄 Transforming data to unified format...")
    unified1 = transform_format1_to_unified(data1)
    unified2 = transform_format2_to_unified(data2)

    # Combine all unified data
    all_unified_data = unified1 + unified2

    print(f"✅ Successfully transformed {len(all_unified_data)} total records")

    # Save unified data
    print("\n💾 Saving unified data...")
    save_unified_data(all_unified_data, 'output-unified.json')

    # Run automated tests
    print("\n🧪 Running automated tests...")
    test_passed = run_tests()

    if test_passed:
        print("\n🎉 Success! All transformations completed and tests passed.")
    else:
        print("\n❌ Some tests failed. Please check the implementation.")

if __name__ == "__main__":
    main()
