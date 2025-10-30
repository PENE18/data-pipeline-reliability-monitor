"""
Clickstream Data Generator
Generates realistic user session data for testing
"""

import random
import uuid
from datetime import datetime, timedelta

# Configuration
PAGES = [
    '/home',
    '/products',
    '/products/laptop',
    '/products/phone',
    '/products/tablet',
    '/cart',
    '/checkout',
    '/account',
    '/about',
    '/contact',
    '/blog',
    '/support'
]

EVENT_TYPES = [
    'page_view',
    'click',
    'scroll',
    'form_submit',
    'button_click',
    'add_to_cart',
    'search'
]

# Failure simulation - set to 0.0 for normal operation
# Set to 0.3 to simulate 30% data loss for testing
FAILURE_RATE = 0.0


def generate_user_session():
    """
    Generate a realistic user session with multiple events
    
    Returns:
        list: List of event dictionaries
    """
    # Generate user and session IDs
    user_id = f"user_{random.randint(1000, 9999)}"
    session_id = str(uuid.uuid4())
    
    # Random session start time (within last 24 hours)
    session_start = datetime.now() - timedelta(hours=random.randint(0, 24))
    
    # Random number of events per session (3-15)
    num_events = random.randint(3, 15)
    events = []
    
    current_time = session_start
    
    for i in range(num_events):
        # Add realistic time gap between events (5-60 seconds)
        time_gap = timedelta(seconds=random.randint(5, 60))
        current_time += time_gap
        
        event = {
            'event_id': str(uuid.uuid4()),
            'user_id': user_id,
            'session_id': session_id,
            'event_type': random.choice(EVENT_TYPES),
            'page_url': random.choice(PAGES),
            'timestamp': current_time.isoformat(),
            'duration_seconds': random.randint(5, 300)  # 5 seconds to 5 minutes
        }
        events.append(event)
    
    return events


def generate_clickstream_data(num_events=1000):
    """
    Generate clickstream data with specified number of events
    
    Args:
        num_events (int): Target number of events to generate
        
    Returns:
        list: List of event dictionaries
    """
    all_events = []
    
    # Generate sessions until we have enough events
    while len(all_events) < num_events:
        session_events = generate_user_session()
        all_events.extend(session_events)
    
    # Trim to exact number requested
    all_events = all_events[:num_events]
    
    # Simulate data loss for testing (if enabled)
    if random.random() < FAILURE_RATE:
        # Randomly drop 20-40% of events
        loss_percentage = random.uniform(0.2, 0.4)
        lost_count = int(len(all_events) * loss_percentage)
        all_events = all_events[:-lost_count]
        print(f"⚠️  SIMULATED DATA LOSS: {lost_count} events dropped ({loss_percentage*100:.1f}%)")
    
    return all_events


def validate_event_data(events):
    """
    Validate generated event data
    
    Args:
        events (list): List of events to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not events:
        return False, "No events generated"
    
    required_fields = ['event_id', 'user_id', 'session_id', 'event_type', 'page_url', 'timestamp']
    
    for i, event in enumerate(events):
        for field in required_fields:
            if field not in event:
                return False, f"Event {i} missing required field: {field}"
    
    return True, "Data validation passed"


# Test function (run this file directly to test)
if __name__ == "__main__":
    print("🧪 Testing data generator...")
    
    test_data = generate_clickstream_data(100)
    print(f"✅ Generated {len(test_data)} events")
    
    is_valid, message = validate_event_data(test_data)
    print(f"{'✅' if is_valid else '❌'} Validation: {message}")
    
    # Show sample event
    if test_data:
        print("\n📊 Sample Event:")
        import json
        print(json.dumps(test_data[0], indent=2))