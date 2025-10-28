import random
import uuid
from datetime import datetime, timedelta

PAGES = [
    '/home', '/products', '/products/laptop', '/products/phone',
    '/cart', '/checkout', '/account', '/about', '/contact'
]

EVENT_TYPES = ['page_view', 'click', 'scroll', 'form_submit']

FAILURE_RATE = 0.0  # Set to 0.5 to simulate 50% data loss for testing

def generate_user_session():
    """Generate realistic user session"""
    user_id = f"user_{random.randint(1000, 9999)}"
    session_id = str(uuid.uuid4())
    session_start = datetime.now() - timedelta(hours=random.randint(0, 24))
    
    num_events = random.randint(3, 15)
    events = []
    
    for i in range(num_events):
        event_time = session_start + timedelta(seconds=i * random.randint(5, 60))
        
        event = {
            'event_id': str(uuid.uuid4()),
            'user_id': user_id,
            'session_id': session_id,
            'event_type': random.choice(EVENT_TYPES),
            'page_url': random.choice(PAGES),
            'timestamp': event_time.isoformat(),
            'duration_seconds': random.randint(5, 300)
        }
        events.append(event)
    
    return events

def generate_clickstream_data(num_events=1000):
    """Generate clickstream data"""
    all_events = []
    
    while len(all_events) < num_events:
        session_events = generate_user_session()
        all_events.extend(session_events)
    
    # Simulate data loss for testing
    if random.random() < FAILURE_RATE:
        lost_count = int(len(all_events) * 0.3)
        all_events = all_events[:-lost_count]
        print(f"⚠️  Simulated data loss: {lost_count} events dropped")
    
    return all_events[:num_events]