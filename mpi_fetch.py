import os
import json
import requests
from datetime import datetime
from tn_districts import get_districts_list
import concurrent.futures
import threading

def fetch_weather_data(districts, output_file, num_processors=4):
    """Simulate MPI behavior using threading for demonstration"""
    
    # Simulate MPI distribution
    districts_per_processor = len(districts) // num_processors
    remainder = len(districts) % num_processors
    
    all_weather_data = []
    
    def process_districts_for_rank(rank):
        """Process districts for a specific 'rank' (simulated processor)"""
        # Calculate start and end indices for this rank
        start_idx = rank * districts_per_processor + min(rank, remainder)
        if rank < remainder:
            end_idx = start_idx + districts_per_processor + 1
        else:
            end_idx = start_idx + districts_per_processor
        
        my_districts = districts[start_idx:end_idx]
        weather_data = []

        for district in my_districts:
            api_key = os.environ.get("OPENWEATHER_API_KEY")
            lat = district.get('lat')
            lon = district.get('lon')

            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
            
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                weather = {
                    'district': district.get('name'),
                    'temperature_c': data['main']['temp'],
                    'humidity_pct': data['main']['humidity'],
                    'wind_speed_ms': data['wind']['speed'],
                    'rainfall_mm': data.get('rain', {}).get('1h', 0),
                    'processor_rank': rank,  # Simulated rank
                    'total_processors': num_processors
                }
                weather_data.append(weather)
                print(f"Processor {rank} processed {district.get('name')}")
                
            except Exception as e:
                weather_data.append({
                    'district': district.get('name'),
                    'error': str(e),
                    'processor_rank': rank,
                    'total_processors': num_processors
                })
                print(f"Processor {rank} failed to process {district.get('name')}: {e}")

        return weather_data

    # Use ThreadPoolExecutor to simulate parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_processors) as executor:
        futures = [executor.submit(process_districts_for_rank, rank) for rank in range(num_processors)]
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            all_weather_data.extend(result)

    # Calculate averages
    temps = [d['temperature_c'] for d in all_weather_data if 'temperature_c' in d]
    humidity = [d['humidity_pct'] for d in all_weather_data if 'humidity_pct' in d]
    rainfall = [d['rainfall_mm'] for d in all_weather_data if 'rainfall_mm' in d]
    wind_speeds = [d['wind_speed_ms'] for d in all_weather_data if 'wind_speed_ms' in d]

    averages = {
        'temperature_c': round(sum(temps) / len(temps), 2) if temps else None,
        'humidity_pct': round(sum(humidity) / len(humidity), 2) if humidity else None,
        'rainfall_mm': round(sum(rainfall) / len(rainfall), 2) if rainfall else None,
        'wind_speed_ms': round(sum(wind_speeds) / len(wind_speeds), 2) if wind_speeds else None,
    }

    # Calculate processor distribution info
    processor_distribution = {}
    for i in range(num_processors):
        processor_districts = [d['district'] for d in all_weather_data if d['processor_rank'] == i]
        processor_distribution[f'processor_{i}'] = processor_districts

    final_data = {
        'last_updated': datetime.now().isoformat(),
        'total_processors_used': num_processors,
        'processor_distribution': processor_distribution,
        'averages': averages,
        'districts': all_weather_data
    }

    # Ensure directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(final_data, f, indent=4)
    
    print(f"Weather data processed with {num_processors} simulated processors")
    return True