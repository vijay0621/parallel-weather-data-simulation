import os
import json
import subprocess
import sys
from flask import Flask, jsonify, send_from_directory, request
from dotenv import load_dotenv

import tn_districts

load_dotenv()
app = Flask(__name__)

DISTRICTS = tn_districts.get_districts_list()
DATA_FILE = 'data/weather.json'

def run_mpi_weather_fetch(num_processors=4):
    """Run the MPI weather fetch as a separate process"""
    try:
        # Create a separate MPI script
        mpi_script_path = 'run_mpi_fetch.py'
        
        # Create the MPI runner script if it doesn't exist
        if not os.path.exists(mpi_script_path):
            with open(mpi_script_path, 'w') as f:
                f.write("""
import sys
import os
sys.path.append(os.path.dirname(__file__))
import mpi_fetch
import tn_districts

districts = tn_districts.get_districts_list()
output_file = sys.argv[1] if len(sys.argv) > 1 else 'data/weather.json'
num_processors = int(sys.argv[2]) if len(sys.argv) > 2 else 4

mpi_fetch.fetch_weather_data(districts, output_file, num_processors)
""")
        
        # Run with mpirun
        cmd = ['mpirun', '-n', str(num_processors), 'python', mpi_script_path, DATA_FILE, str(num_processors)]
        
        # Try mpirun, if not available try mpiexec
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        except FileNotFoundError:
            cmd[0] = 'mpiexec'  # Try mpiexec instead
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            print(f"MPI command failed: {result.stderr}")
            return False
        
        return True
        
    except Exception as e:
        print(f"Error running MPI: {e}")
        return False

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory('static', path)

@app.route('/api/data')
def get_weather_data():
    if not os.path.exists(DATA_FILE):
        # Default to 4 processors if no data exists
        success = run_mpi_weather_fetch(4)
        if not success:
            return jsonify({'error': 'Failed to fetch weather data'}), 500
    
    with open(DATA_FILE, 'r') as f:
        data = json.load(f)
    return jsonify(data)

@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    # Get number of processors from request
    request_data = request.get_json() or {}
    num_processors = request_data.get('num_processors', 4)  # Default to 4
    
    success = run_mpi_weather_fetch(num_processors)
    
    if not success:
        return jsonify({'error': 'Failed to refresh weather data with MPI'}), 500
    
    with open(DATA_FILE, 'r') as f:
        data = json.load(f)
    return jsonify({'message': f'Data refreshed successfully with {num_processors} processors', 'data': data})

@app.route('/api/processor-info')
def get_processor_info():
    """Get information about available processors and current distribution"""
    total_districts = len(DISTRICTS)
    return jsonify({
        'total_districts': total_districts,
        'max_processors': min(total_districts, 8),  # Reasonable max limit
        'districts': [d['name'] for d in DISTRICTS]
    })

if __name__ == '__main__':
    # Run Flask normally - MPI will be called as subprocess
    app.run(debug=True, host='127.0.0.1', port=5000)