from flask import Flask, jsonify

app = Flask(__name__)

# Load the JSON file
import json
with open("medical_service_reviews.json", "r") as file:
    data = json.load(file)

# Define a route to serve the JSON data
@app.route('/reviews', methods=['GET'])
def get_reviews():
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
