from flask import Flask, render_template, jsonify
import subprocess, time, os, json

app = Flask(__name__, template_folder="templates")
OUTPUT = "outputs/ipo_process_status.json"

@app.route("/")
def home():
    return render_template("index.html")   # serves templates/index.html

@app.route("/run-spider")
def run_spider():
    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)
    start = time.time()
    subprocess.run(["scrapy", "crawl", "bse_ipo_process_status", "-O", OUTPUT], check=True)
    return jsonify({"status": "finished", "duration": round(time.time()-start,1)})

@app.route("/data")
def get_data():
    if not os.path.exists(OUTPUT):
        return jsonify({"error": "no output file"}), 404
    with open(OUTPUT, "r", encoding="utf-8") as f:
        return jsonify(json.load(f))

if __name__ == "__main__":
    app.run(port=5000, debug=True)
