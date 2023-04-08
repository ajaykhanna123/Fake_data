from flask import Flask,jsonify
app = Flask(__name__)
import read_file_from_blob_storage

@app.route("/")
def hello():
    return "Hello, World!"

@app.route("read_file/")
def read_file():
    read_file_obj=read_file_from_blob_storage.Read_initial_csv_file()
    df_str=read_file_obj.read_file()
    return jsonify({"result":df_str})

if __name__ == "__main__":
    # Use Gunicorn as the production server
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
