import flask

app = flask.Flask("test_app")

@app.route("/sync")
def sync():
    return {"epoch": "158"}

if __name__ == "__main__":
    app.run(port=5000);