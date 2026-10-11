from mayu import App

app = App(result_ttl=60)  # broker and queue come from MAYU_BROKER / MAYU_DEFAULT_QUEUE


@app.task(name="sample.add")
def add(a, b):
    return a + b
