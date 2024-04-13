from fastapi import FastAPI
from crud import trigger_report, get_report

app = FastAPI()

@app.post("/trigger_report")
async def create_an_item():
    result = await trigger_report()
    return result

@app.get("/get_report/{report_id}")
async def read_item(report_id):
    print(report_id)
    result = await get_report(report_id)
    return result