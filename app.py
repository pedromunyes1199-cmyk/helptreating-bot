from fastapi import FastAPI, Request
import requests

app = FastAPI()

BOT_TOKEN = "7858398114:AAF6p9Z1MctcEbYKpklFK6rCyPfYzBgFXVc"
CHAT_ID = "8302501867"

def send_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text
    })

@app.get("/")
def home():
    return {"status": "ok"}

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()

    text = (
        f"🚨 Alert\n"
        f"Asset: {data.get('asset')}\n"
        f"Side: {data.get('side')}\n"
        f"Zone: {data.get('zone')}\n"
        f"Price: {data.get('price')}"
    )

    send_message(text)

    return {"ok": True}
