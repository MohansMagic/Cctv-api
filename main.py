from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()

# Test Endpoint 1: JSON response
@app.get("/")
def home():
    return {"status": "Online", "message": "Sandbox is live!"}

# Test Endpoint 2: Simple HTML page
@app.get("/hello", response_class=HTMLResponse)
def hello_page():
    return "<h1>Hello from Render!</h1><p>Your sandbox endpoint is working.</p>"

# Test Endpoint 3: A simple calculator/test endpoint
@app.get("/square/{number}")
def square_number(number: int):
    return {"input": number, "result": number * number}
    
