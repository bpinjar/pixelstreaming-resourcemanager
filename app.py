import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import ORJSONResponse
from dotenv import load_dotenv
from typing import Dict, Optional, Tuple

# Load variables from the .env file
load_dotenv()

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

##################################
###### Resource Manager class
##################################

class PixelStreamResourceManager:
    def __init__(self):
        self.active_streams = {
            'Infosys': {},  # Maps stream ID to requester
            'Dubaimall': {}  # Maps stream ID to requester

        }
        self.available_streams = {
            'Infosys': {},  # Maps stream ID to requester
            'Dubaimall': {} # Maps stream ID to requester
         
        }
        self.requester_streams = {}  # Maps requester_id to (stream_id, service)

    def check_available_stream(self, service: str) -> Tuple[Optional[str], Optional[str]]:
        if self.available_streams[service]:
            stream_id, ip_address = next(iter(self.available_streams[service].items()))
            return stream_id, ip_address
        else:
            return None, None

    def request_stream(self, requester_id: str, service: str) -> Optional[str]:
        # Check if the requester already has an active stream
        if requester_id in self.requester_streams:
            stream_id, existing_service = self.requester_streams[requester_id]
            if existing_service == service:
                return self.active_streams[service][stream_id]
            else:
                raise HTTPException(status_code=400, detail=f"Requester already has an active stream with {existing_service}. Please release it before requesting a new one.")

        # Allocate a new stream
        stream_id, ip = self.check_available_stream(service)
        if stream_id:
            self.available_streams[service].pop(stream_id)
            self.active_streams[service][stream_id] = ip
            self.requester_streams[requester_id] = (stream_id, service)
            return ip,stream_id
        return None

    def release_stream(self, requester_id: str) -> bool:
        if requester_id in self.requester_streams:
            stream_id, service = self.requester_streams.pop(requester_id)
            ip_address = self.active_streams[service].pop(stream_id)
            self.available_streams[service][stream_id] = ip_address
            return True
        return False

    def get_active_streams(self) -> Dict[str, Dict[str, str]]:
        return self.active_streams

    def get_requester_streams(self) -> Dict[str, Tuple[str, str]]:
        return self.requester_streams

# Initialize the PixelStreamManager
manager = PixelStreamResourceManager()

# Add some mock available streams for testing
manager.available_streams['Infosys']['stream1'] = 'https://share.streampixel.io/66f57133ee04dcc4dd642b48'


@app.post('/getstream', response_class=ORJSONResponse)
async def getstream(requester_id: str = Form(...), service: str = Form(...)):       
    ip = manager.request_stream(requester_id, service)
    if ip:
        return {"stream details": ip}
    else:
        raise HTTPException(status_code=404, detail="No available streams found for the specified service.")

@app.get("/check_if_stream_available")
def check_stream(service: str):
    stream_id, ip = manager.check_available_stream(service)
    if stream_id:
        return {"stream_id": stream_id, "ip": ip}
    else:
        return {"message": f"No available streams found for {service}."}


@app.post("/release_stream", response_class=ORJSONResponse)
async def release_stream(requester_id: str = Form(...)):
    if requester_id in manager.requester_streams:
        stream_id, service = manager.requester_streams.pop(requester_id)
        ip_address = manager.active_streams[service].pop(stream_id)
        manager.available_streams[service][stream_id] = ip_address
        return {
            "message": f"Stream released and available again for requester {requester_id}.",
            "stream_id": stream_id,
            "ip": ip_address
        }
    else:
        raise HTTPException(status_code=404, detail=f"Requester {requester_id} does not have any active streams.")


@app.get("/check_active_streams", response_class=ORJSONResponse)
def active_streams():
    active_streams = manager.get_active_streams()
    return active_streams

@app.get("/requester_streams", response_class=ORJSONResponse)
def requester_streams():
    requester_streams = manager.get_requester_streams()
    result = {requester_id: {"stream_id": stream_id, "service": service, "ip": manager.active_streams[service][stream_id]} for requester_id, (stream_id, service) in requester_streams.items()}
    return result

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8081)
